import zipfile
import io
import json
import gc
from multiprocessing import Process
from typing import List, Optional
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import AsyncSessionLocal
from app.s3_client import S3Client
from app.journal import load_journal, update_completed_file, update_current_progress
from app.models import WebEvent, MpEvent
from app.schemas import EventSchema
from app.logger import logger, file_handler
import asyncio
from datetime import datetime

s3 = S3Client()
MAX_PARAMS = 20000  # Максимальное число параметров на один execute


async def insert_batch(session: AsyncSession, table_model, data_list: List[dict]):
    """Вставка батча с автоматическим делением на под-батчи по лимиту параметров."""
    if not data_list:
        return

    num_columns = len(data_list[0])
    if num_columns == 0:
        return

    # Подбираем под-батч так, чтобы не превышать MAX_PARAMS
    chunk_size = max(1, MAX_PARAMS // num_columns)

    for i in range(0, len(data_list), chunk_size):
        sub_chunk = data_list[i:i + chunk_size]
        stmt = insert(table_model).values(sub_chunk)
        await session.execute(stmt)

    await session.commit()
    logger.debug(f"Batch inserted successfully to {table_model.__tablename__}")
    file_handler.flush()


async def process_file(file_key: str, table_name: str, batch_size: int = 100):
    """Обработка NDJSON-файла: чтение, парсинг через Pydantic, вставка в БД."""
    logger.info(f"Starting processing file: {file_key} for table: {table_name}")
    try:
        zip_bytes = s3.get_object(file_key)
        with io.BytesIO(zip_bytes) as zip_buffer:
            with zipfile.ZipFile(zip_buffer) as zf:
                ndjson_files = [f for f in zf.namelist() if f.endswith(".ndjson")]
                if not ndjson_files:
                    logger.warning(f"No .ndjson file in {file_key}")
                    return

                ndjson_file = ndjson_files[0]
                logger.info(f"Processing NDJSON: {ndjson_file}")

                with zf.open(ndjson_file) as ndjson:
                    line_num = 0
                    batch = []
                    journal = load_journal()

                    # Восстановление прогресса
                    if journal.get("current_file") == file_key:
                        line_num = journal.get("current_line", 0)
                        logger.info(f"Resuming from line {line_num} in {file_key}")

                    while True:
                        line = ndjson.readline()
                        if not line:
                            break

                        line_num += 1
                        if line_num <= journal.get("current_line", 0):
                            continue
                        if line_num == 1 and line.startswith(b"["):  # пропуск заголовка
                            continue

                        d = json.loads(line)
                        event = EventSchema(**d)
                        record = event.dict(by_alias=False, exclude_unset=True)
                        batch.append(record)

                        if len(batch) >= batch_size:
                            async with AsyncSessionLocal() as session:
                                table_model = WebEvent if table_name == "web" else MpEvent
                                await insert_batch(session, table_model, batch)

                            batch = []
                            update_current_progress(file_key, line_num)

                    # Вставка оставшихся записей
                    if batch:
                        async with AsyncSessionLocal() as session:
                            table_model = WebEvent if table_name == "web" else MpEvent
                            await insert_batch(session, table_model, batch)
                        logger.info(f"Final batch inserted: {len(batch)} records")

        update_completed_file(file_key)
        logger.info(f"Completed processing {file_key}, total lines: {line_num}")

        file_handler.flush()
        gc.collect()
        logger.debug("Garbage collected after file processing")

    except Exception as e:
        logger.error(f"Error processing {file_key}: {e}", exc_info=True)
        file_handler.flush()


def background_processor(
    prefix: str,
    table_name: str,
    start_file: Optional[str] = None,
    start_date: Optional[str] = None,
):
    """Фоновая обработка всех файлов из S3 по префиксу."""
    logger.info(
        f"Background processor started for prefix: {prefix}, table: {table_name}, "
        f"start_file: {start_file}, start_date: {start_date}"
    )

    objects = s3.list_objects(prefix)
    object_keys = [obj["Key"] for obj in objects]

    journal = load_journal()
    last_completed = journal.get("last_completed_file")
    current_file = journal.get("current_file")
    current_line = journal.get("current_line")

    start_idx = 0

    # Определение точки старта
    if start_file:
        try:
            start_idx = object_keys.index(
                prefix + start_file if not start_file.startswith(prefix) else start_file
            )
            logger.info(f"Starting from specified file: {start_file}")
        except ValueError:
            logger.error(f"Specified start_file {start_file} not found; stopping process")
            return
    elif start_date:
        try:
            parsed_date = datetime.fromisoformat(start_date)
            start_idx = next(
                (
                    i
                    for i, obj in enumerate(objects)
                    if obj["LastModified"] >= parsed_date
                ),
                len(objects),
            )
            if start_idx == len(objects):
                logger.error(f"No files found after {start_date}; stopping process")
                return
            logger.info(
                f"Starting from first file after {start_date}: {object_keys[start_idx]}"
            )
        except ValueError as ve:
            logger.error(f"Invalid start_date format: {ve}; stopping process")
            return
    else:
        if last_completed:
            try:
                start_idx = object_keys.index(last_completed) + 1
                logger.info(f"Resuming from after completed file: {last_completed}")
            except ValueError:
                logger.warning(
                    f"Last completed file {last_completed} not found in list"
                )
                start_idx = 0
        elif current_file:
            try:
                start_idx = object_keys.index(current_file)
                logger.info(
                    f"Resuming from current file: {current_file} at line {current_line}"
                )
            except ValueError:
                logger.warning(f"Current file {current_file} not found in list")
                start_idx = 0

    # Обработка оставшихся файлов
    for idx in range(start_idx, len(object_keys)):
        file_key = object_keys[idx]
        logger.info(f"Processing file {idx + 1}/{len(object_keys)}: {file_key}")
        asyncio.run(process_file(file_key, table_name))
        file_handler.flush()

    logger.info("Background processor finished all files")


def start_processing(
    prefix: str,
    table_name: str,
    start_file: Optional[str] = None,
    start_date: Optional[str] = None,
):
    """Запуск процесса обработки в отдельном процессе."""
    logger.info(
        f"Starting background process for {prefix}/{table_name}, "
        f"start_file={start_file}, start_date={start_date}"
    )
    p = Process(
        target=background_processor, args=(prefix, table_name, start_file, start_date)
    )
    p.start()
    logger.info(f"Background process PID: {p.pid}")
    return p
