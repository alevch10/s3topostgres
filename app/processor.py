import zipfile
import io
import json
import gc
from multiprocessing import Process
from typing import List, Optional
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_async_session
from app.s3_client import S3Client
from app.journal import load_journal, update_completed_file, update_current_progress
from app.models import WebEvent, MpEvent
from app.logger import logger
import asyncio
from datetime import datetime  # Добавлено для парсинга client_event_time

s3 = S3Client()


async def insert_batch(session: AsyncSession, table_model, data_list: List[dict]):
    logger.info(
        f"Inserting batch of {len(data_list)} records to {table_model.__tablename__}"
    )
    stmt = insert(table_model).values(
        data_list
    )  # Теперь data_list — готовые dict с id, created_at, data
    await session.execute(stmt)
    await session.commit()
    logger.debug(f"Batch inserted successfully to {table_model.__tablename__}")


async def process_file(file_key: str, table_name: str, batch_size: int = 1000):
    """Process single zip file"""
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
                    session_gen = get_async_session()
                    session = await anext(session_gen)
                    try:
                        while True:
                            line = ndjson.readline().decode("utf-8").strip()
                            if not line:
                                break
                            line_num += 1
                            if line_num == 1 and line.startswith(
                                "["
                            ):  # Пропуск заголовка, если есть
                                continue
                            d = json.loads(line)
                            # Извлечение полей
                            insert_id = d.get("$insert_id")
                            client_event_time_str = d.get("client_event_time")
                            if not insert_id or not client_event_time_str:
                                logger.warning(
                                    f"Skipping line {line_num} in {file_key}: missing $insert_id or client_event_time"
                                )
                                continue
                            try:
                                event_time = datetime.fromisoformat(
                                    client_event_time_str.replace(" ", "T")
                                )  # Парсинг ISO с пробелами (e.g., "2025-09-15 10:10:21.433000" → T)
                            except ValueError as ve:
                                logger.warning(
                                    f"Invalid client_event_time '{client_event_time_str}' at line {line_num}: {ve}"
                                )
                                continue
                            # Подготовка записи для вставки
                            record = {
                                "id": insert_id,
                                "created_at": event_time,
                                "data": d,
                            }
                            batch.append(record)
                            if len(batch) >= batch_size:
                                table_model = (
                                    WebEvent if table_name == "web" else MpEvent
                                )
                                await insert_batch(session, table_model, batch)
                                logger.debug(f"Processed {line_num} lines so far")
                                batch = []
                                update_current_progress(file_key, line_num)
                        if batch:
                            table_model = WebEvent if table_name == "web" else MpEvent
                            await insert_batch(session, table_model, batch)
                            logger.info(f"Final batch inserted: {len(batch)} records")
                    finally:
                        await session.close()
        update_completed_file(file_key)
        logger.info(f"Completed processing {file_key}, total lines: {line_num}")
        gc.collect()
        logger.debug("Garbage collected after file processing")
    except Exception as e:
        logger.error(f"Error processing {file_key}: {e}", exc_info=True)


def background_processor(prefix: str, table_name: str):
    """Main loop in separate process"""
    logger.info(
        f"Background processor started for prefix: {prefix}, table: {table_name}"
    )
    journal = load_journal()
    last_completed = journal.get("last_completed_file")
    current_file = journal.get("current_file")
    current_line = journal.get("current_line")

    object_keys = s3.list_objects(prefix)
    start_idx = 0
    if last_completed:
        try:
            start_idx = object_keys.index(last_completed) + 1
            logger.info(f"Resuming from after completed file: {last_completed}")
        except ValueError:
            logger.warning(f"Last completed file {last_completed} not found in list")
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

    # Process remaining files sequentially
    for idx in range(start_idx, len(object_keys)):
        file_key = object_keys[idx]
        logger.info(f"Processing file {idx + 1}/{len(object_keys)}: {file_key}")
        asyncio.run(process_file(file_key, table_name))
    logger.info("Background processor finished all files")


def start_processing(prefix: str, table_name: str):
    """Start background process"""
    logger.info(f"Starting background process for {prefix}/{table_name}")
    p = Process(target=background_processor, args=(prefix, table_name))
    p.start()
    logger.info(f"Background process PID: {p.pid}")
    return p
