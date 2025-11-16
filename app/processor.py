import zipfile
import io
import json
import gc
import asyncio
from multiprocessing import Process
from typing import List, Optional, Dict, Any
from datetime import datetime

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.s3_client import S3Client
from app.journal import load_journal, update_completed_file, update_current_progress
from app.models import WebEvent, MpEvent
from app.schemas import EventSchema
from app.logger import logger, file_handler

s3 = S3Client()
MAX_PARAMS = 20000  # лимит параметров на execute в asyncpg


# ---------------- ВСТАВКА БАТЧЕЙ ---------------- #
async def insert_batch(session: AsyncSession, table_model, data_list: List[dict]):
    """Безопасная вставка батчей с разделением на под-батчи."""
    if not data_list:
        return

    num_columns = len(data_list[0]) or 1
    chunk_size = max(1, MAX_PARAMS // num_columns)

    for i in range(0, len(data_list), chunk_size):
        sub_chunk = data_list[i : i + chunk_size]
        stmt = insert(table_model).values(sub_chunk)
        await session.execute(stmt)

    await session.commit()
    logger.debug(f"Inserted {len(data_list)} rows into {table_model.__tablename__}")
    file_handler.flush()


# ---------------- ОБРАБОТКА ОДНОГО ФАЙЛА ---------------- #
async def process_file(file_key: str, table_name: str, batch_size: int = 100):
    logger.info(f"Processing file: {file_key} for table: {table_name}")
    try:
        zip_bytes = s3.get_object(file_key)
        with io.BytesIO(zip_bytes) as zip_buffer:
            with zipfile.ZipFile(zip_buffer) as zf:
                ndjson_files = [f for f in zf.namelist() if f.endswith(".ndjson")]
                if not ndjson_files:
                    logger.warning(f"No .ndjson file found in {file_key}")
                    return

                ndjson_file = ndjson_files[0]
                logger.info(f"Found NDJSON: {ndjson_file}")

                with zf.open(ndjson_file) as ndjson:
                    line_num = 0
                    batch: List[Dict[str, Any]] = []
                    journal = load_journal()

                    if journal.get("current_file") == file_key:
                        line_num = journal.get("current_line", 0)
                        logger.info(f"Resuming from line {line_num}")

                    async with AsyncSessionLocal() as session:
                        table_model = WebEvent if table_name == "web" else MpEvent

                        for raw_line in ndjson:
                            text = raw_line.decode("utf-8", errors="ignore").strip()
                            line_num += 1

                            if line_num <= journal.get("current_line", 0):
                                continue
                            if not text or text.startswith("["):
                                continue

                            try:
                                raw_obj = json.loads(text)
                            except json.JSONDecodeError as e:
                                logger.warning(
                                    f"Invalid JSON in {file_key}:{line_num}: {e}"
                                )
                                update_current_progress(file_key, line_num)
                                continue

                            try:
                                ev = EventSchema(**raw_obj)
                                record = ev.model_dump(
                                    by_alias=False, exclude_unset=True
                                )
                            except Exception as e:
                                logger.error(
                                    f"Pydantic parse failed at {file_key}:{line_num}: {e}"
                                )
                                update_current_progress(file_key, line_num)
                                raise

                            insert_id_val = record.get("insert_id")
                            if not insert_id_val:
                                logger.error(
                                    f"Missing insert_id at {file_key}:{line_num}"
                                )
                                update_current_progress(file_key, line_num)
                                raise ValueError(
                                    f"insert_id missing in {file_key}:{line_num}"
                                )

                            # fallback для data_json, если нет
                            if "data_json" not in record:
                                record["data_json"] = raw_obj.get("data", raw_obj)

                            batch.append(record)

                            if len(batch) >= batch_size:
                                await insert_batch(session, table_model, batch)
                                batch.clear()
                                update_current_progress(file_key, line_num)
                                if line_num % 5000 == 0:
                                    logger.info(
                                        f"Processed {line_num} lines in {file_key}"
                                    )

                        if batch:
                            await insert_batch(session, table_model, batch)
                            batch.clear()

        update_completed_file(file_key)
        logger.info(f"Completed file: {file_key} ({line_num} lines)")
        file_handler.flush()
        gc.collect()

    except Exception as e:
        logger.error(f"Error processing {file_key}: {e}", exc_info=True)
        file_handler.flush()


# ---------------- АСИНХРОННЫЙ ПРОЦЕСС ---------------- #
async def background_processor_async(
    prefix: str,
    table_name: str,
    start_file: Optional[str] = None,
    start_date: Optional[str] = None,
):
    logger.info(f"Starting async processor for prefix={prefix}, table={table_name}")
    objects = s3.list_objects(prefix)
    object_keys = [obj["Key"] for obj in objects]
    journal = load_journal()
    last_completed = journal.get("last_completed_file")
    current_file = journal.get("current_file")
    current_line = journal.get("current_line", 0)

    start_idx = 0

    if start_file:
        try:
            start_idx = object_keys.index(
                prefix + start_file if not start_file.startswith(prefix) else start_file
            )
        except ValueError:
            logger.error(f"File {start_file} not found")
            return
    elif start_date:
        parsed_date = datetime.fromisoformat(start_date)
        start_idx = next(
            (i for i, obj in enumerate(objects) if obj["LastModified"] >= parsed_date),
            len(objects),
        )
        if start_idx == len(objects):
            logger.error(f"No files after {start_date}")
            return
    elif last_completed:
        try:
            start_idx = object_keys.index(last_completed) + 1
        except ValueError:
            logger.warning(f"Last completed file {last_completed} not found")
    elif current_file:
        try:
            start_idx = object_keys.index(current_file)
        except ValueError:
            logger.warning(f"Current file {current_file} not found")

    for idx in range(start_idx, len(object_keys)):
        file_key = object_keys[idx]
        logger.info(f"Processing file {idx + 1}/{len(object_keys)}: {file_key}")
        await process_file(file_key, table_name)
        file_handler.flush()

    logger.info("All files processed successfully.")


# ---------------- ОБОЛОЧКИ ---------------- #
def background_processor(
    prefix: str,
    table_name: str,
    start_file: Optional[str] = None,
    start_date: Optional[str] = None,
):
    asyncio.run(background_processor_async(prefix, table_name, start_file, start_date))


def start_processing(
    prefix: str,
    table_name: str,
    start_file: Optional[str] = None,
    start_date: Optional[str] = None,
):
    logger.info(f"Starting background process for {prefix}/{table_name}")
    p = Process(
        target=background_processor, args=(prefix, table_name, start_file, start_date)
    )
    p.start()
    logger.info(f"Spawned process PID={p.pid}")
    return p
