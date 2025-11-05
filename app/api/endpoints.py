from fastapi import APIRouter, Query
from app.processor import start_processing
from app.journal import load_journal
from app.database import init_db
from app.s3_client import S3Client
from app.logger import logger
from typing import Optional, List

router = APIRouter()


@router.on_event("startup")
async def startup_event():
    logger.info("Application startup")
    await init_db()


@router.post("/start")
async def start_transfer(
    prefix: str = Query(..., description="S3 folder prefix"),
    table_name: str = Query(..., description="Table: 'web' or 'mp'"),
    start_file: Optional[str] = Query(
        None, description="Start from this exact file name (overrides journal)"
    ),
    start_date: Optional[str] = Query(
        None,
        description="Start from files added on or after this date (YYYY-MM-DD, overrides journal if start_file not set)",
    ),
):
    logger.info(
        f"API /start called: prefix={prefix}, table={table_name}, start_file={start_file}, start_date={start_date}"
    )
    if table_name not in ["web", "mp"]:
        logger.warning(f"Invalid table_name: {table_name}")
        return {"error": "Table must be 'web' or 'mp'"}
    if start_file and start_date:
        logger.warning(
            "Both start_file and start_date provided; prioritizing start_file"
        )
    p = start_processing(prefix, table_name, start_file, start_date)
    logger.info(f"Processing started, PID: {p.pid}")
    return {"message": "Processing started in background", "pid": p.pid}


@router.get("/status")
async def get_status(
    prefix: Optional[str] = Query(
        None, description="S3 folder prefix for status (required for accurate stats)"
    ),
):
    logger.debug("API /status called")
    if not prefix:
        logger.warning(
            "No prefix provided for /status; using empty prefix (all objects)"
        )
        prefix = ""
    journal = load_journal()
    s3_client = S3Client()
    objects = s3_client.list_objects(prefix)
    object_keys = [obj["Key"] for obj in objects]
    total_files = len(object_keys)
    completed_files = 0
    if journal["last_completed_file"]:
        try:
            completed_idx = object_keys.index(journal["last_completed_file"])
            completed_files = completed_idx + 1
        except ValueError:
            pass
    current = (
        f"{journal['current_file']} at line {journal['current_line']}"
        if journal["current_file"]
        else "Idle"
    )
    logger.debug(f"Status: {completed_files}/{total_files}, current: {current}")
    return {
        "completed_files": completed_files,
        "total_files": total_files,
        "current_progress": current,
        "status": "running" if journal["current_file"] else "idle",
    }


@router.get("/files", response_model=List[str])
async def list_s3_files(
    prefix: str = Query(..., description="S3 folder prefix to list files from"),
):
    logger.info(f"API /files called with prefix: {prefix}")
    s3_client = S3Client()
    objects = s3_client.list_objects(prefix)
    file_names = [obj["Key"] for obj in objects]
    logger.info(f"Found {len(file_names)} files in prefix {prefix}")
    return file_names
