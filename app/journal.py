import json
import os
from typing import Optional, Dict
from app.logger import logger

JOURNAL_FILE = "journal.json"


def load_journal() -> Dict:
    if os.path.exists(JOURNAL_FILE):
        with open(JOURNAL_FILE, "r") as f:
            data = json.load(f)
            logger.debug(f"Loaded journal: {data}")
            return data
    logger.info("No journal file found, starting fresh")
    default = {"last_completed_file": None, "current_file": None, "current_line": 0}
    return default


def save_journal(journal: Dict):
    with open(JOURNAL_FILE, "w") as f:
        json.dump(journal, f, indent=2)
    logger.debug(f"Saved journal: {journal}")


def update_completed_file(file_key: str):
    journal = load_journal()
    journal["last_completed_file"] = file_key
    journal["current_file"] = None
    journal["current_line"] = 0
    save_journal(journal)
    logger.info(f"Updated journal: completed file {file_key}")


def update_current_progress(file_key: str, line: int):
    journal = load_journal()
    journal["current_file"] = file_key
    journal["current_line"] = line
    save_journal(journal)
    logger.debug(f"Updated journal: progress in {file_key} at line {line}")
