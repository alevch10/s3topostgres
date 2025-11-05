import logging
import sys
from app.config import settings

logger = logging.getLogger("analytics_transfer")
logger.setLevel(getattr(logging, settings.log_level.upper()))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG if settings.debug else logging.INFO)

file_handler = logging.FileHandler(settings.log_file)
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

logger.propagate = False
