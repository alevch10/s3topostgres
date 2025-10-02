from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel
from typing import Optional
import logging
import sys  # Для sys.stdout в StreamHandler


class DBSettings(BaseModel):
    host: str
    port: int = 5432
    user: str
    password: str
    name: str  # Изменено: dbname → name (маппинг с db_name в .env)


class S3Settings(BaseModel):
    access_key_id: str
    secret_access_key: str
    region: str
    endpoint_url: str
    bucket_name: str  # Изменено: bucket → bucket_name (маппинг с s3_bucket_name в .env)


class Settings(BaseSettings):
    title: str
    description: str
    debug: bool = False
    version: str
    log_level: str = "INFO"
    log_file: str = "app.log"
    timeout: int = 300

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        env_nested_delimiter="_",
        env_nested_max_split=1,
        case_sensitive=False,
    )

    db: DBSettings
    s3: S3Settings


settings = Settings()

# Импорт базового логгера после создания settings (разрывает цикл)
from app.logger import logger

# Конфигурация логгера
logger.setLevel(getattr(logging, settings.log_level.upper()))

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG if settings.debug else logging.INFO)

# File handler
file_handler = logging.FileHandler(settings.log_file)
file_handler.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Prevent duplicate logs
logger.propagate = False

# Log settings on load
logger.info(f"Settings loaded: debug={settings.debug}, log_level={settings.log_level}")
