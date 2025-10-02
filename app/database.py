from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from app.config import settings
from app.models import Base
from app.logger import logger
import asyncio

# Sync engine for migrations
sync_engine = create_engine(
    f"postgresql://{settings.db.user}:{settings.db.password}@{settings.db.host}:{settings.db.port}/{settings.db.name}"
)  # Изменено: dbname → name

# Async engine for app
async_engine = create_async_engine(
    f"postgresql+asyncpg://{settings.db.user}:{settings.db.password}@{settings.db.host}:{settings.db.port}/{settings.db.name}",  # Изменено: dbname → name
    echo=settings.debug,
)
AsyncSessionLocal = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)


async def get_async_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    """Apply migrations and create tables if not exist"""
    logger.info("Initializing database")
    async with async_engine.begin() as conn:
        # Create tables if not exist (Alembic handles migrations)
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized successfully")
