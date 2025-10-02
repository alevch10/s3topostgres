from sqlalchemy import Column, String, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class WebEvent(Base):
    __tablename__ = "web"
    id = Column(
        String, primary_key=True, index=True
    )  # Изменено: String для $insert_id (UUID)
    created_at = Column(
        DateTime, nullable=False, index=True
    )  # Изменено: nullable=False, без default
    data = Column(JSON)


class MpEvent(Base):
    __tablename__ = "mp"
    id = Column(
        String, primary_key=True, index=True
    )  # Изменено: String для $insert_id (UUID)
    created_at = Column(
        DateTime, nullable=False, index=True
    )  # Изменено: nullable=False, без default
    data = Column(JSON)
