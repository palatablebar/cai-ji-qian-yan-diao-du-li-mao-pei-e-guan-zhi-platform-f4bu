"""SQLAlchemy 2.0 engine / session；启动阶段幂等建表。"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.config.settings import settings
from src.models import Base

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=8,
    max_overflow=16,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)


def get_db() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_columns()


# 命名卷上的既有库：create_all 不会补列，这里做幂等 ALTER（PostgreSQL IF NOT EXISTS）。
_ENSURE_COLUMN_STATEMENTS: tuple[str, ...] = (
    "ALTER TABLE crawl_tasks ADD COLUMN IF NOT EXISTS last_block_reason TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE crawl_tasks ADD COLUMN IF NOT EXISTS last_blocked_at TIMESTAMPTZ",
)


def _ensure_columns() -> None:
    with engine.begin() as conn:
        for statement in _ENSURE_COLUMN_STATEMENTS:
            conn.execute(text(statement))


def ping_db() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
