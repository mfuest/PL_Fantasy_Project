"""Database engine, session, and schema bootstrap. Enables SQLite FKs and WAL per connection."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import Base

logger = logging.getLogger(__name__)


def get_engine(db_path: str = "data/fpl.sqlite") -> Engine:
    """Create SQLAlchemy engine for SQLite. Ensures data dir exists."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"sqlite:///{path.resolve()}"
    engine = __create_engine(url)
    __configure_sqlite(engine)
    return engine


def __create_engine(url: str) -> Engine:
    from sqlalchemy import create_engine
    return create_engine(url, future=True)


def __configure_sqlite(engine: Engine) -> None:
    """Enable foreign keys and WAL per connection. SQLite does not enforce FKs by default."""

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_conn: object, connection_record: object) -> None:
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.close()
        logger.debug("SQLite PRAGMA foreign_keys=ON, journal_mode=WAL applied")


@contextmanager
def get_session(engine: Engine) -> Generator[Session, None, None]:
    """Context manager yielding a session; commits on success, rolls back on exception."""
    SessionLocal = sessionmaker(engine, expire_on_commit=False, autoflush=False)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db(engine: Engine) -> None:
    """Create all tables from ORM models. Idempotent."""
    Base.metadata.create_all(engine)
    logger.info("Database schema created or already up to date")
