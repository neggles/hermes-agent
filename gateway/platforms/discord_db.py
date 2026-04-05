import logging
from datetime import datetime
from functools import lru_cache
from os import getenv
from pathlib import Path
from typing import Annotated

import sqlalchemy as sa
from sqlalchemy import URL, Engine, create_engine
from sqlalchemy.dialects.sqlite.aiosqlite import AsyncAdapt_aiosqlite_connection
from sqlalchemy.engine import make_url
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.event import listens_for
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession as DbAsyncSession,
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, sessionmaker
from sqlalchemy.orm import Session as DbSession
from sqlalchemy.pool import ConnectionPoolEntry

from hermes_constants import get_hermes_dir

# Define type aliases for session makers
type AsyncSessionType = async_sessionmaker[DbAsyncSession]
type SyncSessionType = sessionmaker[DbSession]
type AnySessionType = AsyncSessionType | SyncSessionType

logger = logging.getLogger(__name__)


def _sqlite_db_path() -> Path:
    """Get the default DB URI, ensuring the parent directory exists."""
    hermes_discord_dir = get_hermes_dir("discord", "discord")
    hermes_discord_dir.mkdir(parents=True, exist_ok=True)
    return hermes_discord_dir / "discord_data.sqlite"


def _get_db_url(async_driver: bool = False) -> URL:
    db_url = getenv("HERMES_DISCORD_DB_URL")
    if not db_url:
        db_url = "sqlite:///" + _sqlite_db_path().as_posix()

    db_url = make_url(db_url)
    if async_driver is True:
        match db_url.drivername:
            case "sqlite" | "sqlite+aiosqlite":
                db_url = db_url.set(drivername="sqlite+aiosqlite")
            case "postgresql" | "postgresql+psycopg":
                db_url = db_url.set(drivername="postgresql+psycopg")
            case _:
                raise ValueError(f"Unsupported database driver for async: {db_url.drivername}")

    return db_url


@lru_cache(maxsize=1)
def get_sync_engine() -> Engine:
    db_url = _get_db_url(async_driver=False)
    return create_engine(url=db_url)


SyncSession: SyncSessionType = sessionmaker(get_sync_engine(), expire_on_commit=False)


@lru_cache(maxsize=1)
def get_async_engine() -> AsyncEngine:
    db_url = _get_db_url(async_driver=True)
    return create_async_engine(url=db_url)


AsyncSession: AsyncSessionType = async_sessionmaker(get_async_engine(), expire_on_commit=False)


# declarative base class
class Base(AsyncAttrs, DeclarativeBase):
    """Base class for declarative models."""

    __mapper_args__ = {"eager_defaults": True}


# BigInteger primary key
BigIntPK = Annotated[
    int,
    mapped_column(
        sa.Integer().with_variant(sa.BigInteger, "postgresql"),
        primary_key=True,
        autoincrement=True,
    ),
]


# Timestamp with no default (nullable)
Timestamp = Annotated[
    datetime | None,
    mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
        default=None,
    ),
]

# Timestamp with no default (non-nullable)
RequiredTimestamp = Annotated[
    datetime,
    mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
    ),
]

# Timestamp with default (non-nullable, used for creation time)
CreateTimestamp = Annotated[
    datetime,
    mapped_column(
        sa.DateTime(timezone=True),
        index=True,
        nullable=False,
        server_default=sa.func.current_timestamp(),
        default=None,
    ),
]

# Auto-updating timestamp with default (non-nullable, used for last update time)
UpdateTimestamp = Annotated[
    datetime,
    mapped_column(
        sa.DateTime(timezone=True),
        index=True,
        nullable=False,
        onupdate=sa.func.current_timestamp(),
        server_default=sa.func.current_timestamp(),
        server_onupdate=sa.func.current_timestamp(),
        default=None,
    ),
]

## Annotated types for Discord objects
Snowflake = Annotated[
    int,  # Discord "snowflake" (object ID)
    mapped_column(sa.BigInteger),
]

OptionalSnowflake = Annotated[
    int | None,
    mapped_column(sa.BigInteger, nullable=True, default=None),
]

DiscordID = Annotated[
    int,
    mapped_column(sa.BigInteger),
]

OptionalDiscordID = Annotated[
    int | None,
    mapped_column(sa.BigInteger, nullable=True, default=None),
]

## Event listener to enforce foreign key constraints in SQLite


@listens_for(Engine, "connect", insert=True)
def on_engine_connect(
    dbapi_connection: DBAPIConnection,
    connection_record: ConnectionPoolEntry,
) -> None:
    """Event listener for synchronous engine connections."""
    try:
        if not dbapi_connection:
            return

        db_url = _get_db_url(async_driver=False)

        if "sqlite" in db_url.drivername:
            if isinstance(dbapi_connection, AsyncAdapt_aiosqlite_connection):
                ac = dbapi_connection.isolation_level
                dbapi_connection.isolation_level = None
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()
                dbapi_connection.isolation_level = ac
            else:
                # the sqlite3 driver will not set PRAGMA foreign_keys if autocommit=False; set to True temporarily
                ac = dbapi_connection.autocommit
                dbapi_connection.autocommit = True

                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

                # restore previous autocommit setting
                dbapi_connection.autocommit = ac
            logger.debug(f"SQLite PRAGMA foreign_keys=ON set for connection {dbapi_connection!r}")
        else:
            logger.debug("No PRAGMA settings applied; not an SQLite database.")
    except Exception as e:
        logger.exception(f"Error setting SQLite PRAGMA: {e}")
        raise e
