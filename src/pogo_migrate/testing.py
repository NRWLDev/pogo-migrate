from __future__ import annotations

import typing as t

from pogo_migrate import config, migrate, sql
from pogo_migrate.context import Context

if t.TYPE_CHECKING:
    import asyncpg


async def apply(db: asyncpg.Connection | None = None) -> None:
    context = Context()
    c = config.load_config()
    db_ = db if db is not None else await sql.get_connection(c.database_dsn)
    try:
        await migrate.apply(context, c, db_)
    finally:
        if db is None:
            await db_.close()


async def rollback(db: asyncpg.Connection | None = None) -> None:
    context = Context()
    c = config.load_config()
    db_ = db if db is not None else await sql.get_connection(c.database_dsn)
    try:
        await migrate.rollback(context, c, db_)
    finally:
        if db is None:
            await db_.close()
