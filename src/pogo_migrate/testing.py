from __future__ import annotations

import typing as t

from pogo_migrate import config, migrate, sql

if t.TYPE_CHECKING:
    import asyncpg


async def apply(db: asyncpg.Connection | None = None) -> None:
    c = config.load_config()
    if db is None:
        db = await sql.get_connection(c.database_dsn)
    await migrate.apply(c, db)


async def rollback(db: asyncpg.Connection | None = None) -> None:
    c = config.load_config()
    if db is None:
        db = await sql.get_connection(c.database_dsn)
    await migrate.rollback(c, db)
