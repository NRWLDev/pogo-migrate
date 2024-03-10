from __future__ import annotations

import os
import typing as t

from pogo_migrate import config, migrate, sql

if t.TYPE_CHECKING:
    import asyncpg


async def apply(db: asyncpg.Connection | None = None) -> None:
    c = config.load_config()
    if db is None:
        db = await sql.get_connection(os.environ[c.database_env_key])
    await migrate.apply(c, db)


async def rollback(db: asyncpg.Connection | None = None) -> None:
    c = config.load_config()
    if db is None:
        db = await sql.get_connection(os.environ[c.database_env_key])
    await migrate.rollback(c, db)
