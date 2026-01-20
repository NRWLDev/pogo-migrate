from __future__ import annotations

import typing as t

from pogo_core.util import testing

from pogo_migrate import config

if t.TYPE_CHECKING:
    import asyncpg


async def apply(db: asyncpg.Connection | None = None) -> None:
    c = config.load_config()
    await testing.apply(c.migrations, db=db, database_dsn=c.database_dsn, schema_name=c.schema)


async def rollback(db: asyncpg.Connection | None = None) -> None:
    c = config.load_config()
    await testing.rollback(c.migrations, db=db, database_dsn=c.database_dsn, schema_name=c.schema)
