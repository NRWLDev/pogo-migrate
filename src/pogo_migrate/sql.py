from __future__ import annotations

import typing as t

from pogo_core.util import sql

if t.TYPE_CHECKING:
    from pathlib import Path

    import asyncpg
    from pogo_core.migration import Migration


async def get_connection(connection_string: str) -> asyncpg.Connection:
    return await sql.get_connection(connection_string)


async def read_migrations(migrations_location: Path, db: asyncpg.Connection | None) -> list[Migration]:
    return await sql.read_migrations(migrations_location, db)


async def get_applied_migrations(db: asyncpg.Connection) -> set[str]:
    return await sql.get_applied_migrations(db)


async def ensure_pogo_sync(db: asyncpg.Connection) -> None:
    return await sql.ensure_pogo_sync(db)


async def migration_applied(db: asyncpg.Connection, migration_id: str, migration_hash: str) -> None:
    return await sql.migration_applied(db, migration_id, migration_hash)


async def migration_unapplied(db: asyncpg.Connection, migration_id: str) -> None:
    return await sql.migration_unapplied(db, migration_id)
