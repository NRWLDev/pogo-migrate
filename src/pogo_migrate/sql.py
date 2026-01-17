from __future__ import annotations

import typing as t
from warnings import warn

from pogo_core.util import sql

if t.TYPE_CHECKING:
    from pathlib import Path

    import asyncpg
    from pogo_core.migration import Migration


warn(
    "pogo_migrate.sql usage has been deprecated, please use pogo_core.util.sql",
    FutureWarning,
    stacklevel=2,
)


async def get_connection(
    connection_string: str,
    *,
    schema_name: str = "public",
    schema_create: bool = False,
) -> asyncpg.Connection:
    return await sql.get_connection(connection_string, schema_name=schema_name, schema_create=schema_create)


async def read_migrations(
    migrations_location: Path,
    db: asyncpg.Connection | None,
    *,
    schema_name: str = "public",
) -> list[Migration]:
    return await sql.read_migrations(migrations_location, db, schema_name=schema_name)


async def get_applied_migrations(db: asyncpg.Connection, *, schema_name: str = "public") -> set[str]:
    return await sql.get_applied_migrations(db, schema_name=schema_name)


async def ensure_pogo_sync(db: asyncpg.Connection) -> None:
    return await sql.ensure_pogo_sync(db)


async def migration_applied(
    db: asyncpg.Connection,
    migration_id: str,
    migration_hash: str,
    *,
    schema_name: str = "public",
) -> None:
    return await sql.migration_applied(db, migration_id, migration_hash, schema_name=schema_name)


async def migration_unapplied(db: asyncpg.Connection, migration_id: str, *, schema_name: str = "public") -> None:
    return await sql.migration_unapplied(db, migration_id, schema_name=schema_name)
