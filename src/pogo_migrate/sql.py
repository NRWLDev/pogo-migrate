from __future__ import annotations

import logging
import typing as t

import asyncpg

from pogo_migrate.migration import Migration

if t.TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


async def get_connection(connection_string: str) -> asyncpg.Connection:
    db = await asyncpg.connect(connection_string)
    await ensure_pogo_sync(db)
    return db


async def read_migrations(migrations_location: Path, db: asyncpg.Connection | None) -> list[Migration]:
    applied_migrations = await get_applied_migrations(db) if db else []
    return [
        Migration(path.stem, path, applied_migrations)
        for path in migrations_location.iterdir()
        if path.suffix in {".py", ".sql"}
    ]


async def get_applied_migrations(db: asyncpg.Connection) -> set[str]:
    stmt = """
    SELECT
        migration_id
    FROM _pogo_migration
    """
    results = await db.fetch(stmt)

    return {r["migration_id"] for r in results}


async def ensure_pogo_sync(db: asyncpg.Connection) -> None:
    stmt = """
    SELECT exists (
        SELECT FROM pg_tables
        WHERE  schemaname = 'public'
        AND    tablename  = '_pogo_version'
    );
    """
    r = await db.fetchrow(stmt)
    if not r["exists"]:
        stmt = """
        CREATE TABLE _pogo_migration (
            migration_hash VARCHAR(64),  -- sha256 hash of the migration id
            migration_id VARCHAR(255),   -- The migration id (ie path basename without extension)
            applied TIMESTAMPTZ,         -- When this id was applied
            PRIMARY KEY (migration_hash)
        )
        """
        await db.execute(stmt)

        stmt = """
        CREATE TABLE _pogo_version (
            version INT NOT NULL PRIMARY KEY,
            installed TIMESTAMPTZ
        )
        """
        await db.execute(stmt)

        stmt = """
        INSERT INTO _pogo_version (version, installed) VALUES (0, now())
        """
        await db.execute(stmt)


async def migration_applied(db: asyncpg.Connection, migration_id: str, migration_hash: str) -> None:
    stmt = """
    INSERT INTO _pogo_migration (
        migration_hash,
        migration_id,
        applied
    ) VALUES (
        $1, $2, now()
    )
    """
    await db.execute(stmt, migration_hash, migration_id)


async def migration_unapplied(db: asyncpg.Connection, migration_id: str) -> None:
    stmt = """
    DELETE FROM _pogo_migration
    WHERE migration_id = $1
    """
    await db.execute(stmt, migration_id)
