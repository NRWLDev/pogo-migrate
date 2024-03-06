import logging
import os
from pathlib import Path

import asyncpg

from pogo_migrate.config import Config
from pogo_migrate.migration import Migration

logger = logging.getLogger(__name__)


async def get_connection(config: Config) -> asyncpg.Connection:
    return await asyncpg.connect(os.environ[config.database_env_key])


async def read_migrations(migrations_location: Path, db: asyncpg.Connection) -> list[Migration]:
    applied_migrations = await get_applied_migrations(db)
    return [Migration(path.stem, path, applied_migrations) for path in migrations_location.iterdir()]


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
