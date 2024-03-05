import logging
import os
import typing as t
from pathlib import Path

import asyncpg
import sqlparse

from pogo_migrate import exceptions
from pogo_migrate.config import Config
from pogo_migrate.migration import Migration

logger = logging.getLogger(__name__)


def read_sql_migration(path: Path) -> tuple[str, t.Awaitable, t.Awaitable]:
    """Read a sql migration.

    Parse the message, [depends], apply statements, and rollback statements.
    """
    with path.open() as f:
        contents = f.read()
        try:
            metadata, contents = contents.split("-- migrate: apply")
        except ValueError as e:
            logger.error("No '-- migrate: apply' found.")
            raise exceptions.BadMigrationError(path) from e
        leading_comment = metadata.strip().split("\n")[0].removeprefix("--").strip()
        # TODO(edgy): Extract depends

        try:
            apply_content, rollback_content = contents.split("-- migrate: rollback")
        except ValueError as e:
            logger.error("No '-- migrate: rollback' found.")
            raise exceptions.BadMigrationError(path) from e
        apply_statements = sqlparse.split(apply_content.strip())

        async def apply(db):  # noqa: ANN001, ANN202
            for statement in apply_statements:
                await db.execute(statement)

        rollback_statements = sqlparse.split(rollback_content.strip())

        async def rollback(db):  # noqa: ANN001, ANN202
            for statement in rollback_statements:
                await db.execute(statement)

        return leading_comment, apply, rollback


async def get_connection(config: Config) -> asyncpg.Connection:
    return await asyncpg.connect(os.environ[config.database_env_key])


async def read_migrations(config: Config, db: asyncpg.Connection) -> list[Migration]:
    applied_migrations = await get_applied_migrations(db)
    return [Migration(path.stem, path, applied_migrations) for path in config.migrations.iterdir()]


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
