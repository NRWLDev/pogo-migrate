from __future__ import annotations

import logging
import typing as t

from pogo_migrate import exceptions, sql
from pogo_migrate.migration import topological_sort

if t.TYPE_CHECKING:
    import asyncpg

    from pogo_migrate.config import Config

logger = logging.getLogger(__name__)


async def apply(config: Config, db: asyncpg.Connection) -> None:
    await sql.ensure_pogo_sync(db)
    migrations = await sql.read_migrations(config.migrations, db)
    migrations = topological_sort([m.load() for m in migrations])

    async with db.transaction():
        try:
            for migration in migrations:
                migration.load()
                if not migration.applied:
                    logger.error("Applying %s", migration.id)
                    await migration.apply(db)
                    await sql.migration_applied(db, migration.id, migration.hash)
        except Exception as e:  # noqa: BLE001
            msg = f"Failed to apply {migration.id}"
            raise exceptions.BadMigrationError(msg) from e


async def rollback(config: Config, db: asyncpg.Connection, count: int | None = None) -> None:
    await sql.ensure_pogo_sync(db)
    migrations = await sql.read_migrations(config.migrations, db)
    migrations = reversed(list(topological_sort([m.load() for m in migrations])))

    async with db.transaction():
        i = 0
        try:
            for migration in migrations:
                migration.load()
                if migration.applied and (count is None or i < count):
                    logger.error("Rolling back %s", migration.id)
                    await migration.rollback(db)
                    await sql.migration_unapplied(db, migration.id)
                    i += 1
        except Exception as e:  # noqa: BLE001
            msg = f"Failed to rollback {migration.id}"
            raise exceptions.BadMigrationError(msg) from e
