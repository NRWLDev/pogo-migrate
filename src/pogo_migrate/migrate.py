from __future__ import annotations

import logging
import typing as t

from pogo_migrate import sql

if t.TYPE_CHECKING:
    import asyncpg

    from pogo_migrate.config import Config

logger = logging.getLogger(__name__)


async def apply(config: Config, db: asyncpg.Connection) -> None:
    await sql.ensure_pogo_sync(db)
    migrations = await sql.read_migrations(config.migrations, db)

    async with db.transaction():
        try:
            for migration in migrations:
                if not migration.applied:
                    migration.load()
                    logger.error("Applying %s", migration.id)
                    await migration.apply(db)
                    await sql.migration_applied(db, migration.id, migration.hash)
        except Exception as e:
            logger.warning(str(e))
            raise


async def rollback(config: Config, db: asyncpg.Connection, count: int | None = None) -> None:
    await sql.ensure_pogo_sync(db)
    migrations = reversed(await sql.read_migrations(config.migrations, db))

    async with db.transaction():
        i = 0
        try:
            for migration in migrations:
                if migration.applied and (count is None or i < count):
                    migration.load()
                    logger.error("Rolling back %s", migration.id)
                    await migration.rollback(db)
                    await sql.migration_unapplied(db, migration.id)
                    i += 1
        except Exception as e:
            logger.warning(str(e))
            raise
