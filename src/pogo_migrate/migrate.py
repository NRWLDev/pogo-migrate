import logging

import asyncpg

from pogo_migrate import sql
from pogo_migrate.config import Config

logger = logging.getLogger(__name__)


async def _apply(config: Config, db: asyncpg.Connection) -> None:
    await sql.ensure_pogo_sync(db)
    migrations = await sql.read_migrations(config.migrations, db)

    tr = db.transaction()
    await tr.start()
    try:
        for migration in migrations:
            if not migration.applied:
                migration.load()
                logger.error("Applying %s", migration.id)
                await migration.apply(db)
                stmt = """
                INSERT INTO _pogo_migration (
                    migration_hash,
                    migration_id,
                    applied
                ) VALUES (
                    $1, $2, now()
                )
                """
                await db.execute(stmt, migration.hash, migration.id)
    except Exception as e:
        logger.warning(str(e))
        await tr.rollback()
        raise
    else:
        await tr.commit()


async def _rollback(config: Config, db: asyncpg.Connection) -> None:
    await sql.ensure_pogo_sync(db)
    migrations = reversed(await sql.read_migrations(config.migrations, db))

    tr = db.transaction()
    await tr.start()
    try:
        for migration in migrations:
            if migration.applied:
                migration.load()
                logger.error("Rolling back %s", migration.id)
                await migration.rollback(db)
                stmt = """
                DELETE FROM _pogo_migration
                WHERE migration_id = $1
                """
                await db.execute(stmt, migration.id)
    except Exception as e:
        logger.warning(str(e))
        await tr.rollback()
        raise
    else:
        await tr.commit()
