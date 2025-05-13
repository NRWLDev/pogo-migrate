from __future__ import annotations

import contextlib
import logging
import typing as t

from pogo_migrate import exceptions, sql
from pogo_migrate.migration import Migration, topological_sort

if t.TYPE_CHECKING:
    from pathlib import Path

    import asyncpg

    from pogo_migrate.context import Context

logger_ = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def transaction(db: asyncpg.Connection, migration: Migration) -> t.AsyncIterator[None]:
    tr = None
    if migration.use_transaction:
        tr = db.transaction()
        await tr.start()

    try:
        yield
    except Exception:
        if tr:
            await tr.rollback()
        raise
    else:
        if tr:
            await tr.commit()


async def apply(db: asyncpg.Connection, migrations_dir: Path, logger: Context | logging.Logger | None = None) -> None:
    logger = logger or logger_
    await sql.ensure_pogo_sync(db)
    migrations = await sql.read_migrations(migrations_dir, db)
    migrations = topological_sort([m.load() for m in migrations])

    for migration in migrations:
        try:
            migration.load()
            if not migration.applied:
                logger.warning("Applying %s", migration.id)
                async with transaction(db, migration):
                    await migration.apply(db)
                    await sql.migration_applied(db, migration.id, migration.hash)
        except Exception as e:  # noqa: PERF203
            msg = f"Failed to apply {migration.id}"
            raise exceptions.BadMigrationError(msg) from e


async def rollback(
    db: asyncpg.Connection,
    migrations_dir: Path,
    count: int | None = None,
    logger: Context | logging.Logger | None = None,
) -> None:
    logger = logger or logger_
    await sql.ensure_pogo_sync(db)
    migrations = await sql.read_migrations(migrations_dir, db)
    migrations = reversed(list(topological_sort([m.load() for m in migrations])))

    i = 0
    for migration in migrations:
        try:
            migration.load()
            if migration.applied and (count is None or i < count):
                logger.warning("Rolling back %s", migration.id)

                async with transaction(db, migration):
                    await migration.rollback(db)
                    await sql.migration_unapplied(db, migration.id)
                    i += 1
        except Exception as e:  # noqa: PERF203
            msg = f"Failed to rollback {migration.id}"
            raise exceptions.BadMigrationError(msg) from e
