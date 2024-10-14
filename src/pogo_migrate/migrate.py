from __future__ import annotations

import contextlib
import typing as t

from pogo_migrate import exceptions, sql
from pogo_migrate.migration import Migration, topological_sort

if t.TYPE_CHECKING:
    import asyncpg

    from pogo_migrate.config import Config
    from pogo_migrate.context import Context


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


async def apply(context: Context, config: Config, db: asyncpg.Connection) -> None:
    await sql.ensure_pogo_sync(db)
    migrations = await sql.read_migrations(config.migrations, db)
    migrations = topological_sort([m.load() for m in migrations])

    for migration in migrations:
        try:
            migration.load()
            if not migration.applied:
                context.warning("Applying %s", migration.id)
                async with transaction(db, migration):
                    await migration.apply(db)
                    await sql.migration_applied(db, migration.id, migration.hash)
        except Exception as e:  # noqa: PERF203
            msg = f"Failed to apply {migration.id}"
            raise exceptions.BadMigrationError(msg) from e


async def rollback(context: Context, config: Config, db: asyncpg.Connection, count: int | None = None) -> None:
    await sql.ensure_pogo_sync(db)
    migrations = await sql.read_migrations(config.migrations, db)
    migrations = reversed(list(topological_sort([m.load() for m in migrations])))

    i = 0
    for migration in migrations:
        try:
            migration.load()
            if migration.applied and (count is None or i < count):
                context.warning("Rolling back %s", migration.id)

                async with transaction(db, migration):
                    await migration.rollback(db)
                    await sql.migration_unapplied(db, migration.id)
                    i += 1
        except Exception as e:  # noqa: PERF203
            msg = f"Failed to rollback {migration.id}"
            raise exceptions.BadMigrationError(msg) from e
