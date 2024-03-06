import asyncpg

from pogo_migrate import config, migrate


async def apply(db: asyncpg.Connection) -> None:
    c = config.load_config()
    await migrate.apply(c.migrations, db)


async def rollback(db: asyncpg.Connection) -> None:
    c = config.load_config()
    await migrate.rollback(c.migrations, db)
