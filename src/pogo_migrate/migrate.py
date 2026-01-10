from __future__ import annotations

import logging
import typing as t

from pogo_core.util import migrate

if t.TYPE_CHECKING:
    from pathlib import Path

    import asyncpg

    from pogo_migrate.context import Context

logger_ = logging.getLogger(__name__)


async def apply(db: asyncpg.Connection, migrations_dir: Path, logger: Context | logging.Logger | None = None) -> None:
    return await migrate.apply(db, migrations_dir, logger)


async def rollback(
    db: asyncpg.Connection,
    migrations_dir: Path,
    count: int | None = None,
    logger: Context | logging.Logger | None = None,
) -> None:
    return await migrate.rollback(db, migrations_dir, count, logger)
