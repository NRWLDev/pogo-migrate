from __future__ import annotations

import logging
import typing as t
from warnings import warn

from pogo_core.util import migrate

if t.TYPE_CHECKING:
    from pathlib import Path

    import asyncpg

    from pogo_migrate.context import Context

logger_ = logging.getLogger(__name__)

warn(
    "pogo_migrate.migrate usage has been deprecated, please use pogo_core.util.migrate",
    FutureWarning,
    stacklevel=2,
)


async def apply(
    db: asyncpg.Connection,
    migrations_dir: Path,
    *,
    schema_name: str = "public",
    logger: Context | logging.Logger | None = None,
) -> None:
    return await migrate.apply(db, migrations_dir, schema_name=schema_name, logger=logger)


async def rollback(
    db: asyncpg.Connection,
    migrations_dir: Path,
    *,
    schema_name: str = "public",
    count: int | None = None,
    logger: Context | logging.Logger | None = None,
) -> None:
    return await migrate.rollback(db, migrations_dir, schema_name=schema_name, count=count, logger=logger)
