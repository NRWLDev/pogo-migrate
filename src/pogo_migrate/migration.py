from __future__ import annotations

import importlib.util
import logging
import typing as t

from pogo_migrate import exceptions
from pogo_migrate.sql import read_sql_migration

if t.TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger(__name__)


class Migration:
    def __init__(self: t.Self, mig_id: str | None, path: str) -> None:
        self.mig_id = mig_id
        self.path = path
        self._doc: str | None = None
        self._depends: list[str] | None = None
        self._apply: t.Awaitable | None = None
        self._rollback: t.Awaitable | None = None

    @property
    def is_sql(self: t.Self) -> bool:
        return self.path.suffix == ".sql"

    async def apply(self: t.Self, db: asyncpg.Connection) -> None:
        await self._apply(db)

    async def rollback(self: t.Self, db: asyncpg.Connection) -> None:
        await self.rollback(db)

    def load(self: t.Self) -> None:
        if self.is_sql:
            leading_comment, apply, rollback = read_sql_migration(self.path)
            self._doc = leading_comment
            self._apply = apply
            self._rollback = rollback
            self._depends = []
        else:
            spec = importlib.util.spec_from_file_location(
                str(self.path),
                str(self.path),
            )
            # TODO(edgy): check spec exists
            module = importlib.util.module_from_spec(spec)
            if spec and spec.loader:
                try:
                    spec.loader.exec_module(module)
                except Exception as e:
                    logger.exception(
                        "Could not import migration from %r",
                        self.path,
                    )
                    raise exceptions.BadMigrationError(self.path) from e
                self._doc = module.__doc__
                self._depends = module.__depends__
                self._apply = module.apply
                self._rollback = module.rollback
            else:
                logger.exception(
                    "Could not import migration from %r: "
                    "ModuleSpec has no loader attached",
                    self.path,
                )
                raise exceptions.BadMigrationError(self.path)

    @property
    def __doc__(self: t.Self) -> str:
        return self._doc

