from __future__ import annotations

import hashlib
import importlib.util
import logging
import re
import typing as t

import sqlparse

from pogo_migrate import exceptions, topologicalsort

if t.TYPE_CHECKING:
    from pathlib import Path

    import asyncpg

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

        m = re.match(r".*-- (.*)\s-- depends:(.*)[\s]?", metadata)

        message, depends = "", ""
        if m:
            message = m[1]
            depends = m[2]

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

        return message, depends, apply, rollback


class Migration:
    __migrations: t.ClassVar[dict[str, Migration]] = {}

    def __init__(self: t.Self, mig_id: str | None, path: str, applied_migrations: str[str] | None) -> None:
        applied_migrations = applied_migrations or set()
        self.id = mig_id
        self.path = path
        self.hash = hashlib.sha256(mig_id.encode("utf-8")).hexdigest() if mig_id else None
        self._doc: str | None = None
        self._depends: set[Migration] | None = None
        self._apply: t.Awaitable | None = None
        self._rollback: t.Awaitable | None = None
        self._applied = self.id in applied_migrations
        self.__migrations[self.id] = self

    @property
    def applied(self: t.Self) -> bool:
        return self._applied

    @property
    def depends(self: t.Self) -> list[str]:
        return self._depends

    @property
    def depends_ids(self: t.Self) -> list[str]:
        return [m.id for m in self._depends]

    @property
    def is_sql(self: t.Self) -> bool:
        return self.path.suffix == ".sql"

    async def apply(self: t.Self, db: asyncpg.Connection) -> None:
        await self._apply(db)

    async def rollback(self: t.Self, db: asyncpg.Connection) -> None:
        await self._rollback(db)

    def load(self: t.Self) -> Migration:
        depends = []
        if self.is_sql:
            message, depends, apply, rollback = read_sql_migration(self.path)
            self._doc = message
            self._apply = apply
            self._rollback = rollback
            depends_ = depends.split()
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
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        "Could not import migration from '%s'",
                        self.path,
                    )
                    raise exceptions.BadMigrationError(self.path) from e
                self._doc = module.__doc__
                depends_ = module.__depends__
                self._apply = module.apply
                self._rollback = module.rollback
            else:
                logger.error(
                    "Could not import migration from '%s': ModuleSpec has no loader attached",
                    self.path,
                )
                raise exceptions.BadMigrationError(self.path)

        self._depends = {self.__migrations.get(mig_id) for mig_id in depends_}
        if None in self._depends:
            logger.error(
                "Could not resolve dependencies for '%s'",
                self.path,
            )
            raise exceptions.BadMigrationError(self.path)

        return self

    @property
    def __doc__(self: t.Self) -> str:
        return self._doc


def topological_sort(migrations: t.Iterable[Migration]) -> t.Iterable[Migration]:
    migration_list = list(migrations)
    all_migrations = set(migration_list)
    dependency_graph = {m: (m.depends & all_migrations) for m in migration_list}
    try:
        return topologicalsort.topological_sort(migration_list, dependency_graph)
    except topologicalsort.CycleError as e:
        msg = "Circular dependencies among these migrations {}".format(
            ", ".join(m.id for m in e.args[1]),
        )
        raise exceptions.BadMigrationError(msg) from e
