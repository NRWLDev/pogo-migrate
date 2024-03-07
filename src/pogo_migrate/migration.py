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
            msg = f"{path.name}: No '-- migrate: apply' found."
            raise exceptions.BadMigrationError(msg) from e

        m = re.match(r".*--(.*)\s-- depends:(.*)[\s]?", metadata.strip())

        if m is None:
            msg = f"{path.name}: No '-- depends:' or message found."
            raise exceptions.BadMigrationError(msg)

        message = m[1].strip()
        depends = m[2].strip()

        try:
            apply_content, rollback_content = contents.split("-- migrate: rollback")
        except ValueError as e:
            msg = f"{path.name}: No '-- migrate: rollback' found."
            raise exceptions.BadMigrationError(msg) from e

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

    def __init__(self: t.Self, mig_id: str | None, path: str, applied_migrations: set[str] | None) -> None:
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

    def __repr__(self: t.Self) -> str:
        return self.id

    @property
    def applied(self: t.Self) -> bool:
        return self._applied

    @property
    def depends(self: t.Self) -> set[Migration]:
        return self._depends

    @property
    def depends_ids(self: t.Self) -> set[str]:
        return {m.id for m in self._depends}

    @property
    def is_sql(self: t.Self) -> bool:
        return self.path.suffix == ".sql"

    async def apply(self: t.Self, db: asyncpg.Connection) -> None:
        await self._apply(db)

    async def rollback(self: t.Self, db: asyncpg.Connection) -> None:
        await self._rollback(db)

    def load(self: t.Self) -> Migration:
        depends_ = []
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

            if spec:
                module = importlib.util.module_from_spec(spec)

            if spec and spec.loader:
                try:
                    spec.loader.exec_module(module)
                except Exception as e:  # noqa: BLE001
                    msg = f"Could not import migration from '{self.path.name}'"
                    raise exceptions.BadMigrationError(msg) from e
                self._doc = module.__doc__.strip()
                depends_ = module.__depends__
                self._apply = module.apply
                self._rollback = module.rollback
            else:
                msg = f"Could not import migration from '{self.path.name}': ModuleSpec has no loader attached"
                raise exceptions.BadMigrationError(msg)

        self._depends = {self.__migrations.get(mig_id) for mig_id in depends_}
        if None in self._depends:
            msg = f"Could not resolve dependencies for '{self.path.name}'"
            raise exceptions.BadMigrationError(msg)

        return self

    @property
    def __doc__(self: t.Self) -> str:
        return self._doc


def topological_sort(migrations: t.Iterable[Migration]) -> t.Iterable[Migration]:
    migration_list = list(migrations)
    all_migrations = set(migration_list)
    dependency_graph = {m: (m.depends & all_migrations) for m in migration_list}
    try:
        return list(topologicalsort.topological_sort(migration_list, dependency_graph))
    except topologicalsort.CycleError as e:
        msg = "Circular dependencies among these migrations {}".format(
            ", ".join(m.id for m in e.args[1]),
        )
        raise exceptions.BadMigrationError(msg) from e
