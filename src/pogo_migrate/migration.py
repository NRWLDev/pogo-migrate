from __future__ import annotations

import hashlib
import importlib.util
import re
import typing as t

import asyncpg
import sqlparse

from pogo_migrate import exceptions, topologicalsort

if t.TYPE_CHECKING:
    from pathlib import Path


MigrationFunc = t.Callable[[asyncpg.Connection], t.Coroutine[t.Any, t.Any, t.Any]]


def strip_comments(statement: str) -> str:
    return "\n".join([line for line in statement.split("\n") if not line.startswith("--")])


def terminate_statements(statements: list[str]) -> list[str]:
    """Clean up last statement, if its missing a `;` it can cause issues in squashes."""
    if statements and statements[-1][-1] != ";":
        stmt = statements.pop()
        statements.append(f"{stmt};")

    return statements


def read_sql_migration(
    path: Path,
) -> tuple[str, str, MigrationFunc, MigrationFunc, bool, list[str], list[str]]:
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

        message: str = m[1].strip()
        depends: str = m[2].strip()
        use_transaction = "-- transaction: false" not in metadata

        try:
            apply_content, rollback_content = contents.split("-- migrate: rollback")
        except ValueError as e:
            msg = f"{path.name}: No '-- migrate: rollback' found."
            raise exceptions.BadMigrationError(msg) from e

        apply_statements = terminate_statements(sqlparse.split(apply_content.strip()))

        async def apply(db: asyncpg.Connection) -> None:
            for statement in apply_statements:
                # Skip comments
                statement_ = strip_comments(statement)
                if statement_:
                    await db.execute(statement_)

        rollback_statements = terminate_statements(sqlparse.split(rollback_content.strip()))

        async def rollback(db: asyncpg.Connection) -> None:
            for statement in rollback_statements:
                # Skip comments
                statement_ = strip_comments(statement)
                if statement_:
                    await db.execute(statement_)

        return message, depends, apply, rollback, use_transaction, apply_statements, rollback_statements


class Migration:
    __migrations: t.ClassVar[dict[str, Migration]] = {}

    def __init__(self: t.Self, mig_id: str, path: Path, applied_migrations: set[str] | None) -> None:
        applied_migrations = applied_migrations or set()
        self.id = mig_id
        self.path = path
        self.hash: str = hashlib.sha256(mig_id.encode("utf-8")).hexdigest()
        self._use_transaction: bool = True
        self._doc: str | None = None
        self._depends: set[Migration] | None = None
        self._apply: MigrationFunc | None = None
        self._rollback: MigrationFunc | None = None
        self._applied = self.id in applied_migrations
        self.__migrations[self.id] = self

    def __repr__(self: t.Self) -> str:
        return self.id

    @property
    def applied(self: t.Self) -> bool:
        return self._applied

    @property
    def depends(self: t.Self) -> set[Migration]:
        return self._depends or set()

    @property
    def use_transaction(self: t.Self) -> bool:
        return self._use_transaction

    @property
    def depends_ids(self: t.Self) -> set[str]:
        return {m.id for m in self.depends}

    @property
    def is_sql(self: t.Self) -> bool:
        return self.path.suffix == ".sql"

    async def apply(self: t.Self, db: asyncpg.Connection) -> None:
        if self._apply is None:
            return
        await self._apply(db)

    async def rollback(self: t.Self, db: asyncpg.Connection) -> None:
        if self._rollback is None:
            return
        await self._rollback(db)

    def load(self: t.Self) -> Migration:
        depends_ = []
        if self.is_sql:
            message, depends, apply, rollback, in_transaction, _, _ = read_sql_migration(self.path)
            self._doc = message
            self._apply = apply
            self._rollback = rollback
            self._use_transaction = in_transaction
            depends_ = depends.split()
        else:
            spec = importlib.util.spec_from_file_location(
                str(self.path),
                str(self.path),
            )

            if spec:
                module = importlib.util.module_from_spec(spec)

                if spec.loader:
                    try:
                        spec.loader.exec_module(module)
                    except Exception as e:
                        msg = f"Could not import migration from '{self.path.name}'"
                        raise exceptions.BadMigrationError(msg) from e
                    self._doc = (module.__doc__ or "").strip()
                    depends_ = module.__depends__
                    self._apply = module.apply
                    self._rollback = module.rollback
                    self._use_transaction = getattr(module, "__transaction__", True)
            else:
                msg = f"Could not import migration from '{self.path.name}': ModuleSpec has no loader attached"
                raise exceptions.BadMigrationError(msg)

        found_dependencies = {self.__migrations.get(mig_id) for mig_id in depends_}
        if None in found_dependencies:
            msg = f"Could not resolve dependencies for '{self.path.name}'"
            raise exceptions.BadMigrationError(msg)

        self._depends = {d for d in found_dependencies if d is not None}

        return self

    @property
    def __doc__(self: t.Self) -> str:
        return self._doc or ""


def topological_sort(migrations: t.Iterable[Migration]) -> list[Migration]:
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
