from __future__ import annotations

import typing as t
from warnings import warn

from pogo_core import squash

if t.TYPE_CHECKING:
    from pogo_core.migration import Migration

    from pogo_migrate.context import Context


squash_sql_template: str = squash.squash_sql_template

warn(
    "pogo_migrate.squash usage has been deprecated, please use pogo_core.squash",
    FutureWarning,
    stacklevel=2,
)


def remove(context: Context, current: Migration, dependent: Migration | None, *, backup: bool = False) -> None:
    return squash.remove(current, dependent, context, backup=backup)


def write(
    apply_statements: dict[str, list[str]],
    rollback_statements: dict[str, list[list[str]]],
    latest: Migration,
    depends: str | None,
    squashed: list[str],
) -> Migration | None:
    return squash.write(apply_statements, rollback_statements, latest, depends, squashed)


ParsedStatement = squash.ParsedStatement


def parse(context: Context, statement: str) -> ParsedStatement:
    return squash.parse(statement, context)


ParseError = squash.ParseError


def parse_sqlglot(context: Context, statement: str) -> ParsedStatement:
    return squash.parse_sqlglot(statement, context)
