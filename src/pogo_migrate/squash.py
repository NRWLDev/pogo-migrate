from __future__ import annotations

import re
import typing as t
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent

import sqlglot
import sqlparse
from sqlglot import expressions as exp

from pogo_migrate.migration import Migration

if t.TYPE_CHECKING:
    from pogo_migrate.context import Context


squash_sql_template: str = dedent(
    """\
    --{message}
    -- depends:{depends}

    -- squashed: {squashed}

    -- migrate: apply

    {apply}

    -- migrate: rollback

    {rollback}
    """,
)


def remove(context: Context, current: Migration, dependent: Migration | None, *, backup: bool = False) -> None:
    context.warning("Removing %s", current.id)
    new_depends = ", ".join(current.depends_ids)
    if dependent:
        content = dependent.path.read_text()
        dependent.path.write_text(
            content.replace(current.id, new_depends)
            # Handle python becoming initial migration.
            .replace('__depends__ = [""]', "__depends__ = []"),
        )
        dependent.load()
    if backup:
        current.path.rename(f"{current.path}.bak")
    else:
        current.path.unlink()


def write(
    apply_statements: dict[str, list[str]],
    rollback_statements: dict[str, list[list[str]]],
    latest: Migration,
    depends: str | None,
    squashed: list[str],
) -> Migration | None:
    if squashed[0] == latest.id:
        return None

    path = Path(f"{latest.path}.squash")
    template = squash_sql_template
    depends = f" {depends}" if depends else ""
    message = f" {latest.__doc__}"
    # add comment for which migrations were squashed in
    squashed_comment = "\n-- squashed: ".join(squashed[:-1])

    apply = []
    for ident, statements_ in apply_statements.items():
        if ident == "__data":
            continue
        apply.append(f"-- Squash {ident} statements.")
        apply.extend(statements_)

    data_statements = apply_statements.get("__data", [])
    if data_statements:
        apply.append("-- Squash data statements.")
        apply.extend(data_statements)

    rollback = []
    rollback_data_statements = rollback_statements.get("__data", [])
    if rollback_data_statements:
        rollback.append("-- Squash data statements.")
        for data_statements in reversed(rollback_data_statements):
            rollback.extend(reversed(data_statements))

    for ident, statements_ in reversed(rollback_statements.items()):
        if ident == "__data":
            continue
        rollback.append(f"-- Squash {ident} statements.")
        for migration_statements in reversed(statements_):
            rollback.extend(reversed(migration_statements))

    content = template.format(
        message=message,
        depends=depends,
        apply="\n\n".join(apply),
        rollback="\n\n".join(rollback),
        squashed=squashed_comment,
    )
    path.write_text(content)

    return Migration(path.stem, path, set())


@dataclass
class ParsedStatement:
    statement: str
    statement_type: str
    identifier: str | None


def parse(context: Context, statement: str) -> ParsedStatement:
    parsed = sqlparse.parse(statement)[0]

    type_ = parsed.get_type()

    identifier = None
    if type_ in ("CREATE", "ALTER", "DROP"):
        idx, action = parsed.token_next_by(
            m=[
                (sqlparse.tokens.Keyword, "TABLE"),
                (sqlparse.tokens.Keyword, "AGGREGATE"),
                (sqlparse.tokens.Keyword, "INDEX"),
                (sqlparse.tokens.Keyword, "SCHEMA"),
                (None, "EXTENSION"),
            ],
        )

        exists_idx, _token = parsed.token_next_by(idx=idx, m=(sqlparse.tokens.Keyword, "EXISTS"))
        if action.value.startswith("EXTENSION "):
            # Extension is not picked up as a specific type, so it can include the extension name.
            identifier = action.value.split()[1]
        elif action.value == "INDEX":
            """
            Fetch tbl_ident from CREATE INDEX statements.
            Fetch ident from DROP INDEX statements.

            CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] ident ON tbl_ident;
            DROP INDEX [CONCURRENTLY] [IF EXISTS] ident;
            """
            c_idx, _c_keyword = parsed.token_next_by(idx=exists_idx or idx, m=(sqlparse.tokens.Keyword, "CONCURRENTLY"))
            on_idx, _on_keyword = parsed.token_next_by(
                idx=c_idx or exists_idx or idx,
                m=(sqlparse.tokens.Keyword, "ON"),
            )
            idx, ident_token = parsed.token_next(on_idx or c_idx or exists_idx or idx, skip_ws=True, skip_cm=True)
            identifier = (
                ident_token.get_real_name()
                if isinstance(ident_token, (sqlparse.sql.Identifier, sqlparse.sql.Function))
                else None
            )
        else:
            # TABLE, AGGREGATE, IF [NOT] EXISTS EXTENSION.
            idx, ident_token = parsed.token_next(exists_idx or idx, skip_ws=True, skip_cm=True)
            identifier = (
                ident_token.get_real_name()
                if isinstance(ident_token, (sqlparse.sql.Identifier, sqlparse.sql.Function))
                else None
            )

    context.debug("parsed tokens: %s", parsed.tokens)

    return ParsedStatement(statement, type_, identifier)


class ParseError(Exception): ...


def parse_sqlglot(context: Context, statement: str) -> ParsedStatement:
    try:
        parsed = sqlglot.parse_one(statement, read="postgres", dialect="postgres")
    except sqlglot.errors.ParseError as e:
        r = r"(?P<msg>Expected table name but got) (<.*text: )?(?P<text>\w+)(, .*>)?\. Line (?P<line>\d+), Col: (?P<column>\d+)\.\n(?P<statement>.*)"
        m = re.match(r, str(e))
        msg = "{msg} {text}. Line: {line}, Column: {column}".format(**m.groupdict())
        raise ParseError(msg) from e

    identifier = None
    if isinstance(parsed, (exp.Create, exp.Alter, exp.Drop)):
        ident = parsed.find(exp.Table)
        identifier = ident.name if ident.this is not None else str(ident).replace('"', "")
    elif isinstance(parsed, exp.Command):
        # Unhandled syntax by sqlglot, fallback to sqlparse
        context.warning("sqlglot failed to parse, falling back to sqlparse.")
        return parse(context, statement)

    return ParsedStatement(statement, parsed.__class__.__name__.upper(), identifier)
