from __future__ import annotations

import logging
from pathlib import Path
from textwrap import dedent

from pogo_migrate.migration import Migration

logger = logging.getLogger(__name__)


squash_sql_template = dedent(
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


def remove(current: Migration, dependent: Migration | None) -> None:
    logger.warning("Removing %s", current.id)
    new_depends = ", ".join(current.depends_ids)
    if dependent:
        content = dependent.path.read_text()
        dependent.path.write_text(
            content.replace(current.id, new_depends)
            # Handle python becoming initial migration.
            .replace('__depends__ = [""]', "__depends__ = []"),
        )
        dependent.load()
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
    squashed = "\n-- squashed: ".join(squashed[:-1])

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
        squashed=squashed,
    )
    path.write_text(content)

    return Migration(path.stem, path, [])
