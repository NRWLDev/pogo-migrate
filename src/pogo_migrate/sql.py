import logging
import typing as t
from pathlib import Path

import sqlparse

from pogo_migrate import exceptions

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
        leading_comment = metadata.strip().split("\n")[0].removeprefix("--").strip()
        # TODO(edgy): Extract depends

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

        return leading_comment, apply, rollback
