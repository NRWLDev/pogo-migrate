import re
from pathlib import Path

import asyncpg

from pogo_migrate.context import Context


def convert_sql_migration(migration: Path) -> str:
    with migration.open() as f:
        apply_content = f.read()

    rollback = migration.with_suffix(".rollback.sql")
    rollback_content = ""
    if rollback.exists():
        with rollback.open() as f:
            rollback_content = f.read()
        rollback.unlink()

    m = re.match(r".*--(.*)\s-- depends:(.*)\s", apply_content)

    message, depends = "--", "-- depends:"
    if m:
        message = f"-- {m[1].strip()}"
        depends = f"-- depends: {m[2].strip()}"

    content = [message, depends]

    content.extend(["", "-- migrate: apply", ""])
    apply_content = re.sub(r".*--(.*)\s-- depends:(.*)\s", "", apply_content)
    content.append(apply_content.strip())

    content.extend(["", "-- migrate: rollback", ""])
    rollback_content = re.sub(r".*--(.*)\s-- depends:(.*)\s", "", rollback_content)
    content.append(rollback_content.strip())

    return "\n".join(content)


async def copy_yoyo_migration_history(context: Context, db: asyncpg.Connection) -> None:
    stmt = """
    SELECT count(*)
    FROM _pogo_migration
    """
    r = await db.fetchrow(stmt)

    if r is not None and r["count"] != 0:
        context.warning("migration history exists, skipping yoyo migration.")
        return

    stmt = """
    SELECT EXISTS (
       SELECT FROM information_schema.tables
       WHERE   table_name   = '_yoyo_migration'
    )"""
    r = await db.fetchrow(stmt)

    if r is not None and not r["exists"]:
        context.warning("yoyo migration history missing, skipping yoyo migration.")
        return

    stmt = """
    INSERT INTO _pogo_migration (migration_hash, migration_id, applied)
    SELECT
        migration_hash, migration_id, applied_at_utc AS applied
    FROM _yoyo_migration
    """
    await db.execute(stmt)
