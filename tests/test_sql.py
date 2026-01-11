from unittest import mock

import pytest
from pogo_core.util import sql as sql_core

from pogo_migrate import sql


async def assert_tables(db_session, tables):
    stmt = """
    SELECT tablename
    FROM pg_tables
    WHERE  schemaname = 'public'
    ORDER BY tablename
    """
    results = await db_session.fetch(stmt)

    assert [r["tablename"] for r in results] == tables


@pytest.mark.nosync
@pytest.mark.parametrize("schema", [None, "unit"])
async def test_ensure_pogo_sync_creates_tables(db_session, schema):
    if schema:
        await db_session.execute(f"CREATE SCHEMA {schema}")
        await db_session.execute(f"SET search_path = '{schema}'")

    await sql.ensure_pogo_sync(db_session)

    await assert_tables(db_session, ["_pogo_migration", "_pogo_version"])


@pytest.mark.nosync
@pytest.mark.parametrize("schema", [None, "unit"])
async def test_ensure_pogo_sync_handles_existing_tables(db_session, schema):
    if schema:
        await db_session.execute(f"CREATE SCHEMA {schema}")
        await db_session.execute(f"SET search_path = '{schema}'")

    await sql.ensure_pogo_sync(db_session)
    await sql.ensure_pogo_sync(db_session)

    await assert_tables(db_session, ["_pogo_migration", "_pogo_version"])


@pytest.mark.parametrize("schema", [None, "unit"])
async def test_migration_applied(db_session, schema):
    if schema:
        await db_session.execute(f"CREATE SCHEMA {schema}")
        await db_session.execute(f"SET search_path = '{schema}'")

    await sql.migration_applied(db_session, "migration_id", "migration_hash")

    ids = await sql.get_applied_migrations(db_session)
    assert ids == {"migration_id"}


@pytest.mark.parametrize("schema", [None, "unit"])
async def test_migration_unapplied(db_session, schema):
    if schema:
        await db_session.execute(f"CREATE SCHEMA {schema}")
        await db_session.execute(f"SET search_path = '{schema}'")

    await sql.migration_applied(db_session, "migration_id", "migration_hash")
    await sql.migration_unapplied(db_session, "migration_id")

    ids = await sql.get_applied_migrations(db_session)
    assert ids == set()


async def test_get_connection(db_session, monkeypatch):
    monkeypatch.setattr(sql_core.asyncpg, "connect", mock.AsyncMock(return_value=db_session))
    db = await sql.get_connection("connection-string")

    assert db == db_session


async def test_read_migrations(db_session, cwd):
    migrations = await sql.read_migrations(cwd, db_session)
    assert migrations == []
