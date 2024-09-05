from unittest import mock

import pytest

from pogo_migrate import config, context, testing
from tests.util import AsyncMock


@pytest.fixture(autouse=True)
def pyproject(pyproject_factory):
    return pyproject_factory()


async def test_apply(monkeypatch, db_session, cwd):
    monkeypatch.setattr(testing.migrate, "apply", AsyncMock())
    await testing.apply(db_session)

    assert testing.migrate.apply.call_args == mock.call(
        context.Context(),
        config.Config(
            root_directory=cwd,
            migrations=cwd / "migrations",
            database_config="{POSTGRES_DSN}",
        ),
        db_session,
    )


async def test_apply_loads_db(monkeypatch, db_session, cwd):
    monkeypatch.setattr(testing.migrate, "apply", AsyncMock())
    monkeypatch.setattr(testing.sql, "get_connection", AsyncMock(return_value=db_session))
    await testing.apply()

    assert testing.migrate.apply.call_args == mock.call(
        context.Context(),
        config.Config(
            root_directory=cwd,
            migrations=cwd / "migrations",
            database_config="{POSTGRES_DSN}",
        ),
        db_session,
    )


async def test_rollback(monkeypatch, db_session, cwd):
    monkeypatch.setattr(testing.migrate, "rollback", AsyncMock())
    await testing.rollback(db_session)

    assert testing.migrate.rollback.call_args == mock.call(
        context.Context(),
        config.Config(
            root_directory=cwd,
            migrations=cwd / "migrations",
            database_config="{POSTGRES_DSN}",
        ),
        db_session,
    )


async def test_rollback_loads_db(monkeypatch, db_session, cwd):
    monkeypatch.setattr(testing.migrate, "rollback", AsyncMock())
    monkeypatch.setattr(testing.sql, "get_connection", AsyncMock(return_value=db_session))
    await testing.rollback()

    assert testing.migrate.rollback.call_args == mock.call(
        context.Context(),
        config.Config(
            root_directory=cwd,
            migrations=cwd / "migrations",
            database_config="{POSTGRES_DSN}",
        ),
        db_session,
    )
