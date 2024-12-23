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


async def test_apply_loads_db(monkeypatch, cwd):
    monkeypatch.setattr(testing.migrate, "apply", AsyncMock())
    mock_session = mock.Mock(close=AsyncMock(return_value=None))
    monkeypatch.setattr(testing.sql, "get_connection", AsyncMock(return_value=mock_session))
    await testing.apply()

    assert testing.migrate.apply.call_args == mock.call(
        context.Context(),
        config.Config(
            root_directory=cwd,
            migrations=cwd / "migrations",
            database_config="{POSTGRES_DSN}",
        ),
        mock_session,
    )
    assert mock_session.close.call_count == 1


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


async def test_rollback_loads_db(monkeypatch, cwd):
    monkeypatch.setattr(testing.migrate, "rollback", AsyncMock())
    mock_session = mock.Mock(close=AsyncMock(return_value=None))
    monkeypatch.setattr(testing.sql, "get_connection", AsyncMock(return_value=mock_session))
    await testing.rollback()

    assert testing.migrate.rollback.call_args == mock.call(
        context.Context(),
        config.Config(
            root_directory=cwd,
            migrations=cwd / "migrations",
            database_config="{POSTGRES_DSN}",
        ),
        mock_session,
    )
    assert mock_session.close.call_count == 1
