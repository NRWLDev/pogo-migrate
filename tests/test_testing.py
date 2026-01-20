from unittest import mock

import pytest
from pogo_core.util import testing as core_testing

from pogo_migrate import testing
from tests.util import AsyncMock


@pytest.fixture(autouse=True)
def pyproject(pyproject_factory):
    return pyproject_factory()


async def test_apply(monkeypatch, db_session, cwd):
    monkeypatch.setattr(core_testing.migrate, "apply", AsyncMock())
    await testing.apply(db_session)

    assert core_testing.migrate.apply.call_args == mock.call(
        db_session,
        cwd / "migrations",
        schema_name="public",
    )


async def test_apply_loads_db(monkeypatch, cwd):
    monkeypatch.setattr(core_testing.migrate, "apply", AsyncMock())
    mock_session = mock.Mock(close=AsyncMock(return_value=None))
    monkeypatch.setattr(core_testing.sql, "get_connection", AsyncMock(return_value=mock_session))
    await testing.apply()

    assert core_testing.migrate.apply.call_args == mock.call(
        mock_session,
        cwd / "migrations",
        schema_name="public",
    )
    assert mock_session.close.call_count == 1


async def test_rollback(monkeypatch, db_session, cwd):
    monkeypatch.setattr(core_testing.migrate, "rollback", AsyncMock())
    await testing.rollback(db_session)

    assert core_testing.migrate.rollback.call_args == mock.call(
        db_session,
        cwd / "migrations",
        schema_name="public",
    )


async def test_rollback_loads_db(monkeypatch, cwd):
    monkeypatch.setattr(core_testing.migrate, "rollback", AsyncMock())
    mock_session = mock.Mock(close=AsyncMock(return_value=None))
    monkeypatch.setattr(core_testing.sql, "get_connection", AsyncMock(return_value=mock_session))
    await testing.rollback()

    assert core_testing.migrate.rollback.call_args == mock.call(
        mock_session,
        cwd / "migrations",
        schema_name="public",
    )
    assert mock_session.close.call_count == 1
