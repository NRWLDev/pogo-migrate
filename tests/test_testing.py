from unittest import mock

import pytest

from pogo_migrate import config, testing
from tests.util import AsyncMock


@pytest.fixture(autouse=True)
def pyproject(pyproject_factory):
    return pyproject_factory()


async def test_apply(monkeypatch, db_session, cwd):
    monkeypatch.setattr(testing.migrate, "apply", AsyncMock())
    await testing.apply(db_session)

    assert testing.migrate.apply.call_args == mock.call(
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
        config.Config(
            root_directory=cwd,
            migrations=cwd / "migrations",
            database_config="{POSTGRES_DSN}",
        ),
        db_session,
    )
