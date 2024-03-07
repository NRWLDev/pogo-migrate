import os
import pathlib

import asyncpg
import pytest
import rtoml
import typer.testing

import pogo_migrate.cli
from pogo_migrate.migration import Migration


@pytest.fixture(autouse=True)
def _clear_migration_tracking():
    # ClassVar remembers history across tests. Clear it every test.
    try:
        yield
    except:  # noqa: E722, S110
        pass
    finally:
        Migration._Migration__migrations = {}


@pytest.fixture()
def postgres_dsn():
    return os.environ["POSTGRES_DSN"]


@pytest.fixture(autouse=True)
def cwd(tmp_path):
    orig = pathlib.Path.cwd()

    try:
        os.chdir(str(tmp_path))
        yield tmp_path
    finally:
        os.chdir(orig)


@pytest.fixture()
def pyproject_factory(cwd):
    def factory():
        p = cwd / "pyproject.toml"

        with p.open("w") as f:
            f.write(
                rtoml.dumps(
                    {
                        "tool": {
                            "pogo": {
                                "migrations": "./migrations",
                                "database_env_key": "POSTGRES_DSN",
                            },
                        },
                    },
                ),
            )

    return factory


@pytest.fixture()
def migrations(cwd):
    p = cwd / "migrations"
    p.mkdir()

    return p


@pytest.fixture(autouse=True)
async def db_session(postgres_dsn):
    conn = await asyncpg.connect(postgres_dsn)
    tr = conn.transaction()
    await tr.start()
    try:
        yield conn
    finally:
        await tr.rollback()


class CliRunner(typer.testing.CliRunner):
    target = pogo_migrate.cli.app

    def invoke(self, *args, **kwargs):
        result = super().invoke(self.target, *args, **kwargs)
        if result.exception:
            if isinstance(result.exception, SystemExit):
                # The error is already properly handled. Print it and return.
                print(result.output)  # noqa: T201
            else:
                raise result.exception.with_traceback(result.exc_info[2])
        return result


@pytest.fixture()
def cli_runner():
    return CliRunner()
