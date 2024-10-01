import asyncio
import os
import pathlib
import re
import textwrap

import asyncpg
import nest_asyncio
import pytest
import rtoml
import typer.testing

import pogo_migrate.cli
from pogo_migrate import sql
from pogo_migrate.context import Context
from pogo_migrate.migration import Migration


@pytest.fixture(autouse=True, scope="session")
def _apply_nest_asyncio():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    nest_asyncio.apply(loop)
    try:
        yield
    finally:
        loop.close()


@pytest.fixture(autouse=True)
def _clear_migration_tracking():
    # ClassVar remembers history across tests. Clear it every test.
    try:
        yield
    except:  # noqa: E722, S110
        pass
    finally:
        Migration._Migration__migrations = {}


@pytest.fixture
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


@pytest.fixture
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
                                "database_config": "{POSTGRES_DSN}",
                            },
                        },
                    },
                ),
            )

    return factory


@pytest.fixture
def migrations(cwd):
    p = cwd / "migrations"
    p.mkdir()

    return p


@pytest.fixture
def migration_file_factory(migrations):
    def factory(mig_id, format_, contents):
        p = migrations / f"{mig_id}.{format_}"
        with p.open("w") as f:
            f.write(contents)

        return p

    return factory


@pytest.fixture(autouse=True)
async def db_session(postgres_dsn):
    conn = await asyncpg.connect(postgres_dsn)
    tr = conn.transaction()
    await tr.start()
    await sql.ensure_pogo_sync(conn)
    try:
        yield conn
    finally:
        await tr.rollback()


@pytest.fixture
def context():
    return Context(0)


class CliRunner(typer.testing.CliRunner):
    target = pogo_migrate.cli.app
    result = None

    def invoke(self, *args, **kwargs):
        result = super().invoke(self.target, *args, **kwargs)
        self.result = result
        if result.exception:
            if isinstance(result.exception, SystemExit):
                # The error is already properly handled. Print it and return.
                print(result.output)  # noqa: T201
            else:
                raise result.exception.with_traceback(result.exc_info[2])
        return self.result

    def _clean_output(self, text: str):
        output = text.encode("ascii", errors="ignore").decode()
        output = re.sub(r"\s+\n", "\n", output)
        return textwrap.dedent(output).strip()

    def assert_output(self, expected):
        assert self._clean_output(self.result.output) == self._clean_output(expected)


@pytest.fixture
def cli_runner():
    return CliRunner()
