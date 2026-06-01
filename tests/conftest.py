import collections.abc as cabc
import io
import os
import pathlib
import re
import sys
import textwrap
from typing import Any, BinaryIO, NamedTuple, TextIO

import asyncpg
import pytest
import rtoml
from pogo_core.migration import Migration
from pogo_core.util import sql

import pogo_migrate.cli
from pogo_migrate.context import Context


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
    def factory(configuration=None):
        configuration = configuration or {
            "migrations": "./migrations",
            "database_config": "{POSTGRES_DSN}",
        }
        p = cwd / "pyproject.toml"

        with p.open("w") as f:
            f.write(
                rtoml.dumps(
                    {
                        "tool": {
                            "pogo": configuration,
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
async def db_session(request, postgres_dsn):
    conn = await asyncpg.connect(postgres_dsn)
    tr = conn.transaction()
    await tr.start()
    if request.node.get_closest_marker("nosync") is None:
        await sql.ensure_pogo_sync(conn)
    try:
        yield conn
    finally:
        await tr.rollback()
        await conn.close()


@pytest.fixture(autouse=True)
async def unit_db_session(postgres_dsn):
    # Db session with different default schema
    conn = await asyncpg.connect(postgres_dsn)
    tr = conn.transaction()
    await tr.start()
    try:
        yield conn
    finally:
        await tr.rollback()
        await conn.close()


@pytest.fixture
def context():
    return Context(0)


class Result(NamedTuple):
    exit_code: int
    output: str


class EchoingStdin:
    def __init__(self, stdin: BinaryIO, stdout: TextIO) -> None:
        self._input = stdin
        self._output = stdout

    def __getattr__(self, x: str) -> Any:
        return getattr(self._input, x)

    def _echo(self, rv: bytes) -> bytes:
        self._output.write(rv.decode())

        return rv

    def read(self, n: int = -1) -> bytes:
        return self._echo(self._input.read(n))

    def read1(self, n: int = -1) -> bytes:
        return self._echo(self._input.read1(n))

    def readline(self, n: int = -1) -> bytes:
        return self._echo(self._input.readline(n))

    def readlines(self) -> list[bytes]:
        return [self._echo(x) for x in self._input.readlines()]

    def __iter__(self) -> cabc.Iterator[bytes]:
        return iter(self._echo(x) for x in self._input)

    def __repr__(self) -> str:
        return repr(self._input)


class CliRunner:
    def __init__(self, capsys, monkeypatch):
        self.capsys = capsys
        self.mp = monkeypatch

    def invoke(self, args, stdin=None):
        self.mp.setattr(sys, "argv", ["pogo", *args])
        if stdin:
            stdin = stdin.encode("utf-8")
            stdin = io.BytesIO(stdin)

            bytes_input = EchoingStdin(stdin, sys.stdout)

            text_input = io.TextIOWrapper(
                bytes_input,
                encoding="utf-8",
            )

            # Force unbuffered reads, otherwise TextIOWrapper reads a
            # large chunk which is echoed early.
            text_input._CHUNK_SIZE = 1

            self.mp.setattr(sys, "stdin", text_input)

        with pytest.raises(SystemExit) as e:
            pogo_migrate.cli.main()
        captured = self.capsys.readouterr()
        result = Result(int(str(e.value)), captured.out)
        self.result = result
        return result

    def _clean_output(self, text: str):
        output = text.encode("ascii", errors="ignore").decode()
        output = re.sub(r"\s+\n", "\n", output)
        return textwrap.dedent(output).strip()

    def assert_output(self, expected):
        assert self._clean_output(self.result.output) == self._clean_output(expected)


@pytest.fixture
def cli_runner(capsys, monkeypatch):
    return CliRunner(capsys, monkeypatch)
