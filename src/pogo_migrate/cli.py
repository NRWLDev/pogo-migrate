from __future__ import annotations

import asyncio
import contextlib
import functools
import importlib.metadata
import importlib.util
import logging
import shlex
import subprocess
import sys
import typing as t
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent

import click
import dotenv
import rtoml
import tabulate
import typer
from rich.logging import RichHandler

from pogo_migrate import migrate, sql, yoyo
from pogo_migrate.config import Config, load_config
from pogo_migrate.migration import Migration, topological_sort
from pogo_migrate.util import get_editor, make_file

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

logger = logging.getLogger(__name__)

VERBOSITY = {
    0: logging.ERROR,
    1: logging.WARNING,
    2: logging.INFO,
    3: logging.DEBUG,
}


tempfile_prefix = "_tmp_pogonew"


def load_dotenv() -> None:
    dotenv.load_dotenv(
        dotenv.find_dotenv(usecwd=True),
        override=True,
    )


def setup_logging(verbose: int = 0) -> None:
    """Configure the logging."""
    logging.basicConfig(
        level=VERBOSITY.get(verbose, logging.DEBUG),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                rich_tracebacks=True,
                show_level=False,
                show_path=False,
                show_time=False,
                tracebacks_suppress=[click],
            ),
        ],
    )
    asyncio_logger = logging.getLogger("asyncio")
    asyncio_logger.disabled = True
    root_logger = logging.getLogger("")
    root_logger.setLevel(VERBOSITY.get(verbose, logging.DEBUG))


def _version_callback(*, value: bool) -> None:
    """Get current cli version."""
    if value:  # pragma: no cover
        version = importlib.metadata.version("pogo-migrate")
        typer.echo(f"pogo-migrate {version}")
        raise typer.Exit


def _callback(  # pragma: no cover
    _version: t.Optional[bool] = typer.Option(
        None,
        "-v",
        "--version",
        callback=_version_callback,
        help="Print version and exit.",
    ),
) -> None: ...


app = typer.Typer(name="pogo", callback=_callback)


P = ParamSpec("P")
R = t.TypeVar("R")


def handle_exceptions(verbose: int) -> t.Callable[t.Callable[P, t.Awaitable[R]], t.Callable[P, t.Awaitable[R]]]:
    setup_logging(verbose)

    def inner(f: t.Callable[P, t.Awaitable[R]]) -> t.Callable[P, t.Awaitable[R]]:
        """Decorator to handle exceptions from migrations."""

        @functools.wraps(f)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            """Wrapped function.

            Args:
            ----
                *args: The function args.
                **kwargs: The function kwargs.

            Returns:
            -------
                The function result.

            Raises:
            ------
                PostgresConnectionError: If the connection to postgres fails.
                UnexpectedPostgresError: If an unexpected error occurs.

            """
            try:
                return await f(*args, **kwargs)
            except typer.Exit:
                raise
            except Exception as e:  # noqa: BLE001
                log = logger.exception if verbose else logger.error
                log(str(e))
                raise typer.Exit(code=1) from e

        return wrapped

    return inner


@app.command("init")
def init(
    migrations_location: str = typer.Option("./migrations", "-m", "--migrations-location"),
    database_env_key: str = typer.Option("POGO_DATABASE", "-d", "--database-env-key"),
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    """Initiate pogo configuration.

    Create a migrations folder, and inject pogo configuration into
    pyproject.toml.
    """

    @handle_exceptions(verbose)
    async def init_() -> None:
        pyproject = Path("pyproject.toml")
        if not pyproject.exists():
            pyproject.touch()

        with pyproject.open() as f:  # noqa: ASYNC101
            data = rtoml.load(f)

        if "tool" in data and "pogo" in data["tool"]:
            logger.error("pogo already configured.")
            logger.warning("\n".join(["", "[tool.pogo]"] + [f'{k} = "{v}"' for k, v in data["tool"]["pogo"].items()]))
            raise typer.Exit(code=1)

        cwd = Path.cwd().absolute()
        p = Path(migrations_location).resolve().absolute()

        try:
            loc = p.relative_to(cwd)
        except ValueError as e:
            logger.error("migrations_location is not a child of current location.")
            raise typer.Exit(code=1) from e

        data = {
            "tool": {
                "pogo": {
                    "migrations": f"./{loc}",
                    "database_env_key": database_env_key,
                },
            },
        }

        config = rtoml.dumps(data, pretty=True)
        logger.error(config)
        if typer.confirm(f"Write configuration to {pyproject.absolute()}"):
            loc.mkdir(exist_ok=True, parents=True)
            with pyproject.open("a") as f:  # noqa: ASYNC101
                f.write("\n")
                f.write(config)

    asyncio.run(init_())


migration_template = dedent(
    '''\
    """
    {message}
    """

    __depends__ = [{depends}]


    async def apply(db):
        ...


    async def rollback(db):
        ...
    ''',
)

migration_sql_template = dedent(
    """\
    --{message}
    -- depends:{depends}

    -- migrate: apply

    -- migrate: rollback

    """,
)


def retry() -> str:
    choice = ""
    while choice == "":
        choice = typer.prompt("Retry editing? [Ynqh]", default="y", show_default=False)
        if choice == "q":
            raise typer.Exit(code=0)
        if choice in "yn":
            return choice
        if choice == "h":
            logger.error("""\
y: reopen the migration file in your editor
n: save the migration as-is, without re-editing
q: quit without saving the migration
h: show this help
""")
        choice = ""

    # Unreachable
    return ""  # pragma: no cover


def create_with_editor(config: Config, content: str, extension: str, verbose: int) -> Path:
    editor = get_editor(config)
    tmpfile = NamedTemporaryFile(
        mode="w",
        encoding="UTF-8",
        dir=config.migrations,
        prefix=tempfile_prefix,
        suffix=extension,
        delete=False,
    )

    orig_sys_path = sys.path[::]
    try:
        with tmpfile as f:
            f.write(content)
        editor = [part.format(tmpfile.name) for part in shlex.split(editor)]
        if not any(tmpfile.name in part for part in editor):
            editor.append(tmpfile.name)

        mtime = Path(tmpfile.name).lstat().st_mtime
        sys.path.insert(0, config.migrations)
        while True:
            try:
                subprocess.call(editor)  # noqa: S603
            except OSError as e:
                logger.error("Error: could not open editor!")
                raise typer.Exit(code=1) from e
            else:
                if Path(tmpfile.name).lstat().st_mtime == mtime:
                    logger.error("Abort: no changes made")
                    raise typer.Exit(code=1)

            try:
                migration = Migration(None, Path(tmpfile.name), None)
                migration.load()
                message = migration.__doc__
                break
            except Exception:  # noqa: BLE001
                log = logger.exception if verbose else logger.error
                log("Error loading migration.")
                choice = retry()
                if choice == "n":
                    message = ""
                    break

        filename = make_file(config, message, extension)
        Path(tmpfile.name).rename(filename)
        return filename
    finally:
        sys.path = orig_sys_path
        with contextlib.suppress(OSError):
            Path(tmpfile.name).unlink()


@app.command("new")
def new(
    message_: str = typer.Option("", "-m", "--message", help="Message describing focus of the migration."),
    *,
    interactive: bool = typer.Option(True, help="Open migration for editing."),  # noqa: FBT003
    sql_: bool = typer.Option(False, "--sql", help="Generate a sql migration."),  # noqa: FBT003
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    @handle_exceptions(verbose)
    async def new_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        migrations = await sql.read_migrations(config.migrations, db=None)
        migrations = topological_sort([m.load() for m in migrations])

        template = migration_sql_template if sql_ else migration_template
        depends = migrations[-1].id if migrations else ""
        depends = f" {depends}" if sql_ and depends else depends
        depends = f'''"{'", "'.join(depends)}"''' if not sql_ and depends else depends
        message = f" {message_}" if sql_ and message_ else message_
        content = template.format(message=message, depends=depends)
        extension = ".sql" if sql_ else ".py"

        if not interactive:
            fp = make_file(config, message, extension)
            with fp.open("w", encoding="UTF-8") as f:
                f.write(content)
            raise typer.Exit(code=0)

        p = create_with_editor(config, content, extension, verbose)
        logger.error("Created file: %s", p.as_posix().replace(config.root_directory.as_posix(), "").lstrip("/"))

    asyncio.run(new_())


@app.command("history")
def history(
    database: t.Optional[str] = typer.Option(None, "-d", "--database", help="Database connection string."),
    *,
    dotenv: bool = typer.Option(False, help="Load environment from .env."),  # noqa: FBT003
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    @handle_exceptions(verbose)
    async def history_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        migrations = await sql.read_migrations(config.migrations, db)
        migrations = topological_sort([m.load() for m in migrations])

        data = (
            (
                "A" if m.applied else "U",
                m.id,
                "sql" if m.is_sql else "py",
            )
            for m in migrations
        )
        logger.error(tabulate.tabulate(data, headers=("STATUS", "ID", "FORMAT")))

    asyncio.run(history_())


@app.command("apply")
def apply(
    database: t.Optional[str] = typer.Option(None, "-d", "--database", help="Database connection string."),
    *,
    dotenv: bool = typer.Option(False, help="Load environment from .env."),  # noqa: FBT003
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    @handle_exceptions(verbose)
    async def apply_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        await migrate.apply(config, db)

    asyncio.run(apply_())


@app.command("rollback")
def rollback(
    database: t.Optional[str] = typer.Option(None, "-d", "--database", help="Database connection string."),
    count: int = typer.Option(
        1,
        "-c",
        "--count",
        help="Number of migrations to rollback",
    ),
    *,
    dotenv: bool = typer.Option(False, help="Load environment from .env."),  # noqa: FBT003
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    @handle_exceptions(verbose)
    async def rollback_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        await migrate.rollback(config, db, count=count if count > 0 else None)

    asyncio.run(rollback_())


@app.command("mark")
def mark(
    database: t.Optional[str] = typer.Option(None, "-d", "--database", help="Database connection string."),
    *,
    interactive: bool = typer.Option(True, help="Confirm all changes."),  # noqa: FBT003
    dotenv: bool = typer.Option(False, help="Load environment from .env."),  # noqa: FBT003
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    @handle_exceptions(verbose)
    async def _mark() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        migrations = await sql.read_migrations(config.migrations, db)
        migrations = topological_sort([m.load() for m in migrations])

        async with db.transaction():
            for migration in migrations:
                migration.load()
                if not migration.applied:
                    if interactive and not typer.confirm(f"Mark {migration.id} as applied?"):
                        break

                    await sql.migration_applied(db, migration.id, migration.hash)

    asyncio.run(_mark())


@app.command("unmark")
def unmark(
    database: t.Optional[str] = typer.Option(None, "-d", "--database", help="Database connection string."),
    *,
    dotenv: bool = typer.Option(False, help="Load environment from .env."),  # noqa: FBT003
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    @handle_exceptions(verbose)
    async def _unmark() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        migrations = await sql.read_migrations(config.migrations, db)
        migrations = reversed(topological_sort([m.load() for m in migrations]))

        async with db.transaction():
            for migration in migrations:
                migration.load()
                if migration.applied:
                    if not typer.confirm(f"Unmark {migration.id} as applied?"):
                        break

                    await sql.migration_unapplied(db, migration.id)

    asyncio.run(_unmark())


@app.command("migrate-yoyo")
def migrate_yoyo(
    database: t.Optional[str] = typer.Option(None, "-d", "--database", help="Database connection string."),
    *,
    skip_files: bool = typer.Option(False, help="Skip file migration, just copy yoyo history."),  # noqa: FBT003
    dotenv: bool = typer.Option(False, help="Load environment from .env."),  # noqa: FBT003
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    """Migrate existing 'yoyo' migrations to 'pogo'."""

    @handle_exceptions(verbose)
    async def _migrate() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()
        if not skip_files:
            for path in sorted(config.migrations.iterdir()):
                if path.name.endswith(".rollback.sql"):
                    continue

                if path.suffix == ".sql":
                    content = yoyo.convert_sql_migration(path)

                    with path.open("w") as f:
                        f.write(content)
                    logger.error(
                        "Converted '%s' successfully.",
                        path.as_posix().replace(config.root_directory.as_posix(), "").lstrip("/"),
                    )
                else:
                    logger.error(
                        "Python files can not be migrated reliably, please manually update '%s'.",
                        path.as_posix().replace(config.root_directory.as_posix(), "").lstrip("/"),
                    )
        else:
            logger.debug("skip-files set, ignoring existing migration files.")

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        await yoyo.copy_yoyo_migration_history(db)

    asyncio.run(_migrate())
