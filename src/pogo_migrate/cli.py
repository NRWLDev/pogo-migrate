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
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent

import click
import dotenv
import rtoml
import tabulate
import typer
from rich.logging import RichHandler

from pogo_migrate import exceptions, migrate, sql, squash, yoyo
from pogo_migrate.config import Config, load_config
from pogo_migrate.migration import Migration, read_sql_migration, topological_sort
from pogo_migrate.util import get_editor, make_file

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:  # pragma: no cover
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
                log = logger.exception if verbose > 1 else logger.error
                log(str(e))
                raise typer.Exit(code=1) from e

        return wrapped

    return inner


@app.command("init")
def init(
    migrations_location: str = typer.Option("./migrations", "-m", "--migrations-location"),
    database_config: str = typer.Option("{POGO_DATABASE}", "-d", "--database-env-key"),
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
                    "database_config": database_config,
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
    """Generate a new migration."""

    @handle_exceptions(verbose)
    async def new_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        migrations = await sql.read_migrations(config.migrations, db=None)
        migrations = topological_sort([m.load() for m in migrations])

        template = migration_sql_template if sql_ else migration_template
        depends = migrations[-1].id if migrations else ""
        depends = f" {depends}" if sql_ and depends else ([depends] if depends else depends)
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
    unapplied: bool = typer.Option(False, help="Show only unapplied migrations."),  # noqa: FBT003
    simple: bool = typer.Option(False, help="Show raw data without tabulation."),  # noqa: FBT003
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
    """List migration history.

    If database location is configured, also check applied status of each migration.
    """

    @handle_exceptions(verbose)
    async def history_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        try:
            connection_string = database or config.database_dsn
        except exceptions.InvalidConfigurationError:
            connection_string = database
        db = await sql.get_connection(connection_string) if connection_string else None

        if db is None:
            logger.warning("Database connection can not be established, migration status can not be determined.")

        migrations = await sql.read_migrations(config.migrations, db)
        migrations = topological_sort([m.load() for m in migrations])

        data = (
            (
                "A" if m.applied else "U",
                m.id,
                "sql" if m.is_sql else "py",
            )
            for m in migrations
            if not unapplied or (unapplied and not m.applied)
        )
        if not simple:
            logger.error(tabulate.tabulate(data, headers=("STATUS", "ID", "FORMAT")))
        else:
            for d in data:
                logger.error(" ".join(d))

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
    """Apply migrations."""

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
    """Rollback one or more migrations."""

    @handle_exceptions(verbose)
    async def rollback_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        await migrate.rollback(config, db, count=count if count > 0 else None)

    asyncio.run(rollback_())


@app.command("remove")
def remove(
    migration_id: str = typer.Argument(show_default=False, help="Migration id to remove (message can be excluded)."),
    migrations_location: str = typer.Option("./migrations", "-m", "--migrations-location"),
    *,
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    """Remove a migration from the dependency chain."""
    setup_logging(verbose)
    migrations = [
        Migration(path.stem, path, []) for path in Path(migrations_location).iterdir() if path.suffix in {".py", ".sql"}
    ]
    migrations = topological_sort([m.load() for m in migrations])
    for i, migration in enumerate(migrations):
        if migration.id.startswith(migration_id):
            next_migration = None
            with contextlib.suppress(IndexError):
                next_migration = migrations[i + 1]

            squash.remove(migration, next_migration)


@app.command("squash")
def squash_(  # noqa: C901, PLR0912, PLR0915, PLR0913
    migrations_location: str = typer.Option("./migrations", "-m", "--migrations-location"),
    *,
    backup: bool = typer.Option(False, "--backup/ ", help="Keep .bak copy of original files."),  # noqa: FBT003
    source: bool = typer.Option(False, "--source/ ", help="Add comment for source migration to each statement."),  # noqa: FBT003
    prompt_update: bool = typer.Option(False, "--update-prompt/ ", help="Confirm before including UPDATE statements."),  # noqa: FBT003
    prompt_skip: bool = typer.Option(
        False,  # noqa: FBT003
        "--skip-prompt/ ",
        help="Confirm before skipping unsquashable files, allow removal instead.",
    ),
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    """Squash migrations. [EXPERIMENTAL]

    Python migrations and non transaction based transactions are skipped by default.

    Statements in sql migrations are grouped by table, and applied in the order
    tables where discovered.

    Rollback statements follow the reverse logic, the last table discovered is
    grouped first.
    """
    setup_logging(verbose)
    migrations = [
        Migration(path.stem, path, []) for path in Path(migrations_location).iterdir() if path.suffix in {".py", ".sql"}
    ]
    migrations = topological_sort([m.load() for m in migrations])

    applies = defaultdict(list)
    rollbacks = defaultdict(list)
    replaced = {}
    squashed = []
    depends = None
    latest = None
    for idx, migration in enumerate(migrations):
        if not migration.is_sql or not migration.use_transaction:
            if prompt_skip:
                view = typer.confirm(f"View unsquashable migration {migration.id}", default=True)
                if view:
                    content = migration.path.read_text()
                    logger.error(content)

                remove = typer.confirm(f"Remove unsquashable migration {migration.id}", default=False)
                if remove:
                    next_migration = None
                    with contextlib.suppress(IndexError):
                        next_migration = migrations[idx + 1]

                    squash.remove(migration, next_migration)

                    latest = migration
                    continue

            if applies or rollbacks:
                new = squash.write(applies, rollbacks, latest, depends, squashed)
                replaced[latest.id]["new"] = new
                if new is None:
                    # New returns None if no update would actually occur (single file squash)
                    # Unmark the migration as replaced
                    del replaced[latest.id]
                applies = defaultdict(list)
                rollbacks = defaultdict(list)
                squashed = []
            depends = migration.id
            continue

        logger.warning("Squashing %s", migration.id)
        squashed.append(migration.id)
        latest = migration
        replaced[migration.id] = {"orig": migration}

        _, _, _, _, _, apply_statements, rollback_statements = read_sql_migration(migration.path)
        for i, apply in enumerate(apply_statements):
            parsed = squash.parse(apply)
            if source:
                parsed.statement = f"{parsed.statement} -- source: {migration.id}"

            if parsed.statement_type in ("CREATE", "ALTER", "DROP"):
                if parsed.identifier:
                    applies[parsed.identifier].append(parsed.statement)
                else:
                    logger.error("Can not extract table from DDL statement in migration %s", migration.id)
                    logger.warning(apply)
                    raise typer.Exit(code=1)

            else:
                keep = True
                if parsed.statement_type == "UPDATE" and prompt_update:
                    logger.error("")
                    with contextlib.suppress(IndexError):
                        logger.error("   %s", apply_statements[i - 1])
                    logger.error(">> %s", apply)
                    with contextlib.suppress(IndexError):
                        logger.error("   %s", apply_statements[i + 1])
                    logger.error("")
                    keep = typer.confirm("Include update statement", default=True)

                if keep:
                    applies["__data"].append(parsed.statement)

        rollbacks_ = defaultdict(list)
        for rollback in reversed(rollback_statements):
            parsed = squash.parse(rollback)
            if source:
                parsed.statement = f"{parsed.statement} -- source: {migration.id}"

            if parsed.statement_type in ("CREATE", "ALTER", "DROP"):
                if parsed.identifier:
                    rollbacks_[parsed.identifier].append(parsed.statement)
                else:
                    logger.error("Can not extract table from DDL statement in migration %s", migration.id)
                    logger.warning(rollback)
                    raise typer.Exit(code=1)
            else:
                logger.debug(parsed.statement_type)
                rollbacks_["__data"].append(parsed.statement)

        for ident, statements in rollbacks_.items():
            rollbacks[ident].append(statements)

    if applies or rollbacks:
        new = squash.write(applies, rollbacks, latest, depends, squashed)
        replaced[latest.id]["new"] = new
        if new is None:
            # New returns None if no update would actually occur (single file squash)
            # Unmark the migration as replaced
            del replaced[latest.id]

    for data in replaced.values():
        orig, new = data["orig"], data.get("new")
        if backup:
            orig.path.rename(f"{orig.path}.bak")
        else:
            orig.path.unlink()
        if new:
            new.path.rename(orig.path)


@app.command("clean")
def clean(
    migrations_location: str = typer.Option("./migrations", "-m", "--migrations-location"),
    *,
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    """Clean the migration directory of .bak migrations from squash."""
    setup_logging(verbose)
    for path in Path(migrations_location).iterdir():
        if path.suffix in {".bak"}:
            path.unlink()


"""
Issue with gen_random_uuid() parsing in sqlglot.
https://github.com/tobymao/sqlglot/issues/3774

import sqlglot
from sqlglot import expressions as exp

@app.command("squash-glot")
def squash(
    migrations_location: str = typer.Option("./migrations", "-m", "--migrations-location"),
    # database: str = typer.Option(None, "-d", "--database", help="Database connection string."),
    *,
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    # @handle_exceptions(verbose)
    # async def sqaush_() -> None:
    #     if dotenv:  # pragma: no cover
    #         load_dotenv()
    #     config = load_config()
    #
    #     connection_string = database or config.database_dsn
    #     db = await sql.get_connection(connection_string)
    #
    #     await migrate.apply(config, db)

    setup_logging(verbose)
    migrations = [
        Migration(path.stem, path, []) for path in Path(migrations_location).iterdir() if path.suffix in {".py", ".sql"}
    ]
    migrations = topological_sort([m.load() for m in migrations])
    statements = defaultdict(list)
    for migration in migrations:
        logger.error(migration.is_sql)
        if not migration.is_sql:
            print("break")
            return
        logger.error(migration)
        _, _, _, _, _, apply_statements, rollback_statements = read_sql_migration(migration.path)
        for apply in apply_statements:
            print(apply)
            parsed = sqlglot.parse_one(apply, read="postgres", dialect="postgres")
            # print(parsed)
            # print(parsed.tokens)
            print(parsed)
            if isinstance(parsed, (exp.Create, exp.AlterTable, exp.Drop)):
                for table in parsed.find_all(exp.Table):
                    statements["__data"].append(apply)
                    break
                else:
                    print("no identifier")
            else:
                statements["__data"].append(apply)
    with Path("tmp").open("w") as f:
        for ident, statements_ in statements.items():
            if ident == "__data":
                continue
            print(ident, statements_)
            for statement in statements_:
                f.write(f"{statement}\n\n")
        for statement in statements["__data"]:
            f.write(f"{statement}\n\n")
        # print(rollback_statements)
    # asyncio.run(squash())
"""


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
    """Mark a migration as applied, without running."""

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
    """Mark a migration as unapplied, without rolling back."""

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
