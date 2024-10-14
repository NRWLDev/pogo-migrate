from __future__ import annotations

import asyncio
import contextlib
import functools
import importlib.metadata
import importlib.util
import shlex
import subprocess
import sys
import typing as t
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent

import dotenv
import rtoml
import tabulate
import typer

from pogo_migrate import exceptions, migrate, sql, squash, yoyo
from pogo_migrate.config import Config, load_config
from pogo_migrate.context import Context
from pogo_migrate.migration import Migration, read_sql_migration, topological_sort
from pogo_migrate.util import get_editor, make_file

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:  # pragma: no cover
    from typing import ParamSpec

tempfile_prefix = "_tmp_pogonew"


def load_dotenv() -> None:
    dotenv.load_dotenv(
        dotenv.find_dotenv(usecwd=True),
        override=True,
    )


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


app: typer.Typer = typer.Typer(name="pogo", callback=_callback)


P = ParamSpec("P")
R = t.TypeVar("R")


def handle_exceptions(context: Context) -> t.Callable[t.Callable[P, t.Awaitable[R]], t.Callable[P, t.Awaitable[R]]]:
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
            except Exception as e:
                context.stacktrace()
                context.error(str(e))
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
    context = Context(verbose)

    @handle_exceptions(context)  # type: ignore[reportCallIssue]
    async def init_() -> None:
        pyproject = Path("pyproject.toml")
        if not pyproject.exists():
            pyproject.touch()

        with pyproject.open() as f:  # noqa: ASYNC230
            data = rtoml.load(f)

        if "tool" in data and "pogo" in data["tool"]:
            context.error("pogo already configured.")
            context.warning("\n".join(["", "[tool.pogo]"] + [f'{k} = "{v}"' for k, v in data["tool"]["pogo"].items()]))
            raise typer.Exit(code=1)

        cwd = Path.cwd().absolute()
        p = Path(migrations_location).resolve().absolute()

        try:
            loc = p.relative_to(cwd)
        except ValueError as e:
            context.error("migrations_location is not a child of current location.")
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
        context.error(config)
        if typer.confirm(f"Write configuration to {pyproject.absolute()}"):
            loc.mkdir(exist_ok=True, parents=True)
            with pyproject.open("a") as f:  # noqa: ASYNC230
                f.write("\n")
                f.write(config)

    asyncio.run(init_())


migration_template: str = dedent(
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

migration_sql_template: str = dedent(
    """\
    --{message}
    -- depends:{depends}

    -- migrate: apply

    -- migrate: rollback

    """,
)


def retry(context: Context) -> str:
    choice = ""
    while choice == "":
        choice = typer.prompt("Retry editing? [Ynqh]", default="y", show_default=False)
        if choice == "q":
            raise typer.Exit(code=0)
        if choice in "yn":
            return choice
        if choice == "h":
            context.error("""\
y: reopen the migration file in your editor
n: save the migration as-is, without re-editing
q: quit without saving the migration
h: show this help
""")
        choice = ""

    # Unreachable
    return ""  # pragma: no cover


def create_with_editor(config: Config, content: str, extension: str, context: Context) -> Path:
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
        sys.path.insert(0, str(config.migrations))
        while True:
            try:
                subprocess.call(editor)  # noqa: S603
            except OSError as e:
                context.error("Error: could not open editor!")
                raise typer.Exit(code=1) from e
            else:
                if Path(tmpfile.name).lstat().st_mtime == mtime:
                    context.error("Abort: no changes made")
                    raise typer.Exit(code=1)

            try:
                migration = Migration("temporary", Path(tmpfile.name), None)
                migration.load()
                message = migration.__doc__
                break
            except Exception:  # noqa: BLE001
                context.stacktrace()
                context.error("Error loading migration.")
                choice = retry(context)
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
    py_: bool = typer.Option(False, "--py", help="Generate a python migration."),  # noqa: FBT003
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
    context = Context(verbose)

    @handle_exceptions(context)  # type: ignore[reportCallIssue]
    async def new_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        migrations = await sql.read_migrations(config.migrations, db=None)
        migrations = topological_sort([m.load() for m in migrations])

        template = migration_sql_template if not py_ else migration_template
        depends = migrations[-1].id if migrations else ""
        depends = f" {depends}" if not py_ and depends else ([depends] if depends else depends)
        depends = f'''"{'", "'.join(depends)}"''' if py_ and depends else depends
        message = f" {message_}" if not py_ and message_ else message_
        content = template.format(message=message, depends=depends)
        extension = ".sql" if not py_ else ".py"

        if not interactive:
            fp = make_file(config, message, extension)
            with fp.open("w", encoding="UTF-8") as f:
                f.write(content)
            raise typer.Exit(code=0)

        p = create_with_editor(config, content, extension, context)
        context.error("Created file: %s", p.as_posix().replace(config.root_directory.as_posix(), "").lstrip("/"))

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

    context = Context(verbose)

    @handle_exceptions(context)  # type: ignore[reportCallIssue]
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
            context.warning("Database connection can not be established, migration status can not be determined.")

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
            context.error(tabulate.tabulate(data, headers=("STATUS", "ID", "FORMAT")))
        else:
            for d in data:
                context.error(" ".join(d))

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
    context = Context(verbose)

    @handle_exceptions(context)  # type: ignore[reportCallIssue]
    async def apply_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        await migrate.apply(context, config, db)

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
    context = Context(verbose)

    @handle_exceptions(context)  # type: ignore[reportCallIssue]
    async def rollback_() -> None:
        if dotenv:  # pragma: no cover
            load_dotenv()
        config = load_config()

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        await migrate.rollback(context, config, db, count=count if count > 0 else None)

    asyncio.run(rollback_())


@app.command("remove")
def remove(
    migration_id: str = typer.Argument(show_default=False, help="Migration id to remove (message can be excluded)."),
    migrations_location: str = typer.Option("./migrations", "-m", "--migrations-location"),
    *,
    backup: bool = typer.Option(False, "--backup/ ", help="Keep .bak copy of original files."),  # noqa: FBT003
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
    context = Context(verbose)

    migrations = [
        Migration(path.stem, path, set())
        for path in Path(migrations_location).iterdir()
        if path.suffix in {".py", ".sql"}
    ]
    migrations = topological_sort([m.load() for m in migrations])
    for i, migration in enumerate(migrations):
        if migration.id.startswith(migration_id):
            next_migration = None
            with contextlib.suppress(IndexError):
                next_migration = migrations[i + 1]

            squash.remove(context, migration, next_migration, backup=backup)


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
    """Squash migrations [EXPERIMENTAL].

    Python migrations and non transaction based transactions are skipped by default.

    Statements in sql migrations are grouped by table, and applied in the order
    tables where discovered.

    Rollback statements follow the reverse logic, the last table discovered is
    grouped first.
    """
    context = Context(verbose)

    migrations = [
        Migration(path.stem, path, set())
        for path in Path(migrations_location).iterdir()
        if path.suffix in {".py", ".sql"}
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
                    context.error(content)

                remove = typer.confirm(f"Remove unsquashable migration {migration.id}", default=False)
                if remove:
                    next_migration = None
                    with contextlib.suppress(IndexError):
                        next_migration = migrations[idx + 1]

                    squash.remove(context, migration, next_migration, backup=backup)

                    latest = migration
                    continue

            if (applies or rollbacks) and latest:
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

        context.warning("Squashing %s", migration.id)
        squashed.append(migration.id)
        latest = migration
        replaced[migration.id] = {"orig": migration}

        _, _, _, _, _, apply_statements, rollback_statements = read_sql_migration(migration.path)
        for i, apply in enumerate(apply_statements):
            try:
                parsed = squash.parse_sqlglot(context, apply)
            except squash.ParseError as e:
                context.stacktrace()
                context.error("%s: %s", migration.id, str(e))
                context.warning(apply)
                raise typer.Exit(code=1) from e

            if source:
                parsed.statement = f"{parsed.statement} -- source: {migration.id}"

            if parsed.statement_type in ("CREATE", "ALTER", "DROP"):
                if parsed.identifier:
                    applies[parsed.identifier].append(parsed.statement)
                else:
                    context.error("Can not extract table from DDL statement in migration %s", migration.id)
                    context.warning(apply)
                    raise typer.Exit(code=1)

            else:
                keep = True
                if parsed.statement_type == "UPDATE" and prompt_update:
                    context.error("")
                    with contextlib.suppress(IndexError):
                        context.error("   %s", apply_statements[i - 1])
                    context.error(">> %s", apply)
                    with contextlib.suppress(IndexError):
                        context.error("   %s", apply_statements[i + 1])
                    context.error("")
                    keep = typer.confirm("Include update statement", default=True)

                if keep:
                    applies["__data"].append(parsed.statement)

        rollbacks_ = defaultdict(list)
        for rollback in reversed(rollback_statements):
            try:
                parsed = squash.parse_sqlglot(context, rollback)
            except squash.ParseError as e:
                context.stacktrace()
                context.error("%s: %s", migration.id, str(e))
                context.warning(rollback)
                raise typer.Exit(code=1) from e
            if source:
                parsed.statement = f"{parsed.statement} -- source: {migration.id}"

            if parsed.statement_type in ("CREATE", "ALTER", "DROP"):
                if parsed.identifier:
                    rollbacks_[parsed.identifier].append(parsed.statement)
                else:
                    context.error("Can not extract table from DDL statement in migration %s", migration.id)
                    context.warning(rollback)
                    raise typer.Exit(code=1)
            else:
                context.debug(parsed.statement_type)
                rollbacks_["__data"].append(parsed.statement)

        for ident, statements in rollbacks_.items():
            rollbacks[ident].append(statements)

    if (applies or rollbacks) and latest:
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
    _context = Context(verbose)

    for path in Path(migrations_location).iterdir():
        if path.suffix in {".bak"}:
            path.unlink()


@app.command("validate")
def validate(
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
    """Validate migrations [EXPERIMENTAL].

    Best effort pass through to make sure identifiers aren't keywords.
    """
    context = Context(verbose)

    migrations = [
        Migration(path.stem, path, set())
        for path in Path(migrations_location).iterdir()
        if path.suffix in {".py", ".sql"}
    ]
    migrations = topological_sort([m.load() for m in migrations])

    for migration in migrations:
        if not migration.is_sql:
            from unittest import mock

            mock_asyncpg = mock.Mock()
            mock_asyncpg.execute = mock.AsyncMock(return_value=None)
            mock_asyncpg.fetch = mock.AsyncMock(return_value=[mock.MagicMock()])
            mock_asyncpg.fetchrow = mock.AsyncMock(return_value=mock.MagicMock())
            mock_asyncpg.fetchval = mock.AsyncMock(return_value=mock.MagicMock())
            try:
                asyncio.run(migration.apply(mock_asyncpg))
            except Exception:  # noqa: BLE001
                context.stacktrace()
                context.warning("Can't validate python migration %s (apply), skipping...", migration.id)

            try:
                asyncio.run(migration.rollback(mock_asyncpg))
            except Exception:  # noqa: BLE001
                context.stacktrace()
                context.warning("Can't validate python migration %s (rollback), skipping...", migration.id)

            statements = [c[1].get("query") or c[0][0] for c in mock_asyncpg.execute.call_args_list]
            statements += [c[1].get("query") or c[0][0] for c in mock_asyncpg.fetch.call_args_list]
            statements += [c[1].get("query") or c[0][0] for c in mock_asyncpg.fetchrow.call_args_list]
            statements += [c[1].get("query") or c[0][0] for c in mock_asyncpg.fetchval.call_args_list]
        else:
            _, _, _, _, _, apply_statements, rollback_statements = read_sql_migration(migration.path)
            statements = apply_statements + rollback_statements

        for statement in statements:
            try:
                parsed = squash.parse_sqlglot(context, statement)
            except squash.ParseError as e:
                context.stacktrace()
                context.error("%s: %s", migration.id, str(e))
                context.warning(statement)
                continue

            if parsed.statement_type in ("CREATE", "ALTER", "DROP") and parsed.identifier is None:
                context.error(
                    "Can not extract table from DDL statement in migration %s, check that table name is not a reserved word.",
                    migration.id,
                )
                context.warning(statement)


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
    context = Context(verbose)

    @handle_exceptions(context)  # type: ignore[reportCallIssue]
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
    context = Context(verbose)

    @handle_exceptions(context)  # type: ignore[reportCallIssue]
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
    context = Context(verbose)

    @handle_exceptions(context)  # type: ignore[reportCallIssue]
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
                    context.error(
                        "Converted '%s' successfully.",
                        path.as_posix().replace(config.root_directory.as_posix(), "").lstrip("/"),
                    )
                else:
                    context.error(
                        "Python files can not be migrated reliably, please manually update '%s'.",
                        path.as_posix().replace(config.root_directory.as_posix(), "").lstrip("/"),
                    )
        else:
            context.debug("skip-files set, ignoring existing migration files.")

        connection_string = database or config.database_dsn
        db = await sql.get_connection(connection_string)

        await yoyo.copy_yoyo_migration_history(context, db)

    asyncio.run(_migrate())
