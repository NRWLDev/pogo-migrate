from __future__ import annotations

import asyncio
import contextlib
import importlib.metadata
import importlib.util
import hashlib
import logging
import os
import shlex
import subprocess
import sys
import typing as t
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent

import asyncpg
import click
import rtoml
import tabulate
import typer
from rich.logging import RichHandler

from pogo_migrate.config import Config, load_config
from pogo_migrate.migration import Migration
from pogo_migrate.util import get_editor, make_file

logger = logging.getLogger(__name__)

VERBOSITY = {
    0: logging.ERROR,
    1: logging.WARNING,
    2: logging.INFO,
    3: logging.DEBUG,
}

tempfile_prefix = "_tmp_pogonew"


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
    root_logger = logging.getLogger("")
    root_logger.setLevel(VERBOSITY.get(verbose, logging.DEBUG))


def _version_callback(*, value: bool) -> None:
    """Get current cli version."""
    if value:
        version = importlib.metadata.version("pogo-migrate")
        typer.echo(f"pogo-migrate {version}")
        raise typer.Exit


def _callback(
    _version: t.Optional[bool] = typer.Option(
        None,
        "-v",
        "--version",
        callback=_version_callback,
        help="Print version and exit.",
    ),
) -> None: ...


app = typer.Typer(name="pogo", callback=_callback)


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
    setup_logging(verbose)
    pyproject = Path("pyproject.toml")
    if not pyproject.exists():
        pyproject.touch()

    with pyproject.open() as f:
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
        with pyproject.open("a") as f:
            f.write("\n")
            f.write(config)


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
    -- {message}
    -- depends: {depends}

    -- migrate: apply

    -- migrate: rollback

    """,
)


def retry() -> str:
    choice = ""
    while choice == "":
        choice = typer.prompt("Retry editing? [Ynqh]", default="y")
        if choice == "q":
            raise typer.Exit(code=0)
        if choice in "yn":
            return choice
        if choice == "h":
            logger.error("""
y: reopen the migration file in your editor
n: save the migration as-is, without re-editing
q: quit without saving the migration
h: show this help""")
        choice = ""

    # Shouldn't be reachable
    return ""


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
            except OSError:
                logger.error("Error: could not open editor!")
            else:
                if Path(tmpfile.name).lstat().st_mtime == mtime:
                    logger.error("Abort: no changes made")
                    raise typer.Exit(code=1)

            try:
                migration = Migration(None, Path(tmpfile.name))
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
    message: str = typer.Option("", "-m", "--message", help="Message describing focus of the migration."),
    *,
    interactive: bool = typer.Option(True, help="Open migration for editing."),  # noqa: FBT003
    sql: bool = typer.Option(False, "--sql", help="Generate a sql migration."),  # noqa: FBT003
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    setup_logging(verbose)
    config = load_config()
    template = migration_sql_template if sql else migration_template
    content = template.format(message=message, depends="")
    extension = ".sql" if sql else ".py"

    if not interactive:
        fp = make_file(config, message, extension)
        with fp.open("w", encoding="UTF-8") as f:
            f.write(content)
        raise typer.Exit(code=0)

    p = create_with_editor(config, content, extension, verbose)
    logger.error("Created file: %s", p)


def read_migrations(config: Config) -> list[Migration]:
    return [
        Migration(path.stem, path)
        for path in config.migrations.iterdir()
    ]

@app.command("history")
def history(
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    async def history_() -> None:
        setup_logging(verbose)
        config = load_config()
        migrations = read_migrations(config)

        applied, unapplied = "A", "U"
        applied_migrations = []
        db = await asyncpg.connect(os.environ[config.database_env_key])

        stmt = """
        SELECT
            migration_id
        FROM _pogo_migration
        """
        results = await db.fetch(stmt)
        applied_migrations = {r["migration_id"] for r in results}
        await asyncpg.connect(os.environ[config.database_env_key])
        data = (
            (
                applied if m.id in applied_migrations else unapplied,
                m.id,
                "sql" if m.is_sql else "py",
            )
            for m in migrations
        )
        logger.error(tabulate.tabulate(data, headers=("STATUS", "ID", "FORMAT")))

    asyncio.run(history_())

async def ensure_pogo_sync(db: asyncpg.Connection) -> None:
    stmt = """
    SELECT exists (
        SELECT FROM pg_tables
        WHERE  schemaname = 'public'
        AND    tablename  = '_pogo_version'
    );
    """
    r = await db.fetchrow(stmt)
    if not r["exists"]:
        stmt = """
        CREATE TABLE _pogo_migration (
            migration_hash VARCHAR(64),  -- sha256 hash of the migration id
            migration_id VARCHAR(255),   -- The migration id (ie path basename without extension)
            applied TIMESTAMPTZ,         -- When this id was applied
            PRIMARY KEY (migration_hash)
        )
        """
        await db.execute(stmt)

        stmt = """
        CREATE TABLE _pogo_version (
            version INT NOT NULL PRIMARY KEY,
            installed TIMESTAMPTZ
        )
        """
        await db.execute(stmt)

        stmt = """
        INSERT INTO _pogo_version (version, installed) VALUES (0, now())
        """
        await db.execute(stmt)


@app.command("apply")
def apply(
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    async def apply_() -> None:
        setup_logging(verbose)
        config = load_config()
        migrations = read_migrations(config)

        db = await asyncpg.connect(os.environ[config.database_env_key])
        # TODO(edgy): check in db for applied migrations
        tr = db.transaction()
        await tr.start()
        await ensure_pogo_sync(db)
        try:
            for migration in migrations:
                migration.load()
                logger.error("Applying %s", migration.id)
                await migration.apply(db)
                stmt = """
                INSERT INTO _pogo_migration (
                    migration_hash,
                    migration_id,
                    applied
                ) VALUES (
                    $1, $2, now()
                )
                """
                await db.execute(stmt, hashlib.sha256(migration.id.encode("utf-8")).hexdigest(), migration.id)
        except Exception as e:  # noqa: BLE001
            log = logger.exception if verbose > 1 else logger.error
            log("Error applying migration %s", migration.id)
            if verbose < 2:  # noqa: PLR2004
                logger.warning(str(e))
            await tr.rollback()
        else:
            await tr.commit()

    asyncio.run(apply_())


@app.command("rollback")
def rollback(
    verbose: int = typer.Option(
        0,
        "-v",
        "--verbose",
        help="Verbose output. Use multiple times to increase level of verbosity.",
        count=True,
        max=3,
    ),
) -> None:
    async def rollback_() -> None:
        setup_logging(verbose)
        config = load_config()
        migrations = reversed(read_migrations(config))

        db = await asyncpg.connect(os.environ[config.database_env_key])

        stmt = """
        SELECT
            migration_id
        FROM _pogo_migration
        """
        results = await db.fetch(stmt)
        applied_migrations = {r["migration_id"] for r in results}
        tr = db.transaction()
        await tr.start()
        try:
            await ensure_pogo_sync(db)
            for migration in migrations:
                if migration.id in applied_migrations:
                    migration.load()
                    logger.error("Rolling back %s", migration.id)
                    await migration.rollback(db)
                    stmt = """
                    DELETE FROM _pogo_migration
                    WHERE migration_id = $1
                    """
                    await db.execute(stmt, migration.id)
        except Exception as e:  # noqa: BLE001
            log = logger.exception if verbose > 1 else logger.error
            log("Error applying migration %s", migration.id)
            if verbose < 2:  # noqa: PLR2004
                logger.warning(str(e))
            await tr.rollback()
        else:
            await tr.commit()

    asyncio.run(rollback_())
