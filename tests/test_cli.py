import importlib.metadata
from textwrap import dedent
from unittest import mock

import pytest

from pogo_migrate import cli, sql
from tests.util import AsyncMock


def test_version(cli_runner):
    version = importlib.metadata.version("pogo-migrate")
    result = cli_runner.invoke(["--version"])
    assert result.exit_code == 0

    assert result.output.strip() == f"pogo-migrate {version}"


@pytest.fixture()
def pyproject(pyproject_factory, migrations):  # noqa: ARG001
    return pyproject_factory()


@pytest.fixture(autouse=True)
def _db_patch(db_session, monkeypatch):
    monkeypatch.setattr(cli.sql.asyncpg, "connect", AsyncMock(return_value=db_session))


class TestInit:
    def test_init_no_confirm(self, cwd, cli_runner):
        result = cli_runner.invoke(["init"], input="n\n")
        assert result.exit_code == 0, result.output

        p = cwd / "pyproject.toml"
        with p.open() as f:
            assert f.read() == ""

    def test_init_no_pyproject(self, cwd, cli_runner):
        result = cli_runner.invoke(["init"], input="y\n")
        assert result.exit_code == 0, result.output

        p = cwd / "pyproject.toml"
        with p.open() as f:
            assert f.read() == dedent("""\

            [tool.pogo]
            migrations = './migrations'
            database_env_key = 'POGO_DATABASE'
            """)

    def test_init_invalid_migrations_location(self, cwd, cli_runner):
        result = cli_runner.invoke(["init", "-m", str(cwd.parent / "migrations")])
        assert result.exit_code == 1, result.output
        cli_runner.assert_output(
            dedent("migrations_location is not a child of current location."),
        )

    def test_init_existing_pyproject(self, cwd, cli_runner):
        p = cwd / "pyproject.toml"
        with p.open("w") as f:
            f.write(
                dedent("""\
            [tool.other]
            key = "value"
            """),
            )
        result = cli_runner.invoke(["init"], input="y\n")
        assert result.exit_code == 0, result.output

        with p.open() as f:
            assert f.read() == dedent("""\
            [tool.other]
            key = "value"

            [tool.pogo]
            migrations = './migrations'
            database_env_key = 'POGO_DATABASE'
            """)

    def test_init_overrides(self, cwd, cli_runner):
        result = cli_runner.invoke(["init", "-m", "./my-migrations", "-d", "POSTGRES_DSN"], input="y\n")
        assert result.exit_code == 0, result.output

        p = cwd / "pyproject.toml"
        with p.open() as f:
            assert f.read() == dedent("""\

            [tool.pogo]
            migrations = './my-migrations'
            database_env_key = 'POSTGRES_DSN'
            """)

    def test_init_already_configured(self, cwd, cli_runner):
        p = cwd / "pyproject.toml"
        with p.open("w") as f:
            assert f.write(
                dedent("""\

            [tool.pogo]
            migrations = './my-migrations'
            database_env_key = 'POSTGRES_DSN'
            """),
            )

        result = cli_runner.invoke(["init"])
        assert result.exit_code == 1
        cli_runner.assert_output(
            dedent("pogo already configured."),
        )

        p = cwd / "pyproject.toml"
        with p.open() as f:
            assert f.read() == dedent("""\

            [tool.pogo]
            migrations = './my-migrations'
            database_env_key = 'POSTGRES_DSN'
            """)

    def test_init_already_configured_verbose(self, cwd, cli_runner):
        p = cwd / "pyproject.toml"
        with p.open("w") as f:
            assert f.write(
                dedent("""\

            [tool.pogo]
            migrations = './my-migrations'
            database_env_key = 'POSTGRES_DSN'
            """),
            )

        result = cli_runner.invoke(["init", "-v"])
        assert result.exit_code == 1
        cli_runner.assert_output(
            dedent("""\
            pogo already configured.

            [tool.pogo]
            migrations = "./my-migrations"
            database_env_key = "POSTGRES_DSN"
            """),
        )


class TestNew:
    def test_no_config(self, cli_runner):
        result = cli_runner.invoke(["new"])
        assert result.exit_code == 1, result.output
        cli_runner.assert_output(
            "No configuration found, missing pyproject.toml, run 'pogo init ...'",
        )

    @pytest.mark.usefixtures("pyproject")
    def test_non_interactive_file_written(self, monkeypatch, cli_runner, cwd):
        monkeypatch.setattr(cli, "make_file", mock.Mock(return_value=cwd / "new_file.py"))

        result = cli_runner.invoke(["new", "--sql", "--no-interactive"])

        assert result.exit_code == 0, result.output
        with (cwd / "new_file.py").open() as f:
            assert f.read() == dedent("""\
            --
            -- depends:

            -- migrate: apply

            -- migrate: rollback

            """)

    @pytest.mark.usefixtures("pyproject")
    def test_os_error(self, monkeypatch, cli_runner):
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock(side_effect=OSError))
        result = cli_runner.invoke(["new"])
        assert result.exit_code == 1, result.output
        cli_runner.assert_output(
            "Error: could not open editor!",
        )

    @pytest.mark.usefixtures("pyproject")
    def test_no_changes(self, monkeypatch, cli_runner):
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        result = cli_runner.invoke(["new"])
        assert result.exit_code == 1, result.output
        cli_runner.assert_output(
            "Abort: no changes made",
        )

    @pytest.mark.usefixtures("pyproject")
    def test_changes_made(self, monkeypatch, cli_runner, migrations):
        monkeypatch.setattr(cli, "make_file", mock.Mock(return_value=migrations / "new_file.sql"))
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli.Path, "lstat", mock.Mock(side_effect=[mock.Mock(), mock.Mock()]))

        result = cli_runner.invoke(["new", "--sql", "-v"])

        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Created file: migrations/new_file.sql
            """).strip(),
        )

    @pytest.mark.usefixtures("pyproject")
    def test_file_written(self, monkeypatch, cli_runner, cwd):
        monkeypatch.setattr(cli, "make_file", mock.Mock(return_value=cwd / "new_file.py"))
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli.Path, "lstat", mock.Mock(side_effect=[mock.Mock(), mock.Mock()]))

        result = cli_runner.invoke(["new", "--sql"])

        assert result.exit_code == 0, result.output
        with (cwd / "new_file.py").open() as f:
            assert f.read() == dedent("""\
            --
            -- depends:

            -- migrate: apply

            -- migrate: rollback

            """)

    @pytest.mark.usefixtures("pyproject")
    def test_load_failed_quit(self, monkeypatch, cli_runner):
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli.Path, "lstat", mock.Mock(side_effect=[mock.Mock(), mock.Mock()]))
        monkeypatch.setattr(cli.Migration, "load", mock.Mock(side_effect=Exception))

        result = cli_runner.invoke(["new"], input="q\n")

        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Error loading migration.
            Retry editing? [Ynqh]: q
            """),
        )

    @pytest.mark.usefixtures("pyproject")
    def test_retry_ignores_invalid_input(self, monkeypatch, cli_runner):
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli.Path, "lstat", mock.Mock(side_effect=[mock.Mock(), mock.Mock()]))
        monkeypatch.setattr(cli.Migration, "load", mock.Mock(side_effect=Exception))

        result = cli_runner.invoke(["new"], input="a\nb\nq\n")

        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Error loading migration.
            Retry editing? [Ynqh]: a
            Retry editing? [Ynqh]: b
            Retry editing? [Ynqh]: q
            """),
        )

    @pytest.mark.usefixtures("pyproject")
    def test_retry_help(self, monkeypatch, cli_runner):
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli.Path, "lstat", mock.Mock(side_effect=[mock.Mock(), mock.Mock()]))
        monkeypatch.setattr(cli.Migration, "load", mock.Mock(side_effect=Exception))

        result = cli_runner.invoke(["new"], input="h\nq\n")

        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Error loading migration.
            Retry editing? [Ynqh]: h
            y: reopen the migration file in your editor
            n: save the migration as-is, without re-editing
            q: quit without saving the migration
            h: show this help

            Retry editing? [Ynqh]: q
            """),
        )

    @pytest.mark.usefixtures("pyproject")
    def test_load_failed_retry_and_exit(self, monkeypatch, cli_runner, cwd):
        monkeypatch.setattr(cli, "make_file", mock.Mock(return_value=cwd / "new_file.py"))
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli.Path, "lstat", mock.Mock(side_effect=[mock.Mock(), mock.Mock(), mock.Mock()]))
        monkeypatch.setattr(cli.Migration, "load", mock.Mock(side_effect=Exception))

        result = cli_runner.invoke(["new"], input="y\nn\n")

        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Error loading migration.
            Retry editing? [Ynqh]: y
            Error loading migration.
            Retry editing? [Ynqh]: n
            Created file: new_file.py
            """),
        )

    @pytest.mark.usefixtures("pyproject")
    def test_load_failed_default_and_exit(self, monkeypatch, cli_runner, cwd):
        monkeypatch.setattr(cli, "make_file", mock.Mock(return_value=cwd / "new_file.py"))
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli.Path, "lstat", mock.Mock(side_effect=[mock.Mock(), mock.Mock(), mock.Mock()]))
        monkeypatch.setattr(cli.Migration, "load", mock.Mock(side_effect=Exception))

        result = cli_runner.invoke(["new"], input="\nn\n")

        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Error loading migration.
            Retry editing? [Ynqh]:
            Error loading migration.
            Retry editing? [Ynqh]: n
            Created file: new_file.py
            """),
        )


class TestHistory:
    @pytest.mark.usefixtures("migrations", "pyproject")
    def test_no_migrations(self, cli_runner):
        result = cli_runner.invoke(["history"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            STATUS    ID    FORMAT
            --------  ----  --------
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    def test_migrations_not_applied(self, migration_file_factory, cli_runner):
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_02_rando-commit

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        result = cli_runner.invoke(["history"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            STATUS    ID                        FORMAT
            --------  ------------------------  --------
            U         20210101_02_rando-commit  sql
            U         20210101_01_rando-commit  sql
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_migrations_partial_applied(self, cli_runner, migration_file_factory, db_session):
        await sql.ensure_pogo_sync(db_session)
        await sql.migration_applied(db_session, "20210101_01_rando-commit", "hash")
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        result = cli_runner.invoke(["history"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            STATUS    ID                        FORMAT
            --------  ------------------------  --------
            A         20210101_01_rando-commit  sql
            U         20210101_02_rando-commit  sql
            """),
        )


class TestApply:
    async def assert_tables(self, db_session, tables):
        stmt = """
        SELECT tablename
        FROM pg_tables
        WHERE  schemaname = 'public'
        ORDER BY tablename
        """
        results = await db_session.fetch(stmt)

        assert [r["tablename"] for r in results if not r["tablename"].startswith("_pogo")] == tables

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_apply_success(self, cli_runner, migration_file_factory, db_session):
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            CREATE TABLE table_one()
            -- migrate: rollback
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            CREATE TABLE table_two()
            -- migrate: rollback
            """),
        )
        result = cli_runner.invoke(["apply"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Applying 20210101_01_rando-commit
            Applying 20210101_02_rando-commit
            """),
        )
        await self.assert_tables(db_session, ["table_one", "table_two"])

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_apply_failure(self, cli_runner, migration_file_factory, db_session):
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            CREATE TABLE table_one()
            -- migrate: rollback
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            CREATE TABLE table_one()
            -- migrate: rollback
            """),
        )
        result = cli_runner.invoke(["apply"])
        assert result.exit_code == 1, result.output
        cli_runner.assert_output(
            dedent("""\
            Applying 20210101_01_rando-commit
            Applying 20210101_02_rando-commit
            Failed to apply 20210101_02_rando-commit
            """),
        )

        await self.assert_tables(db_session, [])

    @pytest.mark.usefixtures("migrations", "pyproject")
    def test_apply_failure_verbose(self, cli_runner, migration_file_factory):
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            CREATE TABLE table_one()
            -- migrate: rollback
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            CREATE TABLE table_one()
            -- migrate: rollback
            """),
        )
        result = cli_runner.invoke(["apply", "-v"])
        assert result.exit_code == 1, result.output
        assert 'DuplicateTableError: relation "table_one" already exists' in result.output


class TestRollback:
    async def assert_tables(self, db_session, tables):
        stmt = """
        SELECT tablename
        FROM pg_tables
        WHERE  schemaname = 'public'
        ORDER BY tablename
        """
        results = await db_session.fetch(stmt)

        assert [r["tablename"] for r in results if not r["tablename"].startswith("_pogo")] == tables

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_rollback_success(self, cli_runner, migration_file_factory, db_session):
        await sql.ensure_pogo_sync(db_session)
        await sql.migration_applied(db_session, "20210101_01_rando-commit", "hash")
        await sql.migration_applied(db_session, "20210101_02_rando-commit", "hash2")
        await db_session.execute("create table table_one();create table table_two()")
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            DROP TABLE table_one;
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            -- migrate: rollback
            DROP TABLE table_two;
            """),
        )
        result = cli_runner.invoke(["rollback", "--count", "-1"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Rolling back 20210101_02_rando-commit
            Rolling back 20210101_01_rando-commit
            """),
        )
        await self.assert_tables(db_session, [])

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_rollback_failure(self, cli_runner, migration_file_factory, db_session):
        await sql.ensure_pogo_sync(db_session)
        await sql.migration_applied(db_session, "20210101_01_rando-commit", "hash")
        await db_session.execute("create table table_one();create table table_two()")
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            DROP TABLE table_one;
            DROP TABLE table_two;
            DROP TABLE table_three;
            """),
        )
        result = cli_runner.invoke(["rollback"])
        assert result.exit_code == 1, result.output
        cli_runner.assert_output(
            dedent("""\
            Rolling back 20210101_01_rando-commit
            Failed to rollback 20210101_01_rando-commit
            """),
        )

        await self.assert_tables(db_session, ["table_one", "table_two"])

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_rollback_failure_verbose(self, cli_runner, migration_file_factory, db_session):
        await sql.ensure_pogo_sync(db_session)
        await sql.migration_applied(db_session, "20210101_01_rando-commit", "hash")
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            DROP TABLE table_one;
            """),
        )
        result = cli_runner.invoke(["rollback", "-v"])
        assert result.exit_code == 1, result.output
        assert 'UndefinedTableError: table "table_one" does not exist' in result.output


class TestMark:
    @pytest.mark.usefixtures("migrations", "pyproject")
    def test_mark_no_migrations(self, cli_runner):
        result = cli_runner.invoke(["mark"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            "",
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_mark_migrations_applied(self, cli_runner, migration_file_factory, db_session):
        await sql.ensure_pogo_sync(db_session)
        await sql.migration_applied(db_session, "20210101_01_rando-commit", "hash")
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        migration_file_factory(
            "20210101_03_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_02_rando-commit

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        result = cli_runner.invoke(["mark"], input="y\nn\n")
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Mark 20210101_02_rando-commit as applied? [y/N]: y
            Mark 20210101_03_rando-commit as applied? [y/N]: n
            """),
        )
        applied_migrations = await sql.get_applied_migrations(db_session)
        assert applied_migrations == {"20210101_01_rando-commit", "20210101_02_rando-commit"}


class TestUnMark:
    @pytest.mark.usefixtures("migrations", "pyproject")
    def test_unmark_no_migrations(self, cli_runner):
        result = cli_runner.invoke(["unmark"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            "",
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_unmark_migrations(self, cli_runner, migration_file_factory, db_session):
        await sql.ensure_pogo_sync(db_session)
        await sql.migration_applied(db_session, "20210101_01_rando-commit", "hash")
        await sql.migration_applied(db_session, "20210101_02_rando-commit", "hash2")
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        migration_file_factory(
            "20210101_03_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_02_rando-commit

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        result = cli_runner.invoke(["unmark"], input="y\nn\n")
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Unmark 20210101_02_rando-commit as applied? [y/N]: y
            Unmark 20210101_01_rando-commit as applied? [y/N]: n
            """),
        )
        applied_migrations = await sql.get_applied_migrations(db_session)
        assert applied_migrations == {"20210101_01_rando-commit"}


class TestMigrateYoyo:
    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_sql_files_converted(self, migration_file_factory, cli_runner, db_session):
        await db_session.execute("""
        create table _yoyo_migration (
            migration_hash varchar(64),
            migration_id varchar(255),
            applied_at_utc timestamp
        );""")
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            CREATE TABLE table_one();
            """),
        )
        migration_file_factory(
            "20210101_01_rando-commit",
            "rollback.sql",
            dedent("""
            -- commit
            -- depends:
            DROP TABLE table_one;
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit2",
            "sql",
            dedent("""
            -- commit2
            -- depends: 20210101_01_rando-commit
            CREATE TABLE table_two()
            """),
        )

        result = cli_runner.invoke(["migrate-yoyo"])
        assert result.exit_code == 0
        cli_runner.assert_output(
            dedent("""\
            Converted 'migrations/20210101_01_rando-commit.sql' successfully.
            Converted 'migrations/20210101_02_rando-commit2.sql' successfully.
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_py_files_skipped(self, migration_file_factory, cli_runner, db_session):
        await sql.ensure_pogo_sync(db_session)
        await db_session.execute("""
        create table _yoyo_migration (
            migration_hash varchar(64),
            migration_id varchar(255),
            applied_at_utc timestamp
        );""")
        migration_file_factory(
            "20210101_01_rando-commit",
            "py",
            "ignored",
        )

        result = cli_runner.invoke(["migrate-yoyo"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Python files can not be migrated reliably, please manually update
            'migrations/20210101_01_rando-commit.py'.
            """),
        )
