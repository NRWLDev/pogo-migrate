import importlib.metadata
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from pogo_migrate import cli, sql
from tests.util import AsyncMock


def test_version(cli_runner):
    version = importlib.metadata.version("pogo-migrate")
    result = cli_runner.invoke(["--version"])
    assert result.exit_code == 0

    cli_runner.assert_output(f"pogo-migrate {version}")


@pytest.fixture
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
            database_config = '{POGO_DATABASE}'
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
            database_config = '{POGO_DATABASE}'
            """)

    def test_init_overrides(self, cwd, cli_runner):
        result = cli_runner.invoke(["init", "-m", "./my-migrations", "-d", "{POSTGRES_DSN}"], input="y\n")
        assert result.exit_code == 0, result.output

        p = cwd / "pyproject.toml"
        with p.open() as f:
            assert f.read() == dedent("""\

            [tool.pogo]
            migrations = './my-migrations'
            database_config = '{POSTGRES_DSN}'
            """)

    def test_init_already_configured(self, cwd, cli_runner):
        p = cwd / "pyproject.toml"
        with p.open("w") as f:
            assert f.write(
                dedent("""\

            [tool.pogo]
            migrations = './my-migrations'
            database_config = '{POSTGRES_DSN}'
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
            database_config = '{POSTGRES_DSN}'
            """)

    def test_init_already_configured_verbose(self, cwd, cli_runner):
        p = cwd / "pyproject.toml"
        with p.open("w") as f:
            assert f.write(
                dedent("""\

            [tool.pogo]
            migrations = './my-migrations'
            database_config = '{POSTGRES_DSN}'
            """),
            )

        result = cli_runner.invoke(["init", "-v"])
        assert result.exit_code == 1
        cli_runner.assert_output(
            dedent("""\
            pogo already configured.

            [tool.pogo]
            migrations = "./my-migrations"
            database_config = "{POSTGRES_DSN}"
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

        result = cli_runner.invoke(["new", "--no-interactive"])

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

        result = cli_runner.invoke(["new", "-v"])

        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Created file: migrations/new_file.sql
            """).strip(),
        )

    @pytest.mark.usefixtures("pyproject")
    def test_subprocess_call(self, monkeypatch, tmp_path, cli_runner):
        f = tmp_path / "tmpfile"
        f.touch()
        mock_file = mock.MagicMock()
        mock_file.name = str(f)

        monkeypatch.setenv("EDITOR", "vim")
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli, "NamedTemporaryFile", mock.Mock(return_value=mock_file))

        cli_runner.invoke(["new", "--py"])

        assert cli.subprocess.call.call_args == mock.call(["vim", str(f)])

    @pytest.mark.usefixtures("pyproject")
    def test_subprocess_call_dynamic_editor(self, monkeypatch, tmp_path, cli_runner):
        f = tmp_path / "tmpfile"
        f.touch()
        mock_file = mock.MagicMock()
        mock_file.name = str(f)

        monkeypatch.setenv("EDITOR", "vim {}")
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli, "NamedTemporaryFile", mock.Mock(return_value=mock_file))

        cli_runner.invoke(["new", "--py"])

        assert cli.subprocess.call.call_args == mock.call(["vim", str(f)])

    @pytest.mark.usefixtures("pyproject")
    def test_file_written(self, monkeypatch, cli_runner, cwd):
        monkeypatch.setattr(cli, "make_file", mock.Mock(return_value=cwd / "new_file.py"))
        monkeypatch.setattr(cli.subprocess, "call", mock.Mock())
        monkeypatch.setattr(cli.Path, "lstat", mock.Mock(side_effect=[mock.Mock(), mock.Mock()]))

        result = cli_runner.invoke(["new"])

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
    def test_migrations_nono_config(self, migration_file_factory, cli_runner):
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
    def test_migrations_not_applied(self, migration_file_factory, cli_runner, monkeypatch):
        monkeypatch.delenv("POSTGRES_DSN")
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
        result = cli_runner.invoke(["history", "-v"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Database connection can not be established, migration status can not be determined.
            STATUS    ID                        FORMAT
            --------  ------------------------  --------
            U         20210101_02_rando-commit  sql
            U         20210101_01_rando-commit  sql
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_migrations_partial_applied(self, cli_runner, migration_file_factory, db_session):
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

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_migrations_partial_applied_only_unapplied(self, cli_runner, migration_file_factory, db_session):
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
        result = cli_runner.invoke(["history", "--unapplied"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            STATUS    ID                        FORMAT
            --------  ------------------------  --------
            U         20210101_02_rando-commit  sql
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_migrations_partial_applied_unapplied_simple(self, cli_runner, migration_file_factory, db_session):
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
        result = cli_runner.invoke(["history", "--unapplied", "--simple"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            U 20210101_02_rando-commit sql
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
        result = cli_runner.invoke(["apply", "-v"])
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
            CREATE TABLE table_two
            -- migrate: rollback
            """),
        )
        result = cli_runner.invoke(["apply", "-v"])
        assert result.exit_code == 1, result.output
        cli_runner.assert_output(
            dedent("""\
            Applying 20210101_01_rando-commit
            Applying 20210101_02_rando-commit
            Failed to apply 20210101_02_rando-commit
            """),
        )

        await self.assert_tables(db_session, ["table_one"])

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
        result = cli_runner.invoke(["apply", "-vvv"])
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
        result = cli_runner.invoke(["rollback", "--count", "-1", "-v"])
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
        result = cli_runner.invoke(["rollback", "-v"])
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
        result = cli_runner.invoke(["rollback", "-vvv"])
        assert result.exit_code == 1, result.output
        assert 'UndefinedTableError: table "table_one" does not exist' in result.output


class TestValidate:
    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_validate_clean(self, cli_runner, migration_file_factory):
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
        result = cli_runner.invoke(["validate", "-v"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output("")

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_validate_invalid_sql(self, cli_runner, migration_file_factory):
        migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            CREATE TABLE table_one();
            -- migrate: rollback
            DROP TABLE;
            """),
        )
        migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            CREATE INDEX foo;
            -- migrate: rollback
            DROP INDEX;
            DROP AGGREGATE lock;
            """),
        )
        result = cli_runner.invoke(["validate", "-v"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            20210101_01_rando-commit: Expected table name but got None. Line: 1, Column: 10
            DROP TABLE;
            20210101_02_rando-commit: Expected table name but got None. Line: 1, Column: 16
            CREATE INDEX foo;
            20210101_02_rando-commit: Expected table name but got None. Line: 1, Column: 10
            DROP INDEX;
            sqlglot failed to parse, falling back to sqlparse.
            Can not extract table from DDL statement in migration 20210101_02_rando-commit, check that table name is not a reserved word.
            DROP AGGREGATE lock;
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    def test_validate_checks_py(self, cli_runner, migration_file_factory):
        migration_file_factory(
            "20210101_01_rando-commit",
            "py",
            dedent('''
            """
            second migration
            """
            __depends__ = []
            __transaction__ = False

            async def apply(db):
                await db.execute("CREATE TABLE one();")
                await db.execute("CREATE INDEX foo;")

            async def rollback(db):
                await db.execute("DROP INDEX;")
                await db.execute("DROP TABLE;")
                await db.execute("DROP AGGREGATE lock;")
            '''),
        )
        result = cli_runner.invoke(["validate", "-v"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            20210101_01_rando-commit: Expected table name but got None. Line: 1, Column: 16
            CREATE INDEX foo;
            20210101_01_rando-commit: Expected table name but got None. Line: 1, Column: 10
            DROP INDEX;
            20210101_01_rando-commit: Expected table name but got None. Line: 1, Column: 10
            DROP TABLE;
            sqlglot failed to parse, falling back to sqlparse.
            Can not extract table from DDL statement in migration 20210101_01_rando-commit, check that table name is not a reserved word.
            DROP AGGREGATE lock;
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    def test_validate_py_parametrized(self, cli_runner, migration_file_factory):
        migration_file_factory(
            "20210101_01_abcde-first-migration",
            "py",
            dedent('''
            """
            second migration
            """
            __depends__ = []
            __transaction__ = False

            async def apply(db):
                a = "1"
                await db.execute('UPDATE "table" SET col = $1', a)

            async def rollback(db):
                await db.execute(query='DROP TABLE "table";')
            '''),
        )
        result = cli_runner.invoke(["validate", "-v"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    def test_validate_skips_py_error(self, cli_runner, migration_file_factory):
        migration_file_factory(
            "20210101_01_abcde-first-migration",
            "py",
            dedent('''
            """
            second migration
            """
            __depends__ = []
            __transaction__ = False

            async def apply(db):
                raise Exception

            async def rollback(db):
                raise Exception
            '''),
        )
        result = cli_runner.invoke(["validate", "-v"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent("""\
            Can't validate python migration 20210101_01_abcde-first-migration (apply), skipping...
            Can't validate python migration 20210101_01_abcde-first-migration (rollback), skipping...
            """),
        )


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

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_mark_migrations_non_interactive(self, cli_runner, migration_file_factory, db_session):
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
        result = cli_runner.invoke(["mark", "--no-interactive"])
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent(""),
        )
        applied_migrations = await sql.get_applied_migrations(db_session)
        assert applied_migrations == {
            "20210101_01_rando-commit",
            "20210101_02_rando-commit",
            "20210101_03_rando-commit",
        }


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
    async def test_skip_files(self, migration_file_factory, cli_runner, db_session):
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

        result = cli_runner.invoke(["migrate-yoyo", "--skip-files", "-vvv"])
        assert result.exit_code == 0
        cli_runner.assert_output(
            dedent("""\
            skip-files set, ignoring existing migration files.
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_history_already_loaded(self, cli_runner, db_session):
        await db_session.execute("""
        create table _yoyo_migration (
            migration_hash varchar(64),
            migration_id varchar(255),
            applied_at_utc timestamp
        );""")
        await sql.migration_applied(db_session, "20210101_01_rando-commit", "hash")

        result = cli_runner.invoke(["migrate-yoyo", "-vvv"])
        assert result.exit_code == 0
        cli_runner.assert_output(
            dedent("""\
            migration history exists, skipping yoyo migration.
            """),
        )

    @pytest.mark.usefixtures("migrations", "pyproject")
    async def test_no_yoyo_history(self, cli_runner):
        result = cli_runner.invoke(["migrate-yoyo", "-vvv"])
        assert result.exit_code == 0
        cli_runner.assert_output(
            dedent("""\
            yoyo migration history missing, skipping yoyo migration.
            """),
        )

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
            Python files can not be migrated reliably, please manually update 'migrations/20210101_01_rando-commit.py'.
            """),
        )


class TestRemove:
    def test_migration_removed_from_chain(self, migration_file_factory, cli_runner):
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
        mp = migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )

        result = cli_runner.invoke(["remove", "20210101_02_rando"])
        assert result.exit_code == 0, result.output

        assert mp.exists() is False

    def test_migration_removed_from_chain_with_backup(self, migration_file_factory, cli_runner):
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
        mp = migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )

        result = cli_runner.invoke(["remove", "20210101_02_rando", "--backup"])
        assert result.exit_code == 0, result.output

        assert mp.exists() is False
        assert Path(f"{mp}.bak").exists() is True


class TestClean:
    def test_bak_files_removed(self, migration_file_factory, cli_runner):
        b1 = migration_file_factory(
            "20210101_01_rando-commit",
            "sql.bak",
            dedent("""
            -- commit
            -- depends: 20210101_02_rando-commit

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        b2 = migration_file_factory(
            "20210101_02_rando-commit",
            "sql.bak",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp = migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )

        result = cli_runner.invoke(["clean"])
        assert result.exit_code == 0, result.output

        assert b1.exists() is False
        assert b2.exists() is False
        assert mp.exists() is True


class TestSquash:
    def test_simple_squash_all_sql(self, migration_file_factory, cli_runner):
        old = migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            CREATE TABLE one (id INT);
            INSERT INTO one (id) VALUES (1);
            -- migrate: rollback
            DELETE FROM one;
            DROP TABLE one;
            """),
        )
        new = migration_file_factory(
            "20210101_02_efgh-second-migration",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_abcd-first-migration

            -- migrate: apply
            CREATE TABLE two (id INT);
            UPDATE one SET id = 2;
            -- migrate: rollback
            UPDATE one SET id = 1;
            DROP TABLE two;
            """),
        )

        result = cli_runner.invoke(["squash", "-vvv"])
        assert result.exit_code == 0, result.output

        assert old.exists() is False
        assert new.read_text() == dedent("""\
        -- commit
        -- depends:

        -- squashed: 20210101_01_abcd-first-migration

        -- migrate: apply

        -- Squash one statements.

        CREATE TABLE one (id INT);

        -- Squash two statements.

        CREATE TABLE two (id INT);

        -- Squash data statements.

        INSERT INTO one (id) VALUES (1);

        UPDATE one SET id = 2;

        -- migrate: rollback

        -- Squash data statements.

        UPDATE one SET id = 1;

        DELETE FROM one;

        -- Squash two statements.

        DROP TABLE two;

        -- Squash one statements.

        DROP TABLE one;
        """)

    def test_apply_sqlglot_reserved_keyword_names_errors_trapped(self, migration_file_factory, cli_runner):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            CREATE TABLE lock (id INT);
            -- migrate: rollback
            """),
        )

        result = cli_runner.invoke(["squash"])
        assert result.exit_code == 1, result.output

        cli_runner.assert_output(
            "20210101_01_abcd-first-migration: Expected table name but got lock. Line: 1, Column: 17",
        )

    def test_apply_reserved_keyword_names_errors_trapped(self, migration_file_factory, cli_runner):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            DROP AGGREGATE lock;
            -- migrate: rollback
            """),
        )

        result = cli_runner.invoke(["squash"])
        assert result.exit_code == 1, result.output

        cli_runner.assert_output(
            "Can not extract table from DDL statement in migration 20210101_01_abcd-first-migration",
        )

    def test_rollback_sqlglot_reserved_keyword_names_errors_trapped(self, migration_file_factory, cli_runner):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            CREATE TABLE lock;
            """),
        )

        result = cli_runner.invoke(["squash"])
        assert result.exit_code == 1, result.output

        cli_runner.assert_output(
            "20210101_01_abcd-first-migration: Expected table name but got lock. Line: 1, Column: 17",
        )

    def test_rollback_reserved_keyword_names_errors_trapped(self, migration_file_factory, cli_runner):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            CREATE AGGREGATE lock;
            """),
        )

        result = cli_runner.invoke(["squash"])
        assert result.exit_code == 1, result.output

        cli_runner.assert_output(
            "Can not extract table from DDL statement in migration 20210101_01_abcd-first-migration",
        )

    def test_prompt_update(self, migration_file_factory, cli_runner):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            CREATE TABLE one (id INT);
            UPDATE one SET id = 1;
            -- migrate: rollback
            DROP TABLE one;
            """),
        )
        new = migration_file_factory(
            "20210101_02_efgh-second-migration",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_abcd-first-migration

            -- migrate: apply
            CREATE TABLE two (id INT);
            UPDATE one SET id = 2;
            -- migrate: rollback
            DROP TABLE two;
            """),
        )

        result = cli_runner.invoke(["squash", "--update-prompt"], input="\nn\n")
        assert result.exit_code == 0, result.output

        assert new.read_text() == dedent("""\
        -- commit
        -- depends:

        -- squashed: 20210101_01_abcd-first-migration

        -- migrate: apply

        -- Squash one statements.

        CREATE TABLE one (id INT);

        -- Squash two statements.

        CREATE TABLE two (id INT);

        -- Squash data statements.

        UPDATE one SET id = 1;

        -- migrate: rollback

        -- Squash two statements.

        DROP TABLE two;

        -- Squash one statements.

        DROP TABLE one;
        """)

    def test_python_and_non_transaction_skipped(self, migration_file_factory, cli_runner, migrations):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- first migration
            -- depends:

            -- migrate: apply
            CREATE TABLE one (id INT);

            -- migrate: rollback
            DROP TABLE one;
            """),
        )
        migration_file_factory(
            "20210101_02_efgh-second-migration",
            "py",
            dedent('''
            """
            second migration
            """
            __depends__ = ["20210101_01_abcd-first-migration"]
            __transaction__ = False

            async def apply(db):
                await db.execute("CREATE TABLE two();")

            async def rollback(db):
                await db.execute("DROP TABLE two;")
            '''),
        )
        migration_file_factory(
            "20210101_03_ijkl-third-migration",
            "sql",
            dedent("""
            -- third migration
            -- depends: 20210101_02_efgh-second-migration

            -- migrate: apply
            CREATE TABLE three (id INT);
            -- migrate: rollback
            DROP TABLE three;
            """),
        )
        new = migration_file_factory(
            "20210101_04_mnop-fourth-migration",
            "sql",
            dedent("""
            -- fourth migration
            -- depends: 20210101_03_ijkl-third-migration

            -- migrate: apply
            CREATE TABLE four (id INT);
            -- migrate: rollback
            DROP TABLE four;
            """),
        )
        migration_file_factory(
            "20210102_01_abcd-fifth-migration",
            "sql",
            dedent("""
            -- fifth migration
            -- depends: 20210101_04_mnop-fourth-migration

            -- transaction: false

            -- migrate: apply
            CREATE TABLE five (id INT);
            -- migrate: rollback
            DROP TABLE five;
            """),
        )

        result = cli_runner.invoke(["squash"])
        assert result.exit_code == 0, result.output

        assert new.read_text() == dedent("""\
        -- fourth migration
        -- depends: 20210101_02_efgh-second-migration

        -- squashed: 20210101_03_ijkl-third-migration

        -- migrate: apply

        -- Squash three statements.

        CREATE TABLE three (id INT);

        -- Squash four statements.

        CREATE TABLE four (id INT);

        -- migrate: rollback

        -- Squash four statements.

        DROP TABLE four;

        -- Squash three statements.

        DROP TABLE three;
        """)

        assert sorted([path.stem for path in migrations.iterdir() if path.suffix in {".py", ".sql"}]) == [
            "20210101_01_abcd-first-migration",
            "20210101_02_efgh-second-migration",
            "20210101_04_mnop-fourth-migration",
            "20210102_01_abcd-fifth-migration",
        ]

    def test_skip_initial_file(self, migration_file_factory, cli_runner, migrations):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- first migration
            -- depends:
            -- transaction: false

            -- migrate: apply
            CREATE TABLE one (id INT);
            -- migrate: rollback
            DROP TABLE one;
            """),
        )

        result = cli_runner.invoke(["squash"])
        assert result.exit_code == 0, result.output

        assert sorted([path.stem for path in migrations.iterdir() if path.suffix in {".py", ".sql"}]) == [
            "20210101_01_abcd-first-migration",
        ]

    def test_view_and_remove_python_file(self, migration_file_factory, cli_runner, migrations):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "py",
            dedent('''
            """
            first migration
            """
            __depends__ = []
            __transaction__ = False

            async def apply(db):
                await db.execute("CREATE TABLE two();")

            async def rollback(db):
                await db.execute("DROP TABLE two;")
            '''),
        )

        result = cli_runner.invoke(["squash", "--skip-prompt"], input="y\ny\n")
        assert result.exit_code == 0, result.output
        cli_runner.assert_output(
            dedent('''\
        View unsquashable migration 20210101_01_abcd-first-migration [Y/n]: y

        """
        first migration
        """

        __depends__ = []
        __transaction__ = False

        async def apply(db):
            await db.execute("CREATE TABLE two();")

        async def rollback(db):
            await db.execute("DROP TABLE two;")

        Remove unsquashable migration 20210101_01_abcd-first-migration [y/N]: y
        '''),
        )

        assert sorted([path.stem for path in migrations.iterdir() if path.suffix in {".py", ".sql"}]) == []

    def test_keep_python_file(self, migration_file_factory, cli_runner, migrations):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "py",
            dedent('''
            """
            first migration
            """
            __depends__ = []
            __transaction__ = False

            async def apply(db):
                await db.execute("CREATE TABLE two();")

            async def rollback(db):
                await db.execute("DROP TABLE two;")
            '''),
        )

        result = cli_runner.invoke(["squash", "--skip-prompt"], input="n\nn\n")
        assert result.exit_code == 0, result.output

        assert sorted([path.stem for path in migrations.iterdir() if path.suffix in {".py", ".sql"}]) == [
            "20210101_01_abcd-first-migration",
        ]

    def test_single_squash_skipped(self, migration_file_factory, cli_runner, migrations):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- first migration
            -- depends:

            -- migrate: apply
            CREATE TABLE one (id INT);
            -- migrate: rollback
            DROP TABLE one;
            """),
        )

        result = cli_runner.invoke(["squash"])
        assert result.exit_code == 0, result.output

        assert sorted([path.stem for path in migrations.iterdir() if path.suffix in {".py", ".sql"}]) == [
            "20210101_01_abcd-first-migration",
        ]

    def test_backups_kept(self, migration_file_factory, cli_runner, migrations):
        migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            CREATE TABLE one (id INT);
            -- migrate: rollback
            DROP TABLE one;
            """),
        )
        migration_file_factory(
            "20210101_02_efgh-second-migration",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_abcd-first-migration

            -- migrate: apply
            CREATE TABLE two (id INT);
            -- migrate: rollback
            DROP TABLE two;
            """),
        )

        result = cli_runner.invoke(["squash", "--backup"])
        assert result.exit_code == 0, result.output

        assert sorted([path.name for path in migrations.iterdir()]) == [
            "20210101_01_abcd-first-migration.sql.bak",
            "20210101_02_efgh-second-migration.sql",
            "20210101_02_efgh-second-migration.sql.bak",
        ]

    def test_sources_tracked(self, migration_file_factory, cli_runner):
        old = migration_file_factory(
            "20210101_01_abcd-first-migration",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            CREATE TABLE one (id INT);
            -- migrate: rollback
            DROP TABLE one;
            """),
        )
        new = migration_file_factory(
            "20210101_02_efgh-second-migration",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_abcd-first-migration

            -- migrate: apply
            CREATE TABLE two (id INT);
            -- migrate: rollback
            DROP TABLE two;
            """),
        )

        result = cli_runner.invoke(["squash", "--source"])
        assert result.exit_code == 0, result.output

        assert old.exists() is False
        assert new.read_text() == dedent("""\
        -- commit
        -- depends:

        -- squashed: 20210101_01_abcd-first-migration

        -- migrate: apply

        -- Squash one statements.

        CREATE TABLE one (id INT); -- source: 20210101_01_abcd-first-migration

        -- Squash two statements.

        CREATE TABLE two (id INT); -- source: 20210101_02_efgh-second-migration

        -- migrate: rollback

        -- Squash two statements.

        DROP TABLE two; -- source: 20210101_02_efgh-second-migration

        -- Squash one statements.

        DROP TABLE one; -- source: 20210101_01_abcd-first-migration
        """)
