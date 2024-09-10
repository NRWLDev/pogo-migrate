import inspect
import random
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from pogo_migrate import exceptions, migration
from tests.util import AsyncMock


class TestReadSqlMigration:
    def test_no_apply_section(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: rollback
            """),
        )
        with pytest.raises(exceptions.BadMigrationError) as e:
            migration.read_sql_migration(mp)

        assert str(e.value) == "20210101_01_rando-commit.sql: No '-- migrate: apply' found."

    def test_no_rollback_section(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            """),
        )
        with pytest.raises(exceptions.BadMigrationError) as e:
            migration.read_sql_migration(mp)

        assert str(e.value) == "20210101_01_rando-commit.sql: No '-- migrate: rollback' found."

    def test_invalid_metadata(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit

            -- migrate: apply
            """),
        )
        with pytest.raises(exceptions.BadMigrationError) as e:
            migration.read_sql_migration(mp)

        assert str(e.value) == "20210101_01_rando-commit.sql: No '-- depends:' or message found."

    def test_metadata_parsed(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "sql",
            dedent("""
            -- migration message
            -- depends: 20200101_01_rando-initial-commit

            -- migrate: apply
            CREATE TABLE table_one();
            CREATE TABLE table_two();
            -- migrate: rollback
            """),
        )
        message, depends, _, _, _, _, _ = migration.read_sql_migration(mp)

    async def test_apply_func_created(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "sql",
            dedent("""
            --
            -- depends:

            -- migrate: apply
            CREATE TABLE table_one();
            CREATE TABLE table_two();
            CREATE TABLE public.user (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                CONSTRAINT uc_name UNIQUE (name)
            );
            -- migrate: rollback
            """),
        )
        _, _, apply, _, _, apply_statements, _ = migration.read_sql_migration(mp)

        assert apply_statements == [
            "CREATE TABLE table_one();",
            "CREATE TABLE table_two();",
            dedent("""\
            CREATE TABLE public.user (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                CONSTRAINT uc_name UNIQUE (name)
            );"""),
        ]
        db = mock.Mock(execute=AsyncMock())
        await apply(db)
        assert db.execute.call_args_list == [
            mock.call("CREATE TABLE table_one();"),
            mock.call("CREATE TABLE table_two();"),
            mock.call(
                dedent("""\
            CREATE TABLE public.user (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                CONSTRAINT uc_name UNIQUE (name)
            );"""),
            ),
        ]

    async def test_rollback_func_created(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "sql",
            dedent("""
            -- migration message
            -- depends:

            -- migrate: apply
            CREATE TABLE table_one();
            CREATE TABLE table_two();
            DROP TABLE public.user;
            -- migrate: rollback
            DROP TABLE table_two;
            DROP TABLE table_one;
            CREATE TABLE public.user (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                CONSTRAINT uc_name UNIQUE (name)
            );
            -- final comment
            """),
        )
        _, _, _, rollback, _, _, rollback_statements = migration.read_sql_migration(mp)

        assert rollback_statements == [
            "DROP TABLE table_two;",
            "DROP TABLE table_one;",
            dedent("""\
            CREATE TABLE public.user (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                CONSTRAINT uc_name UNIQUE (name)
            );"""),
            "-- final comment;",
        ]
        db = mock.Mock(execute=AsyncMock())
        await rollback(db)
        assert db.execute.call_args_list == [
            mock.call("DROP TABLE table_two;"),
            mock.call("DROP TABLE table_one;"),
            mock.call(
                dedent("""\
            CREATE TABLE public.user (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                CONSTRAINT uc_name UNIQUE (name)
            );"""),
            ),
        ]

    @pytest.mark.parametrize(
        ("flag", "expected"),
        [
            ("", True),
            ("-- transaction: false", False),
            ("-- transaction: true", True),
        ],
    )
    async def test_transaction_flag(self, migration_file_factory, flag, expected):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "sql",
            dedent(f"""
            -- migration message
            -- depends:
            {flag}

            -- migrate: apply
            CREATE TABLE table_one();

            -- migrate: rollback
            DROP TABLE table_one;

            """),
        )
        _, _, _, _, in_transaction, _, _ = migration.read_sql_migration(mp)
        assert in_transaction == expected


class TestMigration:
    def test_migrations_superset_tracked(self):
        m = migration.Migration("20210101_01_rando-commit", Path("20210101_01_rando-commit.sql"), set())
        m2 = migration.Migration("20210101_02_rando-commit", Path("20210101_02_rando-commit.sql"), set())
        m3 = migration.Migration("20210101_03_rando-commit", Path("20210101_03_rando-commit.sql"), set())

        expected = {
            "20210101_01_rando-commit": m,
            "20210101_02_rando-commit": m2,
            "20210101_03_rando-commit": m3,
        }
        assert m._Migration__migrations == expected
        assert m2._Migration__migrations == expected
        assert m3._Migration__migrations == expected

    def test_applied(self):
        m = migration.Migration(
            "20210101_01_rando-commit",
            Path("20210101_01_rando-commit.sql"),
            {"20210101_01_rando-commit"},
        )
        assert m.applied is True

    def test_not_applied(self):
        m = migration.Migration(
            "20210101_01_rando-commit",
            Path("20210101_01_rando-commit.sql"),
            {"20200101_01_rando-initial-commit"},
        )
        assert m.applied is False

    def test_depends(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp2 = migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            -- migrate: rollback
            """),
        )

        m = migration.Migration(mp.stem, mp, None)
        m2 = migration.Migration(mp2.stem, mp2, None)

        m.load()
        m2.load()

        assert m.depends == set()
        assert m2.depends == {m}

    def test_depends_ids(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp2 = migration_file_factory(
            "20210101_02_rando-commit",
            "sql",
            dedent("""
            -- commit
            -- depends: 20210101_01_rando-commit

            -- migrate: apply
            -- migrate: rollback
            """),
        )

        m = migration.Migration(mp.stem, mp, None)
        m2 = migration.Migration(mp2.stem, mp2, None)

        m.load()
        m2.load()

        assert m.depends_ids == set()
        assert m2.depends_ids == {m.id}

    @pytest.mark.parametrize(
        ("extension", "expected"),
        [
            ("py", False),
            ("sql", True),
        ],
    )
    def test_is_sql(self, extension, expected):
        m = migration.Migration("id", Path(f"20210101_01_rando-commit.{extension}"), set())
        assert m.is_sql == expected

    async def test_apply(self, db_session):
        m = migration.Migration("id", Path("20210101_01_rando-commit.sql"), set())
        m._apply = AsyncMock()

        await m.apply(db_session)
        assert m._apply.call_args == mock.call(db_session)

    async def test_rollback(self, db_session):
        m = migration.Migration("id", Path("20210101_01_rando-commit.sql"), set())
        m._rollback = AsyncMock()

        await m.rollback(db_session)
        assert m._rollback.call_args == mock.call(db_session)

    def test_load_sql(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "sql",
            dedent("""
            -- migration message
            -- depends:

            -- migrate: apply
            CREATE TABLE table_one();
            CREATE TABLE table_two();
            -- migrate: rollback
            DROP TABLE table_two;
            DROP TABLE table_one;
            """),
        )
        m = migration.Migration(mp.stem, mp, set())
        m.load()
        assert m._doc == "migration message"
        assert inspect.iscoroutinefunction(m._apply)
        assert inspect.iscoroutinefunction(m._rollback)

    def test_load_python(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "py",
            dedent('''
            """
            migration message
            """
            __depends__ = []
            __transaction__ = False

            async def apply(db):
                await db.execute("CREATE TABLE table_one();")
                await db.execute("CREATE TABLE table_two();")

            async def rollback(db):
                await db.execute("DROP TABLE table_two;")
                await db.execute("DROP TABLE table_one;")
            '''),
        )
        m = migration.Migration(mp.stem, mp, set())
        m.load()
        assert m._use_transaction is False
        assert m._doc == "migration message"
        assert inspect.getsource(m._apply) == dedent("""\
        async def apply(db):
            await db.execute("CREATE TABLE table_one();")
            await db.execute("CREATE TABLE table_two();")\n""")
        assert inspect.getsource(m._rollback) == dedent("""\
        async def rollback(db):
            await db.execute("DROP TABLE table_two;")
            await db.execute("DROP TABLE table_one;")\n""")

    def test_load_invalid_python(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "py",
            dedent('''
            """
            migration message
            """
            __depends__ = []

            apply(db):
                await db.execute("CREATE TABLE table_one();")
                await db.execute("CREATE TABLE table_two();")

            def class rollback(db):
                await db.execute("DROP TABLE table_two;")
                await db.execute("DROP TABLE table_one;")
            '''),
        )
        m = migration.Migration(mp.stem, mp, set())
        with pytest.raises(exceptions.BadMigrationError) as e:
            m.load()

        assert str(e.value) == "Could not import migration from '20210101_01_rando-migration-message.py'"

    def test_load_unsupported(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "csv",
            "a,b,c\n1,2,3",
        )
        m = migration.Migration(mp.stem, mp, set())
        with pytest.raises(exceptions.BadMigrationError) as e:
            m.load()

        assert (
            str(e.value)
            == "Could not import migration from '20210101_01_rando-migration-message.csv': ModuleSpec has no loader attached"
        )

    def test_load_invalid_dependency(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "sql",
            dedent("""
            -- migration message
            -- depends: 20200101_01_rando-unknown-migration

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        m = migration.Migration(mp.stem, mp, set())
        with pytest.raises(exceptions.BadMigrationError) as e:
            m.load()

        assert str(e.value) == "Could not resolve dependencies for '20210101_01_rando-migration-message.sql'"

    def test_doc(self):
        m = migration.Migration("id", Path("20210101_01_rando-commit.sql"), set())
        m._doc = "a migration message"
        assert m.__doc__ == "a migration message"


class TestTopologicalSort:
    def test_migrations_sorted(self, migration_file_factory):
        mp = migration_file_factory(
            "20210101_01_rando-migration-message",
            "sql",
            dedent("""
            -- migration message
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp2 = migration_file_factory(
            "20210101_02_rando-migration-message",
            "sql",
            dedent("""
            -- migration message
            -- depends: 20210101_01_rando-migration-message

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp3 = migration_file_factory(
            "20210101_03_rando-migration-message",
            "sql",
            dedent("""
            -- migration message
            -- depends: 20210101_02_rando-migration-message

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        m = migration.Migration(mp.stem, mp, None)
        m2 = migration.Migration(mp2.stem, mp2, None)
        m3 = migration.Migration(mp3.stem, mp3, None)

        migrations = [m3.load(), m2.load(), m.load()]
        random.shuffle(migrations)
        assert migration.topological_sort(migrations) == [m, m2, m3]

    def test_cyclic_error(self, migration_file_factory):
        mp = migration_file_factory(
            "mig1",
            "sql",
            dedent("""
            -- migration message
            -- depends: mig3

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp2 = migration_file_factory(
            "mig2",
            "sql",
            dedent("""
            -- migration message
            -- depends: mig1

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp3 = migration_file_factory(
            "mig3",
            "sql",
            dedent("""
            -- migration message
            -- depends: mig2

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        m = migration.Migration(mp.stem, mp, None)
        m2 = migration.Migration(mp2.stem, mp2, None)
        m3 = migration.Migration(mp3.stem, mp3, None)

        migrations = [m3.load(), m2.load(), m.load()]
        with pytest.raises(exceptions.BadMigrationError) as e:
            migration.topological_sort(migrations)

        assert str(e.value) == "Circular dependencies among these migrations mig3, mig2, mig1"

    def test_fork(self, migration_file_factory):
        mp = migration_file_factory(
            "mig1",
            "sql",
            dedent("""
            -- migration message
            -- depends:

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp2 = migration_file_factory(
            "mig2",
            "sql",
            dedent("""
            -- migration message
            -- depends: mig1

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp3 = migration_file_factory(
            "mig3",
            "sql",
            dedent("""
            -- migration message
            -- depends: mig1

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        mp4 = migration_file_factory(
            "mig4",
            "sql",
            dedent("""
            -- migration message
            -- depends: mig3

            -- migrate: apply
            -- migrate: rollback
            """),
        )
        m = migration.Migration(mp.stem, mp, None)
        m2 = migration.Migration(mp2.stem, mp2, None)
        m3 = migration.Migration(mp3.stem, mp3, None)
        m4 = migration.Migration(mp4.stem, mp4, None)

        migrations = [m4.load(), m3.load(), m2.load(), m.load()]
        random.shuffle(migrations)
        # 2 and 3 can come after 1, but 4 always follows 3
        accepted_orderings = [
            [m, m2, m3, m4],
            [m, m3, m4, m2],
            [m, m3, m2, m4],
        ]
        assert migration.topological_sort(migrations) in accepted_orderings
