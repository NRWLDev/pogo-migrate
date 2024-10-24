from pathlib import Path
from textwrap import dedent

import pytest

from pogo_migrate import migration, squash


def test_remove_no_dependent(migration_file_factory, context):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    squash.remove(context, m, None)

    assert mp.exists() is False


def test_remove_with_backup(migration_file_factory, context):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    squash.remove(context, m, None, backup=True)

    assert mp.exists() is False
    assert Path(f"{mp}.bak").exists() is True


def test_remove_no_parent_with_dependent(migration_file_factory, context):
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

    squash.remove(context, m, m2)

    assert m2.depends == set()


def test_remove_python_no_parent_with_dependent(migration_file_factory, context):
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
    mp2 = migration_file_factory(
        "20210101_02_rando-migration-message",
        "py",
        dedent('''
        """
        migration message
        """
        __depends__ = ["20210101_01_rando-migration-message"]
        __transaction__ = False

        async def apply(db):
            await db.execute("CREATE TABLE table_one();")
            await db.execute("CREATE TABLE table_two();")

        async def rollback(db):
            await db.execute("DROP TABLE table_two;")
            await db.execute("DROP TABLE table_one;")
        '''),
    )

    m = migration.Migration(mp.stem, mp, None)
    m2 = migration.Migration(mp2.stem, mp2, None)

    m.load()
    m2.load()

    squash.remove(context, m, m2)

    assert m2.depends == set()


def test_remove_with_parent_with_dependent(migration_file_factory, context):
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
    mp3 = migration_file_factory(
        "20210101_03_rando-commit",
        "sql",
        dedent("""
        -- commit
        -- depends: 20210101_02_rando-commit

        -- migrate: apply
        -- migrate: rollback
        """),
    )

    m = migration.Migration(mp.stem, mp, None)
    m2 = migration.Migration(mp2.stem, mp2, None)
    m3 = migration.Migration(mp3.stem, mp3, None)

    m.load()
    m2.load()
    m3.load()

    squash.remove(context, m2, m3)

    assert m3.depends == {m}


def test_write_short_circuits_for_file_squashed_to_self():
    latest = migration.Migration("mig-id", "path", None)
    new = squash.write({}, {}, latest, None, [latest.id])
    assert new is None


def test_squash_file_created(migration_file_factory):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    new = squash.write({}, {}, m, None, ["squash-1", "squash-2"])

    assert str(new.path) == f"{m.path}.squash"


def test_message_maintained(migration_file_factory):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    new = squash.write({}, {}, m, None, ["squash-1", "squash-2"])

    content = new.path.read_text()
    assert content.startswith("-- commit")


def test_depends_updated(migration_file_factory):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    new = squash.write({}, {}, m, "previous-1", ["squash-1", "squash-2"])

    content = new.path.read_text()
    assert "-- depends: previous-1" in content


def test_squashed_migrations_tracked(migration_file_factory):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    new = squash.write({}, {}, m, "previous-1", ["squash-1", "squash-2", m.id])

    content = new.path.read_text()
    assert "-- squashed: squash-1" in content
    assert "-- squashed: squash-2" in content


def test_apply_statements_stored_in_order(migration_file_factory):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    apply_statements = {
        "table1": ["statement1", "statement2"],
        "table2": ["statement3", "statement4"],
    }
    new = squash.write(apply_statements, {}, m, "previous-1", ["squash-1", "squash-2", m.id])

    content = new.path.read_text()
    assert (
        dedent("""
    -- migrate: apply

    -- Squash table1 statements.

    statement1

    statement2

    -- Squash table2 statements.

    statement3

    statement4

    -- migrate: rollback
    """)
        in content
    )


def test_apply_data_statements_last(migration_file_factory):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    apply_statements = {
        "table1": ["statement1"],
        "__data": ["update", "insert", "delete"],
    }
    new = squash.write(apply_statements, {}, m, "previous-1", ["squash-1", "squash-2", m.id])

    content = new.path.read_text()
    assert (
        dedent("""
    -- migrate: apply

    -- Squash table1 statements.

    statement1

    -- Squash data statements.

    update

    insert

    delete

    -- migrate: rollback
    """)
        in content
    )


def test_rollback_statements_reversed_from_discovery_order(migration_file_factory):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    # Rollback statements are found in file order, in reverse statement order
    """
    file1:
    ALTER table2 DROP COLUMN a;
    DROP table2;
    ALTER table1 DROP COLUMN e;
    DROP table1;

    file2:
    ALTER table2 DROP COLUMN c;
    ALTER table2 DROP COLUMN d;
    ALTER table1 DROP COLUMN f;
    ALTER table1 DROP COLUMN g;
    """

    rollback_statements = {
        "table1": [
            ["DROP table1;", "ALTER table1 DROP COLUMN e;"],
            ["ALTER table1 DROP COLUMN g;", "ALTER table1 DROP COLUMN f;"],
        ],
        "table2": [
            ["DROP table2;", "ALTER table2 DROP COLUMN a;"],
            ["ALTER table2 DROP COLUMN d;", "ALTER table2 DROP COLUMN c;"],
        ],
    }
    new = squash.write({}, rollback_statements, m, "previous-1", ["squash-1", "squash-2", m.id])

    content = new.path.read_text()
    # Rolback statements should be applied last discovered to first
    assert (
        dedent("""
    -- migrate: rollback

    -- Squash table2 statements.

    ALTER table2 DROP COLUMN c;

    ALTER table2 DROP COLUMN d;

    ALTER table2 DROP COLUMN a;

    DROP table2;

    -- Squash table1 statements.

    ALTER table1 DROP COLUMN f;

    ALTER table1 DROP COLUMN g;

    ALTER table1 DROP COLUMN e;

    DROP table1;
    """)
        in content
    )


def test_rollback_data_statements_first_reversed_from_discovery_order(migration_file_factory):
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

    m = migration.Migration(mp.stem, mp, None)
    m.load()

    # Rollback statements are found in file order, in reverse statement order
    """
    file1:
    UPDATE table1;
    DELETE FROM table1;
    DROP table1;

    file2:
    INSERT INTO table1;
    """

    rollback_statements = {
        "table1": [
            ["DROP table1;"],
        ],
        "__data": [
            ["DELETE FROM table1;", "UPDATE table1;"],
            ["INSERT INTO table1;"],
        ],
    }
    new = squash.write({}, rollback_statements, m, "previous-1", ["squash-1", "squash-2", m.id])

    content = new.path.read_text()
    # Rolback statements should be applied last discovered to first
    assert (
        dedent("""
    -- migrate: rollback

    -- Squash data statements.

    INSERT INTO table1;

    UPDATE table1;

    DELETE FROM table1;

    -- Squash table1 statements.

    DROP table1;
    """)
        in content
    )


@pytest.mark.parametrize(
    ("statement", "expected_type"),
    [
        ("CREATE TABLE tbl (id INT);", "CREATE"),
        ("CREATE SCHEMA IF NOT EXISTS test;", "CREATE"),
        ("CREATE EXTENSION pgcrypto;", "CREATE"),
        ("ALTER TABLE tbl ADD COLUMN id INT;", "ALTER"),
        ("DROP TABLE tbl;", "DROP"),
        ("UPDATE tbl SET id = 1;", "UPDATE"),
        ("INSERT INTO TABLE tbl (id) VALUES (1);", "INSERT"),
        ("DELETE FROM TABLE tbl WHERE id = 1", "DELETE"),
    ],
)
def test_parse_sqlglot_type(statement, expected_type, context):
    parsed = squash.parse_sqlglot(context, statement)

    assert parsed.statement_type == expected_type


@pytest.mark.parametrize(
    ("statement", "expected_identifier"),
    [
        ("CREATE TABLE tbl (id INT);", "tbl"),
        ("CREATE TABLE IF NOT EXISTS tbl (id INT);", "tbl"),
        ("CREATE INDEX ix_table_id ON tbl (id);", "tbl"),
        ("CREATE INDEX ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE INDEX IF NOT EXISTS ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE UNIQUE INDEX ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE UNIQUE INDEX IF NOT EXISTS ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE INDEX CONCURRENTLY ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE UNIQUE INDEX CONCURRENTLY ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE AGGREGATE my_agg (numeric) ( SFUNC = numeric_mul, STYPE=numeric );", "my_agg"),
        ("CREATE AGGREGATE IF NOT EXISTS my_agg (numeric) ( SFUNC = numeric_mul, STYPE=numeric );", "my_agg"),
        ("CREATE EXTENSION pgcrypto;", "pgcrypto"),
        ("CREATE EXTENSION IF NOT EXISTS pgcrypto;", "pgcrypto"),
        ("CREATE SCHEMA test;", "test"),
        ('CREATE SCHEMA IF NOT EXISTS "test";', "test"),
        ('ALTER TABLE "lock" ADD COLUMN id INT;', "lock"),
        ('ALTER TABLE "lock" ALTER COLUMN id INT;', "lock"),
        ("DROP TABLE tbl;", "tbl"),
        ("DROP TABLE IF EXISTS tbl;", "tbl"),
        ("DROP AGGREGATE my_agg (numeric);", "my_agg"),
        ("DROP AGGREGATE IF EXISTS my_agg (numeric);", "my_agg"),
        ("DROP EXTENSION IF EXISTS pgcrypto;", "pgcrypto"),
        ("DROP EXTENSION pgcrypto;", "pgcrypto"),
        ("DROP INDEX ix_table_id;", "ix_table_id"),
        ("DROP INDEX IF EXISTS ix_table_id;", "ix_table_id"),
        ("DROP INDEX CONCURRENTLY ix_table_id;", "ix_table_id"),
        ("DROP INDEX CONCURRENTLY IF EXISTS ix_table_id;", "ix_table_id"),
        ('DROP SCHEMA "test";', "test"),
        ('DROP SCHEMA IF EXISTS "test";', "test"),
        ("UPDATE tbl SET id = 1;", None),
        ("INSERT INTO TABLE tbl (id) VALUES (1);", None),
        ("DELETE FROM TABLE tbl WHERE id = 1", None),
    ],
)
def test_parse_sqlglot_identifier(statement, expected_identifier, context):
    parsed = squash.parse_sqlglot(context, statement)

    assert parsed.identifier == expected_identifier


@pytest.mark.parametrize(
    ("statement"),
    [
        ("CREATE TABLE lock (id INT);"),
        ("ALTER TABLE lock ADD COLUMN id INT;"),
        ("DROP TABLE lock;"),
    ],
)
def test_parse_sqlglot_identifier_aborts_at_table_keyword(statement, context):
    # Original code would pick up column identifier.
    with pytest.raises(squash.ParseError, match="Expected table name but got lock."):
        squash.parse_sqlglot(context, statement)


@pytest.mark.parametrize(
    ("statement", "expected_identifier"),
    [
        ("CREATE TABLE tbl (id INT);", "tbl"),
        ("CREATE TABLE IF NOT EXISTS tbl (id INT);", "tbl"),
        ("CREATE INDEX ix_table_id ON tbl (id);", "tbl"),
        ("CREATE INDEX ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE INDEX IF NOT EXISTS ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE UNIQUE INDEX ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE UNIQUE INDEX IF NOT EXISTS ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE INDEX CONCURRENTLY ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE UNIQUE INDEX CONCURRENTLY ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_table_id ON tbl USING btree (id);", "tbl"),
        ("CREATE AGGREGATE my_agg (numeric) ( SFUNC = numeric_mul, STYPE=numeric );", "my_agg"),
        ("CREATE AGGREGATE IF NOT EXISTS my_agg (numeric) ( SFUNC = numeric_mul, STYPE=numeric );", "my_agg"),
        ("CREATE EXTENSION pgcrypto;", "pgcrypto"),
        ("CREATE EXTENSION IF NOT EXISTS pgcrypto;", "pgcrypto"),
        ("CREATE SCHEMA test;", "test"),
        ('CREATE SCHEMA IF NOT EXISTS "test";', "test"),
        ('ALTER TABLE "lock" ADD COLUMN id INT;', "lock"),
        ('ALTER TABLE "lock" ALTER COLUMN id INT;', "lock"),
        ("DROP TABLE tbl;", "tbl"),
        ("DROP TABLE IF EXISTS tbl;", "tbl"),
        ("DROP AGGREGATE my_agg (numeric);", "my_agg"),
        ("DROP AGGREGATE IF EXISTS my_agg (numeric);", "my_agg"),
        ("DROP EXTENSION IF EXISTS pgcrypto;", "pgcrypto"),
        ("DROP EXTENSION pgcrypto;", "pgcrypto"),
        ("DROP INDEX ix_table_id;", "ix_table_id"),
        ("DROP INDEX IF EXISTS ix_table_id;", "ix_table_id"),
        ("DROP INDEX CONCURRENTLY ix_table_id;", "ix_table_id"),
        ("DROP INDEX CONCURRENTLY IF EXISTS ix_table_id;", "ix_table_id"),
        ('DROP SCHEMA "test";', "test"),
        ('DROP SCHEMA IF EXISTS "test";', "test"),
        ("UPDATE tbl SET id = 1;", None),
        ("INSERT INTO TABLE tbl (id) VALUES (1);", None),
        ("DELETE FROM TABLE tbl WHERE id = 1", None),
    ],
)
def test_parse_identifier(statement, expected_identifier, context):
    parsed = squash.parse(context, statement)

    assert parsed.identifier == expected_identifier
