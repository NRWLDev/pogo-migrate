from textwrap import dedent

from pogo_migrate import yoyo


def test_convert_sql_migration_no_rollback(cwd):
    p = cwd / "20200101_01_slug-commit-message.sql"
    with p.open("w") as f:
        f.write(
            dedent("""\
        -- commit message
        -- depends: 20190101_01_slug2-initial-commit

        CREATE TABLE table_one();
        """),
        )

    content = yoyo.convert_sql_migration(p)
    assert content == dedent("""\
    -- commit message
    -- depends: 20190101_01_slug2-initial-commit

    -- migrate: apply

    CREATE TABLE table_one();

    -- migrate: rollback

    """)


def test_convert_sql_migration_with_rollback(cwd):
    p = cwd / "20200101_01_slug-commit-message.sql"
    with p.open("w") as f:
        f.write(
            dedent("""\
        CREATE TABLE table_one();
        """),
        )
    rp = cwd / "20200101_01_slug-commit-message.rollback.sql"
    with rp.open("w") as f:
        f.write(
            dedent("""\
        -- commit message
        -- depends: 20190101_01_slug2-initial-commit

        DROP TABLE table_one;
        """),
        )

    content = yoyo.convert_sql_migration(p)
    assert content == dedent("""\
    --
    -- depends:

    -- migrate: apply

    CREATE TABLE table_one();

    -- migrate: rollback

    DROP TABLE table_one;""")
