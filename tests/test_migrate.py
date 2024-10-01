import pytest

import pogo_migrate.config
from pogo_migrate import exceptions, migrate


@pytest.fixture
def pyproject(pyproject_factory):
    return pyproject_factory()


@pytest.fixture
def config(pyproject):  # noqa: ARG001
    return pogo_migrate.config.load_config()


@pytest.fixture
def _migration_one(migrations):
    p = migrations / "20240317_01_abcde-initial-migration.sql"

    with p.open("w") as f:
        f.write("""
-- initial migration
-- depends:

-- migrate: apply
CREATE TABLE table_one();

-- migrate: rollback
DROP TABLE table_one;
""")


@pytest.fixture
def _migration_two(migrations, _migration_one):
    p = migrations / "20240317_02_12345-second-migration.sql"

    with p.open("w") as f:
        f.write("""
-- second migration
-- depends: 20240317_01_abcde-initial-migration

-- migrate: apply
CREATE TABLE table_two();

-- Auto generated
-- From psql

-- migrate: rollback

-- Auto generated
-- From psql
DROP TABLE table_two;
""")


@pytest.fixture
def _broken_apply(migrations, _migration_two):
    p = migrations / "20240318_01_12345-broken-apply.sql"

    with p.open("w") as f:
        f.write("""
-- broker migration
-- depends: 20240317_02_12345-second-migration

-- migrate: apply
CREATE TABLE table_three;

-- migrate: rollback
DROP TABLE table_two;
""")


@pytest.fixture
def _broken_rollback(migrations, _migration_two):
    p = migrations / "20240318_01_12345-broken-rollback.sql"

    with p.open("w") as f:
        f.write("""
-- broker migration
-- depends: 20240317_02_12345-second-migration

-- migrate: apply
CREATE TABLE table_three();

-- migrate: rollback
DROP TABLE table_four;
""")


class Base:
    async def assert_tables(self, db_session, tables):
        stmt = """
        SELECT tablename
        FROM pg_tables
        WHERE  schemaname = 'public'
        ORDER BY tablename
        """
        results = await db_session.fetch(stmt)

        assert [r["tablename"] for r in results] == tables


class TestApply(Base):
    @pytest.mark.usefixtures("migrations")
    async def test_no_migrations_applies_pogo_tables(self, config, db_session, context):
        await migrate.apply(context, config, db_session)

        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version"])

    @pytest.mark.usefixtures("_migration_two")
    async def test_migrations_applied(self, config, db_session, context):
        await migrate.apply(context, config, db_session)

        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version", "table_one", "table_two"])

    @pytest.mark.usefixtures("_migration_two")
    async def test_already_applied_skips(self, config, db_session, context):
        await migrate.apply(context, config, db_session)
        await migrate.apply(context, config, db_session)

        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version", "table_one", "table_two"])

    @pytest.mark.usefixtures("_broken_apply")
    async def test_broken_migration_not_applied(self, config, db_session, context):
        with pytest.raises(exceptions.BadMigrationError) as e:
            await migrate.apply(context, config, db_session)

        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version", "table_one", "table_two"])
        assert str(e.value) == "Failed to apply 20240318_01_12345-broken-apply"


class TestRollback(Base):
    @pytest.mark.usefixtures("migrations")
    async def test_no_migrations_applies_pogo_tables(self, config, db_session, context):
        await migrate.rollback(context, config, db_session)

        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version"])

    @pytest.mark.usefixtures("_migration_two")
    async def test_latest_removed(self, config, db_session, context):
        await migrate.apply(context, config, db_session)
        await migrate.rollback(context, config, db_session, count=1)

        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version", "table_one"])

    @pytest.mark.usefixtures("_migration_two")
    async def test_all_removed(self, config, db_session, context):
        await migrate.apply(context, config, db_session)
        await migrate.rollback(context, config, db_session)

        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version"])

    @pytest.mark.usefixtures("_broken_rollback")
    async def test_broken_rollback_rollsback(self, config, db_session, context):
        await migrate.apply(context, config, db_session)
        with pytest.raises(exceptions.BadMigrationError) as e:
            await migrate.rollback(context, config, db_session, count=1)

        await self.assert_tables(
            db_session,
            ["_pogo_migration", "_pogo_version", "table_one", "table_three", "table_two"],
        )
        assert str(e.value) == "Failed to rollback 20240318_01_12345-broken-rollback"
