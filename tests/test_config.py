import os

import pytest

from pogo_migrate import config, exceptions


def test_find_config_current_directory(cwd):
    p = cwd / "pyproject.toml"

    with p.open("w") as f:
        f.write("""
[tool.pogo]
migrations = "./migrations"
database_config = "{POSTGRES_DSN}"
""")

    path = config.find_config()

    assert path == p


def test_find_config_parent_directory(cwd):
    p = cwd / "pyproject.toml"

    with p.open("w") as f:
        f.write("""
[tool.pogo]
migrations = "./migrations"
database_config = "{POSTGRES_DSN}"
""")

    subdir = cwd / "sub"
    subdir.mkdir()
    os.chdir(str(subdir))

    path = config.find_config()

    assert path == p


@pytest.mark.usefixtures("cwd")
def test_find_config_not_found():
    p = config.find_config()
    assert p is None


@pytest.mark.usefixtures("cwd")
def test_load_config_not_found():
    with pytest.raises(exceptions.InvalidConfigurationError) as e:
        config.load_config()

    assert str(e.value) == "No configuration found, missing pyproject.toml, run 'pogo init ...'"


def test_load_config_no_configuration(cwd):
    p = cwd / "pyproject.toml"

    with p.open("w") as f:
        f.write("""
[tool.other]
migrations = "./migrations"
database_config = "{POSTGRES_DSN}"
""")
    with pytest.raises(exceptions.InvalidConfigurationError) as e:
        config.load_config()

    assert str(e.value) == "No configuration found, run 'pogo init ...'"


def test_load_config_found(cwd):
    p = cwd / "pyproject.toml"

    with p.open("w") as f:
        f.write("""
[tool.pogo]
migrations = "./migrations"
database_config = "{POSTGRES_DSN}"
""")
    c = config.load_config()

    assert c == config.Config(
        root_directory=cwd,
        migrations=cwd / "migrations",
        database_config="{POSTGRES_DSN}",
    )


def test_load_config_database_config(cwd):
    p = cwd / "pyproject.toml"

    with p.open("w") as f:
        f.write("""
[tool.pogo]
migrations = "./migrations"
database_config = "{POSTGRES_DSN}"
""")
    c = config.load_config()

    assert c == config.Config(
        root_directory=cwd,
        migrations=cwd / "migrations",
        database_config="{POSTGRES_DSN}",
    )


def test_config_database_config_dsn_not_set(cwd):
    c = config.Config(
        root_directory=cwd,
        migrations=cwd / "migrations",
        database_config="postgres://{UNSET_KEY}",
    )

    with pytest.raises(exceptions.InvalidConfigurationError) as e:
        c.database_dsn  # noqa: B018

    assert str(e.value) == "Configured database_config env var 'UNSET_KEY' not set."


def test_config_database_config_no_keys(cwd):
    c = config.Config(
        root_directory=cwd,
        migrations=cwd / "migrations",
        database_config="postgres://fully-qualified",
    )

    assert c.database_dsn == "postgres://fully-qualified"
