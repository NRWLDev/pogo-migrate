import os
from unittest import mock

import pytest

from pogo_migrate import config, exceptions


def test_find_config_current_directory(cwd):
    p = cwd / "pyproject.toml"

    with p.open("w") as f:
        f.write("""
[tool.pogo]
migrations = "./migrations"
database_env_key = "POSTGRES_DSN"
""")

    path = config.find_config()

    assert path == p


def test_find_config_parent_directory(cwd):
    p = cwd / "pyproject.toml"

    with p.open("w") as f:
        f.write("""
[tool.pogo]
migrations = "./migrations"
database_env_key = "POSTGRES_DSN"
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
def test_load_config_not_found(monkeypatch):
    monkeypatch.setattr(config.logger, "error", mock.Mock())
    with pytest.raises(exceptions.InvalidConfigurationError) as e:
        config.load_config()

    assert str(e.value) == "No configuration found, missing pyproject.toml, run 'pogo init ...'"


def test_load_config_not_configuration(monkeypatch, cwd):
    p = cwd / "pyproject.toml"

    with p.open("w") as f:
        f.write("""
[tool.other]
migrations = "./migrations"
database_env_key = "POSTGRES_DSN"
""")
    monkeypatch.setattr(config.logger, "error", mock.Mock())
    with pytest.raises(exceptions.InvalidConfigurationError) as e:
        config.load_config()

    assert str(e.value) == "No configuration found, run 'pogo init ...'"


def test_load_config_found(monkeypatch, cwd):
    p = cwd / "pyproject.toml"

    with p.open("w") as f:
        f.write("""
[tool.pogo]
migrations = "./migrations"
database_env_key = "POSTGRES_DSN"
""")
    monkeypatch.setattr(config.logger, "error", mock.Mock())
    c = config.load_config()

    assert c == config.Config(
        root_directory=cwd,
        migrations=cwd / "migrations",
        database_env_key="POSTGRES_DSN",
    )
