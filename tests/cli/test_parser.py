import importlib.metadata
import os
import sys
from unittest import mock

import pytest

from pogo_migrate import cli


def test_version(cli_runner):
    version = importlib.metadata.version("pogo-migrate")
    result = cli_runner.invoke(["--version"])
    assert result.exit_code == 0

    cli_runner.assert_output(f"pogo-migrate {version}")


class TestParsers:
    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {"verbose": 0},
            ),
            (
                ["-v"],
                {"verbose": 1},
            ),
            (
                ["-vv"],
                {"verbose": 2},
            ),
            (
                ["-vvv"],
                {"verbose": 3},
            ),
            (
                ["--verbose"],
                {"verbose": 1},
            ),
            (
                ["--verbose", "--verbose"],
                {"verbose": 2},
            ),
            (
                ["--verbose", "--verbose", "--verbose"],
                {"verbose": 3},
            ),
        ],
    )
    @pytest.mark.parametrize(
        "command",
        [
            "new",
            "init",
            "history",
            "apply",
            "rollback",
            "remove",
            "squash",
            "clean",
            "validate",
            "mark",
            "unmark",
            "migrate-yoyo",
        ],
    )
    def test_verbose(self, args, expected, command, monkeypatch):
        attr = f"_{command}" if command != "migrate-yoyo" else "_yoyo"
        test_func = mock.Mock()
        getattr(cli, attr)._defaults["func"] = test_func
        if command == "remove":
            args = ["1234", *args]
        monkeypatch.setattr(sys, "argv", ["pogo", command, *args])
        cli.main()

        verbose = test_func.call_args[1]["verbose"]
        assert verbose == expected["verbose"]

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
            (
                ["-m", "./my-migrations", "-d", "{POSTGRES_DSN}", "--schema", "pogo"],
                {"migrations_location": "./my-migrations", "database_env_key": "{POSTGRES_DSN}", "schema": "pogo"},
            ),
            (
                ["--migrations-location", "./my-migrations", "--database", "{POSTGRES_DSN}"],
                {"migrations_location": "./my-migrations", "database_env_key": "{POSTGRES_DSN}"},
            ),
        ],
    )
    def test_init_parser(self, args, expected, monkeypatch):
        defaults = {
            "verbose": 0,
            "migrations_location": "./migrations",
            "database_env_key": "{POGO_DATABASE}",
            "schema": "public",
        }
        defaults.update(expected)
        test_func = mock.create_autospec(cli.init)
        cli._init._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "init", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
            (
                ["-m", "message", "--no-interactive", "--py"],
                {"message_": "message", "interactive": False, "py_": True},
            ),
            (
                ["--message", "message", "--no-interactive", "--py"],
                {"message_": "message", "interactive": False, "py_": True},
            ),
        ],
    )
    def test_new_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "message_": "", "interactive": True, "py_": False}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.new)
        cli._new._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "new", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
            (
                ["-d", os.environ["POSTGRES_DSN"], "--schema", "pogo", "--unapplied", "--simple"],
                {"database": os.environ["POSTGRES_DSN"], "schema": "pogo", "unapplied": True, "simple": True},
            ),
            (
                ["--database", os.environ["POSTGRES_DSN"]],
                {"database": os.environ["POSTGRES_DSN"]},
            ),
        ],
    )
    def test_history_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "database": None, "schema": None, "unapplied": False, "simple": False}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.history)
        cli._history._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "history", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
            (
                ["-d", os.environ["POSTGRES_DSN"], "--schema", "pogo", "--create-schema"],
                {"database": os.environ["POSTGRES_DSN"], "schema": "pogo", "create_schema": True},
            ),
            (
                ["--database", os.environ["POSTGRES_DSN"]],
                {"database": os.environ["POSTGRES_DSN"]},
            ),
        ],
    )
    def test_apply_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "database": None, "schema": None, "create_schema": False}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.apply)
        cli._apply._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "apply", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
            (
                ["-d", os.environ["POSTGRES_DSN"], "-c", "2", "--schema", "pogo", "--drop-schema"],
                {"database": os.environ["POSTGRES_DSN"], "count": 2, "schema": "pogo", "drop_schema": True},
            ),
            (
                ["--database", os.environ["POSTGRES_DSN"], "--count", "2"],
                {"database": os.environ["POSTGRES_DSN"], "count": 2},
            ),
        ],
    )
    def test_rollback_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "database": None, "count": 1, "schema": None, "drop_schema": False}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.rollback)
        cli._rollback._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "rollback", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                ["1234"],
                {"migration_id": "1234"},
            ),
            (
                ["1234", "-m", "./my-migrations", "--backup"],
                {"migration_id": "1234", "migrations_location": "./my-migrations", "backup": True},
            ),
            (
                ["1234", "--migrations-location", "./my-migrations"],
                {"migration_id": "1234", "migrations_location": "./my-migrations"},
            ),
        ],
    )
    def test_remove_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "migrations_location": None, "backup": False}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.remove)
        cli._remove._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "remove", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
            (
                ["-m", "./my-migrations", "--backup", "--source", "--update-prompt", "--skip-prompt"],
                {
                    "migrations_location": "./my-migrations",
                    "backup": True,
                    "source": True,
                    "prompt_update": True,
                    "prompt_skip": True,
                },
            ),
            (
                ["--migrations-location", "./my-migrations"],
                {"migrations_location": "./my-migrations"},
            ),
        ],
    )
    def test_squash_parser(self, args, expected, monkeypatch):
        defaults = {
            "verbose": 0,
            "migrations_location": None,
            "backup": False,
            "source": False,
            "prompt_update": False,
            "prompt_skip": False,
        }
        defaults.update(expected)
        test_func = mock.create_autospec(cli.squash_)
        cli._squash._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "squash", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
            (
                ["-m", "./my-migrations"],
                {"migrations_location": "./my-migrations"},
            ),
            (
                ["--migrations-location", "./my-migrations"],
                {"migrations_location": "./my-migrations"},
            ),
        ],
    )
    def test_clean_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "migrations_location": None}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.clean)
        cli._clean._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "clean", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
            (
                ["-m", "./my-migrations"],
                {"migrations_location": "./my-migrations"},
            ),
            (
                ["--migrations-location", "./my-migrations"],
                {"migrations_location": "./my-migrations"},
            ),
        ],
    )
    def test_validate_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "migrations_location": None}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.validate)
        cli._validate._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "validate", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
        ],
    )
    def test_mark_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "migration_id": None, "database": None, "schema": None, "interactive": True}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.mark)
        cli._mark._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "mark", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
        ],
    )
    def test_unmark_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "migration_id": None, "database": None, "schema": None}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.unmark)
        cli._unmark._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "unmark", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                [],
                {},
            ),
        ],
    )
    def test_migrate_yoyo_parser(self, args, expected, monkeypatch):
        defaults = {"verbose": 0, "database": None, "skip_files": False}
        defaults.update(expected)
        test_func = mock.create_autospec(cli.migrate_yoyo)
        cli._yoyo._defaults["func"] = test_func
        monkeypatch.setattr(sys, "argv", ["pogo", "migrate-yoyo", *args])
        cli.main()

        assert test_func.call_args == mock.call(**defaults)
