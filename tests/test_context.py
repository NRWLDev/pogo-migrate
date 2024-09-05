from pathlib import Path
from unittest import mock

import pytest

from pogo_migrate.context import Context


@pytest.mark.parametrize("verbosity", [0, 1, 2, 3])
def test_verbosity(verbosity, monkeypatch):
    monkeypatch.setattr(Context, "_echo", mock.Mock())
    c = Context(verbosity)

    messages = [
        "error",
        "warning",
        "info",
        "debug",
    ]
    for message in messages:
        getattr(c, message)(message)

    assert c._echo.call_args_list == [mock.call(message) for message in messages[: verbosity + 1]]


def test_stacktrace(monkeypatch):
    monkeypatch.setattr(Context, "_echo", mock.Mock())
    c = Context(3)

    try:
        raise Exception("message")  # noqa: TRY002, EM101
    except:  # noqa: E722
        c.stacktrace()

    name = Path(__file__)
    assert c._echo.call_args == mock.call(
        f"""Traceback (most recent call last):
  File "{name}", line 31, in test_stacktrace
    raise Exception("message")  # noqa: TRY002, EM101
Exception: message
""",
    )


def test_stacktrace_quiet(monkeypatch):
    monkeypatch.setattr(Context, "_echo", mock.Mock())
    c = Context(2)

    try:
        raise Exception("message")  # noqa: TRY002, EM101
    except:  # noqa: E722
        c.stacktrace()

    assert c._echo.call_count == 0
