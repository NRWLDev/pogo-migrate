from __future__ import annotations

import io
import logging
import sys
import traceback
import typing as t
from dataclasses import dataclass, field
from enum import IntEnum

import click


class Verbosity(IntEnum):
    """Verbosity levels."""

    quiet = 0
    verbose1 = 1
    verbose2 = 2
    verbose3 = 3


VERBOSITY = {
    0: logging.ERROR,
    1: logging.WARNING,
    2: logging.INFO,
    3: logging.DEBUG,
}


def setup_logging(verbose: int = 0) -> None:
    """Configure the logging."""
    logging.basicConfig(
        level=VERBOSITY.get(verbose, logging.DEBUG),
        format="%(message)s",
        datefmt="[%X]",
    )
    asyncio_logger = logging.getLogger("asyncio")
    asyncio_logger.disabled = True
    sqlglot_logger = logging.getLogger("sqlglot")
    sqlglot_logger.disabled = True
    root_logger = logging.getLogger("")
    root_logger.setLevel(VERBOSITY.get(verbose, logging.DEBUG))


P = t.ParamSpec("P")


@dataclass
class Context:
    """Global context class."""

    verbose: int = field(default=0)
    _indent: int = field(default=0)

    def __post_init__(self) -> None:
        setup_logging(self.verbose)

    def _echo(self, message: str, *args: P.args, **kwargs: P.kwargs) -> None:  # noqa: ARG002
        """Echo to the console."""
        message = message % args
        click.echo(f"{'  ' * self._indent}{message}")

    def error(self, msg: str, *args: P.args, **kwargs: P.kwargs) -> t.Any:  # noqa: ANN401
        """Echo to the console."""
        self._echo(msg, *args, **kwargs)

    def warning(self, msg: str, *args: P.args, **kwargs: P.kwargs) -> t.Any:  # noqa: ANN401
        """Echo to the console for -v."""
        self.warn(msg, *args, **kwargs)

    def warn(self, msg: str, *args: P.args, **kwargs: P.kwargs) -> t.Any:  # noqa: ANN401
        """Echo to the console for -v."""
        if self.verbose > Verbosity.quiet:
            self._echo(msg, *args, **kwargs)

    def info(self, msg: str, *args: P.args, **kwargs: P.kwargs) -> t.Any:  # noqa: ANN401
        """Echo to the console for -vv."""
        if self.verbose > Verbosity.verbose1:
            self._echo(msg, *args, **kwargs)

    def debug(self, msg: str, *args: P.args, **kwargs: P.kwargs) -> t.Any:  # noqa: ANN401
        """Echo to the console for -vvv."""
        if self.verbose > Verbosity.verbose2:
            self._echo(msg, *args, **kwargs)

    def exception(self, msg: str, *args: P.args, **kwargs: P.kwargs) -> t.Any:  # noqa: ANN401
        """Echo exceptions to console for -vvv."""
        self.stacktrace()
        self._echo(msg, *args, **kwargs)

    def stacktrace(self) -> None:
        """Echo exceptions to console for -vvv."""
        if self.verbose > Verbosity.verbose2:
            t, v, tb = sys.exc_info()
            sio = io.StringIO()
            traceback.print_exception(t, v, tb, None, sio)
            s = sio.getvalue()
            # Clean up odd python 3.11, 3.12 formatting on mac
            s = s.replace("\n    ^^^^^^^^^^^^^^^^^^^^^^^^^^", "")
            sio.close()
            self._echo(s)
