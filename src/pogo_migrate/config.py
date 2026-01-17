from __future__ import annotations

import dataclasses
import os
import typing as t
from pathlib import Path
from string import Formatter

import rtoml

from pogo_migrate import exceptions

CONFIG_FILENAME = "pyproject.toml"


@dataclasses.dataclass
class Squash:
    exclude: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Config:
    root_directory: Path
    migrations: Path
    squash: Squash = dataclasses.field(default_factory=Squash)
    database_config: str | None = None
    schema: str = "public"

    @property
    def database_dsn(self: t.Self) -> str:
        if self.database_config is None:
            msg = "Required config `database_config` is not set."
            raise exceptions.InvalidConfigurationError(msg)
        try:
            format_kwargs = {
                k[1]: os.environ[k[1]] for k in Formatter().parse(self.database_config) if k[1] is not None
            }
        except KeyError as e:
            msg = f"Configured database_config env var {e!s} not set."
            raise exceptions.InvalidConfigurationError(msg) from e

        return self.database_config.format(**format_kwargs)

    @classmethod
    def from_dict(
        cls: type[Config],
        data: dict[str, t.Any],
        root_directory: Path,
    ) -> Config:
        data["root_directory"] = root_directory
        data["migrations"] = root_directory / str(data["migrations"])
        if "squash" in data:
            data["squash"] = Squash(**data["squash"])
        return cls(**data)


def find_config() -> Path | None:
    """Find the closest config file in the cwd or a parent directory"""
    d = Path.cwd()
    while d != d.parent:
        path = d / CONFIG_FILENAME
        if path.is_file():
            return path
        d = d.parent
    return None


def load_config() -> Config:
    config = find_config()
    if config is None:
        msg = f"No configuration found, missing {CONFIG_FILENAME}, run 'pogo init ...'"
        raise exceptions.InvalidConfigurationError(msg)

    with config.open() as f:
        data = rtoml.load(f)

    if "tool" not in data or "pogo" not in data["tool"]:
        msg = "No configuration found, run 'pogo init ...'"
        raise exceptions.InvalidConfigurationError(msg)

    return Config.from_dict(data["tool"]["pogo"], config.parent)
