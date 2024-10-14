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
class Config:
    root_directory: Path
    migrations: Path
    database_config: str

    @property
    def database_dsn(self: t.Self) -> str:
        try:
            format_kwargs = {
                k[1]: os.environ[k[1]] for k in Formatter().parse(self.database_config) if k[1] is not None
            }
        except KeyError as e:
            msg = f"Configured database_config env var {e!s} not set."
            raise exceptions.InvalidConfigurationError(msg) from e

        return self.database_config.format(**format_kwargs)

    @classmethod
    def from_dict(cls: type[Config], data: dict[str, str], root_directory: Path) -> Config:
        data["root_directory"] = root_directory  # type: ignore[reportArgumentType]
        data["migrations"] = root_directory / data["migrations"]  # type: ignore[reportArgumentType]
        return cls(**data)  # type: ignore[reportArgumentType]


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
