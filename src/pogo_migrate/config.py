from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

import rtoml

from pogo_migrate import exceptions

logger = logging.getLogger(__name__)


CONFIG_FILENAME = "pyproject.toml"


@dataclasses.dataclass
class Config:
    root_directory: Path
    migrations: Path
    database_env_key: str

    @classmethod
    def from_dict(cls: type[Config], data: dict, root_directory: Path) -> Config:
        data["root_directory"] = root_directory
        data["migrations"] = root_directory / data["migrations"]
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
