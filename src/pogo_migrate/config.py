from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

import rtoml
import typer

logger = logging.getLogger(__name__)


CONFIG_FILENAME = "pyproject.toml"


@dataclasses.dataclass
class Config:
    migrations: Path
    database_env_key: str

    @classmethod
    def from_dict(cls: type[Config], data: dict) -> Config:
        data["migrations"] = Path(data["migrations"])
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
        logger.error("No configuration found, missing %s, run 'pogo init ...'", CONFIG_FILENAME)
        raise typer.Exit(code=1)

    with config.open() as f:
        data = rtoml.load(f)

    if "tool" not in data or "pogo" not in data["tool"]:
        logger.error("No configuration found, run 'pogo init ...'")
        raise typer.Exit(code=1)

    return Config.from_dict(data["tool"]["pogo"])
