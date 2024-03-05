from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

import rtoml
import typer

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Config:
    migrations: Path
    database_env_key: str

    @classmethod
    def from_dict(cls: type[Config], data: dict) -> Config:
        data["migrations"] = Path(data["migrations"])
        return cls(**data)


def load_config() -> Config:
    pyproject = Path("pyproject.toml")
    if not pyproject.exists():
        logger.error("No configuration found, missing pyproject.toml, run 'pogo init ...'")
        raise typer.Exit(code=1)

    with pyproject.open() as f:
        data = rtoml.load(f)

    if "tool" not in data or "pogo" not in data["tool"]:
        logger.error("No configuration found, run 'pogo init ...'")
        raise typer.Exit(code=1)

    return Config.from_dict(data["tool"]["pogo"])
