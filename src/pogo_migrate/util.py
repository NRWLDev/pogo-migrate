import os
import random
import re
import string
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

from pogo_migrate.config import Config


def unidecode(s: str) -> str:
    """
    Return ``s`` with unicode diacritics removed.
    """
    combining = unicodedata.combining
    return "".join(c for c in unicodedata.normalize("NFD", s) if not combining(c))


def slugify(message: str) -> str:
    s = unidecode(message)
    s = re.sub(r"[^-a-z0-9]+", "-", s.lower())
    return re.sub(r"-{2,}", "-", s).strip("-")


def random_string() -> str:
    """Generate a random 5 digit string."""
    return "".join(random.choices(string.digits + string.ascii_lowercase, k=5))  # noqa: S311


def make_file(config: Config, message: str, extension: str) -> Path:
    slug = f"-{slugify(message)}" if message else ""
    datestr = datetime.now(tz=timezone.utc).date().strftime("%Y%m%d")
    rand = random_string()

    current = max(
        [int(p.name[len(datestr) + 1 :].split("_")[0]) for p in Path(config.migrations).glob(f"{datestr}_*")],
        default=0,
    )

    number = str(int(current) + 1).zfill(2)

    return config.migrations / f"{datestr}_{number}_{rand}{slug}{extension}"


def get_editor(_config: Config) -> str:
    """
    Return the user's preferred visual editor
    """
    for key in ["VISUAL", "EDITOR"]:
        editor = os.environ.get(key, None)
        if editor:
            return editor
    return "vi"
