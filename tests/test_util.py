import string
from datetime import datetime, timezone
from unittest import mock

import pytest

import pogo_migrate.config
from pogo_migrate import util


@pytest.fixture
def pyproject(pyproject_factory):
    return pyproject_factory()


@pytest.fixture
def config(pyproject):  # noqa: ARG001
    return pogo_migrate.config.load_config()


@pytest.mark.parametrize(
    ("message", "slug"),
    [
        ("simple message", "simple-message"),
        ("SIMPLE MESSAGE", "simple-message"),
        ("$tr4nge message", "tr4nge-message"),
        ("   a  lot  of   whitespace", "a-lot-of-whitespace"),
        ("non a$c!! message", "non-a-c-message"),
    ],
)
def test_slugify(message, slug):
    assert util.slugify(message) == slug


def test_random_string():
    s = util.random_string()

    assert len(s) == 5  # noqa: PLR2004
    for c in s:
        assert c in (string.digits + string.ascii_lowercase)


def test_make_file(config, monkeypatch):
    monkeypatch.setattr(util.random, "choices", mock.Mock(return_value="rando"))
    p = util.make_file(config, "a message", ".sql")
    datestr = datetime.now(tz=timezone.utc).date().strftime("%Y%m%d")

    assert p.stem == f"{datestr}_01_rando-a-message"
    assert p.suffix == ".sql"


def test_make_file_increments_counter(monkeypatch, config, migrations):
    config.migrations = migrations
    monkeypatch.setattr(util.random, "choices", mock.Mock(return_value="rando"))
    datestr = datetime.now(tz=timezone.utc).date().strftime("%Y%m%d")

    for i in range(10):
        p = util.make_file(config, "a message", ".sql")
        p.touch()
        assert p.stem == f"{datestr}_{str(i + 1).zfill(2)}_rando-a-message"


@pytest.mark.parametrize(
    ("envkey", "envval", "expected"),
    [
        ("VISUAL", "emacs", "emacs"),
        ("EDITOR", "vim", "vim"),
        (None, None, "vi"),
    ],
)
def test_get_editor(envkey, envval, expected, monkeypatch, config):
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.delenv("EDITOR", raising=False)
    if envkey:
        monkeypatch.setenv(envkey, envval)

    assert util.get_editor(config) == expected
