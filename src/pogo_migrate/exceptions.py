from pogo_core.error import BadMigrationError


class InvalidConfigurationError(Exception): ...  # pragma: no cover


__all__ = [
    "BadMigrationError",
    "InvalidConfigurationError",
]
