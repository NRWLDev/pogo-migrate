from warnings import warn

from pogo_core.migration import (
    Migration,
    find_heads,
    read_sql_migration,
    strip_comments,
    terminate_statements,
    topological_sort,
)

warn(
    "pogo_migrate.migration usage has been deprecated, please use pogo_core.migration",
    FutureWarning,
    stacklevel=2,
)


__all__ = [
    "Migration",
    "find_heads",
    "read_sql_migration",
    "strip_comments",
    "terminate_statements",
    "topological_sort",
]
