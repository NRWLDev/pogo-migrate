# Plugin Architecture

Pogo supports libraries/packages providing their own migrations, this feature
should be used with caution, and is generally recommended only for use in
shared code in a managed project.

## Plugin definition

The layout of a plugin is relatively simple. Add a module to your package
containing a `plugin` attribute. Defining the relative path to the migrations
directory for the package, and optionally the schema that the package manages
(recommended to prevent clashes between main code migrations and package
migrations).


```python
from pathlib import Path

from pogo_core.util.plugins import Plugin


plugin = Plugin(
    migrations=Path(__file__).parent / "migrations",
    schema="auth",
)
```

## Plugin configuration

To expose the migrations to the installing code (and allow `pogo` to pick up
the migrations) add the following to `pyproject.toml`. Providing a unique
plugin name (used for sorting and rendering in history display, and to prevent
clashes), and the dot notation path to the plugin definition module (containing
the `plugin` attribute.

```toml
[project.entry-points.pogo]
name = "package.pogo_module"
```

## Plugin detection

While using the `pogo-migrate` cli, plugins will be automatically detected.

If you are using the `pogo-core` code interfaces to apply/rollback migrations,
provide `include_plugins=True` to the apply/rollback/read_migrations functions.

```python
from pathlib import Path

from pogo_core.util import migrate, sql

migrations_dir = Path("path/to/migrations")
conn = await sql.get_connection(database_dsn)

await migrate.apply(db=conn, migrations_dir=migrations_dir, include_plugins=True)
await migrate.rollback(db=conn, migrations_dir=migrations_dir, include_plugins=True)
