# Configuration

You can configure pogo using `pogo init` or you can add pogo to pyproject.toml
manually.

```toml
[tool.pogo]
migrations_location = "./migrations"
database_config = "{POSTGRES_DSN}"
schema = "public"
```

If you have an existing environment with separate configuration values for
postgres, you can build the DSN in config.

```toml
[tool.pogo]
migrations_location = "./migrations"
database_config = "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{PORTGRES_DATABASE}"
schema = "public"
```

If you are using the cli purely to generate new migrations files and running
them directly via `migrate.apply` the `database_config` configuration can be
left out.

If any commands are run that require a database connection, and the
`--database` option is not provided, it will raise an invalid configuration
error.

## Squash migration exclusions

Some migrations can not be squashed due to complexity, or you might just want
to keep them stand alone. Additional configuration can be added to exclude
specific migrations from being included when `pogo squash` is run.

```toml
[tool.pogo.squash]
exclude = ["migration-id-1", "migration-id-5"]
```

## Schema configuration

If you want migrations to run in a specific (or default) schema, you can define
it in configuration, this value can be overridden per call in the CLI if you
need to run the migrations against multiple schemas.
