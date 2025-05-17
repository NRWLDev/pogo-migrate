# Configuration

You can configure pogo using `pogo init` or you can add pogo to pyproject.toml
manually.

```toml
[tool.pogo]
migrations_location = "./migrations"
database_config = "{POSTGRES_DSN}"
```

If you have an existing environment with separate configuration values for
postgres, you can build the DSN in config.

```toml
[tool.pogo]
migrations_location = "./migrations"
database_config = "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{PORTGRES_DATABASE}"
```

If you are using the cli purely to generate new migrations files and running
them directly via `migrate.apply` the `database_config` configuration can be
left out.

If any commands are run that require a database connection, and the
`--database` option is not provided, it will raise an invalid configuration
error.
