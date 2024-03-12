# Pogo migrate - asyncpg migration tooling
[![image](https://img.shields.io/pypi/v/pogo_migrate.svg)](https://pypi.org/project/pogo_migrate/)
[![image](https://img.shields.io/pypi/l/pogo_migrate.svg)](https://pypi.org/project/pogo_migrate/)
[![image](https://img.shields.io/pypi/pyversions/pogo_migrate.svg)](https://pypi.org/project/pogo_migrate/)
![style](https://github.com/NRWLDev/pogo-migrate/actions/workflows/style.yml/badge.svg)
![tests](https://github.com/NRWLDev/pogo-migrate/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/gh/NRWLDev/pogo-migrate/branch/main/graph/badge.svg)](https://codecov.io/gh/NRWLDev/pogo-migrate)


## Configuration

Add pogo to pyproject.toml

```toml
[tool.pogo]
migrations_location = "./migrations"
database_env_key = "POSTGRES_DSN"
```

If you have an existing environment with separate configuration values for
postgres, you can build the DSN in config.

```toml
[tool.pogo]
migrations_location = "./migrations"
database_config = "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{PORTGRES_DATABASE}"
```

## Thanks and Credit

Inspiration for this tool is drawn from
[yoyo](https://ollycope.com/software/yoyo/latest/) and
[dbmate](https://github.com/amacneil/dbmate).
