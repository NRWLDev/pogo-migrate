# Pogo migrate - asyncpg migration tooling
[![image](https://img.shields.io/pypi/v/pogo_migrate.svg)](https://pypi.org/project/pogo_migrate/)
[![image](https://img.shields.io/pypi/l/pogo_migrate.svg)](https://pypi.org/project/pogo_migrate/)
[![image](https://img.shields.io/pypi/pyversions/pogo_migrate.svg)](https://pypi.org/project/pogo_migrate/)
![style](https://github.com/NRWLDev/pogo-migrate/actions/workflows/style.yml/badge.svg)
![tests](https://github.com/NRWLDev/pogo-migrate/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/gh/NRWLDev/pogo-migrate/branch/main/graph/badge.svg)](https://codecov.io/gh/NRWLDev/pogo-migrate)

`pogo-migrate` assists with maintaining your database schema (and data if
required) as it evolves. Pogo supports migrations written in raw sql, as well
as python files (useful when data needs to be migrated).

A migration can be as simple as:

```sql
-- a descriptive message
-- depends: 20210101_01_abcdef-previous-migration

-- migrate: apply
CREATE TABLE foo (id INT, bar VARCHAR(20), PRIMARY KEY (id));

-- migrate: rollback
DROP TABLE foo;
```

Pogo manages these migration scripts and provides command line tools to apply,
rollback and show migration history.

## Configuration

Add pogo to pyproject.toml

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

## New migrations

To create a new migration use `pogo new`. This will template out the migration
file and open the file in your configured text editor (`vi` by default).

Supported flags:

- `--sql` generate a sql migration (defaults to `.py`)
- `--no-interactive` skip the editor step and just write the migration template
  to the migrations directory.

```bash
$ pogo new -m "a descriptive message"
```

## Testing

To assist in testing, `pogo-migrate` provides the `pogo_migrate.testing`
module. The apply/rollback methods in the testing module will pick up your
configuration and connect to the configured test database based on environment
variables, or you can provide a database connection directly.

```python
import asyncpg
import pogo_migrate.testing

@pytest.fixture(scope="session")
async def _engine(config):  # noqa: PT005
    db = await asyncpg.connect(config.my_postgres_dsn)

    await pogo_migrate.testing.apply(db)

    yield

    await pogo_migrate.testing.rollback(db)
```

Alternatively add `pytest-pogo` to your test dependencies and use the provided
fixture `pogo_engine` which will apply and rollback your migrations for your
test session, like the above example.


## Thanks and Credit

Inspiration for this tool is drawn from
[yoyo](https://ollycope.com/software/yoyo/latest/) and
[dbmate](https://github.com/amacneil/dbmate).
