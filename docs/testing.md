# Testing

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

Alternatively add [pytest-pogo](https://pypi.org/project/pytest-pogo) to your
test dependencies and use the provided fixture `pogo_engine` which will apply
and rollback your migrations for your test session, like the above example.
