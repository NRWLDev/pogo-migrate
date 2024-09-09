# Pogo migrate - asyncpg migration tooling
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![image](https://img.shields.io/pypi/v/pogo_migrate.svg)](https://pypi.org/project/pogo_migrate/)
[![image](https://img.shields.io/pypi/l/pogo_migrate.svg)](https://pypi.org/project/pogo_migrate/)
[![image](https://img.shields.io/pypi/pyversions/pogo_migrate.svg)](https://pypi.org/project/pogo_migrate/)
![style](https://github.com/NRWLDev/pogo-migrate/actions/workflows/style.yml/badge.svg)
![tests](https://github.com/NRWLDev/pogo-migrate/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/gh/NRWLDev/pogo-migrate/branch/main/graph/badge.svg)](https://codecov.io/gh/NRWLDev/pogo-migrate)

`pogo-migrate` is a migration tool intended for use with `asyncpg` and assists
with maintaining your database schema (and data if required) as it evolves.
Pogo supports migrations written in raw sql, as well as python files (useful
when data needs to be migrated).

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

See the [docs](https://nrwldev.github.io/pogo-migrate/) for more details.

## Thanks and Credit

Inspiration for this tool is drawn from
[yoyo](https://ollycope.com/software/yoyo/latest/) and
[dbmate](https://github.com/amacneil/dbmate).
