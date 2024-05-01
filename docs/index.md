# Overview

`pogo-migrate` assists with maintaining your database schema (and data if
required) as it evolves.  Pogo supports migrations written in raw sql, as well
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
