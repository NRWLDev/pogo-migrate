# Setting up

To help with getting started, the `pogo init` command will initialise the
migrations folder, as well as setup the basic configuration for `pogo`.

Supported flags:

- `-m, --migrations-location` defines the name of the migrations folder
  (defaults to `./migrations`)
- `-d, --database-env-key` defines the environment variable (or template to
  build from environment variables) for the database dsn. See
  [configuration](https://nrwldev.github.io/pogo-migrate/configuration/) for
  examples.

## Migrating from yoyo

If you are coming here having previously used `yoyo`, a lot of migrations can
be directly converted to `pogo` (namely sql migrations). Python migrations will
be copied across as is, and will require manual updating to use async functions.

Supported flags:

- `-d, --database` postgres dsn to load yoyo history, and store pogo history
  tables.
- `--skip-files, --no-skip-files` skip migration files and just copy the yoyo
  history in the database. (defaults to `--no-skip-files`)
- `--dotenv, --no-dotenv` load environment from local `.env` file. (defaults to
  `--no-dotenv`)

Locally you will want to migrate files and data, but in a deployed environment
your code will likely already contain the migrated files, and as such just the
database history needs to be copied. In deployed environments `--skip-files`
would be used.

# New migrations

To create a new migration use `pogo new`. This will template out the migration
file and open the file in your configured text editor (`vi` by default).

Supported flags:

- `--py` generate a python migration (defaults to `.sql`)
- `--no-interactive` skip the editor step and just write the migration template
  to the migrations directory.

```bash
$ pogo new -m "a descriptive message"
```

# Applying migrations

To apply (unapplied) migrations, run `pogo apply`. Any previously run
migrations will be skipped over, and any new ones will be run in (topological,
based on dependency graph) order.

Supported flags:

- `-d, --database` optional database dsn to connect to, if not provided will
  fall back to configuration.
- `--dotenv, --no-dotenv` load environment from local `.env` file. (defaults to
  `--no-dotenv`)

## Marking a migration as applied

In some scenarios a migration will be added to track changes to the database
that might have been made on the fly as part of a fix. To maintain history and
keep later databases in sync. In this scenario, the migration does not need to
be applied, as such `pogo mark` will step through unapplied migrations and
confirm which ones to mark as applied.

- `-d, --database` optional database dsn to connect to, if not provided will
  fall back to configuration.
- `--dotenv, --no-dotenv` load environment from local `.env` file. (defaults to
  `--no-dotenv`)
- `--interactive, --no-interactive` confirm all changes. (defaults to `--interactive)

# Rolling back migrations

To rollback (applied) migrations, run `pogo rollback`. By default the most
recently applied migration will be rolled back.

Supported flags:

- `-c, --count` number of migrations to rollback. (defaults to `1`)
- `-d, --database` optional database dsn to connect to, if not provided will
  fall back to configuration.
- `--dotenv, --no-dotenv` load environment from local `.env` file. (defaults to
  `--no-dotenv`)

## Marking a migration as rolled back

To flag a migration as rolled back (without actually rolling back), `pogo
unmark` will mark a migration as unapplied.

- `-d, --database` optional database dsn to connect to, if not provided will
  fall back to configuration.
- `--dotenv, --no-dotenv` load environment from local `.env` file. (defaults to
  `--no-dotenv`)

# View migration status

`pogo history` will list available migrations. Each migration will be prefixed
with one of U (unapplied) or A (applied), as well as the migration format `sql` or `py`.

- `--unapplied, --no-applied` show only unapplied migrations. (defaults to `--no-unapplied`)
- `--simple, --no-simple` show history as raw data, instead of a pretty printed table. (defaults to `--no-simple`)
- `-d, --database` optional database dsn to connect to, if not provided will
  fall back to configuration.
- `--dotenv, --no-dotenv` load environment from local `.env` file. (defaults to
  `--no-dotenv`)

`pogo history` can be useful in docker containers to prevent start up of an
application until migrations are completed, i.e. checking that there are no
unapplied migrations.

```bash
$ pogo history --unapplied --simple | wc -l
```

# Managing migrations

These commands are mostly experimental and should be used with caution.  They
have been tested on multiple projects but it is likely not all edge cases have
been found yet.  Please raise any issues you find when using them in github.

## Remove a migration from the dependency chain

Remove a specific migration from the dependency chain. This can be useful to
remove hotfixes or data migrations that are only needed in live environments,
but not in newly deployed environments.

For example a migration to clean up
legacy data is not needed in a fresh environment and can be removed once all
live databases have been updated.

Supported flags:

- `-m, --migrations-location` defines the name of the migrations folder
  (defaults to `./migrations`)
- `--backup` keep any removed files with `.bak` suffix.

## Squashing migrations

`pogo squash` will perform a best effort to iterate through all migrations, and
detect where multiple migrations can be condensed into a single migration.

Python migrations and non transaction based transactions are skipped by
default.

Statements in sql migrations are grouped by table, and applied in the order
tables where discovered.

Rollback statements follow the reverse logic, the last table discovered is
grouped first.

Supported flags:

- `-m, --migrations-location` defines the name of the migrations folder
  (defaults to `./migrations`)
- `--backup` keep any removed files with `.bak` suffix.
- `--source` add a comment to each extracted statement with the source
  migration id.
- `--update-prompt` prompt before inclusion of an update statement, the
  previous and following statements are included in the prompt. This is helpful
  for removing `NULL, update, NOT NULL` flows that are not required in a
  squashed migration.
- `--skip-prompt` prompt before skipping files that can not be squashed (python
  migrations, non transaction migrations). Allows for removal of unnecessary
  migrations.

## Cleaning up backups

`pogo clean` will remove `.bak` files created during `remove` and `squash`
commands.

Supported flags:

- `-m, --migrations-location` defines the name of the migrations folder
  (defaults to `./migrations`)

# Validate migration sql

`pogo validate` will iterate through all migrations and do a best effort
attempt to validate each sql statement to report any potential issues, like
using reserved keywords as table names without quoting etc.

Supported flags:

- `-m, --migrations-location` defines the name of the migrations folder
  (defaults to `./migrations`)
