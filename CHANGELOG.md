# Changelog

## v0.0.5 (released 2024-03-07)

### Bug fixes

- Remove usage of ParamSpec as not available in 3.9

## v0.0.4 (released 2024-03-07)

### Bug fixes

- Clean up error handling and transaction usage in cli.

## v0.0.3 (released 2024-03-07)

### Bug fixes

- Expose bad migration information on failed apply/rollback.

## v0.0.2 (released 2024-03-06)

### Bug fixes

- Fix issue with migrations loading non migration files in migration

## v0.0.1 (released 2024-03-06)

### Features and Improvements

- Support async migration support for asyncpg.
- Support migration from existing yoyo migrations.
- Support marking existing migrations as applied.
- Support pyproject.toml configuration.
