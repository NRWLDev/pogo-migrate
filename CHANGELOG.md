# Changelog

## v0.0.11 (released 2024-03-26)

### Bug fixes

- Fix issue where a single python dependency would be rendered by comma separated characters. [2a6a773](https://github.com/NRWLDev/pogo-migrate/commit/2a6a773e803609c393942bcd6523f44ec7f4e140)

## v0.0.10 (released 2024-03-13)

### Bug fixes

- drop support for database_env_key [1d96e4f](https://github.com/NRWLDev/pogo-migrate/commit/1d96e4fcca822164f47ae65f5351d09054036c86)

### Documentation

- Expand README documentation. [8dfee98](https://github.com/NRWLDev/pogo-migrate/commit/8dfee9861ddde2837007b70f006d5676a419b7a2)

## v0.0.9 (released 2024-03-12)

### Features and Improvements

- Support separate database configuration env keys [#7](https://github.com/NRWLDev/pogo-migrate/issues/7) [72e7894](https://github.com/NRWLDev/pogo-migrate/commit/72e7894e9adfea024c53045fa2162c1d0d46016a)

## v0.0.8 (released 2024-03-10)]

### Bug fixes

- Bugfix python template depends, and allow pytest fixture to generate database connection based on configuration [66149a6](https://github.com/NRWLDev/pogo-migrate/commit/66149a62f46833fe6d22fde00b579d580ba2266c)]

## v0.0.7 (released 2024-03-08)]

### Features and Improvements

- Support non-interactive 'pogo mark' [[#5](https://github.com/NRWLDev/pogo-migrate/issues/5)] [[bdd24bf](https://github.com/NRWLDev/pogo-migrate/commit/bdd24bf72e85aa25f2aad7c9f7d87b66f9bf0663)]

- Support ability to just migrate yoyo migration history in db. [[#3](https://github.com/NRWLDev/pogo-migrate/issues/3)] [[ab773a6](https://github.com/NRWLDev/pogo-migrate/commit/ab773a6d4d314408af7797fd5d537759cfc171b7)]

### Bug fixes

- Add clearer messaging on missing env key [[#1](https://github.com/NRWLDev/pogo-migrate/issues/1)]

## v0.0.6 (released 2024-03-08)]

### Bug fixes

- Pick up migrations from correct location when running pogo from a subdirectory
- Drop ast/eval usage and return to closures.

## v0.0.5 (released 2024-03-07)]

### Bug fixes

- Remove usage of ParamSpec as not available in 3.9

## v0.0.4 (released 2024-03-07)]

### Bug fixes

- Clean up error handling and transaction usage in cli.

## v0.0.3 (released 2024-03-07)]

### Bug fixes

- Expose bad migration information on failed apply/rollback.

## v0.0.2 (released 2024-03-06)]

### Bug fixes

- Fix issue with migrations loading non migration files in migration

## v0.0.1 (released 2024-03-06)]

### Features and Improvements

- Support async migration support for asyncpg.
- Support migration from existing yoyo migrations.
- Support marking existing migrations as applied.
- Support pyproject.toml configuration.
