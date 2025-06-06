# Changelog

## v0.3.3 (released 2025-05-27)

### Features and Improvements

- Add configuration support for excluding specific migrations from the squash command. [[46](https://github.com/NRWLDev/pogo-migrate/issues/46)] [[14156d3](https://github.com/NRWLDev/pogo-migrate/commit/14156d32d86fea325377fd7bc8a1541d9e0cd3fc)]

### Bug fixes

- Add support for TRIGGER statements in squash. [[44](https://github.com/NRWLDev/pogo-migrate/issues/44)] [[c831d5f](https://github.com/NRWLDev/pogo-migrate/commit/c831d5f6b2752545e20cc0710f3b49b55382a63d)]
- Read migrations dir from config, continue to support manual override in squash, remove, and validate. [[45](https://github.com/NRWLDev/pogo-migrate/issues/45)] [[0101a65](https://github.com/NRWLDev/pogo-migrate/commit/0101a65aa3584d28f2307fdb5b477a840580a884)]

## v0.3.2 (released 2025-05-17)

### Features and Improvements

- Support optional database_config configuration for pure in code usage. [[42](https://github.com/NRWLDev/pogo-migrate/issues/42)] [[9f52823](https://github.com/NRWLDev/pogo-migrate/commit/9f52823140851c01c86abcf8dce53f77f2ca209d)]

## v0.3.1 (released 2025-05-14)

### Bug fixes

- Ensure builtin tables are all created in the expected (public) schema. [[40](https://github.com/NRWLDev/pogo-migrate/issues/40)] [[c367270](https://github.com/NRWLDev/pogo-migrate/commit/c367270e5d5a34de67a3fc32bc75316406520aa4)]

## v0.3.0 (released 2025-05-13)

### Features and Improvements

- **Breaking** Reimplement the apply/rollback interface to better support usage from within python. [[38](https://github.com/NRWLDev/pogo-migrate/issues/38)] [[a9a081e](https://github.com/NRWLDev/pogo-migrate/commit/a9a081ea44b8e3301cb5ca057876985a6482086a)]

### Bug fixes

- Load dotenv by default to remove additional arguments required when running locally. [[e9a2561](https://github.com/NRWLDev/pogo-migrate/commit/e9a2561a4546aff48a9cfefb7cc133e1c0d394e6)]

### Miscellaneous

- Update sqlparse library with EXTENSION support [[37](https://github.com/NRWLDev/pogo-migrate/issues/37)] [[79fe724](https://github.com/NRWLDev/pogo-migrate/commit/79fe724229f682c2d8b1f85f6957856c7840c02c)]

## v0.2.9 (released 2025-05-05)

### Documentation

- Add documentation url for pypi. [[b3c15bc](https://github.com/NRWLDev/pogo-migrate/commit/b3c15bcbbd975d5d300a225d7ae703650fe76ff8)]

## v0.2.8 (released 2025-05-05)

### Bug fixes

- Ensure urls render in pypi. [[bd908ba](https://github.com/NRWLDev/pogo-migrate/commit/bd908ba625adb38b6471d0252223f0d6f5b904bf)]

## v0.2.7 (released 2025-04-16)

### Bug fixes

- Detect incorrectly defined multiple dependencies in sql migrations. [[33](https://github.com/NRWLDev/pogo-migrate/issues/33)] [[1f76338](https://github.com/NRWLDev/pogo-migrate/commit/1f76338f7a6cce6ec13bfbc7ea6944656a56597b)]
- Detect multiple heads when determining dependencies for a new migration. [[34](https://github.com/NRWLDev/pogo-migrate/issues/34)] [[54438d6](https://github.com/NRWLDev/pogo-migrate/commit/54438d6090bc1ab09d2aea5183a3edc167a8f5d6)]

### Miscellaneous

- Ensure test db connection is closed during tests. [[1081ebb](https://github.com/NRWLDev/pogo-migrate/commit/1081ebb2107f304c62017a43eb73ebc0310a5e04)]

## v0.2.6 (released 2024-12-23)

### Bug fixes

- Ensure manually created database connections are closed during test fixtures. [[d483224](https://github.com/NRWLDev/pogo-migrate/commit/d483224c4c8c4400f720449fe743c23940d49b8b)]

## v0.2.5 (released 2024-11-28)

### Features and Improvements

- Support mark/unmark of a single migration. [[f68abac](https://github.com/NRWLDev/pogo-migrate/commit/f68abacbc8fc9395d34bc46ec56073e4e1e90ea5)]

## v0.2.4 (released 2024-10-24)

### Bug fixes

- Handle create/drop schema parsing. [[dbe3d20](https://github.com/NRWLDev/pogo-migrate/commit/dbe3d20a7823782d48da32a676ad5c2bf07e24b0)]

### Miscellaneous

- Clean up more type warnings [[20f464f](https://github.com/NRWLDev/pogo-migrate/commit/20f464f3dd17e547dd21eeed58c66f30109b49f7)]

## v0.2.3 (released 2024-10-14)

### Miscellaneous

- Clean up external type hints [[bb608a4](https://github.com/NRWLDev/pogo-migrate/commit/bb608a4ff0cc0aa30e3c57b63d87deed52016aae)]

## v0.2.2 (released 2024-10-14)

### Bug fixes

- Add py.typed [[29c0c3a](https://github.com/NRWLDev/pogo-migrate/commit/29c0c3ae4ba7b5c08de8034fc97e262d2c6c9958)]

## v0.2.1 (released 2024-10-02)

### Bug fixes

- CLI internals need to be async, revert invalid previous fix. [[aa34f1f](https://github.com/NRWLDev/pogo-migrate/commit/aa34f1f648c32e07505c28472e285d3b8760dc93)]

## v0.2.0 (released 2024-10-01)

### Features and Improvements

- **Breaking** Default to sql migrations with optin for python [[31](https://github.com/NRWLDev/pogo-migrate/issues/31)] [[25a7445](https://github.com/NRWLDev/pogo-migrate/commit/25a7445564138ab83a98a9a6392039551cb8dbf5)]

### Miscellaneous

- Update pre-commit ruff and apply fixes. [[54e2521](https://github.com/NRWLDev/pogo-migrate/commit/54e2521d2a40126eccb4d26c499aa82617998b07)]

## v0.1.1 (released 2024-09-09)

### Miscellaneous

- Migrate from poetry to uv for dependency and build management [[e94e595](https://github.com/NRWLDev/pogo-migrate/commit/e94e59573c0b3d4e7ec7da03bde55053dd687239)]

## v0.1.0 (released 2024-09-06)

### Features and Improvements

- Add validate command to check for unquoted reserved keywords etc. [[16](https://github.com/NRWLDev/pogo-migrate/issues/16)] [[159d50a](https://github.com/NRWLDev/pogo-migrate/commit/159d50a46bbc2b0a84e72628c8038213b6d22371)]
- Use sqlglot as the default parsing library, fall back to sqlparse for aggregations. [[17](https://github.com/NRWLDev/pogo-migrate/issues/17)] [[1e6d5d3](https://github.com/NRWLDev/pogo-migrate/commit/1e6d5d3225d9a1dc62cc6e04127b33a51e32d3b6)]
- Use unittest.AsyncMock to extract statements from python migrations for validation. [[20](https://github.com/NRWLDev/pogo-migrate/issues/20)] [[d16643f](https://github.com/NRWLDev/pogo-migrate/commit/d16643f5390e77252f1ed42814ea4813b8b98351)]
- Implement custom messaging object to avoid using logging module. [[26](https://github.com/NRWLDev/pogo-migrate/issues/26)] [[0874318](https://github.com/NRWLDev/pogo-migrate/commit/0874318251fd8f607bae9cdb2d9ecbee41d757af)]

### Documentation

- Expand usage documentation to all commands, including new experimental commands [[22](https://github.com/NRWLDev/pogo-migrate/issues/22)] [[cd248e5](https://github.com/NRWLDev/pogo-migrate/commit/cd248e51bf8b953b601e4c1f09bc1226bf759d45)]

### Miscellaneous

- Update changelog-gen and related configuration. [[87457a6](https://github.com/NRWLDev/pogo-migrate/commit/87457a67be6ae5b1ab9a10be39d439fc30083026)]

## v0.0.22 (released 2024-08-07)

### Features and Improvements

- Add support for backup flag when removing migrations. [[#21](https://github.com/NRWLDev/pogo-migrate/issues/21)] [[f4f8ba8](https://github.com/NRWLDev/pogo-migrate/commit/f4f8ba8baa756f3e2dc6f51becada138afb24881)]

## v0.0.21 (released 2024-07-24)

### Features and Improvements

- Rework statement parsing for `pogo squash` [[#15](https://github.com/NRWLDev/pogo-migrate/issues/15)] [[3229404](https://github.com/NRWLDev/pogo-migrate/commit/3229404449dfd9f83dcca82ee1ebb29eabfcaf52)]

### Bug fixes

- Relax rtoml dependency constraint [[b7ac4f6](https://github.com/NRWLDev/pogo-migrate/commit/b7ac4f64d4f1b201b74acc38cdc99503ca0d734f)]

## v0.0.20 (released 2024-07-18)

### Bug fixes

- Remove sqlglot as a dependency until it can support all use cases. [[5b706a5](https://github.com/NRWLDev/pogo-migrate/commit/5b706a5c76f281b19235a84344560a393bc028b5)]
- Add additional cli output tests. [[363d8e5](https://github.com/NRWLDev/pogo-migrate/commit/363d8e58a5d7fd386119420cf7abfbecf0beccd8)]

## v0.0.19 (released 2024-07-17)

### Features and Improvements

- Add experimental squash command. [[#13](https://github.com/NRWLDev/pogo-migrate/issues/13)] [[3ebfb0a](https://github.com/NRWLDev/pogo-migrate/commit/3ebfb0a12f1355aa28d81ca1002140005d5ebd9a)]

## v0.0.18 (released 2024-05-28)

### Features and Improvements

- Ability to flag a specific migration to not be run in a transaction, support INDEX CONCURRENTLY. [[7ebdbf7](https://github.com/NRWLDev/pogo-migrate/commit/7ebdbf759278841f0e328d5b70b2f1dee0cba9fa)]

## v0.0.17 (released 2024-05-24)

### Bug fixes

- Ignore comments, not statements preceded by a comment. [[57fcede](https://github.com/NRWLDev/pogo-migrate/commit/57fcede53eca0dd782e94684e748a606c9fcbc3d)]

## v0.0.16 (released 2024-05-24)

### Bug fixes

- Skip trailing comments as they break db.execute [[#11](https://github.com/NRWLDev/pogo-migrate/issues/11)] [[9d6c674](https://github.com/NRWLDev/pogo-migrate/commit/9d6c6747abe251ebf5e5402a137e1e607ba901c9)]

## v0.0.15 (released 2024-05-22)

### Bug fixes

- Reduce verbosity of apply/rollback to prevent excess information in failed tests [[#9](https://github.com/NRWLDev/pogo-migrate/issues/9)] [[2b1da1c](https://github.com/NRWLDev/pogo-migrate/commit/2b1da1c4ed90aa08d13b9eb49e86ccb14aa3cd97)]

## v0.0.14 (released 2024-04-16)

### Features and Improvements

- Support simple output in history command, and only showing unapplied migrations. [[d243c39](https://github.com/NRWLDev/pogo-migrate/commit/d243c39f2935f3af1105d0a808818dbe4f04af17)]

## v0.0.13 (released 2024-04-11)

### Bug fixes

- Abort migration data copy, if no yoyo table found. [[915dfc2](https://github.com/NRWLDev/pogo-migrate/commit/915dfc2dfaac791986a52d120b693fa87e7c3539)]

## v0.0.12 (released 2024-04-08)

### Miscellaneous

- Relax typer versioning [[e2c5a8e](https://github.com/NRWLDev/pogo-migrate/commit/e2c5a8e3d893dedfb9c3d9be783518c6e5d91714)]

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
