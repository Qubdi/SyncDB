# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**Qubdi-SyncDB** is a Python ETL library (`pip install Qubdi-SyncDB`) for moving tabular data between MSSQL, PostgreSQL, MySQL, SQLite, and local files (CSV, Parquet, Excel, Pickle). The package is published under the `syncdb` namespace but lives in the `Library/` source directory.

## Commands

```bash
# Install in editable mode with all dependencies
pip install -e ".[dev]"

# Run the default test suite (unit + component tests, no live DB required)
pytest

# Run a single test file or test function
pytest Tests/Library/Components/sync/test_sync_modes.py
pytest Tests/Library/Components/sync/test_sync_modes.py::TestAppendMode::test_basic_append

# Run with readable live output (colored blocks per test)
pytest --syncdb-live-output

# Run with full detail including SyncDB progress bars
pytest --syncdb-live-output-detail

# Run database-to-database integration tests (requires Docker; see Tests/DataBase/)
pytest Tests/Library/DatabaseToDatabase/

# Lint
ruff check Library/

# Format
ruff format Library/

# Type check
mypy Library/

# Measure coverage against the 90% gate (currently ~93%)
pytest Tests/Library/Components --cov=Library

# Install local quality hooks (ruff + commit-message format on commit;
# mypy + component tests on push) — installs all hook types automatically
pre-commit install

```

## Architecture

### Source layout

The package source is in `Library/` but installed as the `syncdb` namespace (mapped in `pyproject.toml`). The public API is exclusively what `Library/__init__.py` exports via `__all__`.

```
Library/
  __init__.py        # stable public API surface
  config.py          # DatabaseConfig (frozen dataclass) + engine normalization
  connections.py     # create_connector() factory — maps engine string → connector class
  connectors/
    base.py          # BaseConnector ABC — the contract all engines must implement
    mssql.py / postgres.py / mysql.py / sqlite.py
  type_mapping.py    # Column dataclass + SchemaMapper (cross-engine type translation)
  sql.py             # identifier quoting, WHERE/ORDER BY builders
  files.py           # FileTransfer — file ↔ database, FileFormat enum
  progress.py        # ProgressReporter, ProgressMode
  sync/
    core.py          # SyncDB — the main orchestration class
    models.py        # TransferMode enum, TableSyncResult dataclass
    watermark.py     # high-watermark tracking (JSON file persistence)
    quality.py       # expectation validation
    reporting.py     # summary output
    inference.py     # column type inference from Python values (file imports)
    staging.py       # staging-table helpers for APPEND_STAGING mode
    retry.py         # retry logic
```

### Key design rules

**Layering**: `SyncDB` (core.py) expresses *workflow policy* only — it coordinates connectors, schema mapping, batching, retries, and file IO. Engine-specific SQL stays inside the connector subclasses. Don't put SQL syntax in `core.py`.

**Adding a new engine**: five files must be updated in the same commit — `connectors/<engine>.py`, `connectors/__init__.py`, `connections.py`, `config.py` (aliases + default port/schema), and `type_mapping.py` (a new `_to_<engine>()` method on `SchemaMapper`). The checklist is in `connections.py`'s module docstring and `base.py`'s class docstring.

**Connector contract** (`BaseConnector`): each subclass sets three class-level attributes (`engine`, `quote_char`, `placeholder`) that drive SQL generation. `connect()` must be idempotent. `execute_query()` must auto-commit DML/DDL outside a transaction. `fetch_batches()` must use `cursor.fetchmany()`, never `fetchall()`. `upsert_batch()` should use native syntax (ON CONFLICT / MERGE / ON DUPLICATE KEY UPDATE) rather than the portable delete+insert fallback in the base class.

**`DatabaseConfig`**: frozen dataclass — engine is normalized to a canonical string in `__post_init__`. Accepts `connection_string` (passed through verbatim) or `host+database+user` fields. Credentials should come from `DatabaseConfig.from_env()` in production.

**Transfer modes** (`TransferMode` enum): `append`, `insert_only`, `upsert`, `full_refresh`, `append_staging`, `snapshot`, `soft_delete`. Each has distinct semantics; see `models.py` docstrings.

**Type mapping**: `SchemaMapper` translates `Column` objects between engine type systems. PostgreSQL is the internal canonical type representation — `inference.py` uses Postgres names as the intermediate form before mapping to the target engine.

### Tests

- `Tests/Library/Components/` — fast unit tests, no live database needed.
- `Tests/Library/DatabaseToDatabase/` — integration tests that run real cross-engine syncs; require the Docker stack in `Tests/DataBase/` to be running.
- `conftest.py` (root) — adds `--syncdb-live-output` and `--syncdb-live-output-detail` pytest options.
- `Tests/Library/DatabaseToDatabase/helpers.py` and `parameters.py` — shared fixtures and parameterization for cross-engine test classes.

## Skills

Use these skills at the right moments rather than waiting to be asked:

| Skill | When to use |
|---|---|
| `/run` | After any change to a connector, `core.py`, or `files.py` — run the component test suite with `--syncdb-live-output` to confirm real behavior, not just type correctness. |
| `/code-review` | Before any PR that touches batching logic, upsert/delete semantics, watermark persistence, or `TransferMode` behavior — correctness bugs here are silent data loss. |
| `/security-review` | When touching `sql.py` (identifier quoting), `config.py` (credential handling), or any connector's `execute_query` / `insert_batch` — SQL injection and credential leakage are the primary risk surface. |
| `/verify` | After adding a new connector or transfer mode, and before merging integration test changes — confirm the Docker-based cross-engine tests actually pass end-to-end. |
| `/simplify` | After completing a feature in `sync/core.py` — the orchestration layer accumulates complexity quickly; a simplification pass keeps the workflow policy readable. |
