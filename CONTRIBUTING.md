# Contributing to SyncDB

This guide is for the founding development team. It covers the project layout,
how to run tests, and how to extend the library.

---

## Project Layout

```text
SyncDB/
├── Library/                   # Package source (installed as "syncdb")
│   ├── __init__.py            # Public API surface
│   ├── config.py              # DatabaseConfig, Engine enum, engine aliases
│   ├── connections.py         # Connector factory (create_connector)
│   ├── files.py               # FileTransfer — CSV, Parquet, Excel, Pickle
│   ├── progress.py            # ProgressReporter — ONE_LINE / MULTI_LINE / NONE
│   ├── sql.py                 # SQL-building helpers, identifier validation
│   ├── sync.py                # SyncDB orchestrator — main entry point
│   ├── type_mapping.py        # SchemaMapper — cross-engine column type mapping
│   ├── connectors/
│   │   ├── base.py            # BaseConnector — abstract contract + shared logic
│   │   ├── mssql.py           # pyodbc-based MSSQL connector
│   │   ├── postgres.py        # psycopg2-based PostgreSQL connector
│   │   └── mysql.py           # mysql-connector-python / pymysql connector
│   └── pipelines/             # Thin re-export aliases (reserved for future expansion)
├── Tests/
│   ├── Library/               # Unit tests (no DB required)
│   │   ├── test_config_validation.py
│   │   ├── test_type_mapping.py
│   │   ├── test_query_builders.py
│   │   ├── test_progress.py
│   │   ├── test_sync.py       # Uses MemoryConnector — no real DB needed
│   │   └── test_file_transfer.py
│   └── DataBase/              # Docker integration environment
│       ├── docker-compose.yml
│       ├── seed/              # SQL seed scripts for MSSQL, PostgreSQL, MySQL
│       └── images/            # Custom Dockerfiles + data-check validator
├── Current/                   # Legacy scripts (pre-refactor, kept for reference)
├── pyproject.toml             # Package metadata and setuptools config
├── requirements.txt           # Full dev dependency set
├── README.md                  # User-facing documentation
└── CONTRIBUTING.md            # This file
```

---

## Development Setup

```bash
# 1. Clone the repository
git clone <repo-url>
cd SyncDB

# 2. Create and activate a virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# 3. Install the package in editable mode with all dev dependencies
pip install -e .
pip install -r requirements.txt
```

The `pip install -e .` step is required. It maps the `Library/` directory to the
`syncdb` package name via the `[tool.setuptools.package-dir]` config in
`pyproject.toml`. Without it, `import syncdb` will fail.

---

## Running Tests

### Unit tests (no database required)

```bash
pytest
```

### Integration tests (requires Docker)

Start the test database environment:

```bash
cd Tests/DataBase
docker compose up -d --build
```

Wait for the `Qubdi-SyncDB-data-check` container to exit with code 0 — that
confirms all three databases are seeded and row counts match. Then run the
integration test suite (to be added under `Tests/Integration/`).

To rebuild from scratch (wipes all data):

```bash
docker compose down -v
docker compose up -d --build
```

**Connection details for the test databases:**

| Engine | Host | Port | User | Password | Database |
| --- | --- | --- | --- | --- | --- |
| MSSQL | localhost | 11433 | admin | admin | syncdb_test |
| PostgreSQL | localhost | 15432 | admin | admin | syncdb_test |
| MySQL | localhost | 13306 | admin | admin | syncdb_test |

Browser IDEs: pgAdmin at `http://localhost:18080`, phpMyAdmin at
`http://localhost:18081`, CloudBeaver at `http://localhost:18082`.

---

## Adding a New Connector

1. Create `Library/connectors/<engine>.py` implementing `BaseConnector`.
   - Set the class attributes `engine`, `quote_char`, and `placeholder`.
   - Lazy-import the driver inside `connect()` so the driver is optional.
2. Register it in `Library/connectors/__init__.py`.
3. Add it to the `create_connector` factory in `Library/connections.py`.
4. Add engine aliases to `_ENGINE_ALIASES` in `Library/config.py`.
5. Add default port and schema to `_DEFAULT_PORTS` / `_DEFAULT_SCHEMAS`.
6. Extend `SchemaMapper` in `Library/type_mapping.py` with `_to_<engine>`.
7. Add seed data under `Tests/DataBase/seed/<engine>/` and a new service in
   `docker-compose.yml` following the existing pattern.

---

## Adding a New File Format

1. Add a value to `FileFormat` in `Library/files.py`.
2. Handle the new format in `FileTransfer.read` and `FileTransfer.write`.
3. Map any non-obvious file extensions in `_resolve_format`.
4. Add a test case in `Tests/Library/test_file_transfer.py`.

---

## Key Design Decisions

**`Library/` is named Library, not `syncdb/`.**
The `pyproject.toml` remaps `Library/` to the `syncdb` package name on install.
This keeps source layout flexible while keeping the import name consistent.

**Connectors hold a single bare connection.**
No connection pooling is done inside the connectors; `pool_min`/`pool_max` on
`DatabaseConfig` are reserved for a future pooled-connector variant. The current
connectors are appropriate for batch ETL (one long-lived connection per sync).

**`append_staging` is not yet differentiated from `append`.**
True staging requires connector-level support (CREATE TEMP TABLE, bulk copy,
swap). When implementing it, add a `upsert_via_staging` method to
`BaseConnector` and override in each concrete connector.

**`delete_matching_rows` builds one `OR`-joined DELETE per batch.**
This is simple and correct but can be slow for large `batch_size`. Connectors
can override the method with a more efficient engine-specific strategy
(e.g., `DELETE ... WHERE pk IN (?)`).

**Type mapping is best-effort.**
`SchemaMapper` maps common types conservatively (widening rather than losing
data). Engine-specific types with no cross-engine equivalent (e.g., PostgreSQL
`inet`, `point`, MySQL `enum`) fall back to `text` / `longtext`.
