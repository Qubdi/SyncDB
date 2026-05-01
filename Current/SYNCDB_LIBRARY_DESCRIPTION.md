# SyncDB Library Description

`SyncDB` is a Python data synchronization library for moving data between Microsoft SQL Server, PostgreSQL, MySQL, and local data files. It supports database-to-database synchronization, SQL-based data downloads, and file-based data uploads with batching, schema handling, progress reporting, and validation.

The library is designed for data engineering workflows where teams need to export data from a database, insert local datasets into a database, or replicate tables between MSSQL, PostgreSQL, and MySQL using append, staging, or full-refresh strategies.

## Target Design

The final library should be class-based, not only a collection of standalone functions. Database engines, sync pipelines, file import/export, progress rendering, and test infrastructure should be represented by reusable classes with clear responsibilities.

Main supported databases:

- Microsoft SQL Server
- PostgreSQL
- MySQL

Core design goals:

- Class-based public API.
- Reusable database connector classes.
- Reusable sync pipeline classes.
- Shared schema inspection and type mapping layer.
- Shared progress reporting with two rendering modes.
- Docker-based database testing for MSSQL, PostgreSQL, and MySQL.

## Current Capabilities

- Export database query results to local files.
- Import local files into database tables.
- Sync tables between MSSQL, PostgreSQL, and MySQL.
- Create PostgreSQL schemas and tables from MSSQL metadata.
- Map column types between supported database engines.
- Add and drop target columns to match source schemas.
- Transfer data in configurable batches.
- Support full-load and streaming modes.
- Support append, staging append, and full-refresh database transfer modes.
- Use database connection pooling where supported.
- Use retry handling around transfer jobs.
- Log progress bars and transfer summaries.
- Validate transferred row counts.
- Run dry-run syncs before making changes.

## Supported Data Directions

### Database to Local Files

Executes `.sql` files from a directory and writes query results from MSSQL, PostgreSQL, or MySQL to local files.

### Local Files to Database

Reads local data files and inserts them into MSSQL, PostgreSQL, or MySQL tables.

### Database to Database

Synchronizes schema and transfers table data between supported database engines.

Initial priority combinations:

- MSSQL to PostgreSQL
- MSSQL to MySQL
- PostgreSQL to MSSQL
- PostgreSQL to MySQL
- MySQL to MSSQL
- MySQL to PostgreSQL

## Supported File Formats

- CSV
- Parquet
- Excel
- Pickle

Batch streaming is currently implemented for CSV and Parquet exports.

## Main Features

- SQL script based downloads.
- Directory-based batch processing.
- Engine-specific bulk insert support where available.
- Pandas and DB-driver batch insert support.
- Automatic target table creation for batch inserts.
- Optional fresh insert mode with table drop and recreate.
- Optional column selection during file import.
- Schema creation for supported databases.
- Target table creation from source metadata.
- Primary key detection from source database metadata.
- Primary key override support.
- Incremental-style append using delete-and-insert by primary key.
- Staging-table transfer mode.
- Full refresh mode using truncate-and-load.
- Parameterized filtering support.
- Optional ordering for deterministic reads.
- Transfer metrics for rows, columns, batches, file size, and elapsed time.
- Dry-run mode for previewing schema and transfer actions.
- Two progress bar modes:
  - one-line updateable progress for interactive terminals;
  - several-line log progress for scripts, logs, notebooks, and schedulers.

## README / PyPI Description

`SyncDB` is a lightweight Python ETL and database synchronization toolkit for SQL Server, PostgreSQL, MySQL, and local analytical files. It helps data teams download query results, upload local files, and replicate tables between databases with schema synchronization, batch loading, progress reporting, Docker-backed testing, and validation.

## Current Module Overview

### `mssql_local.py`

Exports data from Microsoft SQL Server to local files. It scans a directory for `.sql` scripts, executes each script against MSSQL, and saves the result in the requested format.

Key capabilities:

- Executes multiple SQL files from a directory.
- Supports CSV, Parquet, Excel, and Pickle output.
- Supports full in-memory downloads.
- Supports batch streaming for CSV and Parquet.
- Attempts row-count discovery for progress tracking.
- Handles SQL Server `TOP N` limits in progress calculations.
- Logs export summaries with row count, column count, file size, and duration.

### `local_mssql.py`

Imports local files into Microsoft SQL Server tables. It can use SQL Server `BULK INSERT` for CSV files or pandas/pyodbc batch inserts for supported file formats.

Key capabilities:

- Reads CSV, Parquet, Excel, and Pickle files.
- Supports CSV `BULK INSERT`.
- Supports pandas/pyodbc batch inserts.
- Can insert one file or all files in a directory.
- Can create a target table from dataframe column types.
- Supports optional fresh insert behavior.
- Supports optional column filtering.
- Logs import summaries with rows, columns, file size, and duration.

### `mssql_postgresql.py`

Synchronizes data from Microsoft SQL Server to PostgreSQL. It includes schema discovery, PostgreSQL schema/table creation, column synchronization, and batch data transfer.

Key capabilities:

- Maps MSSQL data types to PostgreSQL data types.
- Discovers MSSQL columns and primary keys.
- Creates PostgreSQL schemas when missing.
- Creates PostgreSQL tables from MSSQL metadata.
- Synchronizes columns by adding missing columns and dropping extra columns.
- Supports append, append staging, and full refresh transfer modes.
- Uses PostgreSQL connection pooling.
- Transfers rows in batches using `psycopg2.extras.execute_values`.
- Supports parameterized filters.
- Supports optional ordering.
- Supports primary key override.
- Supports dry-run execution.
- Logs schema sync and transfer reports.

## Transfer Modes

### `append`

Transfers source rows in batches. If primary keys are available, matching rows are deleted from PostgreSQL first, then the incoming rows are inserted. This works like a batch upsert strategy.

### `append_staging`

Loads all source rows into an unlogged PostgreSQL staging table, deletes matching primary-key rows from the main table once, then inserts all staging rows into the main table. The staging table is dropped after completion.

### `full_refresh`

Truncates the target PostgreSQL table and inserts all source rows again.

## Suggested Public API

```python
from syncdb import SyncDB, DatabaseConfig, ProgressMode
```

Example:

```python
from syncdb import SyncDB, DatabaseConfig, ProgressMode

source = DatabaseConfig(
    engine="mssql",
    connection_string="Driver={ODBC Driver 17 for SQL Server};Server=...;",
)

target = DatabaseConfig(
    engine="postgresql",
    connection_string="postgresql://user:password@host:5432/db",
)

sync = SyncDB(
    source=source,
    target=target,
    batch_size=10000,
    progress_mode=ProgressMode.ONE_LINE,
)

sync.sync_tables(
    tables={
        "Applications": {
            "source": "dbo.Applications",
            "destination": "public.applications",
            "mode": "append_staging",
            "primary_key": ["ApplicationId"],
        }
    },
)
```

## Recommended Class Design

### `DatabaseConfig`

Stores database engine, connection string, credentials, driver options, schema defaults, timeout, and pool settings.

### `BaseConnector`

Defines the shared connector contract for supported databases:

- `connect()`
- `close()`
- `execute_query()`
- `fetch_batches()`
- `insert_batch()`
- `bulk_insert()`
- `get_columns()`
- `get_primary_keys()`
- `table_exists()`
- `create_schema()`
- `create_table()`
- `truncate_table()`

### `MSSQLConnector`

SQL Server implementation using `pyodbc`.

### `PostgresConnector`

PostgreSQL implementation using `psycopg2` or another PostgreSQL driver.

### `MySQLConnector`

MySQL implementation using `mysql-connector-python`, `pymysql`, or SQLAlchemy-compatible drivers.

### `SchemaMapper`

Maps column types between MSSQL, PostgreSQL, and MySQL.

### `SyncDB`

Main high-level class used by library users. It coordinates schema sync, table transfer, file export, file import, progress display, validation, and dry-run execution.

### `FileTransfer`

Handles local file reads and writes for CSV, Parquet, Excel, and Pickle.

### `ProgressReporter`

Handles progress output with two modes:

- `one_line`: updates a single terminal line in place.
- `multi_line`: writes a separate progress line per update for logs and schedulers.

## Progress Bar Modes

### `one_line`

Best for local interactive terminal runs. The progress bar updates in place on the same line.

Example:

```text
public.applications [████████████████░░░░░░░░░░░░░░░░░░░░] 40% (40,000/100,000)
```

### `multi_line`

Best for log files, scheduled jobs, CI runs, notebooks, and environments where carriage-return updates are not readable.

Example:

```text
public.applications 10% (10,000/100,000)
public.applications 20% (20,000/100,000)
public.applications 30% (30,000/100,000)
```

## Docker-Based Testing

Testing should use Docker containers for all main supported databases:

- MSSQL test container.
- PostgreSQL test container.
- MySQL test container.

Recommended test approach:

- Use `docker-compose.yml` for local integration tests.
- Start all database containers before running integration tests.
- Seed source tables with known test data.
- Run sync jobs between every supported database pair.
- Validate row counts, schema creation, type mapping, primary keys, nullability, and transfer modes.
- Keep pure unit tests separate from Docker integration tests.

Example test services:

```text
tests/docker-compose.yml
  mssql
  postgres
  mysql
```

## Important Gaps Before Packaging

- `mssql_postgresql.py` depends on global variables `MSSQL_CONN_STR` and `PG_CONN_STR`, but they are not defined in the file. A proper library should pass connection strings or config objects explicitly.
- The current modules are script-style and should be reorganized into a package with clear public APIs.
- Encoding/logging symbols appear corrupted in some strings and should be cleaned.
- Tests are missing.
- Configuration should be centralized.
- CSV bulk insert mode assumes target table readiness and does not fully recreate schema after `fresh_insert=True`.
- Error handling and validation should be normalized across all workflows.
- MySQL support must be added.
- The class-based API must be implemented and old function-level workflows should become compatibility wrappers or internal methods.
- Docker integration tests must be added for MSSQL, PostgreSQL, and MySQL.
- Progress output should support both one-line updateable mode and multi-line logging mode.

## Recommended Package Structure

```text
syncdb/
  __init__.py
  config.py
  connections.py
  files.py
  progress.py
  type_mapping.py
  connectors/
    __init__.py
    base.py
    mssql.py
    postgres.py
    mysql.py
  pipelines/
    __init__.py
    database_to_local.py
    local_to_database.py
    database_to_database.py
  tests/
    docker-compose.yml
    test_type_mapping.py
    test_query_builders.py
    test_config_validation.py
    integration/
      test_mssql_postgres.py
      test_mssql_mysql.py
      test_postgres_mysql.py
```

## Short Positioning

`SyncDB` should be positioned as a practical class-based ETL/sync helper for Python teams that need controlled movement of tabular data between SQL Server, PostgreSQL, MySQL, and local files without introducing a heavy orchestration framework.
