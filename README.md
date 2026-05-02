# SyncDB

Python ETL helper for moving tabular data between **Microsoft SQL Server**, **PostgreSQL**, **MySQL**, and **local files** (CSV, Parquet, Excel, Pickle) — with automatic schema creation, schema evolution, and batch progress reporting.

---

## Table of Contents

- [What SyncDB Does](#what-syncdb-does)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Connecting to Databases](#connecting-to-databases)
- [Transfer Modes](#transfer-modes) (`append`, `insert_only`, `full_refresh`, `append_staging`)
- [Syncing Tables](#syncing-tables)
- [Filtering Data](#filtering-data)
- [Schema Evolution](#schema-evolution)
- [Working with Files](#working-with-files)
- [Progress Reporting](#progress-reporting)
- [Reading Sync Results](#reading-sync-results)
- [Complete Examples](#complete-examples)
- [API Reference](#api-reference)
- [Supported File Formats](#supported-file-formats)
- [Running Tests](#running-tests)
- [Planned & Proposed Features](#planned--proposed-features)

---

## What SyncDB Does

SyncDB copies data from a **source** (database table or file) to a **destination** (database table or file). It handles:

- Creating the destination table if it does not exist
- Adding or dropping columns when the schema changes
- Chunking large tables into batches so you never load millions of rows into memory at once
- Translating data types between engines (e.g. PostgreSQL `boolean` → MSSQL `bit`)
- Showing a live progress bar while data moves

A 5-minute transfer job looks like this:

```python
from syncdb import DatabaseConfig, SyncDB

src = DatabaseConfig(engine="mssql", connection_string="...")
dst = DatabaseConfig(engine="postgresql", connection_string="...")

sync = SyncDB(source=src, target=dst)
sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
    }
})
```

That is all you need. SyncDB creates the `public.orders` table if it does not exist, maps every column type, and streams the data in batches.

---

## Installation

```bash
pip install -e .
```

Install only the database connectors and file formats you actually need:

```bash
pip install -e ".[mssql]"       # MSSQL / SQL Server
pip install -e ".[postgres]"    # PostgreSQL
pip install -e ".[mysql]"       # MySQL / MariaDB
pip install -e ".[files]"       # Parquet + Excel (requires pandas)
pip install -e ".[all]"         # Everything
```

> **CSV and Pickle** work without any extras — they use Python's standard library.

---

## Quick Start

### Copy a table from SQL Server to PostgreSQL

```python
from syncdb import DatabaseConfig, SyncDB

source = DatabaseConfig(
    engine="mssql",
    connection_string=(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost,1433;Database=sales_db;"
        "UID=admin;PWD=secret;TrustServerCertificate=yes;"
    ),
)

target = DatabaseConfig(
    engine="postgresql",
    connection_string="postgresql://admin:secret@localhost:5432/sales_db",
)

sync = SyncDB(source=source, target=target, batch_size=10_000)

results = sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "order_by": ["order_id"],
    }
})

for r in results:
    print(f"{r.destination}: {r.rows_written:,} rows written")
```

### Export a query result to a Parquet file

```python
rows = sync.export_query_to_file(
    query="SELECT * FROM dbo.orders WHERE status = 'shipped'",
    output_path="shipped_orders.parquet",
)
print(f"Exported {rows:,} rows")
```

### Load a file into a database table

```python
rows = sync.import_file_to_table(
    input_path="shipped_orders.parquet",
    destination="public.shipped_orders",
    fresh_insert=True,   # truncate before inserting
)
print(f"Inserted {rows:,} rows")
```

---

## Core Concepts

### Batching

SyncDB never loads an entire table into memory. It reads `batch_size` rows at a time from the source, writes them to the target, then reads the next batch. The default is 5,000 rows. Raise it for fast networks with plenty of RAM; lower it for slow connections or wide rows.

```python
sync = SyncDB(source=src, target=dst, batch_size=50_000)
```

### Automatic Table Creation

If the destination table does not exist, SyncDB creates it automatically by reading the source schema. You do not need to write any `CREATE TABLE` statements.

### Schema Evolution

When you run a sync on an existing table and the source schema has changed, SyncDB can:

- **Add** new columns that appear in the source but not the target (always on)
- **Drop** extra columns from the target that are no longer in the source (opt-in via `drop_extra_columns=True`)

Existing column types are never altered — this protects manually added columns and audit fields.

### Dry Run

Pass `dry_run=True` to see what SyncDB *would* do without writing any data. Schema changes are still reported but not applied.

```python
sync = SyncDB(source=src, target=dst, dry_run=True)
results = sync.sync_tables({"orders": {"source": "dbo.orders", "destination": "public.orders"}})

for r in results:
    if r.columns_added:
        print(f"Would add columns: {r.columns_added}")
    if r.columns_dropped:
        print(f"Would drop columns: {r.columns_dropped}")
    if r.table_created:
        print(f"Would create table: {r.destination}")
```

---

## Connecting to Databases

`DatabaseConfig` describes a single database connection. You can use either a connection string or individual parameters.

### Option 1: Connection String

```python
from syncdb import DatabaseConfig

# SQL Server / MSSQL
mssql = DatabaseConfig(
    engine="mssql",
    connection_string=(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=db.example.com,1433;Database=mydb;"
        "UID=sa;PWD=Password123;TrustServerCertificate=yes;"
    ),
)

# PostgreSQL
pg = DatabaseConfig(
    engine="postgresql",
    connection_string="postgresql://user:password@db.example.com:5432/mydb",
)

# MySQL
mysql = DatabaseConfig(
    engine="mysql",
    connection_string="mysql://user:password@db.example.com:3306/mydb",
)
```

### Option 2: Individual Parameters

```python
pg = DatabaseConfig(
    engine="postgresql",
    host="db.example.com",
    port=5432,
    database="mydb",
    user="admin",
    password="secret",
    connect_timeout=60,
)

mssql = DatabaseConfig(
    engine="mssql",
    host="db.example.com",
    database="mydb",
    user="sa",
    password="Password123",
)
```

### Engine Name Aliases

SyncDB accepts several spellings for each engine:

| You write | Resolved to |
| --- | --- |
| `"mssql"`, `"sqlserver"`, `"sql_server"` | `"mssql"` |
| `"postgresql"`, `"postgres"`, `"pg"` | `"postgresql"` |
| `"mysql"` | `"mysql"` |

### Default Ports and Schemas

| Engine | Default Port | Default Schema |
| --- | --- | --- |
| MSSQL | 1433 | `dbo` |
| PostgreSQL | 5432 | `public` |
| MySQL | 3306 | *(uses database name)* |

### DatabaseConfig Parameters

| Parameter | Description | Default |
| --- | --- | --- |
| `engine` | Database engine (see aliases above) | **required** |
| `connection_string` | Full DSN or URL | `None` |
| `host` | Server hostname | `None` |
| `port` | Server port | engine default |
| `database` | Database name | `None` |
| `user` | Login username | `None` |
| `password` | Login password | `None` |
| `default_schema` | Schema prefix for unqualified table names | engine default |
| `connect_timeout` | Seconds before a connection attempt fails | `30` |
| `pool_min` / `pool_max` | Connection pool size bounds | `1` / `5` |
| `options` | Extra driver-specific keyword arguments | `{}` |

---

## Transfer Modes

The `mode` key inside a table spec controls how SyncDB handles existing rows in the target.

### Quick reference

| Mode | Touches existing rows? | Deletes from target? | Best for |
| --- | --- | --- | --- |
| `append` | Yes — upsert by PK | Per-batch delete before insert | Incremental loads with updates |
| `insert_only` | No | Never | Append-only event/log tables |
| `full_refresh` | Replaces everything | Truncate once at start | Small lookup tables |
| `append_staging` | Yes — atomic swap | Staging table + rename | Zero-downtime production loads *(planned)* |

---

### `append` — Upsert by Primary Key (default)

For each batch, SyncDB deletes any target rows whose primary keys appear in that batch, then inserts the batch. Updated source rows replace stale target rows without duplicating them.

Use this when you want to keep adding new rows **and** keep existing rows up to date (equivalent to Airbyte's *Incremental | Append + Dedup*).

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "mode": "append",
    "primary_key": ["order_id"],
}
```

### `insert_only` — Pure Append, Never Touch Existing Rows *(planned)*

Inserts every source row without checking for duplicates. Existing target rows are never deleted or updated.

Use this for immutable event logs, audit trails, or any table where every source row is a new fact (equivalent to Airbyte's *Incremental | Append*).

```python
"page_views": {
    "source": "dbo.page_views",
    "destination": "public.page_views",
    "mode": "insert_only",
}
```

### `full_refresh` — Truncate and Reload

Truncates the target table once, then inserts all source rows. The target table is empty at the start of each run (equivalent to Airbyte's *Full Refresh | Overwrite*).

Use this for small lookup/reference tables where a complete reload every run is fine.

```python
"product_categories": {
    "source": "dbo.product_categories",
    "destination": "public.product_categories",
    "mode": "full_refresh",
}
```

### `append_staging` — Atomic Swap via Staging Table *(planned)*

Bulk-loads all rows into a temporary staging table, then renames it over the live target in a single transaction. Readers see either the old table or the new table — never a half-loaded state (similar to dbt's `--full-refresh` with swap strategy).

Currently behaves the same as `append` while the staging implementation is in progress.

---

## Syncing Tables

`sync_tables` accepts a dictionary where each key is a logical name for the operation and each value is a table specification.

```python
results = sync.sync_tables({
    "customers": {
        "source": "dbo.customers",         # source table (schema.table or table)
        "destination": "public.customers", # target table
        "mode": "append",                  # transfer mode
        "primary_key": ["customer_id"],    # override auto-detected PKs
        "order_by": ["customer_id"],       # deterministic read order
        "filter": {"where": "is_active = ?", "params": [1]},
    },
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "full_refresh",
    },
})
```

You can sync many tables in a single call. SyncDB opens the source and target connections once and reuses them for all tables.

### Table Spec Fields

| Key | Required | Description |
| --- | --- | --- |
| `source` | yes | Source table name: `"schema.table"` or `"table"` |
| `destination` | yes | Target table name: `"schema.table"` or `"table"` |
| `mode` | no | Transfer mode: `"append"`, `"full_refresh"`, `"append_staging"`. Default: `"append"` |
| `primary_key` | no | Override PK columns. Auto-detected from source schema when omitted |
| `order_by` | no | Column(s) to sort source reads for deterministic batching |
| `filter` | no | Restrict which source rows are read (see [Filtering Data](#filtering-data)) |

---

## Filtering Data

Use the `filter` key to copy only a subset of source rows.

### Parameterized filter (recommended)

Pass a dict with `where` (the SQL expression) and `params` (the values). The `?` placeholders prevent SQL injection.

```python
# Only active customers
"filter": {"where": "is_active = ?", "params": [1]}

# Orders from a specific date range
"filter": {"where": "created_at >= ? AND created_at < ?", "params": ["2024-01-01", "2025-01-01"]}

# Orders for specific customers
"filter": {"where": "customer_id IN (?, ?, ?)", "params": [101, 202, 303]}
```

### Plain string filter

Pass a raw SQL expression string. Use only when the values are literals you control.

```python
"filter": "status = 'shipped' AND region = 'US'"
```

> **Note:** SyncDB validates WHERE clauses and rejects dangerous tokens like `;`, `--`, `/*`, `xp_`, and `sp_`. Parameterized filters are always safer.

### Full example with filter

```python
results = sync.sync_tables({
    "recent_orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "order_by": ["order_id"],
        "filter": {
            "where": "created_at >= ? AND status != ?",
            "params": ["2024-01-01", "cancelled"],
        },
    }
})
```

---

## Schema Evolution

SyncDB automatically keeps the target schema in sync with the source. Here is what happens on each run:

| Situation | What SyncDB does |
| --- | --- |
| Target table does not exist | Creates it with matching columns and primary key |
| Source has a new column | Adds the column to target |
| Source dropped a column | Drops column from target (only if `drop_extra_columns=True`) |
| Column type changed in source | Does nothing — type changes are never applied automatically |

### Example: Adding columns automatically

Suppose you add a `loyalty_tier` column to `dbo.customers` in MSSQL. On the next sync, SyncDB detects it is missing from `public.customers` and adds it before copying data.

```python
results = sync.sync_tables({
    "customers": {
        "source": "dbo.customers",
        "destination": "public.customers",
        "mode": "append",
    }
})

for r in results:
    if r.columns_added:
        print(f"Added columns: {r.columns_added}")   # ['loyalty_tier']
```

### Example: Dropping extra columns

```python
sync = SyncDB(source=src, target=dst, drop_extra_columns=True)
```

When `drop_extra_columns=True`, columns that exist in the target but not in the source are dropped. Leave it `False` (the default) to protect audit columns or computed columns you add manually.

---

## Working with Files

SyncDB can export query results to local files and import files into database tables.

### Export: Database → File

```python
# Export a query to Parquet
rows = sync.export_query_to_file(
    query="SELECT * FROM dbo.orders WHERE status = 'shipped'",
    output_path="exports/shipped_orders.parquet",
)
print(f"Exported {rows:,} rows")

# Export to CSV
rows = sync.export_query_to_file(
    query="SELECT customer_id, email FROM dbo.customers",
    output_path="customers.csv",
)

# Export to Excel
rows = sync.export_query_to_file(
    query="SELECT * FROM dbo.summary",
    output_path="summary.xlsx",
)

# With query parameters (prevents SQL injection)
rows = sync.export_query_to_file(
    query="SELECT * FROM dbo.orders WHERE region = ? AND year = ?",
    params=["US", 2024],
    output_path="us_orders_2024.parquet",
)
```

Output parent directories are created automatically — no need to `mkdir` beforehand.

### Import: File → Database

```python
# Load a Parquet file into PostgreSQL (append by default)
rows = sync.import_file_to_table(
    input_path="exports/shipped_orders.parquet",
    destination="public.shipped_orders",
)

# Truncate first, then load
rows = sync.import_file_to_table(
    input_path="customers.csv",
    destination="public.customers",
    fresh_insert=True,
)
print(f"Inserted {rows:,} rows")
```

If the target table does not exist, SyncDB creates it using column types inferred from the first row of data.

### File-only operations (no database)

You can use `FileTransfer` directly to convert between file formats:

```python
from syncdb import FileTransfer

ft = FileTransfer()

# Read any supported format
rows = ft.read("data.csv")              # list of dicts
rows = ft.read("data.parquet")
rows = ft.read("data.xlsx")

# Write any supported format
ft.write(rows, "output.parquet")
ft.write(rows, "output.csv")
ft.write(rows, "output.xlsx")

# Convert CSV → Parquet in two lines
rows = ft.read("data.csv")
ft.write(rows, "data.parquet")
```

---

## Progress Reporting

SyncDB prints a progress bar as data moves. Three modes are available:

| Mode | Behavior | Best for |
| --- | --- | --- |
| `ProgressMode.multi_line` | New line per batch (default) | CI logs, log files |
| `ProgressMode.one_line` | Overwrites the same line | Interactive terminals |
| `ProgressMode.none` | Silent | Scheduled jobs, custom logging |

```python
from syncdb import ProgressMode, SyncDB

# Interactive terminal — animated progress on one line
sync = SyncDB(source=src, target=dst, progress_mode=ProgressMode.one_line)

# CI pipeline — each batch on its own line
sync = SyncDB(source=src, target=dst, progress_mode=ProgressMode.multi_line)

# No output at all
sync = SyncDB(source=src, target=dst, progress_mode=ProgressMode.none)

# String values also accepted
sync = SyncDB(source=src, target=dst, progress_mode="one_line")
```

When a total row count is available (SELECT COUNT(*) succeeds), the bar shows percentage and estimated position. When the count query fails due to permissions, it falls back to displaying the running row count.

---

## Reading Sync Results

`sync_tables` returns a list of `TableSyncResult` objects — one per table in the spec. The easiest way to see results is the `verbose` parameter *(planned — see [Planned & Proposed Features](#planned--proposed-features))*:

```python
# prints a formatted summary table automatically when the sync finishes
sync = SyncDB(source=src, target=dst, verbose="standard")
results = sync.sync_tables({
    "orders":    {"source": "dbo.orders",    "destination": "public.orders"},
    "customers": {"source": "dbo.customers", "destination": "public.customers"},
})
```

Until `verbose` is available you can inspect the returned list directly:

```python
sync = SyncDB(source=src, target=dst)
results = sync.sync_tables({
    "orders":    {"source": "dbo.orders",    "destination": "public.orders"},
    "customers": {"source": "dbo.customers", "destination": "public.customers"},
})

for r in results:
    print(f"{r.destination}: {r.rows_written:,} rows in {r.batches} batches")
    if r.table_created:
        print(f"  → table created")
    if r.columns_added:
        print(f"  → added columns: {r.columns_added}")
```

### All `TableSyncResult` fields

| Field | Type | Description |
| --- | --- | --- |
| `name` | `str` | Logical name from the table spec dict key |
| `source` | `str` | Source table as specified |
| `destination` | `str` | Destination table as specified |
| `mode` | `str` | Transfer mode used |
| `rows_read` | `int` | Total rows read from source |
| `rows_written` | `int` | Total rows written to target |
| `batches` | `int` | Number of batches processed |
| `table_created` | `bool` | `True` if the target table was created fresh |
| `schema_created` | `bool` | `True` if the target schema was created |
| `columns_added` | `list[str]` | Column names added to target |
| `columns_dropped` | `list[str]` | Column names dropped from target |
| `dry_run` | `bool` | `True` if this was a dry run |

---

## Complete Examples

### Example 1: Nightly incremental load from SQL Server to PostgreSQL

```python
from syncdb import DatabaseConfig, ProgressMode, SyncDB

src = DatabaseConfig(
    engine="mssql",
    connection_string=(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=prod-sql.internal,1433;Database=operations;"
        "UID=etl_user;PWD=etl_pass;TrustServerCertificate=yes;"
    ),
)

dst = DatabaseConfig(
    engine="postgresql",
    host="analytics-db.internal",
    database="warehouse",
    user="loader",
    password="loader_pass",
)

sync = SyncDB(
    source=src,
    target=dst,
    batch_size=20_000,
    progress_mode=ProgressMode.one_line,
)

results = sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "order_by": ["order_id"],
        "filter": {"where": "updated_at >= ?", "params": ["2024-12-01"]},
    },
    "order_lines": {
        "source": "dbo.order_lines",
        "destination": "public.order_lines",
        "mode": "append",
        "primary_key": ["line_id"],
        "order_by": ["line_id"],
    },
    "customers": {
        "source": "dbo.customers",
        "destination": "public.customers",
        "mode": "full_refresh",   # small table, reload completely each night
    },
})

for r in results:
    status = "created" if r.table_created else f"{r.rows_written:,} rows"
    print(f"{r.name}: {status}")
```

### Example 2: Preview schema changes before applying them

```python
from syncdb import DatabaseConfig, SyncDB

src = DatabaseConfig(engine="mssql", connection_string="...")
dst = DatabaseConfig(engine="postgresql", connection_string="...")

# Dry run — see what would change without touching any data
sync = SyncDB(source=src, target=dst, dry_run=True, drop_extra_columns=True)

results = sync.sync_tables({
    "products": {
        "source": "dbo.products",
        "destination": "public.products",
    }
})

for r in results:
    if r.table_created:
        print(f"[DRY RUN] Would CREATE table: {r.destination}")
    if r.columns_added:
        print(f"[DRY RUN] Would ADD columns: {', '.join(r.columns_added)}")
    if r.columns_dropped:
        print(f"[DRY RUN] Would DROP columns: {', '.join(r.columns_dropped)}")
    if not any([r.table_created, r.columns_added, r.columns_dropped]):
        print(f"[DRY RUN] No schema changes for: {r.destination}")
```

### Example 3: Export filtered data to Parquet, then reload into a second database

```python
from syncdb import DatabaseConfig, SyncDB

mssql_cfg = DatabaseConfig(engine="mssql", connection_string="...")
pg_cfg     = DatabaseConfig(engine="postgresql", connection_string="...")

# Step 1: export from MSSQL to a local Parquet file
sync_export = SyncDB(source=mssql_cfg)
sync_export.export_query_to_file(
    query="SELECT * FROM dbo.daily_report WHERE report_date = ?",
    params=["2024-12-31"],
    output_path="daily_report_2024_12_31.parquet",
)

# Step 2: load the file into PostgreSQL
sync_load = SyncDB(target=pg_cfg)
rows = sync_load.import_file_to_table(
    input_path="daily_report_2024_12_31.parquet",
    destination="public.daily_report",
    fresh_insert=False,   # append to existing rows
)
print(f"Loaded {rows:,} rows into PostgreSQL")
```

### Example 4: MySQL to PostgreSQL with column drop

```python
from syncdb import DatabaseConfig, SyncDB

mysql_cfg = DatabaseConfig(
    engine="mysql",
    host="mysql.internal",
    database="app_db",
    user="reader",
    password="reader_pass",
)

pg_cfg = DatabaseConfig(
    engine="postgresql",
    connection_string="postgresql://writer:pass@pg.internal:5432/warehouse",
)

sync = SyncDB(
    source=mysql_cfg,
    target=pg_cfg,
    drop_extra_columns=True,   # remove stale columns from target
    batch_size=5_000,
)

sync.sync_tables({
    "users": {
        "source": "users",            # unqualified — uses config.database
        "destination": "public.users",
        "mode": "append",
        "primary_key": ["user_id"],
    },
    "events": {
        "source": "events",
        "destination": "public.events",
        "mode": "append",
        "primary_key": ["event_id"],
        "order_by": ["event_id"],
        "filter": "status != 'deleted'",
    },
})
```

### Example 5: Convert file formats without a database

```python
from syncdb import FileTransfer

ft = FileTransfer()

# CSV → Parquet
rows = ft.read("data.csv")
ft.write(rows, "data.parquet")

# Parquet → Excel
rows = ft.read("data.parquet")
ft.write(rows, "data.xlsx")

# Filter rows in Python before writing
rows = ft.read("orders.parquet")
active = [r for r in rows if r["status"] == "active"]
ft.write(active, "active_orders.csv")
print(f"Wrote {len(active):,} active orders")
```

### Example 6: Silent sync inside a script with custom logging

```python
import logging
from syncdb import DatabaseConfig, ProgressMode, SyncDB

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("etl")

src = DatabaseConfig(engine="mssql", connection_string="...")
dst = DatabaseConfig(engine="postgresql", connection_string="...")

sync = SyncDB(source=src, target=dst, progress_mode=ProgressMode.none)

results = sync.sync_tables({
    "invoices": {
        "source": "dbo.invoices",
        "destination": "public.invoices",
        "mode": "append",
        "primary_key": ["invoice_id"],
    }
})

for r in results:
    log.info(
        "sync complete table=%s rows_written=%d batches=%d",
        r.destination, r.rows_written, r.batches,
    )
```

---

## API Reference

### `SyncDB`

```python
SyncDB(
    source: DatabaseConfig | None = None,
    target: DatabaseConfig | None = None,
    batch_size: int = 5000,
    progress_mode: ProgressMode | str = ProgressMode.multi_line,
    dry_run: bool = False,
    drop_extra_columns: bool = False,
)
```

| Parameter | Description | Default |
| --- | --- | --- |
| `source` | Source database config | `None` |
| `target` | Target database config | `None` |
| `batch_size` | Rows per read/write batch | `5000` |
| `progress_mode` | Progress display mode | `MULTI_LINE` |
| `dry_run` | Report changes without writing data | `False` |
| `drop_extra_columns` | Drop target columns not in source | `False` |

### `SyncDB.sync_tables(tables)`

```python
sync.sync_tables(tables: dict[str, dict]) -> list[TableSyncResult]
```

Syncs one or more tables. Opens connections once, reuses them for all tables, always closes both on completion or error.

### `SyncDB.export_query_to_file(query, output_path, params, file_format)`

```python
sync.export_query_to_file(
    query: str,
    output_path: str | Path,
    params: list | None = None,
    file_format: str | None = None,   # inferred from extension when omitted
) -> int   # rows written
```

### `SyncDB.import_file_to_table(input_path, destination, file_format, fresh_insert)`

```python
sync.import_file_to_table(
    input_path: str | Path,
    destination: str,
    file_format: str | None = None,   # inferred from extension when omitted
    fresh_insert: bool = False,       # truncate before inserting
) -> int   # rows inserted
```

---

## Supported File Formats

| Format | Extension | Extra dependency | Notes |
| --- | --- | --- | --- |
| CSV | `.csv` | none | All values are strings; no type inference |
| Parquet | `.parquet` | `pandas`, `pyarrow` | Preserves types; best for large datasets |
| Excel | `.xlsx`, `.xls` | `pandas`, `openpyxl` | Readable by humans; slow for large files |
| Pickle | `.pickle` | none | Python-only; not portable across languages |

File format can be inferred from the file extension or passed explicitly:

```python
sync.export_query_to_file(
    query="SELECT * FROM dbo.data",
    output_path="output.dat",
    file_format="parquet",   # override extension-based detection
)
```

---

## Running Tests

Unit tests (no database required):

```bash
pytest
```

Integration tests against real databases require Docker:

```bash
cd Tests/DataBase
docker compose up -d --build
pytest
```

See [Tests/DataBase/README.md](Tests/DataBase/README.md) for details on test database setup.

---

## Planned & Proposed Features

### In Progress

- **`append_staging` mode** — bulk-load rows into a temporary staging table, then rename it over the live target atomically. Zero downtime, clean rollback on failure.

- **`insert_only` mode** — pure append with no duplicate checking. For immutable event logs and audit tables.

---

### On the Roadmap

#### 1. Lowercase `ProgressMode` members

Rename enum members from `ONE_LINE / MULTI_LINE / NONE` to `one_line / multi_line / none` so they follow Python conventions and match the string values you already pass.

```python
# today
sync = SyncDB(source=src, target=dst, progress_mode=ProgressMode.one_line)

# after rename — identical, no migration needed
sync = SyncDB(source=src, target=dst, progress_mode="one_line")
```

---

#### 2. Automatic summary reporting (`verbose` parameter)

A `verbose` parameter on `SyncDB` that prints a formatted summary automatically after `sync_tables` completes — no manual `for r in results: print(...)` needed.

```python
sync = SyncDB(source=src, target=dst, verbose="standard")
results = sync.sync_tables({...})
# prints automatically:
#
# ┌──────────────┬───────────────┬─────────┬──────────┐
# │ table        │  rows written │ batches │ created  │
# ├──────────────┼───────────────┼─────────┼──────────┤
# │ orders       │        52,341 │      11 │ no       │
# │ customers    │         8,200 │       2 │ yes      │
# │ order_lines  │       104,820 │      21 │ no       │
# └──────────────┴───────────────┴─────────┴──────────┘
# total: 165,361 rows in 34 batches — 4.2s
```

Three levels:

| `verbose=` | Output |
| --- | --- |
| `"detailed"` | Full table with all `TableSyncResult` fields including schema changes |
| `"standard"` | One-line-per-table summary with totals row |
| `None` | Silent — return results only, print nothing |

---

#### 3. More transfer modes (Airbyte-style)

| Planned mode | Airbyte equivalent | Description |
| --- | --- | --- |
| `insert_only` | Incremental \| Append | Pure append, never touch existing rows |
| `upsert` | Incremental \| Append + Dedup | SQL MERGE — more efficient than delete + insert for large PKs |
| `snapshot` | Full Refresh \| Append | Append all rows each run with a `_synced_at` timestamp column, building a full history |
| `soft_delete` | — | Sync a `deleted_at` column instead of removing rows from target |

Example of the planned `snapshot` mode — useful for slowly-changing dimension history:

```python
"customers": {
    "source": "dbo.customers",
    "destination": "public.customers_history",
    "mode": "snapshot",   # adds _synced_at = now() to every inserted row
}
```

---

### More Ideas

#### Incremental high-watermark sync

Track the maximum value of a cursor column (e.g. `updated_at`) between runs and automatically set the filter on the next run — no manual `filter` key needed.

```python
sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "incremental_column": "updated_at",   # SyncDB remembers the max value
        "watermark_store": "watermarks.json", # persisted between runs
    }
})
```

#### Row-level transforms

Pass a Python function that receives each batch as a list of dicts and returns the modified list — useful for masking PII, unit conversion, or enrichment without a separate pipeline step.

```python
def mask_pii(rows):
    for r in rows:
        r["email"] = "***@***.***"
        r["phone"] = "***"
    return rows

sync.sync_tables({
    "customers": {
        "source": "dbo.customers",
        "destination": "public.customers",
        "transform": mask_pii,
    }
})
```

#### Column rename mapping

Declare a `rename` dict so a column that was renamed in the source maps to the existing target column instead of being treated as an add + drop.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "rename": {"cust_id": "customer_id", "ord_dt": "order_date"},
}
```

#### Column type overrides

Override the inferred target type for specific columns when the schema mapper's default choice is not right for your workload.

```python
"type_overrides": {"price": "numeric(18,4)", "notes": "text", "flags": "jsonb"}
```

#### Parallel table sync

Sync independent tables concurrently using a thread pool to cut total wall-clock time.

```python
sync = SyncDB(source=src, target=dst, max_workers=4)
```

#### Schema-level sync

Copy every table in a source schema without listing them one by one.

```python
sync.sync_schema(source_schema="dbo", destination_schema="public")
# or with exclusions
sync.sync_schema(source_schema="dbo", destination_schema="public", exclude=["temp_*", "audit_log"])
```

#### Data quality checks

Assert row counts, null rates, or value ranges after each table sync. Fail loudly when the data does not meet expectations.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "expect": {
        "min_rows": 1000,
        "not_null": ["order_id", "customer_id"],
        "unique": ["order_id"],
    }
}
```

#### YAML / JSON job config

Define entire sync jobs in a config file and run them from the CLI, making SyncDB usable in scheduled tasks without writing Python code.

```yaml
# syncdb.yaml
source:
  engine: mssql
  connection_string: "Driver=..."

target:
  engine: postgresql
  connection_string: "postgresql://..."

settings:
  batch_size: 10000
  verbose: standard

tables:
  orders:
    source: dbo.orders
    destination: public.orders
    mode: append
    primary_key: [order_id]
  customers:
    source: dbo.customers
    destination: public.customers
    mode: full_refresh
```

```bash
syncdb run syncdb.yaml
```

#### `on_batch` callback

Call a user-supplied function after each batch — useful for custom metrics, rate limiting, or alerting mid-sync.

```python
def emit_metric(result_so_far):
    statsd.gauge("etl.rows_written", result_so_far.rows_written)

sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "on_batch": emit_metric,
    }
})
```

#### Retry on transient errors

Automatically retry a failed batch with exponential backoff for network hiccups, deadlocks, or transient timeouts — configurable retry count and delay.

#### SQLite support

A lightweight SQLite connector so SyncDB works entirely locally with no server — great for development, local testing, and small one-off migrations.

```python
sqlite = DatabaseConfig(engine="sqlite", database="local.db")
sync = SyncDB(source=pg_cfg, target=sqlite)
```
