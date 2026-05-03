# SyncDB

Python ETL helper for moving tabular data between **Microsoft SQL Server**, **PostgreSQL**, **MySQL**, and **local files** (CSV, Parquet, Excel, Pickle), with automatic schema creation, schema evolution, and batch progress reporting.

---

## Table of Contents

- [What SyncDB Does](#what-syncdb-does)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Connecting to Databases](#connecting-to-databases)
- [Transfer Modes](#transfer-modes) (`append`, `insert_only`, `upsert`, `full_refresh`, `append_staging`, `snapshot`, `soft_delete`)
- [Syncing Tables](#syncing-tables)
- [Filtering Data](#filtering-data)
- [Schema Evolution](#schema-evolution)
- [Working with Files](#working-with-files)
- [Advanced Features](#advanced-features)
- [Progress Reporting](#progress-reporting)
- [Reading Sync Results](#reading-sync-results)
- [Complete Examples](#complete-examples)
- [API Reference](#api-reference)
- [Supported File Formats](#supported-file-formats)
- [Running Tests](#running-tests)
- [Roadmap](#roadmap)

---

## What SyncDB Does

SyncDB copies data from a **source** (database table or file) to a **destination** (database table or file). It handles:

- Creating the destination table if it does not exist
- Adding or dropping columns when the schema changes
- Chunking large tables into batches so you never load millions of rows into memory at once
- Translating data types between engines (e.g. PostgreSQL `boolean` to MSSQL `bit`)
- Showing a live progress bar while data moves
- Returning structured sync results and optionally printing a final summary table

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

After the package is published to PyPI:

```bash
pip install Qubdi-SyncDB
```

Install only the database connectors and file formats you actually need:

```bash
pip install "Qubdi-SyncDB[mssql]"       # MSSQL / SQL Server
pip install "Qubdi-SyncDB[postgres]"    # PostgreSQL
pip install "Qubdi-SyncDB[mysql]"       # MySQL / MariaDB
pip install "Qubdi-SyncDB[files]"       # Parquet + Excel (requires pandas)
pip install "Qubdi-SyncDB[all]"         # Everything
```

For local development from this repository:

```bash
pip install -e .
```

Or with extras:

```bash
pip install -e ".[mssql]"       # MSSQL / SQL Server
pip install -e ".[postgres]"    # PostgreSQL
pip install -e ".[mysql]"       # MySQL / MariaDB
pip install -e ".[files]"       # Parquet + Excel (requires pandas)
pip install -e ".[all]"         # Everything
```

> **CSV and Pickle** work without any extras; they use Python's standard library.

The distribution name is `Qubdi-SyncDB`. The Python import name stays lowercase:

```python
from syncdb import DatabaseConfig, SyncDB
```

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

sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "order_by": ["order_id"],
    }
})
# SyncDB prints a summary automatically:
#
# SyncDB summary (standard)
# +-----------------+--------+--------------+---------+---------+
# | table           | mode   | rows written | batches | created |
# +-----------------+--------+--------------+---------+---------+
# | public.orders   | append | 52,341       | 11      | no      |
# +-----------------+--------+--------------+---------+---------+
# total: 52,341 rows in 11 batches across 1 tables
```

### Export a query result to a Parquet file

```python
# Pass a SQL string directly
sync.export_query_to_file(
    query="SELECT * FROM dbo.orders WHERE status = 'shipped'",
    output_path="shipped_orders.parquet",
)

# Or point to a .sql file on disk
sync.export_query_to_file(
    query="queries/shipped_orders.sql",
    output_path="shipped_orders.parquet",
)
```

### Load a file into a database table

```python
sync.import_file_to_table(
    input_path="shipped_orders.parquet",
    destination="public.shipped_orders",
    fresh_insert=True,   # truncate before inserting
)
```

---

## Core Concepts

### Batching

SyncDB never loads an entire table into memory. It reads `batch_size` rows at a time from the source, writes them to the target, then reads the next batch. The default is 5,000 rows. Raise it for fast networks with plenty of RAM; lower it for slow connections or wide rows.

`batch_size` accepts either an integer count or a percentage string. A percentage is resolved against the total row count before the first batch is read — useful when you want each batch to represent a fixed share of the table regardless of its size.

```python
sync = SyncDB(source=src, target=dst, batch_size=50_000)   # fixed count
sync = SyncDB(source=src, target=dst, batch_size="10%")    # 10% of total rows per batch
```

When a percentage is given but the total row count cannot be determined (for example, due to missing `SELECT COUNT(*)` permission), SyncDB falls back to the default of 5,000 rows.

### Automatic Table Creation

If the destination table does not exist, SyncDB creates it automatically by reading the source schema. You do not need to write any `CREATE TABLE` statements.

### Automatic Schema Management

When you run a sync on an existing table and the source schema has changed, SyncDB can:

- **Add** new columns that appear in the source but not the target (always on)
- **Drop** extra columns from the target that are no longer in the source (opt-in via `drop_extra_columns=True`)

Existing column types are never altered — this protects manually added columns and audit fields.

### Dry Run

Pass `dry_run=True` to see what SyncDB *would* do without writing any data. Schema changes are still reported but not applied.

```python
sync = SyncDB(source=src, target=dst, dry_run=True)
results = sync.sync_tables({"orders": {"source": "dbo.orders", "destination": "public.orders"}})
# SyncDB prints a summary of what would change. To also inspect results in code:
r = results[0]
# r.columns_added, r.columns_dropped, r.table_created are populated even in dry_run mode
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
| `upsert` | Yes — upsert by PK | Per-batch delete before insert | Explicit append + dedup mode |
| `full_refresh` | Replaces everything | Truncate once at start | Small lookup tables |
| `append_staging` | Replaces everything | Staging table + final replace | Safer full-table reloads |
| `snapshot` | No | Never | Historical snapshots with `_synced_at` |
| `soft_delete` | Yes | Marks missing rows | Targets with `deleted_at` lifecycle |

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

### `insert_only` — Pure Append, Never Touch Existing Rows

Inserts every source row without checking for duplicates. Existing target rows are never deleted or updated.

Use this for immutable event logs, audit trails, or any table where every source row is a new fact (equivalent to Airbyte's *Incremental | Append*).

```python
"page_views": {
    "source": "dbo.page_views",
    "destination": "public.page_views",
    "mode": "insert_only",
}
```

### `upsert` — Explicit Append + Dedup

Uses the same portable delete-then-insert primary-key behavior as `append`, but lets jobs state that they want upsert semantics directly.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "mode": "upsert",
    "primary_key": ["order_id"],
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

### `append_staging` — Load Through a Staging Table

Bulk-loads all rows into a staging table, then replaces the live table contents from that staging table. This keeps the live table untouched while the source is being read and staging rows are inserted.

Connector-native transactional rename/swap is still a future optimization; the current implementation is portable across supported engines.

### `snapshot` — Append a Historical Copy

Appends every source row and adds `_synced_at` to show when that snapshot was captured.

```python
"customers": {
    "source": "dbo.customers",
    "destination": "public.customers_history",
    "mode": "snapshot",
}
```

### `soft_delete` — Mark Missing Rows

Upserts rows that still exist in the source and sets `deleted_at` on target rows whose primary key is no longer present in the source.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "mode": "soft_delete",
    "primary_key": ["order_id"],
}
```

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
| `mode` | no | Transfer mode: `"append"`, `"insert_only"`, `"upsert"`, `"full_refresh"`, `"append_staging"`, `"snapshot"`, `"soft_delete"`. Default: `"append"` |
| `primary_key` | no | Override PK columns. Auto-detected from source schema when omitted |
| `order_by` | no | Column(s) to sort source reads for deterministic batching |
| `filter` | no | Restrict which source rows are read (see [Filtering Data](#filtering-data)) |
| `rename` | no | Source-to-target column rename map |
| `type_overrides` | no | Target type overrides by target column name |
| `transform` | no | Callable that receives and returns each batch as `list[dict]` |
| `incremental_column` | no | Cursor column for persisted high-watermark filtering |
| `watermark_store` | no | JSON file path for persisted watermark values |
| `watermark_key` | no | Override the default storage key for the watermark (default: `"source->destination:column"`) |
| `expect` | no | Data quality checks: `min_rows`, `not_null`, `unique`, `range` |
| `on_batch` | no | Callback called after each written batch with the current `TableSyncResult` |

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
# SyncDB prints a summary automatically. To inspect the result in code:
added = results[0].columns_added   # ['loyalty_tier']
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
# Export a query string to Parquet
sync.export_query_to_file(
    query="SELECT * FROM dbo.orders WHERE status = 'shipped'",
    output_path="exports/shipped_orders.parquet",
)

# Or point to a .sql file — its contents are read and executed
sync.export_query_to_file(
    query="queries/shipped_orders.sql",
    output_path="exports/shipped_orders.parquet",
)

# Export to CSV
sync.export_query_to_file(
    query="SELECT customer_id, email FROM dbo.customers",
    output_path="customers.csv",
)

# Export to Excel
sync.export_query_to_file(
    query="SELECT * FROM dbo.summary",
    output_path="summary.xlsx",
)

# With query parameters (prevents SQL injection)
sync.export_query_to_file(
    query="SELECT * FROM dbo.orders WHERE region = ? AND year = ?",
    params=["US", 2024],
    output_path="us_orders_2024.parquet",
)
```

Output parent directories are created automatically — no need to `mkdir` beforehand.

### Import: File → Database

```python
# Load a Parquet file into PostgreSQL (append by default)
sync.import_file_to_table(
    input_path="exports/shipped_orders.parquet",
    destination="public.shipped_orders",
)

# Truncate first, then load
sync.import_file_to_table(
    input_path="customers.csv",
    destination="public.customers",
    fresh_insert=True,
)
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

## Advanced Features

### Incremental High-Watermark Sync

Use `incremental_column` to track the maximum value of a cursor column (for example `updated_at`) between runs. SyncDB saves the high-water mark automatically after each sync — no manual `filter` key needed on subsequent runs.

```python
sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "incremental_column": "updated_at",   # SyncDB remembers the max value
        "watermark_store": "watermarks.json", # persisted between runs
    }
})
```

| Key | Description |
| --- | --- |
| `incremental_column` | Column whose maximum value is saved as the cursor |
| `watermark_store` | Path to a JSON file where values are saved. Defaults to `.syncdb_watermarks.json` |
| `watermark_key` | Override the storage key used inside the JSON file. Defaults to `"source->destination:column"` |

On the first run there is no saved value, so all rows are copied. From the second run onward only rows with a value greater than the saved mark are read.

---

### Row Transforms

Pass a Python callable to `transform` to modify each batch before it is written — useful for masking PII, unit conversion, or enrichment without a separate pipeline step.

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

The function receives each batch as a `list[dict]` and must return a `list[dict]`.

---

### Column Renaming

Use `rename` to map source column names to different target column names. This prevents a renamed column from being treated as a dropped column + new column, which would lose data.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "rename": {"cust_id": "customer_id", "ord_dt": "order_date"},
}
```

---

### Column Type Overrides

Use `type_overrides` to specify the exact target column type when the automatic type mapping is not appropriate for your workload.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "type_overrides": {"price": "numeric(18,4)", "notes": "text", "flags": "jsonb"},
}
```

---

### Data Quality Checks

Use `expect` to assert data conditions after each table sync. SyncDB raises `ValueError` and populates `result.expectations_failed` when a check fails.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "expect": {
        "min_rows": 1000,
        "not_null": ["order_id", "customer_id"],
        "unique": ["order_id"],
        "range": {
            "total_amount": {"min": 0},
            "discount_pct": {"min": 0, "max": 100},
        },
    },
}
```

| Check | Description |
| --- | --- |
| `min_rows` | Fail if the target table has fewer than this many rows after the sync |
| `not_null` | List of column names that must contain no null values |
| `unique` | List of column names (or lists of names) that must have no duplicate values |
| `range` | Dict of `{column: {min: value, max: value}}` — fail if any value falls outside the bounds |

---

### Per-Batch Callbacks

Supply an `on_batch` callable to be called after every batch. The function receives the current `TableSyncResult` snapshot — useful for custom metrics, rate limiting, or mid-sync alerting.

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

---

### Retry on Transient Errors

Set `retry_count` and `retry_delay_seconds` on the `SyncDB` constructor to automatically retry failed batch writes with exponential backoff.

```python
sync = SyncDB(
    source=src,
    target=dst,
    retry_count=3,           # retry up to 3 times per batch
    retry_delay_seconds=2.0, # initial delay; doubles after each attempt (2s, 4s, 8s)
)
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

`sync_tables` returns a list of `TableSyncResult` objects — one per table in the spec. The easiest way to see results is the `verbose` parameter:

```python
# prints a formatted summary table automatically when the sync finishes
sync = SyncDB(source=src, target=dst, verbose="standard")
results = sync.sync_tables({
    "orders":    {"source": "dbo.orders",    "destination": "public.orders"},
    "customers": {"source": "dbo.customers", "destination": "public.customers"},
})
# output:
#
# SyncDB summary (standard)
# +-------------+--------+--------------+---------+---------+
# | table       | mode   | rows written | batches | created |
# +-------------+--------+--------------+---------+---------+
# | public.orders    | append | 52,341  | 11      | no      |
# | public.customers | append | 8,200   | 2       | yes     |
# +-------------+--------+--------------+---------+---------+
# total: 60,541 rows in 13 batches across 2 tables
```

Use `verbose="detailed"` for a full row with every `TableSyncResult` field, including schema changes, watermark values, and quality-check results.

| `verbose=` | Output |
| --- | --- |
| `"standard"` | One line per table — destination, mode, rows written, batches, created flag, and a totals row |
| `"detailed"` | Full table with all `TableSyncResult` fields including schema changes, watermark, and check results |
| `None` | Silent — return results only, print nothing |

The returned list is also useful for programmatic checks — for example, wiring results into a monitoring system or raising an alert when new tables are created:

```python
results = sync.sync_tables({
    "orders":    {"source": "dbo.orders",    "destination": "public.orders"},
    "customers": {"source": "dbo.customers", "destination": "public.customers"},
})

for r in results:
    if r.table_created:
        alert(f"New table created: {r.destination}")
    if r.columns_added:
        notify_schema_change(r.destination, r.columns_added)
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
| `rows_soft_deleted` | `int` | Rows marked `deleted_at` in `soft_delete` mode |
| `expectations_failed` | `list[str]` | Failure messages from `expect` checks (empty if all passed) |
| `watermark_value` | `Any` | Highest watermark value seen in this sync run |
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

# SyncDB prints a summary table automatically
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

# SyncDB prints a [DRY RUN] summary automatically.
# Inspect results in code if needed:
r = results[0]
# r.table_created, r.columns_added, r.columns_dropped are populated
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
sync_load.import_file_to_table(
    input_path="daily_report_2024_12_31.parquet",
    destination="public.daily_report",
    fresh_insert=False,   # append to existing rows
)
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
```

### Example 6: Routing results to a custom logger

Pass `verbose=None` to suppress the built-in summary table and `progress_mode=ProgressMode.none` to suppress the progress bar. Then use the returned `results` list to feed your own logger or monitoring system.

```python
import logging
from syncdb import DatabaseConfig, ProgressMode, SyncDB

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("etl")

src = DatabaseConfig(engine="mssql", connection_string="...")
dst = DatabaseConfig(engine="postgresql", connection_string="...")

sync = SyncDB(
    source=src,
    target=dst,
    verbose=None,                    # suppress built-in summary
    progress_mode=ProgressMode.none, # suppress progress bar
)

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
    batch_size: int | str = 5000,
    progress_mode: ProgressMode | str = ProgressMode.multi_line,
    dry_run: bool = False,
    drop_extra_columns: bool = False,
    verbose: str | None = "standard",
    verbose_stream: TextIO | None = None,
    retry_count: int = 0,
    retry_delay_seconds: float = 1.0,
)
```

| Parameter | Description | Default |
| --- | --- | --- |
| `source` | Source database config | `None` |
| `target` | Target database config | `None` |
| `batch_size` | Rows per batch — integer count (`10_000`) or percentage of total rows (`"10%"`) | `5000` |
| `progress_mode` | Progress display mode | `MULTI_LINE` |
| `dry_run` | Report changes without writing data | `False` |
| `drop_extra_columns` | Drop target columns not in source | `False` |
| `verbose` | Automatic summary after sync: `"standard"`, `"detailed"`, or `None` to silence | `"standard"` |
| `verbose_stream` | Output stream for the summary table | `sys.stdout` |
| `retry_count` | Retry failed batch writes this many times | `0` |
| `retry_delay_seconds` | Initial retry delay; doubles after each retry | `1.0` |

### `SyncDB.sync_tables(tables)`

```python
sync.sync_tables(tables: dict[str, dict]) -> list[TableSyncResult]
```

Syncs one or more tables. Opens connections once, reuses them for all tables, always closes both on completion or error.

### `SyncDB.sync_schema(source_schema, destination_schema, exclude, mode, **table_defaults)`

```python
sync.sync_schema(
    source_schema="dbo",
    destination_schema="public",
    exclude=["tmp_*", "audit_log"],
    mode="append",
    expect={"not_null": ["id"]},
)
```

Discovers source tables through the connector and builds a `sync_tables` spec automatically. Extra keyword arguments are copied into every generated table spec.

### `SyncDB.run_config_file(path)`

```python
from syncdb import SyncDB

results = SyncDB.run_config_file("syncdb.json")
```

Loads a `.json`, `.yaml`, or `.yml` job file with `source`, `target`, `settings`, and `tables` sections, then runs `sync_tables`.

### `SyncDB.export_query_to_file(query, output_path, params, file_format)`

```python
sync.export_query_to_file(
    query: str | Path,             # SQL string or path to a .sql file
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

To inspect each test and see live SyncDB progress/summary output:

```bash
pytest Tests/Library/sync --syncdb-live-output
```

On Windows you can use the helper script:

```powershell
.\run_tests.ps1 sync -live
```

Integration tests against real databases require Docker:

```bash
cd Tests/DataBase
docker compose up -d --build
pytest
```

See [Tests/DataBase/README.md](Tests/DataBase/README.md) for details on test database setup.

---

## Roadmap

### In Progress

- **Connector-native staging swaps** — upgrade `append_staging` from the current portable truncate-and-copy strategy to an engine-specific transactional rename/swap where the engine supports it.
- **Connector-native upsert** — replace the portable delete-then-insert upsert with engine-specific implementations: PostgreSQL `ON CONFLICT`, MySQL `ON DUPLICATE KEY UPDATE`, and MSSQL `MERGE`.

### Planned

**Parallel table sync** — sync independent tables concurrently using a thread pool to cut total wall-clock time.

```python
sync = SyncDB(source=src, target=dst, max_workers=4)
```

**CLI command** — a `syncdb run <config.yaml>` console entry point so sync jobs can be run from scheduled tasks without writing Python code.

```bash
syncdb run syncdb.yaml
```
