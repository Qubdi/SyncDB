# SyncDB

Python ETL helper for moving tabular data between **MSSQL**, **PostgreSQL**, **MySQL**, and **local files** (CSV, Parquet, Excel, Pickle), with automatic schema creation, schema evolution, and batch progress reporting.

---

## What it does

- **Database → Database** — copy tables across engines with automatic type mapping
- **Database → File** — export query results to CSV, Parquet, Excel, or Pickle
- **File → Database** — load files into tables, creating them if they don't exist
- **Schema management** — auto-creates tables, adds new columns, optionally drops stale ones
- **Incremental sync** — high-watermark tracking so only changed rows are copied each run
- **Seven transfer modes** — `append`, `insert_only`, `upsert`, `full_refresh`, `append_staging`, `snapshot`, `soft_delete`

---

## Installation

```bash
pip install Qubdi-SyncDB
```

Install only the extras you need:

```bash
pip install "Qubdi-SyncDB[mssql]"      # SQL Server
pip install "Qubdi-SyncDB[postgres]"   # PostgreSQL
pip install "Qubdi-SyncDB[mysql]"      # MySQL / MariaDB
pip install "Qubdi-SyncDB[files]"      # Parquet + Excel
pip install "Qubdi-SyncDB[all]"        # Everything
```

> CSV and Pickle work with no extras — they use Python's standard library.

---

## Quick example

Credentials come from environment variables via `DatabaseConfig.from_env()` —
never hardcode passwords or connection strings with embedded credentials in
source code or config files:

```python
from syncdb import DatabaseConfig, SyncDB

# Reads SRC_ENGINE, SRC_HOST, SRC_DATABASE, SRC_USER, SRC_PASSWORD, ...
# Load these from your secrets manager at process startup.
src = DatabaseConfig.from_env(prefix="SRC")   # e.g. SRC_ENGINE=mssql
dst = DatabaseConfig.from_env(prefix="DST")   # e.g. DST_ENGINE=postgresql

sync = SyncDB(source=src, target=dst, batch_size=20_000)

sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "incremental_column": "updated_at",   # only rows changed since last run
        "watermark_store": "watermarks.json",
    },
    "customers": {
        "source": "dbo.customers",
        "destination": "public.customers",
        "mode": "full_refresh",
    },
})
```

```text
SyncDB summary (standard)
+------------------+--------------+--------------+---------+---------+------+
| table            | mode         | rows written | batches | created | time |
+------------------+--------------+--------------+---------+---------+------+
| public.orders    | append       | 1,842        | 1       | no      | 0.4s |
| public.customers | full_refresh | 8,200        | 2       | no      | 0.9s |
+------------------+--------------+--------------+---------+---------+------+
total: 10,042 rows in 3 batches across 2 tables in 1.3s
```

For local development you can construct `DatabaseConfig` with explicit fields
(`host=`, `database=`, `user=`, `password=`) — just keep real credentials out of
version control.

---

## Performance notes

- **MSSQL bulk loads:** pyodbc's `fast_executemany` is off by default because it
  can mis-size string buffers for mixed-length varchar batches.  For homogeneous
  bulk loads it is typically 10–100× faster — enable it per endpoint with
  `DatabaseConfig(..., options={"fast_executemany": True})`.
- **PostgreSQL / MySQL reads stream server-side** (named cursors / `SSCursor`),
  so memory use is bounded by `batch_size` regardless of table size.

---

## Documentation

Full documentation — configuration, all transfer modes, incremental sync, data quality checks, file operations, and API reference — is available at:

**[https://qubdi-syncdb.readthedocs.io](https://qubdi-syncdb.readthedocs.io)**

---

## License

Apache 2.0
