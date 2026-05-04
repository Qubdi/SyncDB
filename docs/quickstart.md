# Quick Start

## Copy a table between databases

```python
from syncdb import DatabaseConfig, SyncDB

source = DatabaseConfig(
    engine="mssql",
    connection_string=(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost,1433;Database=sales;"
        "UID=sa;PWD=Password1;TrustServerCertificate=yes;"
    ),
)

target = DatabaseConfig(
    engine="postgresql",
    connection_string="postgresql://admin:secret@localhost:5432/warehouse",
)

sync = SyncDB(source=source, target=target, batch_size=10_000)

results = sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
    }
})
```

SyncDB creates `public.orders` if it does not exist, maps column types, streams data in batches, and prints a summary:

```text
SyncDB summary (standard)
+----------------+--------+--------------+---------+---------+-------+
| table          | mode   | rows written | batches | created | time  |
+----------------+--------+--------------+---------+---------+-------+
| public.orders  | append | 52,341       | 6       | yes     | 3.2s  |
+----------------+--------+--------------+---------+---------+-------+
total: 52,341 rows in 6 batches across 1 table in 3.2s
```

## Sync an entire schema

```python
sync.sync_schema(
    source_schema="dbo",
    destination_schema="public",
    mode="append",
    exclude=["tmp_*", "audit_log"],
)
```

## Export a query to a file

```python
sync.export_query_to_file(
    query="SELECT * FROM dbo.orders WHERE status = 'shipped'",
    output_path="shipped_orders.parquet",
)
```

## Load a file into a table

```python
sync.import_file_to_table(
    input_path="shipped_orders.parquet",
    destination="public.shipped_orders",
    fresh_insert=True,
)
```

## Incremental load with watermarks

```python
sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "incremental_column": "updated_at",
        "watermark_store": "watermarks.json",
    }
})
```

On the first run every row is copied. On subsequent runs only rows newer than the saved high-water mark are read.

## Next steps

- {doc}`user-guide/configuration` — all `DatabaseConfig` options
- {doc}`user-guide/transfer-modes` — `append`, `upsert`, `full_refresh`, and more
- {doc}`user-guide/syncing` — `sync_tables` and `sync_schema` in depth
