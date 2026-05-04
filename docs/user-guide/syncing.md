# Syncing Tables

## sync_tables

`sync_tables` accepts a dictionary where each key is a logical name and each value is a **table spec** dict.

```python
results = sync.sync_tables({
    "customers": {
        "source": "dbo.customers",
        "destination": "public.customers",
        "mode": "append",
        "primary_key": ["customer_id"],
        "order_by": ["customer_id"],
    },
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "full_refresh",
    },
})
```

SyncDB opens connections once and reuses them across all tables. Both connections are closed on completion or error.

Returns `list[TableSyncResult]` — one result per table. See {doc}`../api/models`.

### Table spec fields

| Key | Required | Description |
|-----|----------|-------------|
| `source` | yes | Source table: `"schema.table"` or `"table"` |
| `destination` | yes | Target table: `"schema.table"` or `"table"` |
| `mode` | no | Transfer mode. Default: `"append"` |
| `batch_size` | no | Override batch size for this table only |
| `primary_key` | no | PK column(s). Auto-detected from source when omitted |
| `order_by` | no | Column(s) to sort source reads for deterministic batching |
| `filter` | no | Restrict which source rows are read |
| `rename` | no | Map of source column names → target column names |
| `type_overrides` | no | Map of target column name → target SQL type |
| `transform` | no | `Callable[[list[dict]], list[dict]]` applied to each batch |
| `incremental_column` | no | Cursor column for persisted high-watermark filtering |
| `watermark_store` | no | JSON file for persisting watermark values |
| `watermark_key` | no | Override the storage key inside the watermark file |
| `expect` | no | Data quality checks |
| `on_batch` | no | Callback called after each written batch |

---

## sync_schema

`sync_schema` auto-discovers all tables in a source schema and builds a `sync_tables` call automatically.

```python
results = sync.sync_schema(
    source_schema="dbo",
    destination_schema="public",
    mode="append",
    exclude=["tmp_*", "audit_log"],
    batch_size=10_000,
)
```

### Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `source_schema` | Schema to read from | **required** |
| `destination_schema` | Schema to write to | **required** |
| `mode` | Transfer mode for all tables | `"append"` |
| `exclude` | Table name patterns to skip (supports `*` wildcard) | `[]` |
| `batch_size` | Batch size override for this call | instance default |
| `table_prefix` | String prepended to every destination table name | `""` |
| `table_suffix` | String appended to every destination table name | `""` |
| `**table_defaults` | Any other kwargs are copied into every table spec | — |

### Destination name transforms

```python
# dbo.customers  →  public.raw_customers
# dbo.orders     →  public.raw_orders
sync.sync_schema("dbo", "public", table_prefix="raw_", mode="full_refresh")

# dbo.customers  →  public.customers_20250101
sync.sync_schema("dbo", "public", table_suffix="_20250101", mode="snapshot")
```

### Passing defaults to every table spec

Any extra keyword argument is copied into every generated table spec:

```python
sync.sync_schema(
    "dbo", "public",
    mode="append",
    expect={"not_null": ["id"]},    # applied to every table
    order_by=["id"],                # applied to every table
)
```

---

## Filtering data

Use the `filter` key to copy only a subset of source rows.

### Parameterized filter (recommended)

```python
# Only active customers
"filter": {"where": "is_active = ?", "params": [1]}

# Date range
"filter": {"where": "created_at >= ? AND created_at < ?", "params": ["2024-01-01", "2025-01-01"]}

# Specific IDs
"filter": {"where": "customer_id IN (?, ?, ?)", "params": [101, 202, 303]}
```

Parameterized filters use `?` placeholders and never interpolate values into the SQL string, preventing SQL injection.

### Plain string filter

```python
"filter": "status = 'shipped' AND region = 'US'"
```

SyncDB validates WHERE clauses and rejects dangerous tokens (`;`, `--`, `/*`, `xp_`, `sp_`). Use parameterized filters whenever the values come from user input or external systems.

---

## Column renaming

Map source column names to different target column names. Without `rename`, a renamed source column would look like a dropped column plus a new one, losing data.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "rename": {
        "cust_id": "customer_id",
        "ord_dt":  "order_date",
    },
}
```

---

## Row transforms

Apply a Python function to each batch before it is written. The function receives a `list[dict]` and must return a `list[dict]`.

```python
def mask_pii(rows):
    for r in rows:
        r["email"] = "***@***.***"
        r["phone"] = "***"
    return rows

"customers": {
    "source": "dbo.customers",
    "destination": "public.customers",
    "transform": mask_pii,
}
```

---

## Type overrides

Override the inferred target column type for specific columns:

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "type_overrides": {
        "price": "numeric(18,4)",
        "notes": "text",
        "flags": "jsonb",
    },
}
```

---

## Per-batch callbacks

Supply an `on_batch` callable that receives the running `TableSyncResult` snapshot after each batch is written. Useful for metrics, alerting, or rate limiting.

```python
def emit_metric(result_so_far):
    statsd.gauge("etl.rows_written", result_so_far.rows_written)

"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "on_batch": emit_metric,
}
```

---

## Reading results

`sync_tables` returns `list[TableSyncResult]`. The verbose summary table is printed automatically (controlled by the `verbose` constructor parameter):

```text
SyncDB summary (standard)
+------------------+--------+--------------+---------+---------+------+
| table            | mode   | rows written | batches | created | time |
+------------------+--------+--------------+---------+---------+------+
| public.orders    | append | 52,341       | 11      | no      | 3.2s |
| public.customers | append | 8,200        |  2      | yes     | 0.9s |
+------------------+--------+--------------+---------+---------+------+
total: 60,541 rows in 13 batches across 2 tables in 4.1s
```

Inspect the results programmatically:

```python
for r in results:
    if r.table_created:
        alert(f"New table created: {r.destination}")
    if r.columns_added:
        notify_schema_change(r.destination, r.columns_added)
    if r.expectations_failed:
        raise RuntimeError(f"Quality checks failed: {r.expectations_failed}")
```

See {doc}`../api/models` for the full `TableSyncResult` field list.

---

## Dry run

Preview what would change without writing any data:

```python
sync = SyncDB(source=src, target=dst, dry_run=True, drop_extra_columns=True)
results = sync.sync_tables({
    "products": {
        "source": "dbo.products",
        "destination": "public.products",
    }
})

r = results[0]
print(r.columns_added)    # columns that would be added
print(r.columns_dropped)  # columns that would be dropped
print(r.table_created)    # True if the table would be created
```

Schema changes are still reported in results even in dry-run mode; no data is written.
