# Incremental Sync

Incremental sync copies only rows that are **new or updated since the last run** by tracking the maximum value of a cursor column — the *high-water mark*.

## Basic usage

```python
sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "incremental_column": "updated_at",   # cursor column
        "watermark_store": "watermarks.json", # persisted between runs
    }
})
```

On the **first run**, there is no saved watermark so all rows are copied. After the sync, SyncDB saves the maximum `updated_at` value it saw.

On **subsequent runs**, SyncDB adds `WHERE updated_at > <saved_value>` to the source query automatically. Only rows changed since the last run are transferred.

## How watermarks are stored

Two storage backends are available via `watermark_storage`:

**`"file"` (default)** — values are written to a JSON file after each
successful sync:

```json
{
  "dbo.orders->public.orders:updated_at": "2024-12-31T23:59:59.000000"
}
```

The default file is `.syncdb_watermarks.json` in the current working
directory.  Writes are atomic and serialised with a cross-process file lock,
so overlapping runs on the same machine (or replicas sharing a volume) are
safe.

**`"database"`** — values live in a `__syncdb_watermarks` table on the
*target* database (created automatically on first save) and are written with
the engine's native atomic upsert:

```python
{
    "source": "dbo.orders",
    "destination": "public.orders",
    "incremental_column": "updated_at",
    "watermark_storage": "database",
}
```

Use this for multi-replica deployments with independent local disks — every
replica reads and writes the same authoritative row, and the cursor travels
with the target data (restore the database, restore the watermark).

## Configuration reference

| Key | Description | Default |
|-----|-------------|---------|
| `incremental_column` | Column whose max value is used as the cursor | **required** |
| `watermark_storage` | `"file"` or `"database"` (table on the target) | `"file"` |
| `watermark_store` | Path to the JSON file (file storage only) | `.syncdb_watermarks.json` |
| `watermark_key` | Override the key used in the store | `"source->destination:column"` |
| `watermark_comparison` | `">"` (strict) or `">="` (re-reads boundary rows; pair with `upsert` or `append`+PK) | `">"` |

## Custom watermark key

If you sync the same table with different filters (for example, by region), use `watermark_key` to give each job its own slot in the watermark file:

```python
for region in ["US", "EU", "APAC"]:
    sync.sync_tables({
        "orders": {
            "source": "dbo.orders",
            "destination": f"public.orders_{region.lower()}",
            "mode": "append",
            "primary_key": ["order_id"],
            "incremental_column": "updated_at",
            "watermark_store": "watermarks.json",
            "watermark_key": f"orders_{region}:updated_at",
            "filter": {"where": "region = ?", "params": [region]},
        }
    })
```

## Combining with filters

`incremental_column` and `filter` can be used together. SyncDB merges them with `AND`:

```python
sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "incremental_column": "updated_at",
        "watermark_store": "watermarks.json",
        "filter": {"where": "status != ?", "params": ["cancelled"]},
    }
})
# Effective WHERE: updated_at > <watermark> AND status != 'cancelled'
```

## Reading the watermark value

The result object includes the highest watermark value seen in the sync run:

```python
results = sync.sync_tables({...})
r = results[0]
print(r.watermark_value)   # e.g. datetime(2024, 12, 31, 23, 59, 59)
```

## Best practices

- Choose a column that is always set or updated when a row changes — `updated_at` is ideal.
- Ensure the cursor column is **indexed** on the source for efficient filtering.
- Use `mode: "append"` with a `primary_key` so updated rows replace their older copies rather than duplicating them.
- Store watermarks outside the repository (e.g., a shared network path or object storage) when running across multiple machines.
