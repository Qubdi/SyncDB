# Schema Evolution

SyncDB automatically keeps the target schema in sync with the source on every run.

## What SyncDB manages automatically

| Situation | Behavior |
|-----------|----------|
| Target table does not exist | Creates it with matching columns and primary key |
| Source has a new column | Adds the column to the target |
| Source dropped a column | Drops it from target (only if `drop_extra_columns=True`) |
| Column type changed in source | **Does nothing** — type changes are never applied |

Column type changes are intentionally left alone to protect manually added audit columns, computed columns, and any type widening you apply to the target.

## Auto table creation

If the destination table does not exist, SyncDB creates it automatically by reading the source schema. No `CREATE TABLE` statement is needed.

```python
sync.sync_tables({
    "customers": {
        "source": "dbo.customers",
        "destination": "public.customers",   # does not exist yet
        "mode": "append",
    }
})
# SyncDB creates public.customers and syncs all rows
```

The result reports `table_created=True`:

```python
r = results[0]
print(r.table_created)   # True
```

## Adding new columns

When a column appears in the source but not the target, SyncDB adds it before copying the batch that introduced it:

```python
# Suppose dbo.customers gained a new "loyalty_tier" column
results = sync.sync_tables({
    "customers": {
        "source": "dbo.customers",
        "destination": "public.customers",
    }
})
print(results[0].columns_added)   # ['loyalty_tier']
```

## Dropping extra columns

By default, columns that exist in the target but not in the source are kept. Enable `drop_extra_columns` to remove them:

```python
sync = SyncDB(source=src, target=dst, drop_extra_columns=True)
```

```{warning}
`drop_extra_columns=True` will drop any column in the target that is not in the source, including manually added audit columns, computed columns, or denormalized fields. Leave it `False` unless you actively manage the target schema through SyncDB alone.
```

The dropped column names are reported in `TableSyncResult.columns_dropped`.

## Cross-engine type mapping

SyncDB translates column types between engines. The mapping is conservative — it widens types rather than truncating data. For example:

| MSSQL source type | PostgreSQL target type |
|-------------------|----------------------|
| `nvarchar(max)` | `text` |
| `bit` | `boolean` |
| `datetime2` | `timestamp` |
| `decimal(18,4)` | `numeric(18,4)` |
| `uniqueidentifier` | `uuid` |

Use `type_overrides` to override the inferred target type for specific columns:

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "type_overrides": {
        "price":  "numeric(18,4)",
        "notes":  "text",
        "flags":  "jsonb",
    },
}
```

## Previewing schema changes

Use `dry_run=True` to see what would change without touching data:

```python
sync = SyncDB(source=src, target=dst, dry_run=True, drop_extra_columns=True)
results = sync.sync_tables({
    "products": {"source": "dbo.products", "destination": "public.products"},
})

r = results[0]
print("Would create table:", r.table_created)
print("Would add columns:", r.columns_added)
print("Would drop columns:", r.columns_dropped)
```
