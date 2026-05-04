# Data Quality Checks

Use the `expect` key in a table spec to assert data conditions after each table sync. SyncDB raises `ValueError` when a check fails and records the failure message in `TableSyncResult.expectations_failed`.

## Available checks

| Check | Description |
|-------|-------------|
| `min_rows` | Target must have at least this many rows after the sync |
| `not_null` | Listed columns must contain no null values |
| `unique` | Listed columns (or column groups) must have no duplicates |
| `range` | Column values must fall within the specified min/max bounds |

## Example

```python
results = sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "expect": {
            "min_rows": 1000,
            "not_null": ["order_id", "customer_id", "order_date"],
            "unique": ["order_id"],
            "range": {
                "total_amount":  {"min": 0},
                "discount_pct":  {"min": 0, "max": 100},
            },
        },
    }
})
```

## Check reference

### `min_rows`

Fails if the target table has fewer than the specified number of rows after the sync:

```python
"expect": {"min_rows": 1000}
```

### `not_null`

Fails if any row in the specified columns contains a `NULL`:

```python
"expect": {"not_null": ["order_id", "customer_id"]}
```

### `unique`

Fails if any value in the specified column (or column group) appears more than once:

```python
# Single column uniqueness
"expect": {"unique": ["order_id"]}

# Composite uniqueness (both columns together must be unique)
"expect": {"unique": [["order_id", "line_number"]]}

# Mix of single and composite
"expect": {"unique": ["order_id", ["product_id", "warehouse_id"]]}
```

### `range`

Fails if any value in a column falls outside the specified bounds. Both `min` and `max` are optional:

```python
"expect": {
    "range": {
        "total_amount": {"min": 0},               # no upper bound
        "discount_pct": {"min": 0, "max": 100},   # both bounds
        "temperature":  {"max": 150},              # no lower bound
    }
}
```

## Reading check results

Failed checks raise `ValueError` immediately and are also recorded in the result:

```python
try:
    results = sync.sync_tables({...})
except ValueError as e:
    print(f"Quality check failed: {e}")

# Inspect results for detail even after failure
for r in results:
    if r.expectations_failed:
        for msg in r.expectations_failed:
            print(msg)
```

## Combining checks with incremental sync

Quality checks run after the sync completes, so they always reflect the current full state of the target table, not just the rows written in this run:

```python
sync.sync_tables({
    "orders": {
        "source": "dbo.orders",
        "destination": "public.orders",
        "mode": "append",
        "primary_key": ["order_id"],
        "incremental_column": "updated_at",
        "expect": {
            "not_null": ["order_id"],   # checks ALL rows in target, not just new ones
            "range": {"total_amount": {"min": 0}},
        },
    }
})
```
