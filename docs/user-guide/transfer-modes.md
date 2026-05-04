# Transfer Modes

The `mode` key in a table spec controls how SyncDB handles existing rows in the target table.

## Quick reference

| Mode | Touches existing rows | Deletes from target | Best for |
|------|-----------------------|---------------------|----------|
| `append` | Yes — upsert by PK | Per-batch delete before insert | Incremental loads with updates |
| `insert_only` | No | Never | Append-only event/log tables |
| `upsert` | Yes — upsert by PK | Per-batch delete before insert | Explicit upsert semantics |
| `full_refresh` | Replaces everything | Truncate once at start | Small lookup/reference tables |
| `append_staging` | Replaces everything | Staging → live swap | Safer full-table reloads |
| `snapshot` | No | Never | Historical snapshots with `_synced_at` |
| `soft_delete` | Yes | Marks missing rows as deleted | Tables with `deleted_at` lifecycle |

---

## `append` — Upsert by primary key

For each batch, SyncDB deletes target rows whose primary keys appear in the batch, then inserts the batch. Updated source rows replace stale target rows without creating duplicates.

Use `append` when you want to **add new rows and keep existing rows up to date**.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "mode": "append",
    "primary_key": ["order_id"],
}
```

---

## `insert_only` — Pure append

Inserts every source row without checking for duplicates. Existing target rows are never deleted or updated.

Use `insert_only` for **immutable event logs or audit trails** where every source row represents a new fact.

```python
"page_views": {
    "source": "dbo.page_views",
    "destination": "public.page_views",
    "mode": "insert_only",
}
```

---

## `upsert` — Explicit upsert

Uses the same portable delete-then-insert behavior as `append`. Choose `upsert` when you want to make the intent explicit in your config.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "mode": "upsert",
    "primary_key": ["order_id"],
}
```

---

## `full_refresh` — Truncate and reload

Truncates the target table once at the start of the sync, then inserts all source rows.

Use `full_refresh` for **small lookup or reference tables** where a complete reload on every run is acceptable.

```python
"product_categories": {
    "source": "dbo.product_categories",
    "destination": "public.product_categories",
    "mode": "full_refresh",
}
```

---

## `append_staging` — Load through a staging table

Bulk-loads all rows into a temporary staging table, then replaces the live table's contents from staging. The live table remains untouched while the source is being read.

Use `append_staging` when you need a **safer full reload** that keeps the live table consistent during the transfer.

---

## `snapshot` — Historical copy with timestamp

Appends every source row and adds a `_synced_at` column populated with the current UTC timestamp.

Use `snapshot` to **accumulate a history** of table state over time.

```python
"customers": {
    "source": "dbo.customers",
    "destination": "public.customers_history",
    "mode": "snapshot",
}
```

Each sync appends a full copy of the source with `_synced_at` set to the run time. Query snapshots by `_synced_at` to see what the table looked like at any point in history.

---

## `soft_delete` — Mark missing rows

Upserts rows that still exist in the source and sets `deleted_at` on target rows whose primary key is no longer present.

Use `soft_delete` when the target table has a **`deleted_at` column** and you want to mark logically deleted rows rather than physically removing them.

```python
"orders": {
    "source": "dbo.orders",
    "destination": "public.orders",
    "mode": "soft_delete",
    "primary_key": ["order_id"],
}
```

Rows whose `order_id` disappears from the source will have `deleted_at` set to the current UTC timestamp. Rows that reappear will have `deleted_at` cleared.

---

## Choosing a mode

```{tip}
When in doubt, start with `append`. It is the default, handles updates gracefully, and works for most incremental load patterns.
```

| Your situation | Recommended mode |
|---------------|-----------------|
| Copy new and updated records | `append` |
| Append-only logs, never update | `insert_only` |
| Reload a small reference table every run | `full_refresh` |
| Keep a history of table state over time | `snapshot` |
| Detect and mark deleted records | `soft_delete` |
| Full reload, keep live table stable during transfer | `append_staging` |
