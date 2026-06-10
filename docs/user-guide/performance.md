# Performance and Scaling Guide

## Batch size tuning

`batch_size` controls how many rows are read from the source and written to the target in each round-trip.
The default of 5 000 rows works well for most tables.  Tune it when:

| Situation | Recommendation |
|-----------|---------------|
| Wide rows (many columns / large text) | Decrease to 500–1 000 |
| Narrow rows (few small columns) | Increase to 10 000–50 000 |
| Memory-constrained environment | Decrease until RSS stays acceptable |
| High-latency network between source and target | Increase to amortise round-trip cost |

```python
SyncDB(source=src, target=tgt, batch_size=10_000)
```

Use a **percentage** to auto-size relative to the total row count (useful for tables whose size changes over time):

```python
SyncDB(source=src, target=tgt, batch_size="5%")
```

---

## SOFT_DELETE on large tables

`SOFT_DELETE` mode previously loaded all target primary-key rows into Python memory to find rows missing from the source.  As of v2.0, SyncDB creates a temporary key table in the target database and uses a single SQL `NOT EXISTS` subquery — no Python-side target scan.

For a table with 50 M target rows and 10 M source rows, the v1 approach required ~400 MB of RAM for the key set.  The v2 approach requires only the RAM to hold the source keys (~80 MB) and lets the database do the comparison.

**Remaining caveat**: `seen_keys` (source primary keys) is still accumulated in Python during the source scan.  If the *source* table is very large (> 10 M rows with multi-column PKs), memory use grows linearly with source size.  In that case, consider splitting the soft-delete cleanup into a separate scheduled job that issues a direct database-to-database JOIN.

---

## delete_matching_rows parameter limits

In `APPEND` and `UPSERT` modes, existing rows are deleted before each batch is inserted.  `delete_matching_rows` builds a single `DELETE WHERE pk IN (...)` statement whose parameter count scales with `batch_size × num_pk_columns`.

Driver limits:
- **pyodbc (MSSQL)**: ~2 100 parameters per statement → keep `batch_size < 2100 / num_pk_columns`
- **psycopg2 (PostgreSQL)**: no hard limit, but very long statements slow the planner
- **mysql-connector / pymysql**: no hard limit

If you hit parameter limit errors, reduce `batch_size`:

```python
SyncDB(source=src, target=tgt, batch_size=500)
```

---

## Parallel table sync

When syncing many independent tables, use `max_workers` to parallelise across threads:

```python
SyncDB(source=src, target=tgt, max_workers=4).sync_tables(tables)
```

Each worker thread gets its own database connection so connections are never shared across threads.  `max_workers` requires `source` and `target` to be passed as `DatabaseConfig` (not raw connectors), so fresh connectors can be created per thread.

Recommended values:
- **I/O-bound workloads** (network latency dominates): 4–8 workers
- **CPU-bound transforms**: match `os.cpu_count()`
- **Connection-limited databases**: stay within the DB's `max_connections` budget

---

## File export streaming

`export_query_to_file` uses `cursor.fetchmany()` internally and writes rows to disk as each batch arrives — it never loads the full result set into memory.

- **CSV**: fully streamed (header written once, rows appended in batches)
- **Parquet**: streamed via pyarrow `ParquetWriter` (row groups per batch)
- **Excel / Pickle**: materialised (all rows loaded before write — no streaming API)

For multi-GB exports, prefer CSV or Parquet.

---

## Query timeout

Set `query_timeout` in `DatabaseConfig` to cancel runaway queries automatically:

```python
DatabaseConfig(engine="postgresql", host="...", query_timeout=300)  # 5 minutes
```

| Engine | Mechanism |
|--------|-----------|
| PostgreSQL | `SET statement_timeout = N` (ms) |
| MSSQL | pyodbc query execution timeout |
| MySQL | `SET SESSION max_execution_time = N` (ms, MySQL 5.7.8+) |
| SQLite | Not supported (SQLite has no query timeout) |

`connect_timeout` (default 30 s) controls only the connection handshake; `query_timeout` controls individual query execution.

---

## APPEND_STAGING for zero-downtime loads

For tables that are queried by other processes during a sync, use `APPEND_STAGING` mode:

```python
{"mode": "append_staging", "source": "...", "destination": "..."}
```

This writes all rows to a `__syncdb_{table}_staging` table first, then performs a single `TRUNCATE + INSERT INTO ... SELECT` swap.  Readers see the old data until the swap completes; they never see a partially-loaded state.

The swap itself is not transactional on MySQL (TRUNCATE is DDL and auto-commits).  On PostgreSQL and MSSQL, the swap is atomic within a transaction.
