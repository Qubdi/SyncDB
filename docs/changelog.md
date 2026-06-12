# Changelog

## Unreleased

### Fixed

- **Batches containing duplicate primary keys no longer crash PK-driven
  writes.**  PostgreSQL `ON CONFLICT` raises "command cannot affect row a
  second time" and MSSQL `MERGE` errors on duplicate source rows, so a
  CDC-style feed carrying two updates to the same key in one batch failed
  mid-sync with a cryptic driver error.  `UPSERT`, `APPEND`, and `SOFT_DELETE`
  batches are now de-duplicated by primary key before writing, keeping the
  LAST occurrence (latest-wins).  `INSERT_ONLY` and `SNAPSHOT` are unaffected.
- **MSSQL `MERGE` now uses `WITH (HOLDLOCK)`**, closing the classic MERGE race
  where two concurrent upserts to the same target could both pass the
  NOT MATCHED check and collide on a primary-key violation.
- **`repr(DatabaseConfig)` no longer leaks credentials embedded in a
  connection string.**  `connection_string` is excluded from repr the same way
  `password` always was, so DSNs like `postgresql://user:secret@host/db` stay
  out of tracebacks and logs.
- A type mismatch in an `expect.range` check (e.g. a string bound against a
  datetime column) now raises a `ValueError` naming the column and both
  types instead of a bare `TypeError`.

### New

- **Schema-drift warnings.**  When an existing target column no longer matches
  the mapped source type — or the source string length has grown past the
  target's — schema sync emits a `RuntimeWarning` naming the table, column,
  and both types up front, instead of letting the drift surface later as an
  opaque truncation or conversion error at insert time.  (Schema evolution
  still never ALTERs an existing column; SQLite targets are exempt because
  its types are affinity hints.)
- **`order_by` accepts a direction suffix**: `"updated_at DESC"` (or `ASC`,
  case-insensitive).  Only the two literal keywords are accepted after the
  validated column name.

### Changed

- **Parallel sync reuses one connector pair per worker thread** instead of
  opening and closing fresh connections for every table, so connection setup
  (TLS handshakes especially) no longer dominates runs with many small
  tables.  A pair whose table failed is discarded, never reused.

### Internal

- Documented the read-side recovery contract on the batch loop: source reads
  are not retried mid-stream (server-side cursors cannot resume); re-run the
  job — watermark specs resume from the last committed watermark.
- The `transform` and `on_batch` spec callbacks are now fully typed
  (`TransformFn`, `OnBatchFn`).
- Failed pre-sync `COUNT(*)` queries (usually missing SELECT permission) are
  now logged at DEBUG level instead of silently disabling progress totals.
- New concurrency tests hammer the watermark file store from multiple threads
  and multiple OS processes to prove the cross-process lock loses no keys.

## 2.1.0 — Reliability and security hardening

### Fixed

- **FULL_REFRESH now truncates inside the transaction.**  With
  `use_transaction=True`, the `TRUNCATE` previously ran (and auto-committed)
  *before* `BEGIN`, so a mid-sync failure left the target permanently empty.
  The write-target preparation now runs after `begin()`, so a failure rolls the
  truncate back together with the partial load (PostgreSQL/MSSQL; MySQL DDL
  still auto-commits — see the `TransferMode.FULL_REFRESH` docs).
- **PostgreSQL and pymysql batch reads now truly stream.**  Unnamed psycopg2
  cursors materialise the entire result set client-side at `execute()`;
  `fetch_batches` / `execute_query_batches` now use named (server-side) cursors
  on PostgreSQL and `SSCursor` on pymysql, so memory stays bounded by
  `batch_size` regardless of table size.
- **APPEND/SOFT_DELETE delete+insert is now atomic per batch.**  Outside an
  explicit transaction the two statements previously auto-committed
  independently; a crash between them silently dropped the batch's existing
  rows.  They are now wrapped in a per-batch transaction.
- **Soft-delete counts use the UPDATE's rowcount** instead of re-counting rows
  whose `deleted_at` equals the run timestamp (which double-counted if the
  timestamp was not unique to the run).
- **WHERE deny-list bypasses closed.**  Keyword matching now uses word
  boundaries after stripping string literals, so `id IN(SELECT ...)` and
  `1 UNION(SELECT 1)` are rejected while legitimate values like
  `hex_val = '0x1f'` are no longer false positives.
- The temp-table name documented in the 2.0.0 notes was wrong: the seen-keys
  table is `__syncdb_{table}_{uid}_keys` (with a per-run uid suffix), not
  `__syncdb_{table}_seen_keys`.
- `py.typed` is now shipped, so consumers' type checkers see the package's
  inline types.

### Changed

- **Parallel sync failures raise `ParallelSyncError`** (a `RuntimeError`
  subclass) carrying `.results` — the `TableSyncResult` of every table that
  completed before the failure — and `.errors`.  Previously successful tables'
  results were discarded, leaving no audit trail of what was written.
- `SOFT_DELETE` combined with a `filter` now emits a `RuntimeWarning`: rows the
  filter excludes are absent from the seen-keys table and would be mass-marked
  as deleted.
- Large `delete_matching_rows` batches (> 1,000 rows) use a temp-key-table
  anti-join instead of OR-chained predicates, which scale poorly with composite
  primary keys.

### New

- **Watermark store is now safe for concurrent writers.**  save_watermark()
  serialises its read-modify-write with an exclusive OS-level lock on a
  ``.lock`` sidecar (msvcrt on Windows, flock elsewhere) plus an in-process
  mutex, so overlapping cron runs or replicas sharing a volume cannot lose
  each other's updates.  Replicas with independent disks still need distinct
  store paths.
- **Database-backed watermark storage.**  `"watermark_storage": "database"`
  keeps incremental state in a `__syncdb_watermarks` table on the target
  (created automatically, written with the engine's native atomic upsert)
  instead of a local JSON file — the correct choice for multi-replica
  deployments with independent disks, and the cursor travels with the target
  data on restore.
- **Streaming file imports.**  `import_file_to_table` now consumes
  `FileTransfer.read_streaming()`: CSV, Parquet, and Excel files are read and
  inserted batch by batch and are never fully materialised in memory (CSV
  natively, Parquet via pyarrow `iter_batches`, Excel via openpyxl read-only
  mode; Pickle has no incremental reader and is loaded once, then inserted in
  chunks).
- `"watermark_comparison": ">="` table-spec option for incremental syncs.  The
  default strict `>` skips rows committed late with a timestamp equal to the
  saved watermark; `>=` re-reads boundary rows each run (pair with `upsert`, or
  `append` + primary key, so re-processing is idempotent).
- `import_file_to_table` accepts `hmac_key=` / `hmac_alg=` for pickle integrity
  verification (previously the safe pickle path was unreachable through SyncDB)
  and inserts in `batch_size` chunks with the configured retry policy instead of
  one statement for the whole file.
- `BaseConnector.execute_update()` — executes DML and returns the affected-row
  count.
- Connector class attributes `timestamp_type` and `ddl_transactional` describe
  engine traits that previously leaked into the orchestration layer.
- `SECURITY.md` documenting the threat model and reporting process.
- `.pre-commit-config.yaml` mirroring the manual quality gates (ruff, mypy,
  component tests on push, pip-audit on the manual stage); `pip-audit` and
  `pre-commit` added to the dev extras; `requirements.txt` is now a thin
  pointer to the pyproject extras (single source of truth;
  `requirements-dev.txt` remains the pip-compile lockfile).
- `publish.py` always runs the test/lint/type gate before a release — the
  silent `--no-test` flag is gone (an env-var escape hatch exists for
  emergencies and prints an unmissable warning); `--yes`/`--push` enable
  non-interactive automation.

### Internal

- `_sync_one_table` was split into plan → execute → finalize
  (`_TableSyncPlan`, `_plan_table_sync`, `_execute_table_sync`,
  `_copy_batches`) so the transaction-ordering rules live in one auditable
  place.  No behavior change.
- The coverage gate (`fail_under = 90`) is now actually measured: the
  component suite covers 93% of `Library/`.
- Reformatted `Library/` with `ruff format` (the project's stated formatter,
  previously never run).

---

## 2.0.0 — Performance, security, and feature release

### Breaking changes

| Area | v1 behaviour | v2 behaviour |
|------|-------------|-------------|
| `UPSERT` mode | delete + insert (same as APPEND) | Native atomic upsert: `ON CONFLICT DO UPDATE` (PostgreSQL/SQLite), `MERGE` (MSSQL), `ON DUPLICATE KEY UPDATE` (MySQL) |
| `SOFT_DELETE` | Loads **all** target PKs into Python memory | SQL `NOT EXISTS` subquery via a temporary key table — no Python-side target scan |
| `export_query_to_file` | Loads entire result set before writing | Streaming `cursor.fetchmany()` + incremental file writes (CSV/Parquet) |
| `execute_query` / `insert_batch` | Always auto-commits DML | Defers commit when `_in_transaction = True` (set via `begin()`) |

### Migration guide

#### UPSERT mode
No code changes required.  The underlying implementation is now atomic per-row
(native upsert statement) rather than delete + insert.  Result counts are unchanged.

#### SOFT_DELETE mode
No code changes required.  If you relied on Python-level `update_matching_rows`
being called (e.g. via a monkey-patch for testing), replace it with the new
`apply_soft_deletes_sql` method on the connector.

A temporary key table `__syncdb_{table}_{uid}_keys` (uid is a per-run 8-char hex
token preventing collisions between concurrent syncs) is created and dropped
during the soft-delete step.  Ensure the sync user has `CREATE TABLE` and
`DROP TABLE` permissions on the target schema.

#### `export_query_to_file`
A new optional `batch_size` parameter controls the streaming fetch size.
Existing calls with no `batch_size` argument are unaffected (default `5 000` rows
per fetch batch).  The return value (row count) is unchanged.

#### Transaction support (new)
No migration needed — `use_transaction=False` by default.  To opt in:

```python
SyncDB(source=src, target=tgt, use_transaction=True)
```

MySQL note: DDL statements inside MySQL (`TRUNCATE`, `CREATE TABLE`) are auto-committed
by the engine regardless of the transaction setting.

#### Pickle HMAC (new, opt-in)
Existing `FileTransfer.read()` / `write()` calls are unaffected — `hmac_key`
defaults to `None` (no verification).  Opt in by passing `hmac_key=...`.

#### `query_timeout` field (new, opt-in)
`DatabaseConfig` now accepts `query_timeout: int | None = None`.  Existing configs
without this field continue to work; the default is no query timeout.

#### WHERE clause deny-list expanded
The `validate_where_clause` deny-list now blocks additional injection patterns:
`union`, `select`, `insert`, `update`, `delete`, `drop`, `alter`, `create`,
`exec`, `execute`, `declare`, `truncate`, `sleep(`, `waitfor`, `benchmark(`,
`pg_sleep(`, `into outfile`, `load_file(`, `0x`, and null bytes.

If a legitimate developer-authored filter expression is blocked by the expanded
list, use the parameterised dict form instead:

```python
{"where": "status = %s", "params": ["active"]}
```

### New features

- **Native UPSERT** (`upsert_batch`) per connector — atomic, no delete round-trip
- **SQL-based SOFT_DELETE** (`apply_soft_deletes_sql`) — eliminates full target PK scan
- **Streaming file export** — `export_query_to_file` never loads full result set
- **Transaction boundaries** — `use_transaction=True` wraps each table in BEGIN/COMMIT
- **Parallel table sync** — `max_workers=N` syncs N tables simultaneously
- **Query timeout** — `DatabaseConfig(query_timeout=N)` cancels runaway queries
- **Pickle HMAC verification** — `FileTransfer.read/write(hmac_key=...)` for integrity
- **Expanded WHERE deny-list** — blocks 18 additional injection patterns
- **Three new doc pages** — performance, deployment, secrets management
- **Structure refactor** — `retry.py`, `inference.py`, `staging.py` extracted from core

---

## 1.0.0 — Initial release

- Database-to-database sync: MSSQL, PostgreSQL, MySQL, SQLite
- File operations: export query to CSV/Parquet/Excel/Pickle, import file to table
- Transfer modes: `append`, `insert_only`, `upsert`, `full_refresh`, `append_staging`, `snapshot`, `soft_delete`
- Automatic table creation and schema evolution (add/drop columns)
- Incremental high-watermark sync with JSON persistence
- Data quality checks: `min_rows`, `not_null`, `unique`, `range`
- Batch progress reporting: `one_line`, `multi_line`, `none`
- Sync result summary: `standard` and `detailed` tables
- Row transforms, column renaming, type overrides
- Per-batch callbacks
- Retry on transient errors with exponential backoff
- `sync_schema` for whole-schema auto-discovery
- `run_config_file` for YAML/JSON job files
- Dry-run mode
