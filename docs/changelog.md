# Changelog

## 0.1.0 — Initial release

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
