"""High-level SyncDB orchestration API.

This module coordinates connectors, schema mapping, batching, retries, progress,
and file IO. Keep connector-specific SQL out of this layer; SyncDB should express
workflow policy while connector classes own engine syntax and driver behavior.
"""

from __future__ import annotations

import fnmatch
import json
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, TextIO

from .config import DatabaseConfig
from .connections import create_connector
from .connectors.base import BaseConnector
from .files import FileTransfer
from .progress import ProgressMode, ProgressReporter, _format_elapsed
from .sql import build_order_by, build_where_clause, parse_qualified_name, quote_identifier, validate_identifier
from .type_mapping import Column, SchemaMapper


class TransferMode(str, Enum):
    # Upsert-style: delete-then-insert for rows that match on primary key,
    # append new rows.  Safe for incremental loads where source rows may be updated.
    APPEND = "append"
    # Pure append: insert every source row as-is and never delete/update existing
    # target rows.  Useful for immutable event streams, audit logs, and history tables.
    INSERT_ONLY = "insert_only"
    # Explicit upsert mode.  Today it uses the same portable delete+insert strategy
    # as APPEND; connector-native MERGE/ON CONFLICT optimisations can replace this later.
    UPSERT = "upsert"
    # Append every source row with a _synced_at timestamp for historical snapshots.
    SNAPSHOT = "snapshot"
    # Upsert active source rows, then mark target rows missing from the source with deleted_at.
    SOFT_DELETE = "soft_delete"
    # Portable staging load: write all source rows to a staging table first, then
    # replace the live target contents from staging in a final step.
    APPEND_STAGING = "append_staging"
    # Truncate the target before loading.  Suitable for full daily refreshes.
    FULL_REFRESH = "full_refresh"


@dataclass
class TableSyncResult:
    """Runtime statistics and schema-change summary for one synced table.

    Returned by SyncDB.sync_tables so callers can audit what happened without
    parsing log output.  dry_run=True means no data or DDL was actually applied.
    """
    name: str
    source: str
    destination: str
    mode: str
    rows_read: int = 0
    rows_written: int = 0
    rows_soft_deleted: int = 0
    batches: int = 0
    schema_created: bool = False
    table_created: bool = False
    columns_added: list[str] = field(default_factory=list)
    columns_dropped: list[str] = field(default_factory=list)
    expectations_failed: list[str] = field(default_factory=list)
    watermark_value: Any = None
    dry_run: bool = False
    duration_seconds: float = 0.0


class SyncDB:
    """Main class-based API for database and local-file synchronization.

    Typical usage patterns:
      - Database to database: supply both source and target; call sync_tables().
      - Database to file:     supply source only; call export_query_to_file().
      - File to database:     supply target only; call import_file_to_table().

    source/target accept either a DatabaseConfig (connector is created internally)
    or an already-constructed BaseConnector (useful for testing with a mock connector).
    The source_connector/target_connector keyword arguments are legacy aliases kept
    for backwards compatibility; prefer the positional source/target parameters.
    """

    def __init__(
        self,
        source: DatabaseConfig | BaseConnector | None = None,
        target: DatabaseConfig | BaseConnector | None = None,
        batch_size: int | str = 5000,
        progress_mode: ProgressMode | str = ProgressMode.MULTI_LINE,
        dry_run: bool = False,
        drop_extra_columns: bool = False,
        source_connector: BaseConnector | None = None,
        target_connector: BaseConnector | None = None,
        schema_mapper: SchemaMapper | None = None,
        file_transfer: FileTransfer | None = None,
        verbose: str | None = "standard",
        verbose_stream: TextIO | None = None,
        retry_count: int = 0,
        retry_delay_seconds: float = 1.0,
    ) -> None:
        self.batch_size, self._batch_pct = self._parse_batch_size(batch_size)
        if retry_count < 0:
            raise ValueError("retry_count must be zero or greater")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be zero or greater")
        # source_connector/target_connector take precedence over source/target when both
        # are supplied, allowing callers to inject test doubles without refactoring.
        self.source = source_connector or self._coerce_connector(source)
        self.target = target_connector or self._coerce_connector(target)
        self.progress = ProgressReporter(progress_mode)
        self.dry_run = dry_run
        # False by default: extra target columns are left untouched to avoid
        # accidentally dropping manually-added audit or computed columns.
        self.drop_extra_columns = drop_extra_columns
        self.schema_mapper = schema_mapper or SchemaMapper()
        self.file_transfer = file_transfer or FileTransfer()
        # verbose controls an optional post-run summary.  It is intentionally
        # separate from progress reporting: progress is per batch, while verbose
        # is a final audit view over TableSyncResult objects.
        self.verbose = self._normalize_verbose(verbose)
        self.verbose_stream = verbose_stream or sys.stdout
        self.retry_count = retry_count
        self.retry_delay_seconds = retry_delay_seconds

    def sync_tables(
        self,
        tables: dict[str, dict[str, Any]],
        batch_size: int | str | None = None,
    ) -> list[TableSyncResult]:
        """Synchronize one or more database tables from source to target.

        tables is a dict keyed by a user-assigned logical name.  Each value is
        a spec dict with at minimum "source" and "destination" table names, plus
        optional "mode", "filter", "order_by", "primary_key", and "batch_size" keys.

        batch_size overrides the instance-level batch_size for every table in this
        call.  A per-table "batch_size" key inside the spec takes precedence over
        this argument, which in turn takes precedence over the SyncDB default.

        Connections are opened once and reused across all tables; both are always
        closed (even on error) via the finally block.
        """
        if self.source is None or self.target is None:
            raise ValueError("source and target connectors/configs are required for database sync")
        results: list[TableSyncResult] = []
        # Connections are opened once here and reused across all tables in the dict.
        # This avoids per-table connection overhead (especially relevant for MSSQL
        # where ODBC connection setup can take hundreds of milliseconds).
        # Both are closed unconditionally in `finally` even if a table sync raises.
        self.source.connect()
        self.target.connect()
        self.progress.label_width = max((len(spec.get("destination", "")) for spec in tables.values()), default=0)
        try:
            for name, spec in tables.items():
                # Per-table batch_size in the spec wins; method-level batch_size fills
                # in when the spec doesn't specify one.
                if batch_size is not None and "batch_size" not in spec:
                    spec = {**spec, "batch_size": batch_size}
                results.append(self._sync_one_table(name, spec))
        finally:
            # finish() emits the trailing newline for ONE_LINE progress mode.
            self.progress.finish()
            self.source.close()
            self.target.close()
        self._emit_summary(results)
        return results

    def sync_schema(
        self,
        source_schema: str | None,
        destination_schema: str | None,
        exclude: Sequence[str] | None = None,
        mode: str = TransferMode.APPEND.value,
        batch_size: int | str | None = None,
        **table_defaults: Any,
    ) -> list[TableSyncResult]:
        """Synchronize every table in a source schema.

        Exclusion patterns use fnmatch syntax, so callers can skip tables with
        values like ["tmp_*", "audit_log"].  table_defaults are copied into every
        generated table spec, letting callers set mode, batch options, or expect
        rules once for the whole schema.

        batch_size overrides the instance-level batch_size for every table in this
        schema sync.  A per-table "batch_size" key inside table_defaults takes
        precedence over this argument.
        """
        if self.source is None:
            raise ValueError("source connector/config is required for schema sync")
        self.source.connect()
        try:
            names = self.source.list_tables(source_schema)
        finally:
            self.source.close()
        patterns = list(exclude or [])
        tables = {
            name: {
                **table_defaults,
                "source": f"{source_schema}.{name}" if source_schema else name,
                "destination": f"{destination_schema}.{name}" if destination_schema else name,
                "mode": mode,
            }
            for name in names
            if not any(fnmatch.fnmatch(name, pattern) for pattern in patterns)
        }
        return self.sync_tables(tables, batch_size=batch_size)

    @classmethod
    def from_job_config(cls, config: dict[str, Any]) -> "SyncDB":
        """Build a SyncDB instance from a parsed YAML/JSON job config."""
        settings = dict(config.get("settings") or {})
        source = DatabaseConfig(**config["source"]) if config.get("source") else None
        target = DatabaseConfig(**config["target"]) if config.get("target") else None
        allowed_settings = {
            "batch_size",
            "progress_mode",
            "dry_run",
            "drop_extra_columns",
            "verbose",
            "retry_count",
            "retry_delay_seconds",
        }
        kwargs = {key: value for key, value in settings.items() if key in allowed_settings}
        return cls(source=source, target=target, **kwargs)

    @classmethod
    def run_config_file(cls, path: str | Path) -> list[TableSyncResult]:
        """Load a YAML/JSON config file and run its table sync job."""
        config_path = Path(path)
        config = cls._load_job_config(config_path)
        sync = cls.from_job_config(config)
        return sync.sync_tables(config.get("tables") or {})

    @staticmethod
    def _load_job_config(path: Path) -> dict[str, Any]:
        """Parse a JSON or YAML job file.

        YAML support is optional; JSON works with the standard library.  Raising a
        clear ImportError keeps scheduled jobs from failing later with an obscure
        missing-module traceback.
        """
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".json":
            return json.loads(text)
        if path.suffix.lower() in {".yaml", ".yml"}:
            try:
                import yaml
            except ImportError as exc:
                raise ImportError("PyYAML is required to read YAML job configs; use JSON or install pyyaml") from exc
            data = yaml.safe_load(text)
            return data if isinstance(data, dict) else {}
        raise ValueError("Job config file must end with .json, .yaml, or .yml")

    def export_query_to_file(
        self,
        query: str | Path,
        output_path: str | Path,
        params: Sequence[Any] | None = None,
        file_format: str | None = None,
    ) -> int:
        """Execute a source query and write its rows to a local file.

        query can be a SQL string or a path to a .sql file; the file is read
        and its contents used as the query string.
        Returns the number of rows written.  file_format overrides extension-based
        detection when the output path's suffix is ambiguous or missing.
        """
        if self.source is None:
            raise ValueError("source connector/config is required for export")
        query_str = self._resolve_query(query)
        self.source.connect()
        try:
            rows = self.source.execute_query(query_str, params or [])
        finally:
            self.source.close()
        return self.file_transfer.write(rows, output_path, file_format)

    def import_file_to_table(
        self,
        input_path: str | Path,
        destination: str,
        file_format: str | None = None,
        fresh_insert: bool = False,
    ) -> int:
        """Read a local file and insert it into a target table.

        The target table is created automatically if it doesn't exist; column types
        are inferred from the first row of the file via _infer_columns.
        fresh_insert=True truncates an existing table before inserting.

        Returns the number of rows inserted.
        """
        if self.target is None:
            raise ValueError("target connector/config is required for import")
        rows = self.file_transfer.read(input_path, file_format)
        target_name = parse_qualified_name(destination, self.target.config.default_schema)
        self.target.connect()
        try:
            if not self.target.table_exists(target_name.schema, target_name.table):
                self.target.create_schema(target_name.schema)
                self.target.create_table(target_name.schema, target_name.table, self._infer_columns(rows))
            elif fresh_insert:
                self.target.truncate_table(target_name.schema, target_name.table)
            if not rows:
                return 0
            return self.target.insert_batch(target_name.schema, target_name.table, rows, list(rows[0].keys()))
        finally:
            self.target.close()

    def _sync_one_table(self, name: str, spec: dict[str, Any]) -> TableSyncResult:
        """Synchronize a single table spec and return audited runtime details.

        This is the main workflow body. Keep it readable and policy-oriented:
        parse the spec, align schema, stream batches, then apply optional modes
        such as staging, snapshots, soft deletes, watermarks, and expectations.
        """
        _t0 = time.monotonic()
        if "source" not in spec or "destination" not in spec:
            raise ValueError(f"Table spec '{name}' must include source and destination")

        mode = TransferMode(spec.get("mode", TransferMode.APPEND.value))
        source_name = parse_qualified_name(spec["source"], self.source.config.default_schema)
        target_name = parse_qualified_name(spec["destination"], self.target.config.default_schema)
        rename_map = self._normalize_rename_map(spec.get("rename"))
        result = TableSyncResult(
            name=name,
            source=spec["source"],
            destination=spec["destination"],
            mode=mode.value,
            dry_run=self.dry_run,
        )

        # Schema sync always runs (even in dry_run) so the result captures what
        # would be created/altered; actual DDL is gated inside _sync_schema.
        source_columns = self.source.get_columns(source_name.schema, source_name.table)
        target_columns = self.schema_mapper.map_columns(source_columns, self.source.engine, self.target.engine)
        target_columns = self._apply_column_options(target_columns, rename_map, spec.get("type_overrides"))
        if mode == TransferMode.SNAPSHOT:
            target_columns = self._ensure_system_column(target_columns, "_synced_at", self._timestamp_type())
        if mode == TransferMode.SOFT_DELETE:
            target_columns = self._ensure_system_column(target_columns, "deleted_at", self._timestamp_type())
        self._sync_schema(target_name.schema, target_name.table, target_columns, result)

        if self.dry_run:
            return result

        write_schema, write_table = target_name.schema, target_name.table
        staging_table = f"__syncdb_{target_name.table}_staging" if mode == TransferMode.APPEND_STAGING else None
        if staging_table:
            # The generic staging path keeps the live table untouched while rows are
            # loaded, then does a final truncate/copy.  It is portable across engines;
            # a future connector-native implementation can upgrade this to true
            # transactional rename/swap where the engine supports it.
            #
            # The staging table is always dropped first (idempotent re-runs) and is
            # dropped again in the `finally` block below even on failure, so stale
            # staging tables never accumulate across runs.
            self.target.drop_table(target_name.schema, staging_table)
            self.target.create_table(target_name.schema, staging_table, target_columns)
            write_table = staging_table
        elif mode == TransferMode.FULL_REFRESH:
            self.target.truncate_table(target_name.schema, target_name.table)

        filter_sql, params = build_where_clause(spec.get("filter"))
        watermark_cfg = self._load_watermark(spec)
        if watermark_cfg:
            filter_sql, params = self._apply_watermark_filter(filter_sql, params, watermark_cfg["column"], watermark_cfg["value"])
        order_sql = build_order_by(spec.get("order_by"), self.source.quote_char)
        total = self._safe_source_count(source_name.schema, source_name.table, filter_sql, params)
        batch_size = self._resolve_batch_size(total, spec.get("batch_size"))
        column_names = [column.name for column in source_columns]
        target_column_names = [column.name for column in target_columns]
        # Prefer an explicit primary_key list from the spec; fall back to columns
        # flagged is_primary_key by the source connector's metadata query.
        source_primary_key = list(spec.get("primary_key") or [column.name for column in source_columns if column.is_primary_key])
        target_primary_key = [rename_map.get(column, column) for column in source_primary_key]
        transform = spec.get("transform")
        on_batch = spec.get("on_batch")
        snapshot_ts = datetime.now(timezone.utc).isoformat() if mode == TransferMode.SNAPSHOT else None
        seen_keys: set[tuple[Any, ...]] = set()

        self.progress.start()
        try:
            for raw_batch in self.source.fetch_batches(
                source_name.schema,
                source_name.table,
                columns=column_names,
                where=filter_sql,
                params=params,
                order_by=order_sql,
                batch_size=batch_size,
            ):
                if watermark_cfg:
                    result.watermark_value = self._max_value(result.watermark_value, raw_batch, watermark_cfg["column"])
                batch = self._prepare_batch(raw_batch, rename_map, transform, target_column_names, mode, snapshot_ts)
                if not batch:
                    continue
                if target_primary_key:
                    seen_keys.update(tuple(row[column] for column in target_primary_key) for row in batch)

                def write_batch() -> int:
                    # APPEND/UPSERT/SOFT_DELETE replace rows that match on PK. INSERT_ONLY
                    # and SNAPSHOT deliberately preserve existing target rows.
                    #
                    # This closure captures `write_schema` and `write_table`, NOT
                    # `target_name.schema`/`target_name.table` directly.  In
                    # APPEND_STAGING mode those two variables are rebound to point
                    # at the staging table, so the closure automatically routes
                    # writes to staging without any extra branching.
                    if mode in {TransferMode.APPEND, TransferMode.UPSERT, TransferMode.SOFT_DELETE} and target_primary_key:
                        self.target.delete_matching_rows(write_schema, write_table, batch, target_primary_key)
                    return self.target.insert_batch(write_schema, write_table, batch, target_column_names)

                written = self._with_retries(write_batch)
                result.batches += 1
                result.rows_read += len(raw_batch)
                result.rows_written += written
                self.progress.update(result.destination, result.rows_written, total)
                if on_batch:
                    on_batch(result)

            if staging_table:
                self._replace_from_staging(target_name.schema, target_name.table, staging_table, target_column_names)
            if mode == TransferMode.SOFT_DELETE and target_primary_key:
                result.rows_soft_deleted = self._apply_soft_deletes(
                    target_name.schema,
                    target_name.table,
                    target_primary_key,
                    seen_keys,
                )
            if watermark_cfg and result.watermark_value is not None:
                self._save_watermark(watermark_cfg, result.watermark_value)
            self._validate_expectations(target_name.schema, target_name.table, spec.get("expect"), result)
        finally:
            if staging_table:
                self.target.drop_table(target_name.schema, staging_table)

        result.duration_seconds = time.monotonic() - _t0
        return result

    def _sync_schema(
        self,
        schema: str | None,
        table: str,
        columns: list[Column],
        result: TableSyncResult,
    ) -> None:
        """Create or evolve the target table to match the mapped source columns.

        Column matching is case-insensitive (both sides lowercased) so MSSQL's
        case-insensitive collation and PostgreSQL's case-sensitive one don't cause
        false mismatches when the only difference is letter case.

        Columns are only added, never altered in type.  Drop only happens when
        drop_extra_columns=True (off by default to protect manually-added columns).
        """
        exists = self.target.table_exists(schema, table)
        if not exists:
            result.schema_created = bool(schema)
            result.table_created = True
            if not self.dry_run:
                self.target.create_schema(schema)
                self.target.create_table(schema, table, columns)
            return

        target_columns = {column.name.lower(): column for column in self.target.get_columns(schema, table)}
        source_columns = {column.name.lower(): column for column in columns}

        for key, column in source_columns.items():
            if key not in target_columns:
                result.columns_added.append(column.name)
                if not self.dry_run:
                    self.target.add_column(schema, table, column)

        if self.drop_extra_columns:
            for key, column in target_columns.items():
                if key not in source_columns:
                    result.columns_dropped.append(column.name)
                    if not self.dry_run:
                        self.target.drop_column(schema, table, column.name)

    def _normalize_rename_map(self, rename: dict[str, str] | None) -> dict[str, str]:
        """Validate source-to-target column rename configuration."""
        mapping = dict(rename or {})
        for source, target in mapping.items():
            validate_identifier(source)
            validate_identifier(target)
        return mapping

    def _apply_column_options(
        self,
        columns: list[Column],
        rename_map: dict[str, str],
        type_overrides: dict[str, str] | None,
    ) -> list[Column]:
        """Apply per-table rename and target-type override options to mapped columns."""
        overrides = dict(type_overrides or {})
        for name in overrides:
            validate_identifier(name)
        result: list[Column] = []
        for column in columns:
            target_name = rename_map.get(column.name, column.name)
            data_type = overrides.get(target_name, column.data_type)
            result.append(replace(column, name=target_name, data_type=data_type))
        return result

    def _ensure_system_column(self, columns: list[Column], name: str, data_type: str) -> list[Column]:
        """Append a SyncDB-managed metadata column when it is not already present."""
        if any(column.name.lower() == name.lower() for column in columns):
            return columns
        return [*columns, Column(name=name, data_type=data_type, nullable=True)]

    def _timestamp_type(self) -> str:
        """Return a portable timestamp type for SyncDB-managed metadata columns."""
        if self.target.engine == "mssql":
            return "datetime2"
        if self.target.engine == "sqlite":
            return "text"
        return "timestamp"

    def _prepare_batch(
        self,
        raw_batch: list[dict[str, Any]],
        rename_map: dict[str, str],
        transform: Any,
        target_columns: Sequence[str],
        mode: TransferMode,
        snapshot_ts: str | None,
    ) -> list[dict[str, Any]]:
        """Transform rows, apply target column names, and add system columns."""
        rows = [dict(row) for row in raw_batch]
        if transform:
            transformed = transform(rows)
            if transformed is not None:
                rows = [dict(row) for row in transformed]
        prepared: list[dict[str, Any]] = []
        for row in rows:
            mapped = {rename_map.get(column, column): value for column, value in row.items()}
            if mode == TransferMode.SNAPSHOT:
                mapped["_synced_at"] = snapshot_ts
            if mode == TransferMode.SOFT_DELETE:
                mapped["deleted_at"] = None
            prepared.append({column: mapped.get(column) for column in target_columns})
        return prepared

    def _with_retries(self, operation):
        """Run a database write operation with simple exponential backoff."""
        attempt = 0
        while True:
            try:
                return operation()
            except Exception:
                if attempt >= self.retry_count:
                    raise
                time.sleep(self.retry_delay_seconds * (2 ** attempt))
                attempt += 1

    def _replace_from_staging(
        self,
        schema: str | None,
        table: str,
        staging_table: str,
        columns: Sequence[str],
    ) -> None:
        """Replace live rows from a staging table using portable SQL."""
        def replace_rows() -> None:
            self.target.truncate_table(schema, table)
            self.target.copy_table_rows(schema, staging_table, schema, table, columns)

        self._with_retries(replace_rows)

    def _apply_soft_deletes(
        self,
        schema: str | None,
        table: str,
        primary_key: Sequence[str],
        seen_keys: set[tuple[Any, ...]],
    ) -> int:
        """Mark target rows missing from the source as deleted.

        PERFORMANCE NOTE: This method fetches ALL target rows (primary key columns
        only) to find rows that were absent from the source.  For tables with tens of
        millions of rows this can be slow and memory-intensive.  If that becomes a
        problem, consider splitting the SOFT_DELETE logic into a separate scheduled
        cleanup job that compares source/target via a JOIN rather than pulling all
        keys into Python.
        """
        missing: list[dict[str, Any]] = []
        for batch in self.target.fetch_batches(schema, table, columns=primary_key, batch_size=self.batch_size):
            for row in batch:
                key = tuple(row[column] for column in primary_key)
                if key not in seen_keys:
                    missing.append(row)
        if not missing:
            return 0
        deleted_at = datetime.now(timezone.utc).isoformat()
        return self._with_retries(lambda: self.target.update_matching_rows(schema, table, missing, primary_key, {"deleted_at": deleted_at}))

    def _load_watermark(self, spec: dict[str, Any]) -> dict[str, Any] | None:
        """Load incremental-sync state for a table spec, if configured."""
        column = spec.get("incremental_column")
        store = spec.get("watermark_store")
        if not column:
            return None
        validate_identifier(column)
        path = Path(store or ".syncdb_watermarks.json")
        key = spec.get("watermark_key") or f"{spec['source']}->{spec['destination']}:{column}"
        values = self._read_watermark_file(path)
        return {"path": path, "key": key, "column": column, "value": values.get(key)}

    def _read_watermark_file(self, path: Path) -> dict[str, Any]:
        """Read the JSON watermark store, returning an empty mapping when absent."""
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}

    def _save_watermark(self, config: dict[str, Any], value: Any) -> None:
        """Persist the latest processed incremental value after a successful sync.

        CONSISTENCY NOTE: _max_value() updates the in-memory watermark value as each
        batch is streamed, but _save_watermark() is only called after ALL batches
        complete without error.  If the sync fails mid-stream, the watermark file is
        NOT updated — the next run re-reads from the last persisted value, which means
        some rows near the watermark boundary will be re-processed.  This is an
        at-least-once delivery guarantee, NOT exactly-once.  Target tables should be
        idempotent (i.e. APPEND/UPSERT mode) when using incremental sync to tolerate
        duplicate rows arriving from the overlap window.
        """
        path: Path = config["path"]
        values = self._read_watermark_file(path)
        values[config["key"]] = value.isoformat() if hasattr(value, "isoformat") else value
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(values, handle, indent=2, sort_keys=True)

    def _apply_watermark_filter(
        self,
        where_sql: str,
        params: list[Any],
        column: str,
        value: Any,
    ) -> tuple[str, list[Any]]:
        """Append an incremental-column predicate to an existing WHERE clause."""
        if value in {None, ""}:
            return where_sql, params
        condition = f"{quote_identifier(column, self.source.quote_char)} > {self.source.placeholder}"
        if not where_sql:
            return f" WHERE {condition} ", [*params, value]
        existing = where_sql.strip()
        if existing.upper().startswith("WHERE "):
            existing = existing[6:].strip()
        return f" WHERE ({existing}) AND ({condition}) ", [*params, value]

    def _max_value(self, current: Any, rows: list[dict[str, Any]], column: str) -> Any:
        """Track the maximum non-null watermark value seen across fetched batches."""
        values = [row.get(column) for row in rows if row.get(column) is not None]
        if not values:
            return current
        batch_max = max(values)
        if current is None or batch_max > current:
            return batch_max
        return current

    def _validate_expectations(
        self,
        schema: str | None,
        table: str,
        expect: dict[str, Any] | None,
        result: TableSyncResult,
    ) -> None:
        """Run optional data-quality checks after a table sync.

        PERFORMANCE NOTE: All target rows are fetched into memory for checking.
        This is intentionally simple and suitable for tables up to a few million rows.
        For very large tables, consider running expectations via a separate SQL-based
        monitoring job (e.g. dbt tests, Great Expectations) rather than loading all
        rows into Python.  The `expect` block is best used as a lightweight sanity
        check, not a full data-quality framework.
        """
        if not expect:
            return
        rows = [row for batch in self.target.fetch_batches(schema, table, batch_size=self.batch_size) for row in batch]
        failures: list[str] = []
        min_rows = expect.get("min_rows")
        if min_rows is not None and len(rows) < int(min_rows):
            failures.append(f"expected at least {min_rows} rows, found {len(rows)}")
        for column in expect.get("not_null", []) or []:
            validate_identifier(column)
            null_count = sum(1 for row in rows if row.get(column) is None)
            if null_count:
                failures.append(f"{column} has {null_count} null values")
        for key in expect.get("unique", []) or []:
            columns = [key] if isinstance(key, str) else list(key)
            for column in columns:
                validate_identifier(column)
            seen = set()
            duplicates = 0
            for row in rows:
                value = tuple(row.get(column) for column in columns)
                if value in seen:
                    duplicates += 1
                seen.add(value)
            if duplicates:
                failures.append(f"{', '.join(columns)} has {duplicates} duplicate rows")
        for column, bounds in (expect.get("range") or {}).items():
            validate_identifier(column)
            minimum = bounds.get("min")
            maximum = bounds.get("max")
            for row in rows:
                value = row.get(column)
                if value is None:
                    continue
                if minimum is not None and value < minimum:
                    failures.append(f"{column} has value below {minimum}: {value}")
                    break
                if maximum is not None and value > maximum:
                    failures.append(f"{column} has value above {maximum}: {value}")
                    break
        result.expectations_failed = failures
        if failures:
            raise ValueError(f"Data quality checks failed for {result.destination}: " + "; ".join(failures))

    def _safe_source_count(
        self,
        schema: str | None,
        table: str,
        where: str,
        params: Sequence[Any],
    ) -> int | None:
        # Row count is used only for progress display; swallowing errors here
        # means a missing SELECT COUNT(*) permission won't abort an otherwise valid sync.
        try:
            return self.source.get_row_count(schema, table, where, params)
        except Exception:
            return None

    def _infer_columns(self, rows: list[dict[str, Any]]) -> list[Column]:
        """Infer column types from the Python types in the first row of file data.

        Uses PostgreSQL type names as the intermediate representation and then
        maps them to the target engine via SchemaMapper.  This keeps the type
        inference logic engine-agnostic.

        Only four broad types are produced (boolean, bigint, double precision, text)
        because file formats like CSV carry no type metadata; the target table can
        always be pre-created manually for finer control.

        CAVEAT: Only the FIRST row is sampled.  If a column is None in that row but
        contains integers in subsequent rows, the column will be inferred as "text".
        Pre-create the target table with explicit types when type accuracy matters,
        especially for CSV files where every value arrives as a string anyway.
        """
        if not rows:
            raise ValueError("Cannot infer a target table from an empty file")
        sample = rows[0]
        columns: list[Column] = []
        for name, value in sample.items():
            if isinstance(value, bool):
                data_type = "boolean"
            elif isinstance(value, int):
                data_type = "bigint"
            elif isinstance(value, float):
                data_type = "double precision"
            else:
                data_type = "text"
            columns.append(Column(name=name, data_type=data_type, nullable=True))
        # bool check must come before int because bool is a subclass of int in Python.
        return self.schema_mapper.map_columns(columns, "postgresql", self.target.engine)

    @staticmethod
    def _parse_batch_size(batch_size: int | str) -> tuple[int, float | None]:
        """Parse batch_size; accepts an integer count or a percentage string like '10%'."""
        if isinstance(batch_size, str):
            stripped = batch_size.strip()
            if not stripped.endswith("%"):
                raise ValueError("batch_size string must be a percentage like '10%'")
            pct = float(stripped[:-1])
            if not (0 < pct <= 100):
                raise ValueError("batch_size percentage must be between 0 and 100")
            return 5000, pct / 100
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        return batch_size, None

    def _resolve_batch_size(self, total: int | None, override: int | str | None = None) -> int:
        """Return the effective batch size, resolving a percentage against the total row count.

        override takes precedence over the instance-level batch_size when provided,
        allowing per-table or per-schema customisation without mutating the object.
        """
        if override is not None:
            size, pct = self._parse_batch_size(override)
            if pct is not None and total and total > 0:
                return max(1, int(total * pct))
            return size
        if self._batch_pct is None:
            return self.batch_size
        if total and total > 0:
            return max(1, int(total * self._batch_pct))
        return self.batch_size

    @staticmethod
    def _resolve_query(query: str | Path) -> str:
        """Return the SQL string, reading it from a .sql file when a path is given."""
        path = Path(query)
        if path.suffix.lower() == ".sql" and path.exists():
            return path.read_text(encoding="utf-8")
        return str(query)

    def _coerce_connector(self, value: DatabaseConfig | BaseConnector | None) -> BaseConnector | None:
        """Accept DatabaseConfig, a ready-made connector, or None."""
        if value is None:
            return None
        if isinstance(value, BaseConnector):
            return value
        if isinstance(value, DatabaseConfig):
            return create_connector(value)
        raise TypeError("Expected DatabaseConfig, BaseConnector, or None")

    def _normalize_verbose(self, verbose: str | None) -> str | None:
        """Validate the summary mode once during construction.

        Accepting "none" as a string is convenient for YAML/JSON job configs, where
        the natural `None` value may arrive as text after parsing environment input.
        """
        if verbose is None:
            return None
        value = str(verbose).strip().lower()
        if value in {"", "none"}:
            return None
        if value in {"standard", "detailed"}:
            return value
        raise ValueError("verbose must be one of: None, 'standard', 'detailed'")

    def _emit_summary(self, results: list[TableSyncResult]) -> None:
        """Print a final sync summary when verbose mode is enabled.

        The method receives completed TableSyncResult objects only.  If a sync
        raises before finishing, Python propagates the exception after the finally
        cleanup block in sync_tables, so no misleading partial summary is printed.
        """
        if self.verbose is None:
            return
        if self.verbose == "standard":
            headers = ["table", "mode", "rows written", "batches", "created", "time"]
            rows = [
                [
                    result.destination,
                    result.mode,
                    f"{result.rows_written:,}",
                    str(result.batches),
                    "yes" if result.table_created else "no",
                    _format_elapsed(result.duration_seconds),
                ]
                for result in results
            ]
        else:
            headers = [
                "name",
                "source",
                "destination",
                "mode",
                "read",
                "written",
                "soft deleted",
                "batches",
                "schema",
                "table",
                "added",
                "dropped",
                "checks",
                "watermark",
                "dry run",
                "time",
            ]
            rows = [
                [
                    result.name,
                    result.source,
                    result.destination,
                    result.mode,
                    f"{result.rows_read:,}",
                    f"{result.rows_written:,}",
                    f"{result.rows_soft_deleted:,}",
                    str(result.batches),
                    "yes" if result.schema_created else "no",
                    "yes" if result.table_created else "no",
                    ", ".join(result.columns_added) or "-",
                    ", ".join(result.columns_dropped) or "-",
                    "fail" if result.expectations_failed else "ok",
                    str(result.watermark_value) if result.watermark_value is not None else "-",
                    "yes" if result.dry_run else "no",
                    _format_elapsed(result.duration_seconds),
                ]
                for result in results
            ]

        total_duration = sum(r.duration_seconds for r in results)
        self.verbose_stream.write(f"\nSyncDB summary ({self.verbose})\n")
        self._write_table(headers, rows)
        self.verbose_stream.write(
            f"total: {sum(result.rows_written for result in results):,} rows "
            f"in {sum(result.batches for result in results):,} batches "
            f"across {len(results):,} tables "
            f"in {_format_elapsed(total_duration)}\n"
        )
        self.verbose_stream.flush()

    def _write_table(self, headers: list[str], rows: list[list[str]]) -> None:
        """Render a small ASCII table to the configured verbose stream.

        Keeping this formatter local avoids a runtime dependency for one reporting
        feature, and ASCII output remains readable in Windows terminals, CI logs,
        and redirected files.
        """
        widths = [
            max(len(header), *(len(row[index]) for row in rows)) if rows else len(header)
            for index, header in enumerate(headers)
        ]
        separator = "+" + "+".join("-" * (width + 2) for width in widths) + "+\n"
        header_line = "| " + " | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)) + " |\n"
        self.verbose_stream.write(separator)
        self.verbose_stream.write(header_line)
        self.verbose_stream.write(separator)
        for row in rows:
            self.verbose_stream.write(
                "| " + " | ".join(value.ljust(widths[index]) for index, value in enumerate(row)) + " |\n"
            )
        self.verbose_stream.write(separator)
