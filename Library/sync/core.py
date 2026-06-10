"""High-level SyncDB orchestration API.

This module coordinates connectors, schema mapping, batching, retries, progress,
and file IO. Keep connector-specific SQL out of this layer; SyncDB should express
workflow policy while connector classes own engine syntax and driver behavior.
"""

from __future__ import annotations

import contextlib
import dataclasses
import fnmatch
import json
import logging
import sys
import threading
import time
import uuid
import warnings
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

from ..config import DatabaseConfig
from ..connections import create_connector
from ..connectors.base import BaseConnector
from ..files import FileTransfer
from ..progress import ProgressMode, ProgressReporter
from ..sql import build_order_by, build_where_clause, parse_qualified_name, validate_identifier, validate_type
from ..type_mapping import Column, SchemaMapper
from . import watermark as wm
from .inference import infer_columns
from .models import TableSyncResult, TransferMode
from .quality import validate_expectations
from .reporting import emit_summary
from .retry import with_retries
from .staging import create_staging_table, replace_from_staging

logger = logging.getLogger(__name__)

# Allowlist of valid DatabaseConfig constructor fields.  Used by from_job_config
# to reject unexpected keys from user-supplied config files before they reach the
# DatabaseConfig constructor, preventing unknown kwargs from propagating to
# database drivers.
_CONFIG_FIELDS: frozenset[str] = frozenset(
    f.name for f in dataclasses.fields(DatabaseConfig)
)

# Settings keys accepted by from_job_config.  Unknown keys are warned about so
# typos in job files surface immediately rather than silently doing nothing.
_ALLOWED_SETTINGS: frozenset[str] = frozenset({
    "batch_size",
    "progress_mode",
    "dry_run",
    "drop_extra_columns",
    "verbose",
    "retry_count",
    "retry_delay_seconds",
    "use_transaction",
    "max_workers",
})


class SyncDB:
    """Main class-based API for database and local-file synchronization.

    Typical usage patterns:
      - Database to database: supply both source and target; call sync_tables().
      - Database to file:     supply source only; call export_query_to_file().
      - File to database:     supply target only; call import_file_to_table().

    source/target accept either a DatabaseConfig (connector is created internally)
    or an already-constructed BaseConnector (useful for testing with a mock connector).
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
        use_transaction: bool = False,
        max_workers: int = 1,
    ) -> None:
        if source_connector is not None:
            warnings.warn(
                "source_connector is deprecated; pass a BaseConnector as the "
                "positional source= argument instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        if target_connector is not None:
            warnings.warn(
                "target_connector is deprecated; pass a BaseConnector as the "
                "positional target= argument instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        self.batch_size, self._batch_pct = self._parse_batch_size(batch_size)
        if retry_count < 0:
            raise ValueError("retry_count must be zero or greater")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be zero or greater")
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self.source = source_connector or self._coerce_connector(source)
        self.target = target_connector or self._coerce_connector(target)
        self.progress = ProgressReporter(progress_mode)
        self.dry_run = dry_run
        self.drop_extra_columns = drop_extra_columns
        self.schema_mapper = schema_mapper or SchemaMapper()
        self.file_transfer = file_transfer or FileTransfer()
        self.verbose = self._normalize_verbose(verbose)
        self.verbose_stream = verbose_stream or sys.stdout
        self.retry_count = retry_count
        self.retry_delay_seconds = retry_delay_seconds
        # When True, each table sync is wrapped in a single BEGIN/COMMIT transaction.
        # A mid-sync failure triggers ROLLBACK so no partial data is written.
        # Note: MySQL DDL (TRUNCATE/CREATE) is auto-committed by the engine and
        # cannot be rolled back regardless of this setting.
        self.use_transaction = use_transaction
        # max_workers > 1 enables parallel table syncs using a thread pool.
        # Each worker thread gets its own connector pair created from the source/target
        # DatabaseConfig.  Passing raw BaseConnector instances as source/target is not
        # supported with max_workers > 1 (raises ValueError at sync time).
        self.max_workers = max_workers
        # Seam for testing: override to inject a custom connector factory.
        # Production code leaves this None and _sync_tables_parallel uses create_connector.
        self._connector_factory: Callable[[DatabaseConfig], BaseConnector] | None = None

    def __repr__(self) -> str:
        src = self.source.config.engine if self.source else None
        tgt = self.target.config.engine if self.target else None
        return (
            f"SyncDB(source={src!r}, target={tgt!r}, "
            f"batch_size={self.batch_size!r}, dry_run={self.dry_run!r})"
        )

    def sync_tables(
        self,
        tables: dict[str, dict[str, Any]],
        batch_size: int | str | None = None,
    ) -> list[TableSyncResult]:
        """Synchronize one or more database tables from source to target.

        tables is a dict keyed by a user-assigned logical name.  Each value is
        a spec dict with at minimum "source" and "destination" table names, plus
        optional "mode", "filter", "order_by", "primary_key", and "batch_size" keys.

        When max_workers > 1, tables are synced in parallel — each thread receives
        fresh connectors created from the source/target DatabaseConfig.  Passing a
        raw BaseConnector as source or target raises ValueError in parallel mode.
        """
        if self.source is None or self.target is None:
            raise ValueError("source and target connectors/configs are required for database sync")
        specs: dict[str, dict[str, Any]] = {}
        for name, spec in tables.items():
            if batch_size is not None and "batch_size" not in spec:
                spec = {**spec, "batch_size": batch_size}
            specs[name] = spec

        sync_id = uuid.uuid4().hex[:8]
        if self.max_workers > 1:
            return self._sync_tables_parallel(specs, sync_id)
        return self._sync_tables_sequential(specs, sync_id)

    def _sync_tables_sequential(
        self, specs: dict[str, dict[str, Any]], sync_id: str
    ) -> list[TableSyncResult]:
        results: list[TableSyncResult] = []
        _log = logging.LoggerAdapter(logger, {"sync_id": sync_id})
        _log.info("Starting sequential sync of %d table(s)", len(specs))
        self.source.connect()
        self.target.connect()
        self.progress.label_width = max(
            (len(spec.get("destination", "")) for spec in specs.values()), default=0
        )
        try:
            for name, spec in specs.items():
                results.append(self._sync_one_table(name, spec, self.source, self.target, sync_id=sync_id))
        finally:
            self.progress.finish()
            self.source.close()
            self.target.close()
        emit_summary(results, self.verbose, self.verbose_stream)
        return results

    def _sync_tables_parallel(
        self, specs: dict[str, dict[str, Any]], sync_id: str
    ) -> list[TableSyncResult]:
        if not hasattr(self.source, "config") or not hasattr(self.target, "config"):
            raise ValueError(
                "max_workers > 1 requires DatabaseConfig-backed connectors. "
                "Pass source/target as DatabaseConfig, not raw BaseConnector instances."
            )
        _log = logging.LoggerAdapter(logger, {"sync_id": sync_id})
        _log.info(
            "Starting parallel sync of %d table(s) with %d workers",
            len(specs), self.max_workers,
        )
        results: list[TableSyncResult | None] = [None] * len(specs)
        # abort is set by the first failing future so later queued workers exit early.
        abort = threading.Event()
        errors: list[BaseException] = []

        factory = self._connector_factory or create_connector

        def sync_in_thread(index: int, name: str, spec: dict[str, Any]) -> tuple[int, TableSyncResult]:
            if abort.is_set():
                raise RuntimeError(f"Sync of '{name}' cancelled due to earlier failure")
            src = factory(self.source.config)
            tgt = factory(self.target.config)
            reporter = ProgressReporter(ProgressMode.NONE)
            src.connect()
            tgt.connect()
            try:
                return index, self._sync_one_table(
                    name, spec, src, tgt, progress=reporter, sync_id=sync_id, abort=abort
                )
            finally:
                src.close()
                tgt.close()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(sync_in_thread, i, name, spec): name
                for i, (name, spec) in enumerate(specs.items())
            }
            for future in as_completed(futures):
                try:
                    idx, result = future.result()
                    results[idx] = result
                except Exception as exc:
                    errors.append(exc)
                    _log.error(
                        "Table '%s' failed: %s",
                        futures[future], exc,
                        exc_info=True,
                    )
                    abort.set()
                    for f in futures:
                        f.cancel()

        if errors:
            if len(errors) == 1:
                raise errors[0]
            # Raise a combined error that preserves all failure details.
            combined = RuntimeError(
                f"{len(errors)} table(s) failed during parallel sync: "
                + "; ".join(f"{type(e).__name__}: {e}" for e in errors)
            )
            raise combined from errors[0]

        final = [r for r in results if r is not None]
        emit_summary(final, self.verbose, self.verbose_stream)
        return final

    def sync_schema(
        self,
        source_schema: str | None,
        destination_schema: str | None,
        exclude: Sequence[str] | None = None,
        mode: str = TransferMode.APPEND.value,
        batch_size: int | str | None = None,
        table_prefix: str = "",
        table_suffix: str = "",
        **table_defaults: Any,
    ) -> list[TableSyncResult]:
        """Synchronize every table in a source schema."""
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
                "destination": (
                    f"{destination_schema}.{table_prefix}{name}{table_suffix}"
                    if destination_schema
                    else f"{table_prefix}{name}{table_suffix}"
                ),
                "mode": mode,
            }
            for name in names
            if not any(fnmatch.fnmatch(name, pattern) for pattern in patterns)
        }
        return self.sync_tables(tables, batch_size=batch_size)

    @classmethod
    def from_job_config(cls, config: dict[str, Any]) -> SyncDB:
        """Build a SyncDB instance from a parsed YAML/JSON job config."""
        settings = dict(config.get("settings") or {})

        unknown_settings = set(settings) - _ALLOWED_SETTINGS
        if unknown_settings:
            logger.warning(
                "Unknown setting(s) in job config will be ignored: %s. "
                "Valid settings: %s",
                sorted(unknown_settings),
                sorted(_ALLOWED_SETTINGS),
            )

        source = cls._parse_db_config(config.get("source"), "source") if config.get("source") else None
        target = cls._parse_db_config(config.get("target"), "target") if config.get("target") else None
        kwargs = {key: value for key, value in settings.items() if key in _ALLOWED_SETTINGS}
        return cls(source=source, target=target, **kwargs)

    @staticmethod
    def _parse_db_config(raw: dict[str, Any], context: str) -> DatabaseConfig:
        """Validate and construct a DatabaseConfig from a user-supplied dict.

        Only keys that are valid DatabaseConfig fields are forwarded.  Unknown
        keys raise ValueError so job config typos surface immediately rather
        than being silently ignored or forwarded to database drivers.
        """
        unknown = set(raw) - _CONFIG_FIELDS
        if unknown:
            raise ValueError(
                f"Unknown DatabaseConfig field(s) in job config '{context}': "
                f"{sorted(unknown)}. Valid fields: {sorted(_CONFIG_FIELDS)}"
            )
        return DatabaseConfig(**{k: raw[k] for k in raw if k in _CONFIG_FIELDS})

    @classmethod
    def run_config_file(cls, path: str | Path) -> list[TableSyncResult]:
        """Load a YAML/JSON config file and run its table sync job."""
        config_path = Path(path)
        config = cls._load_job_config(config_path)
        sync = cls.from_job_config(config)
        return sync.sync_tables(config.get("tables") or {})

    @staticmethod
    def _load_job_config(path: Path) -> dict[str, Any]:
        """Parse a JSON or YAML job file."""
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
        batch_size: int | None = None,
    ) -> int:
        """Execute a source query and write its rows to a local file.

        Uses streaming batch reads (cursor.fetchmany) so the full result set is
        never loaded into Python memory at once.  CSV and Parquet are written
        incrementally; Excel and Pickle still materialise all rows.

        query can be a SQL string or a path to a .sql file.
        Returns the number of rows written.
        """
        if self.source is None:
            raise ValueError("source connector/config is required for export")
        query_str = self._resolve_query(query)
        effective_batch_size = batch_size or self.batch_size
        self.source.connect()
        try:
            batches = self.source.execute_query_batches(query_str, params or [], effective_batch_size)
            return self.file_transfer.write_streaming(batches, output_path, file_format)
        finally:
            self.source.close()

    def import_file_to_table(
        self,
        input_path: str | Path,
        destination: str,
        file_format: str | None = None,
        fresh_insert: bool = False,
    ) -> int:
        """Read a local file and insert it into a target table.

        The target table is created automatically if it doesn't exist; column types
        are inferred from the first row of the file via infer_columns().
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
                self.target.create_table(
                    target_name.schema, target_name.table,
                    infer_columns(rows, self.target.engine, self.schema_mapper),
                )
            elif fresh_insert:
                self.target.truncate_table(target_name.schema, target_name.table)
            if not rows:
                return 0
            return self.target.insert_batch(target_name.schema, target_name.table, rows, list(rows[0].keys()))
        finally:
            self.target.close()

    def _sync_one_table(
        self,
        name: str,
        spec: dict[str, Any],
        source: BaseConnector,
        target: BaseConnector,
        progress: ProgressReporter | None = None,
        sync_id: str | None = None,
        abort: threading.Event | None = None,
    ) -> TableSyncResult:
        """Synchronize a single table spec and return audited runtime details.

        abort, when supplied (parallel mode), is checked at the top of each batch
        so a worker stops promptly after a sibling table fails instead of running
        to completion — ThreadPoolExecutor cannot interrupt an in-flight future.
        """
        _t0 = time.monotonic()
        _log = logging.LoggerAdapter(logger, {"sync_id": sync_id or "?"})

        if "source" not in spec or "destination" not in spec:
            raise ValueError(f"Table spec '{name}' must include source and destination")
        _log.info(
            "Syncing '%s': %s → %s (mode=%s)",
            name, spec["source"], spec["destination"], spec.get("mode", "append"),
        )
        reporter = progress or self.progress
        use_tx = self.use_transaction or bool(spec.get("use_transaction", False))

        mode = TransferMode(spec.get("mode", TransferMode.APPEND.value))
        source_name = parse_qualified_name(spec["source"], source.config.default_schema)
        target_name = parse_qualified_name(spec["destination"], target.config.default_schema)
        rename_map = self._normalize_rename_map(spec.get("rename"))
        result = TableSyncResult(
            name=name,
            source=spec["source"],
            destination=spec["destination"],
            mode=mode.value,
            dry_run=self.dry_run,
        )

        source_columns = source.get_columns(source_name.schema, source_name.table)
        target_columns = self.schema_mapper.map_columns(source_columns, source.engine, target.engine)
        target_columns = self._apply_column_options(target_columns, rename_map, spec.get("type_overrides"))
        if mode == TransferMode.SNAPSHOT:
            target_columns = self._ensure_system_column(target_columns, "_synced_at", self._timestamp_type(target))
            target_columns = [replace(col, is_primary_key=False) for col in target_columns]
        if mode == TransferMode.SOFT_DELETE:
            target_columns = self._ensure_system_column(target_columns, "deleted_at", self._timestamp_type(target))
        self._sync_schema(target_name.schema, target_name.table, target_columns, result, target)

        if self.dry_run:
            return result

        # Per-call uid prevents concurrent syncs of the same table from colliding
        # on deterministic temp table names (staging, seen-keys).
        uid = uuid.uuid4().hex[:8]

        write_schema, write_table, staging_table = self._prepare_write_target(
            mode, target, target_name, target_columns, uid
        )

        # For SOFT_DELETE, pre-create the seen-keys accumulation table so PKs can
        # be streamed directly to the database during the batch loop instead of
        # building an unbounded Python set that would OOM on large tables.
        seen_keys_table: str | None = None
        pk_cols_for_sd: list[Column] = []
        if mode == TransferMode.SOFT_DELETE:
            source_primary_key_pre = list(
                spec.get("primary_key") or [col.name for col in source_columns if col.is_primary_key]
            )
            target_primary_key_pre = [rename_map.get(col, col) for col in source_primary_key_pre]
            if target_primary_key_pre:
                pk_col_map_pre = {col.name: col for col in target_columns}
                pk_cols_for_sd = [pk_col_map_pre[pk] for pk in target_primary_key_pre if pk in pk_col_map_pre]
                if pk_cols_for_sd:
                    seen_keys_table = target.init_seen_keys_table(
                        target_name.schema, target_name.table, pk_cols_for_sd, uid
                    )

        filter_sql, params = build_where_clause(spec.get("filter"))
        watermark_cfg = wm.load_watermark(spec)
        if watermark_cfg:
            filter_sql, params = wm.apply_watermark_filter(
                filter_sql, params,
                watermark_cfg["column"], watermark_cfg["value"],
                source.quote_char, source.placeholder,
            )
        order_sql = build_order_by(spec.get("order_by"), source.quote_char)

        if spec.get("count_source_rows", True):
            total = self._safe_source_count(source, source_name.schema, source_name.table, filter_sql, params)
        else:
            total = None

        batch_size = self._resolve_batch_size(total, spec.get("batch_size"))
        column_names = [column.name for column in source_columns]
        target_column_names = [column.name for column in target_columns]
        source_primary_key = list(
            spec.get("primary_key") or [col.name for col in source_columns if col.is_primary_key]
        )
        target_primary_key = [rename_map.get(col, col) for col in source_primary_key]
        transform = spec.get("transform")
        on_batch = spec.get("on_batch")
        snapshot_ts = datetime.now(timezone.utc).isoformat() if mode == TransferMode.SNAPSHOT else None

        if use_tx:
            target.begin()
        reporter.start()
        try:
            for raw_batch in source.fetch_batches(
                source_name.schema, source_name.table,
                columns=column_names,
                where=filter_sql,
                params=params,
                order_by=order_sql,
                batch_size=batch_size,
            ):
                if abort is not None and abort.is_set():
                    raise RuntimeError(f"Sync of '{name}' aborted due to a sibling table failure")
                if watermark_cfg:
                    result.watermark_value = wm.max_watermark_value(
                        result.watermark_value, raw_batch, watermark_cfg["column"]
                    )
                batch = self._prepare_batch(
                    raw_batch, rename_map, transform, target_column_names, mode, snapshot_ts
                )
                if not batch:
                    continue

                def write_batch(
                    _batch=batch,
                    _mode=mode,
                    _ws=write_schema,
                    _wt=write_table,
                    _pk=target_primary_key,
                    _cols=target_column_names,
                ) -> int:
                    if _mode == TransferMode.UPSERT and _pk:
                        return target.upsert_batch(_ws, _wt, _batch, _cols, _pk)
                    if _mode in {TransferMode.APPEND, TransferMode.SOFT_DELETE} and _pk:
                        target.delete_matching_rows(_ws, _wt, _batch, _pk)
                    return target.insert_batch(_ws, _wt, _batch, _cols)

                written = self._retry(write_batch, on_retry=target.reconnect)

                # Stream source PKs directly into the seen-keys table to avoid
                # accumulating an unbounded Python set for SOFT_DELETE mode.
                if seen_keys_table and target_primary_key:
                    pk_rows = [
                        {pk: row.get(pk) for pk in target_primary_key}
                        for row in batch
                    ]
                    if pk_rows:
                        def _insert_keys(
                            _rows=pk_rows,
                            _sk=seen_keys_table,
                            _pk=target_primary_key,
                        ) -> int:
                            return target.insert_batch(target_name.schema, _sk, _rows, _pk)
                        self._retry(_insert_keys, on_retry=target.reconnect)

                result.batches += 1
                result.rows_read += len(raw_batch)
                result.rows_written += written
                reporter.update(result.destination, result.rows_written, total)
                if on_batch:
                    on_batch(result)

            # Staging swap and soft-delete are part of the same logical unit of
            # work as the batch writes, so they run BEFORE commit — a crash here
            # rolls back everything (where the engine supports DDL rollback) and
            # the next run re-processes cleanly under the at-least-once guarantee.
            if staging_table:
                replace_from_staging(
                    target, target_name.schema, target_name.table,
                    staging_table, target_column_names,
                    lambda op: self._retry(op, on_retry=target.reconnect),
                )

            if seen_keys_table and pk_cols_for_sd:
                deleted_at = datetime.now(timezone.utc).isoformat()
                result.rows_soft_deleted = target.apply_soft_deletes_from_keys_table(
                    target_name.schema, target_name.table,
                    seen_keys_table, pk_cols_for_sd, deleted_at,
                )

            # Validate expectations BEFORE committing and saving the watermark so
            # that a failed quality check rolls back the write (when use_tx) and
            # does not advance the cursor — the next run re-processes and re-checks.
            validate_expectations(
                target, target_name.schema, target_name.table, spec.get("expect"), result, self.batch_size
            )

            # Commit only after every mutation and the quality gate have succeeded.
            if use_tx:
                target.commit()

            # Persist the watermark only after a durable commit.  Saving it before
            # the commit could skip rows on a commit failure (at-most-once / data
            # loss); saving after means at-worst re-processing (at-least-once).
            if watermark_cfg and result.watermark_value is not None:
                wm.save_watermark(watermark_cfg, result.watermark_value)

        except Exception:
            if use_tx:
                with contextlib.suppress(Exception):
                    target.rollback()
            raise
        finally:
            if staging_table:
                with contextlib.suppress(Exception):
                    target.drop_table(target_name.schema, staging_table)
            if seen_keys_table:
                with contextlib.suppress(Exception):
                    target.drop_table(target_name.schema, seen_keys_table)

        result.duration_seconds = time.monotonic() - _t0
        _log.info(
            "Finished '%s': %d rows written in %d batches (%.2fs)",
            name, result.rows_written, result.batches, result.duration_seconds,
        )
        return result

    def _prepare_write_target(
        self,
        mode: TransferMode,
        target: BaseConnector,
        target_name: Any,
        target_columns: list[Column],
        uid: str,
    ) -> tuple[str | None, str, str | None]:
        """Set up the physical write destination for this transfer mode.

        Returns (write_schema, write_table, staging_table).  staging_table is
        non-None only for APPEND_STAGING, in which case write_table is the
        staging table name.  The caller must drop staging_table in a finally block.
        """
        staging_table: str | None = None
        write_schema = target_name.schema
        write_table = target_name.table
        if mode == TransferMode.APPEND_STAGING:
            staging_table = create_staging_table(
                target, target_name.schema, target_name.table, target_columns, uid=uid
            )
            write_table = staging_table
        elif mode == TransferMode.FULL_REFRESH:
            target.truncate_table(target_name.schema, target_name.table)
        return write_schema, write_table, staging_table

    def _retry(self, operation: Any, on_retry: Callable[[], None] | None = None) -> Any:
        """Convenience wrapper that binds instance retry settings to with_retries().

        on_retry is called between attempts; pass connector.reconnect to recover
        from dropped connections rather than retrying against a stale session.
        """
        return with_retries(operation, self.retry_count, self.retry_delay_seconds, on_retry=on_retry)

    def _sync_schema(
        self,
        schema: str | None,
        table: str,
        columns: list[Column],
        result: TableSyncResult,
        target: BaseConnector,
    ) -> None:
        """Create or evolve the target table to match the mapped source columns."""
        exists = target.table_exists(schema, table)
        if not exists:
            result.schema_created = bool(schema)
            result.table_created = True
            if not self.dry_run:
                target.create_schema(schema)
                target.create_table(schema, table, columns)
            return

        target_columns = {col.name.lower(): col for col in target.get_columns(schema, table)}
        source_columns = {col.name.lower(): col for col in columns}

        for key, col in source_columns.items():
            if key not in target_columns:
                result.columns_added.append(col.name)
                if not self.dry_run:
                    target.add_column(schema, table, col)

        if self.drop_extra_columns:
            for key, col in target_columns.items():
                if key not in source_columns:
                    result.columns_dropped.append(col.name)
                    if not self.dry_run:
                        target.drop_column(schema, table, col.name)

    def _normalize_rename_map(self, rename: dict[str, str] | None) -> dict[str, str]:
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
        overrides = dict(type_overrides or {})
        for name, data_type in overrides.items():
            validate_identifier(name)
            # The override VALUE lands verbatim in CREATE/ALTER TABLE DDL, so it
            # must be validated too — otherwise a job config could inject SQL.
            validate_type(data_type)
        result: list[Column] = []
        for column in columns:
            target_name = rename_map.get(column.name, column.name)
            data_type = overrides.get(target_name, column.data_type)
            result.append(replace(column, name=target_name, data_type=data_type))
        return result

    def _ensure_system_column(self, columns: list[Column], name: str, data_type: str) -> list[Column]:
        if any(col.name.lower() == name.lower() for col in columns):
            return columns
        return [*columns, Column(name=name, data_type=data_type, nullable=True)]

    def _timestamp_type(self, target: BaseConnector) -> str:
        if target.engine == "mssql":
            return "datetime2"
        if target.engine == "sqlite":
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
        rows = [dict(row) for row in raw_batch]
        if transform:
            transformed = transform(rows)
            if transformed is not None:
                rows = [dict(row) for row in transformed]
        prepared: list[dict[str, Any]] = []
        for row in rows:
            mapped = {rename_map.get(col, col): value for col, value in row.items()}
            if mode == TransferMode.SNAPSHOT:
                mapped["_synced_at"] = snapshot_ts
            if mode == TransferMode.SOFT_DELETE:
                mapped["deleted_at"] = None
            prepared.append({col: mapped.get(col) for col in target_columns})
        return prepared

    def _safe_source_count(
        self,
        source: BaseConnector,
        schema: str | None,
        table: str,
        where: str,
        params: Sequence[Any],
    ) -> int | None:
        try:
            return source.get_row_count(schema, table, where, params)
        except Exception:
            return None

    @staticmethod
    def _parse_batch_size(batch_size: int | str) -> tuple[int, float | None]:
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
        path = Path(query)
        if path.suffix.lower() == ".sql" and path.exists():
            return path.read_text(encoding="utf-8")
        return str(query)

    def _coerce_connector(self, value: DatabaseConfig | BaseConnector | None) -> BaseConnector | None:
        if value is None:
            return None
        if isinstance(value, BaseConnector):
            return value
        if isinstance(value, DatabaseConfig):
            return create_connector(value)
        raise TypeError("Expected DatabaseConfig, BaseConnector, or None")

    def _normalize_verbose(self, verbose: str | None) -> str | None:
        if verbose is None:
            return None
        value = str(verbose).strip().lower()
        if value in {"", "none"}:
            return None
        if value in {"standard", "detailed"}:
            return value
        raise ValueError(
            f"verbose must be one of: None, 'standard', 'detailed'; got {verbose!r}"
        )
