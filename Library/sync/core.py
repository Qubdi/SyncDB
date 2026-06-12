"""High-level SyncDB orchestration API.

This module coordinates connectors, schema mapping, batching, retries, progress,
and file IO. Keep connector-specific SQL out of this layer; SyncDB should express
workflow policy while connector classes own engine syntax and driver behavior.
"""

from __future__ import annotations

import contextlib
import dataclasses
import fnmatch
import itertools
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
from functools import partial
from pathlib import Path
from typing import Any, TextIO

from ..config import DatabaseConfig
from ..connections import create_connector
from ..connectors.base import BaseConnector
from ..files import FileTransfer
from ..progress import ProgressMode, ProgressReporter
from ..sql import (
    QualifiedName,
    build_order_by,
    build_where_clause,
    parse_qualified_name,
    validate_identifier,
    validate_type,
)
from ..type_mapping import Column, SchemaMapper
from . import watermark as wm
from .inference import infer_columns
from .models import ParallelSyncError, TableSyncResult, TransferMode
from .quality import validate_expectations
from .reporting import emit_summary
from .retry import with_retries
from .staging import create_staging_table, replace_from_staging

logger = logging.getLogger(__name__)

# Allowlist of valid DatabaseConfig constructor fields.  Used by from_job_config
# to reject unexpected keys from user-supplied config files before they reach the
# DatabaseConfig constructor, preventing unknown kwargs from propagating to
# database drivers.
_CONFIG_FIELDS: frozenset[str] = frozenset(f.name for f in dataclasses.fields(DatabaseConfig))

# Settings keys accepted by from_job_config.  Unknown keys are warned about so
# typos in job files surface immediately rather than silently doing nothing.
_ALLOWED_SETTINGS: frozenset[str] = frozenset(
    {
        "batch_size",
        "progress_mode",
        "dry_run",
        "drop_extra_columns",
        "verbose",
        "retry_count",
        "retry_delay_seconds",
        "use_transaction",
        "max_workers",
    }
)

# Every key a table spec may carry.  Unknown keys are warned about in
# _plan_table_sync because a typo here fails silently otherwise — e.g.
# "incremental_colum" would quietly disable the watermark and re-sync the
# full table instead of raising or filtering.
_ALLOWED_SPEC_KEYS: frozenset[str] = frozenset(
    {
        "source",
        "destination",
        "mode",
        "filter",
        "order_by",
        "primary_key",
        "batch_size",
        "rename",
        "type_overrides",
        "transform",
        "on_batch",
        "use_transaction",
        "expect",
        "count_source_rows",
        "incremental_column",
        "watermark_comparison",
        "watermark_storage",
        "watermark_store",
        "watermark_key",
    }
)


@dataclasses.dataclass
class _TableSyncPlan:
    """Everything _execute_table_sync needs, resolved up front.

    Produced by _plan_table_sync, which performs reads only (source metadata,
    row count, watermark file).  Splitting planning from execution keeps the
    highest-risk function in the codebase auditable: every data mutation and
    the transaction boundary live in _execute_table_sync alone.
    """

    name: str
    mode: TransferMode
    use_tx: bool
    uid: str
    source_name: QualifiedName
    target_name: QualifiedName
    rename_map: dict[str, str]
    target_columns: list[Column]
    column_names: list[str]
    target_column_names: list[str]
    target_primary_key: list[str]
    pk_cols_for_sd: list[Column]
    filter_sql: str
    params: list[Any]
    order_sql: str
    batch_size: int
    total: int | None
    transform: Any
    on_batch: Any
    snapshot_ts: str | None
    watermark_cfg: dict[str, Any] | None


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
                "source_connector is deprecated; pass a BaseConnector as the positional source= argument instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        if target_connector is not None:
            warnings.warn(
                "target_connector is deprecated; pass a BaseConnector as the positional target= argument instead.",
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
        return f"SyncDB(source={src!r}, target={tgt!r}, batch_size={self.batch_size!r}, dry_run={self.dry_run!r})"

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

    def _sync_tables_sequential(self, specs: dict[str, dict[str, Any]], sync_id: str) -> list[TableSyncResult]:
        # sync_tables() guarantees both are set before dispatching here.
        assert self.source is not None and self.target is not None
        results: list[TableSyncResult] = []
        _log = logging.LoggerAdapter(logger, {"sync_id": sync_id})
        _log.info("Starting sequential sync of %d table(s)", len(specs))
        self.source.connect()
        self.target.connect()
        self.progress.label_width = max((len(spec.get("destination", "")) for spec in specs.values()), default=0)
        try:
            for name, spec in specs.items():
                results.append(self._sync_one_table(name, spec, self.source, self.target, sync_id=sync_id))
        finally:
            self.progress.finish()
            self.source.close()
            self.target.close()
        emit_summary(results, self.verbose, self.verbose_stream)
        return results

    def _sync_tables_parallel(self, specs: dict[str, dict[str, Any]], sync_id: str) -> list[TableSyncResult]:
        assert self.source is not None and self.target is not None
        if not hasattr(self.source, "config") or not hasattr(self.target, "config"):
            raise ValueError(
                "max_workers > 1 requires DatabaseConfig-backed connectors. "
                "Pass source/target as DatabaseConfig, not raw BaseConnector instances."
            )
        _log = logging.LoggerAdapter(logger, {"sync_id": sync_id})
        _log.info(
            "Starting parallel sync of %d table(s) with %d workers",
            len(specs),
            self.max_workers,
        )
        results: list[TableSyncResult | None] = [None] * len(specs)
        # abort is set by the first failing future so later queued workers exit early.
        abort = threading.Event()
        errors: list[BaseException] = []

        factory = self._connector_factory or create_connector
        # Bind configs outside the worker closure so each thread builds its own
        # connector pair from the shared (frozen, thread-safe) DatabaseConfig.
        source_config = self.source.config
        target_config = self.target.config

        def sync_in_thread(index: int, name: str, spec: dict[str, Any]) -> tuple[int, TableSyncResult]:
            if abort.is_set():
                raise RuntimeError(f"Sync of '{name}' cancelled due to earlier failure")
            src = factory(source_config)
            tgt = factory(target_config)
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
                executor.submit(sync_in_thread, i, name, spec): name for i, (name, spec) in enumerate(specs.items())
            }
            for future in as_completed(futures):
                try:
                    idx, result = future.result()
                    results[idx] = result
                except Exception as exc:
                    errors.append(exc)
                    _log.error(
                        "Table '%s' failed: %s",
                        futures[future],
                        exc,
                        exc_info=True,
                    )
                    abort.set()
                    for f in futures:
                        f.cancel()

        final = [r for r in results if r is not None]
        if errors:
            # Attach the successful tables' results so callers can audit partial
            # completion — those tables' writes are durable and must not vanish
            # from the audit trail just because a sibling failed.
            raise ParallelSyncError(
                f"{len(errors)} table(s) failed during parallel sync "
                f"({len(final)} completed): " + "; ".join(f"{type(e).__name__}: {e}" for e in errors),
                results=final,
                errors=errors,
            ) from errors[0]

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
                "Unknown setting(s) in job config will be ignored: %s. Valid settings: %s",
                sorted(unknown_settings),
                sorted(_ALLOWED_SETTINGS),
            )

        raw_source = config.get("source")
        raw_target = config.get("target")
        source = cls._parse_db_config(raw_source, "source") if raw_source else None
        target = cls._parse_db_config(raw_target, "target") if raw_target else None
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
    def run_config_file(cls, path: str | Path, dry_run: bool | None = None) -> list[TableSyncResult]:
        """Load a YAML/JSON config file and run its table sync job.

        dry_run, when not None, overrides the settings.dry_run value from the
        file — this backs the CLI --dry-run flag so a job can be previewed
        without writing any data or DDL.
        """
        config_path = Path(path)
        config = cls._load_job_config(config_path)
        sync = cls.from_job_config(config)
        if dry_run is not None:
            sync.dry_run = dry_run
        return sync.sync_tables(config.get("tables") or {})

    @staticmethod
    def _load_job_config(path: Path) -> dict[str, Any]:
        """Parse a JSON or YAML job file."""
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".json":
            data = json.loads(text)
            return data if isinstance(data, dict) else {}
        if path.suffix.lower() in {".yaml", ".yml"}:
            try:
                import yaml
            except ImportError as exc:
                raise ImportError("PyYAML is required to read YAML job configs; use JSON or install pyyaml") from exc
            loaded = yaml.safe_load(text)
            return loaded if isinstance(loaded, dict) else {}
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
        hmac_key: bytes | str | None = None,
        hmac_alg: str = "sha256",
    ) -> int:
        """Read a local file and insert it into a target table.

        The target table is created automatically if it doesn't exist; column
        types are inferred from the first batch of rows via infer_columns().
        fresh_insert=True truncates an existing table before inserting.

        Rows are streamed in batches of self.batch_size (with the configured
        retry policy): CSV and Parquet files are never fully materialised in
        memory; Excel and Pickle have no incremental reader and are loaded
        once, then inserted in chunks.

        hmac_key enables HMAC integrity verification for pickle files — see
        FileTransfer.read().  Always pass it when importing pickle files you did
        not produce in the same trusted pipeline.

        Returns the number of rows inserted.
        """
        if self.target is None:
            raise ValueError("target connector/config is required for import")
        batches = self.file_transfer.read_streaming(
            input_path,
            file_format,
            batch_size=self.batch_size,
            hmac_key=hmac_key,
            hmac_alg=hmac_alg,
        )
        first_batch = next(iter(batches), None)
        target_name = parse_qualified_name(destination, self.target.config.default_schema)
        self.target.connect()
        try:
            if not self.target.table_exists(target_name.schema, target_name.table):
                if first_batch is None:
                    # Match infer_columns' empty-input contract without creating
                    # an empty table of unknowable shape.
                    raise ValueError("Cannot infer a target table from an empty file")
                self.target.create_schema(target_name.schema)
                self.target.create_table(
                    target_name.schema,
                    target_name.table,
                    infer_columns(first_batch, self.target.engine, self.schema_mapper),
                )
            elif fresh_insert:
                self.target.truncate_table(target_name.schema, target_name.table)
            if first_batch is None:
                return 0
            columns = list(first_batch[0].keys())
            total = 0
            for chunk in itertools.chain([first_batch], batches):
                total += self._retry(
                    partial(
                        self.target.insert_batch,
                        target_name.schema,
                        target_name.table,
                        chunk,
                        columns,
                    ),
                    on_retry=self.target.reconnect,
                )
            return total
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

        Structured as plan → execute → finalize: _plan_table_sync resolves
        everything up front with reads only, _execute_table_sync owns all data
        mutations and the transaction boundary, and this method handles spec
        validation, schema alignment, dry-run short-circuit, and timing.

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
            name,
            spec["source"],
            spec["destination"],
            spec.get("mode", "append"),
        )
        reporter = progress or self.progress

        plan = self._plan_table_sync(name, spec, source, target)
        result = TableSyncResult(
            name=name,
            source=spec["source"],
            destination=spec["destination"],
            mode=plan.mode.value,
            dry_run=self.dry_run,
        )
        self._sync_schema(plan.target_name.schema, plan.target_name.table, plan.target_columns, result, target)

        if self.dry_run:
            return result

        self._execute_table_sync(plan, result, spec, source, target, reporter, abort)

        result.duration_seconds = time.monotonic() - _t0
        _log.info(
            "Finished '%s': %d rows written in %d batches (%.2fs)",
            name,
            result.rows_written,
            result.batches,
            result.duration_seconds,
        )
        return result

    def _plan_table_sync(
        self,
        name: str,
        spec: dict[str, Any],
        source: BaseConnector,
        target: BaseConnector,
    ) -> _TableSyncPlan:
        """Resolve names, columns, filters, keys, and batch sizing for one table.

        Reads only: source metadata, an optional row count, and the watermark
        file.  No tables are created or written here, so a dry run can stop
        after this plus schema alignment.
        """
        unknown_keys = set(spec) - _ALLOWED_SPEC_KEYS
        if unknown_keys:
            warnings.warn(
                f"Table spec '{name}' contains unknown key(s) that will be ignored: "
                f"{sorted(unknown_keys)}. Valid keys: {sorted(_ALLOWED_SPEC_KEYS)}",
                RuntimeWarning,
                stacklevel=2,
            )
        mode = TransferMode(spec.get("mode", TransferMode.APPEND.value))
        use_tx = self.use_transaction or bool(spec.get("use_transaction", False))
        source_name = parse_qualified_name(spec["source"], source.config.default_schema)
        target_name = parse_qualified_name(spec["destination"], target.config.default_schema)
        rename_map = self._normalize_rename_map(spec.get("rename"))

        source_columns = source.get_columns(source_name.schema, source_name.table)
        target_columns = self.schema_mapper.map_columns(source_columns, source.engine, target.engine)
        target_columns = self._apply_column_options(target_columns, rename_map, spec.get("type_overrides"))
        if mode == TransferMode.SNAPSHOT:
            target_columns = self._ensure_system_column(target_columns, "_synced_at", target.timestamp_type)
            target_columns = [replace(col, is_primary_key=False) for col in target_columns]
        if mode == TransferMode.SOFT_DELETE:
            target_columns = self._ensure_system_column(target_columns, "deleted_at", target.timestamp_type)
            if spec.get("filter"):
                warnings.warn(
                    f"Table '{name}': SOFT_DELETE combined with a filter marks EVERY "
                    "target row outside the filtered source set as deleted — rows the "
                    "filter excludes are absent from the seen-keys table and will be "
                    "soft-deleted in bulk. Either drop the filter, or sync filtered "
                    "subsets with mode='upsert' and handle deletions separately.",
                    RuntimeWarning,
                    stacklevel=2,
                )

        source_primary_key = list(spec.get("primary_key") or [col.name for col in source_columns if col.is_primary_key])
        target_primary_key = [rename_map.get(col, col) for col in source_primary_key]
        pk_cols_for_sd: list[Column] = []
        if mode == TransferMode.SOFT_DELETE and target_primary_key:
            pk_col_map = {col.name: col for col in target_columns}
            pk_cols_for_sd = [pk_col_map[pk] for pk in target_primary_key if pk in pk_col_map]
        if mode == TransferMode.SOFT_DELETE and not pk_cols_for_sd:
            warnings.warn(
                f"Table '{name}': SOFT_DELETE needs a primary key to detect rows "
                "missing from the source; none was found or specified, so NO rows "
                "will be marked deleted this run. Set 'primary_key' in the table spec.",
                RuntimeWarning,
                stacklevel=2,
            )

        filter_sql, params = build_where_clause(spec.get("filter"))
        watermark_cfg = wm.load_watermark(spec, target)
        if watermark_cfg:
            filter_sql, params = wm.apply_watermark_filter(
                filter_sql,
                params,
                watermark_cfg["column"],
                watermark_cfg["value"],
                source.quote_char,
                source.placeholder,
                comparison=watermark_cfg["comparison"],
            )
        order_sql = build_order_by(spec.get("order_by"), source.quote_char)

        if spec.get("count_source_rows", True):
            total = self._safe_source_count(source, source_name.schema, source_name.table, filter_sql, params)
        else:
            total = None

        return _TableSyncPlan(
            name=name,
            mode=mode,
            use_tx=use_tx,
            # Per-call uid prevents concurrent syncs of the same table from
            # colliding on deterministic temp table names (staging, seen-keys).
            uid=uuid.uuid4().hex[:8],
            source_name=source_name,
            target_name=target_name,
            rename_map=rename_map,
            target_columns=target_columns,
            column_names=[column.name for column in source_columns],
            target_column_names=[column.name for column in target_columns],
            target_primary_key=target_primary_key,
            pk_cols_for_sd=pk_cols_for_sd,
            filter_sql=filter_sql,
            params=params,
            order_sql=order_sql,
            batch_size=self._resolve_batch_size(total, spec.get("batch_size")),
            total=total,
            transform=spec.get("transform"),
            on_batch=spec.get("on_batch"),
            snapshot_ts=datetime.now(timezone.utc).isoformat() if mode == TransferMode.SNAPSHOT else None,
            watermark_cfg=watermark_cfg,
        )

    def _execute_table_sync(
        self,
        plan: _TableSyncPlan,
        result: TableSyncResult,
        spec: dict[str, Any],
        source: BaseConnector,
        target: BaseConnector,
        reporter: ProgressReporter,
        abort: threading.Event | None,
    ) -> None:
        """Run every data mutation for a planned table sync.

        Ordering is the correctness story here:
          1. begin() precedes _prepare_write_target so FULL_REFRESH's TRUNCATE
             (and APPEND_STAGING's staging DDL) participate in the transaction —
             a truncate that auto-commits ahead of begin() would leave the
             target permanently empty if the sync then failed.
          2. Staging swap and soft-delete run BEFORE commit — they are the same
             logical unit of work as the batch writes.
          3. The quality gate runs BEFORE commit so a failed check rolls the
             write back (when use_tx) instead of certifying bad data.
          4. The watermark is saved only AFTER a durable commit: saving earlier
             could skip rows on a commit failure (at-most-once / data loss);
             saving after means at-worst re-processing (at-least-once).
        """
        # Pre-create the seen-keys accumulation table so SOFT_DELETE PKs can be
        # streamed to the database during the batch loop instead of building an
        # unbounded Python set that would OOM on large tables.
        seen_keys_table: str | None = None
        if plan.mode == TransferMode.SOFT_DELETE and plan.pk_cols_for_sd:
            seen_keys_table = target.init_seen_keys_table(
                plan.target_name.schema, plan.target_name.table, plan.pk_cols_for_sd, plan.uid
            )

        if plan.use_tx:
            target.begin()
        reporter.start()
        staging_table: str | None = None
        try:
            write_schema, write_table, staging_table = self._prepare_write_target(
                plan.mode, target, plan.target_name, plan.target_columns, plan.uid
            )

            self._copy_batches(
                plan,
                result,
                source,
                target,
                reporter,
                abort,
                write_schema,
                write_table,
                seen_keys_table,
            )

            if staging_table:
                replace_from_staging(
                    target,
                    plan.target_name.schema,
                    plan.target_name.table,
                    staging_table,
                    plan.target_column_names,
                    lambda op: self._retry(op, on_retry=target.reconnect),
                )

            if seen_keys_table and plan.pk_cols_for_sd:
                deleted_at = datetime.now(timezone.utc).isoformat()
                result.rows_soft_deleted = target.apply_soft_deletes_from_keys_table(
                    plan.target_name.schema,
                    plan.target_name.table,
                    seen_keys_table,
                    plan.pk_cols_for_sd,
                    deleted_at,
                )

            validate_expectations(
                target,
                plan.target_name.schema,
                plan.target_name.table,
                spec.get("expect"),
                result,
                self.batch_size,
            )

            if plan.use_tx:
                target.commit()

            if plan.watermark_cfg and result.watermark_value is not None:
                wm.save_watermark(plan.watermark_cfg, result.watermark_value, target)

        except Exception:
            if plan.use_tx:
                with contextlib.suppress(Exception):
                    target.rollback()
            raise
        finally:
            if staging_table:
                with contextlib.suppress(Exception):
                    target.drop_table(plan.target_name.schema, staging_table)
            if seen_keys_table:
                with contextlib.suppress(Exception):
                    target.drop_table(plan.target_name.schema, seen_keys_table)

    def _copy_batches(
        self,
        plan: _TableSyncPlan,
        result: TableSyncResult,
        source: BaseConnector,
        target: BaseConnector,
        reporter: ProgressReporter,
        abort: threading.Event | None,
        write_schema: str | None,
        write_table: str,
        seen_keys_table: str | None,
    ) -> None:
        """The batch loop: fetch → prepare → write, with retries and progress."""
        for raw_batch in source.fetch_batches(
            plan.source_name.schema,
            plan.source_name.table,
            columns=plan.column_names,
            where=plan.filter_sql,
            params=plan.params,
            order_by=plan.order_sql,
            batch_size=plan.batch_size,
        ):
            if abort is not None and abort.is_set():
                raise RuntimeError(f"Sync of '{plan.name}' aborted due to a sibling table failure")
            if plan.watermark_cfg:
                result.watermark_value = wm.max_watermark_value(
                    result.watermark_value, raw_batch, plan.watermark_cfg["column"]
                )
            batch = self._prepare_batch(
                raw_batch,
                plan.rename_map,
                plan.transform,
                plan.target_column_names,
                plan.mode,
                plan.snapshot_ts,
            )
            if not batch:
                continue

            # partial binds this iteration's values into a zero-arg callable for
            # _retry, without a closure over loop variables (which would be a
            # late-binding hazard and trip linters).
            written: int = self._retry(
                partial(
                    self._write_batch,
                    target,
                    plan.mode,
                    write_schema,
                    write_table,
                    batch,
                    plan.target_column_names,
                    plan.target_primary_key,
                ),
                on_retry=target.reconnect,
            )

            # Stream source PKs directly into the seen-keys table to avoid
            # accumulating an unbounded Python set for SOFT_DELETE mode.
            if seen_keys_table and plan.target_primary_key:
                pk_rows = [{pk: row.get(pk) for pk in plan.target_primary_key} for row in batch]
                if pk_rows:
                    self._retry(
                        partial(
                            target.insert_batch,
                            plan.target_name.schema,
                            seen_keys_table,
                            pk_rows,
                            plan.target_primary_key,
                        ),
                        on_retry=target.reconnect,
                    )

            result.batches += 1
            result.rows_read += len(raw_batch)
            result.rows_written += written
            reporter.update(result.destination, result.rows_written, plan.total)
            if plan.on_batch:
                plan.on_batch(result)

    @staticmethod
    def _write_batch(
        target: BaseConnector,
        mode: TransferMode,
        schema: str | None,
        table: str,
        batch: list[dict[str, Any]],
        columns: list[str],
        primary_key: list[str],
    ) -> int:
        """Write one prepared batch using the statement that fits the transfer mode.

        UPSERT uses the connector's native upsert; APPEND/SOFT_DELETE delete the
        incoming primary keys first (so updated rows replace their predecessors)
        and then insert; everything else is a plain insert.

        The delete+insert pair is wrapped in a per-batch transaction when no
        outer transaction is open — otherwise both statements auto-commit
        independently and a crash between them silently drops the deleted rows.
        """
        if mode == TransferMode.UPSERT and primary_key:
            return target.upsert_batch(schema, table, batch, columns, primary_key)
        if mode in {TransferMode.APPEND, TransferMode.SOFT_DELETE} and primary_key:
            if target.is_in_transaction:
                target.delete_matching_rows(schema, table, batch, primary_key)
                return target.insert_batch(schema, table, batch, columns)
            target.begin()
            try:
                target.delete_matching_rows(schema, table, batch, primary_key)
                written = target.insert_batch(schema, table, batch, columns)
                target.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    target.rollback()
                raise
            return written
        return target.insert_batch(schema, table, batch, columns)

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

        Must be called AFTER target.begin() when a transaction is in use, so the
        FULL_REFRESH TRUNCATE rolls back with everything else on failure.  Without
        a transaction (or on MySQL, whose TRUNCATE auto-commits), a failure after
        the truncate leaves the target empty until the next successful run —
        prefer APPEND_STAGING when that window is unacceptable.
        """
        staging_table: str | None = None
        write_schema = target_name.schema
        write_table = target_name.table
        if mode == TransferMode.APPEND_STAGING:
            staging_table = create_staging_table(target, target_name.schema, target_name.table, target_columns, uid=uid)
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

    def _prepare_batch(
        self,
        raw_batch: list[dict[str, Any]],
        rename_map: dict[str, str],
        transform: Any,
        target_columns: Sequence[str],
        mode: TransferMode,
        snapshot_ts: str | None,
    ) -> list[dict[str, Any]]:
        rows: Sequence[dict[str, Any]] = raw_batch
        if transform:
            # Transforms may mutate rows in place, so hand them defensive copies.
            copies = [dict(row) for row in raw_batch]
            transformed = transform(copies)
            rows = copies if transformed is None else [dict(row) for row in transformed]
        # Single dict build per row: each target column looks up its source key
        # directly (rename map inverted) instead of materialising an intermediate
        # renamed dict — one allocation per row instead of three.
        source_key = {target: source for source, target in rename_map.items()}
        prepared: list[dict[str, Any]] = []
        for row in rows:
            out = {col: row.get(source_key.get(col, col)) for col in target_columns}
            if mode == TransferMode.SNAPSHOT:
                out["_synced_at"] = snapshot_ts
            if mode == TransferMode.SOFT_DELETE:
                out["deleted_at"] = None
            prepared.append(out)
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
        raise ValueError(f"verbose must be one of: None, 'standard', 'detailed'; got {verbose!r}")
