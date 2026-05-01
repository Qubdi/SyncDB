"""High-level SyncDB orchestration API."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from .config import DatabaseConfig
from .connections import create_connector
from .connectors.base import BaseConnector
from .files import FileTransfer
from .progress import ProgressMode, ProgressReporter
from .sql import build_order_by, build_where_clause, parse_qualified_name
from .type_mapping import Column, SchemaMapper


class TransferMode(str, Enum):
    APPEND = "append"
    APPEND_STAGING = "append_staging"
    FULL_REFRESH = "full_refresh"


@dataclass
class TableSyncResult:
    name: str
    source: str
    destination: str
    mode: str
    rows_read: int = 0
    rows_written: int = 0
    batches: int = 0
    schema_created: bool = False
    table_created: bool = False
    columns_added: list[str] = field(default_factory=list)
    columns_dropped: list[str] = field(default_factory=list)
    dry_run: bool = False


class SyncDB:
    """Main class-based API for database and local-file synchronization."""

    def __init__(
        self,
        source: DatabaseConfig | BaseConnector | None = None,
        target: DatabaseConfig | BaseConnector | None = None,
        batch_size: int = 5000,
        progress_mode: ProgressMode | str = ProgressMode.MULTI_LINE,
        dry_run: bool = False,
        drop_extra_columns: bool = False,
        source_connector: BaseConnector | None = None,
        target_connector: BaseConnector | None = None,
        schema_mapper: SchemaMapper | None = None,
        file_transfer: FileTransfer | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        self.source = source_connector or self._coerce_connector(source)
        self.target = target_connector or self._coerce_connector(target)
        self.batch_size = batch_size
        self.progress = ProgressReporter(progress_mode)
        self.dry_run = dry_run
        self.drop_extra_columns = drop_extra_columns
        self.schema_mapper = schema_mapper or SchemaMapper()
        self.file_transfer = file_transfer or FileTransfer()

    def sync_tables(self, tables: dict[str, dict[str, Any]]) -> list[TableSyncResult]:
        """Synchronize one or more database tables from source to target."""
        if self.source is None or self.target is None:
            raise ValueError("source and target connectors/configs are required for database sync")
        results: list[TableSyncResult] = []
        self.source.connect()
        self.target.connect()
        try:
            for name, spec in tables.items():
                results.append(self._sync_one_table(name, spec))
        finally:
            self.progress.finish()
            self.source.close()
            self.target.close()
        return results

    def export_query_to_file(
        self,
        query: str,
        output_path: str | Path,
        params: Sequence[Any] | None = None,
        file_format: str | None = None,
    ) -> int:
        """Execute a source query and write its rows to a local file."""
        if self.source is None:
            raise ValueError("source connector/config is required for export")
        self.source.connect()
        try:
            rows = self.source.execute_query(query, params or [])
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
        """Read a local file and insert it into a target table."""
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
        if "source" not in spec or "destination" not in spec:
            raise ValueError(f"Table spec '{name}' must include source and destination")

        mode = TransferMode(spec.get("mode", TransferMode.APPEND.value))
        source_name = parse_qualified_name(spec["source"], self.source.config.default_schema)
        target_name = parse_qualified_name(spec["destination"], self.target.config.default_schema)
        result = TableSyncResult(
            name=name,
            source=spec["source"],
            destination=spec["destination"],
            mode=mode.value,
            dry_run=self.dry_run,
        )

        source_columns = self.source.get_columns(source_name.schema, source_name.table)
        target_columns = self.schema_mapper.map_columns(source_columns, self.source.engine, self.target.engine)
        self._sync_schema(target_name.schema, target_name.table, target_columns, result)

        if self.dry_run:
            return result

        if mode == TransferMode.FULL_REFRESH:
            self.target.truncate_table(target_name.schema, target_name.table)

        filter_sql, params = build_where_clause(spec.get("filter"))
        order_sql = build_order_by(spec.get("order_by"), self.source.quote_char)
        total = self._safe_source_count(source_name.schema, source_name.table, filter_sql, params)
        column_names = [column.name for column in source_columns]
        primary_key = list(spec.get("primary_key") or [column.name for column in source_columns if column.is_primary_key])

        # NOTE: append_staging is not yet distinguished from append at the
        # pipeline level. True staging (bulk-load to a temp table, swap once)
        # requires connector-level support and will be added later.
        for batch in self.source.fetch_batches(
            source_name.schema,
            source_name.table,
            columns=column_names,
            where=filter_sql,
            params=params,
            order_by=order_sql,
            batch_size=self.batch_size,
        ):
            if mode in {TransferMode.APPEND, TransferMode.APPEND_STAGING} and primary_key:
                self.target.delete_matching_rows(target_name.schema, target_name.table, batch, primary_key)
            written = self.target.insert_batch(target_name.schema, target_name.table, batch, column_names)
            result.batches += 1
            result.rows_read += len(batch)
            result.rows_written += written
            self.progress.update(result.destination, result.rows_written, total)

        return result

    def _sync_schema(
        self,
        schema: str | None,
        table: str,
        columns: list[Column],
        result: TableSyncResult,
    ) -> None:
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

    def _safe_source_count(
        self,
        schema: str | None,
        table: str,
        where: str,
        params: Sequence[Any],
    ) -> int | None:
        # Row count is used only for progress display; swallowing errors here
        # means a missing COUNT permission won't abort an otherwise valid sync.
        try:
            return self.source.get_row_count(schema, table, where, params)
        except Exception:
            return None

    def _infer_columns(self, rows: list[dict[str, Any]]) -> list[Column]:
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
        return self.schema_mapper.map_columns(columns, "postgresql", self.target.engine)

    def _coerce_connector(self, value: DatabaseConfig | BaseConnector | None) -> BaseConnector | None:
        if value is None:
            return None
        if isinstance(value, BaseConnector):
            return value
        if isinstance(value, DatabaseConfig):
            return create_connector(value)
        raise TypeError("Expected DatabaseConfig, BaseConnector, or None")
