import os

from syncdb import Column, DatabaseConfig, ProgressMode, SyncDB
from syncdb.connectors.base import BaseConnector


class MemoryConnector(BaseConnector):
    def __init__(self, engine, default_schema, rows_by_table=None, columns_by_table=None):
        super().__init__(DatabaseConfig(engine=engine, connection_string="memory", default_schema=default_schema))
        self.engine = self.config.engine
        self.quote_char = {"mssql": "[", "mysql": "`"}.get(self.engine, '"')
        self.placeholder = "?" if self.engine in {"mssql", "sqlite"} else "%s"
        self.rows_by_table = rows_by_table or {}
        self.columns_by_table = columns_by_table or {}
        self.created_schemas = []
        self.truncated = []
        self.dropped_tables = []
        self.fetch_calls = []
        self.insert_batches = []
        self.deleted_batches = []
        self.updated_batches = []
        self.copied_tables = []
        self.connected = False
        self.closed = False
        self.fail_insert_times = 0

    def connect(self):
        self.connected = True

    def close(self):
        self.closed = True
        super().close()

    def execute_query(self, query, params=None):
        # Minimal query support for export tests.  This is intentionally small:
        # the production connectors own SQL execution; the memory connector only
        # needs to return rows for simple "SELECT ... FROM table" fixtures.
        lowered = query.lower()
        for (_schema, table), rows in self.rows_by_table.items():
            if f" from {table.lower()}" in lowered:
                return [dict(row) for row in rows]
        return []

    def list_tables(self, schema=None):
        return sorted(table for table_schema, table in self.columns_by_table if table_schema == schema)

    def fetch_batches(self, schema, table, columns=None, where="", params=None, order_by="", batch_size=5000):
        self.fetch_calls.append(
            {
                "schema": schema,
                "table": table,
                "columns": list(columns) if columns else None,
                "where": where,
                "params": list(params or []),
                "order_by": order_by,
                "batch_size": batch_size,
            }
        )
        rows = self.rows_by_table.get((schema, table), [])
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            if columns:
                yield [{column: row[column] for column in columns} for row in batch]
            else:
                yield batch

    def insert_batch(self, schema, table, rows, columns):
        if self.fail_insert_times > 0:
            self.fail_insert_times -= 1
            raise RuntimeError("planned insert failure")
        records = [dict(row) for row in rows]
        self.insert_batches.append(
            {
                "schema": schema,
                "table": table,
                "rows": records,
                "columns": list(columns),
            }
        )
        self.rows_by_table.setdefault((schema, table), []).extend(records)
        return len(records)

    def get_columns(self, schema, table):
        return list(self.columns_by_table[(schema, table)])

    def get_primary_keys(self, schema, table):
        return [column.name for column in self.get_columns(schema, table) if column.is_primary_key]

    def table_exists(self, schema, table):
        return (schema, table) in self.columns_by_table

    def create_schema(self, schema):
        if schema:
            self.created_schemas.append(schema)

    def create_table(self, schema, table, columns):
        self.columns_by_table[(schema, table)] = list(columns)
        self.rows_by_table.setdefault((schema, table), [])

    def add_column(self, schema, table, column):
        self.columns_by_table[(schema, table)].append(column)

    def drop_column(self, schema, table, column_name):
        self.columns_by_table[(schema, table)] = [
            column for column in self.columns_by_table[(schema, table)] if column.name != column_name
        ]

    def truncate_table(self, schema, table):
        self.truncated.append((schema, table))
        self.rows_by_table[(schema, table)] = []

    def get_row_count(self, schema, table, where="", params=None):
        return len(self.rows_by_table.get((schema, table), []))

    def delete_matching_rows(self, schema, table, rows, primary_key):
        self.deleted_batches.append(
            {
                "schema": schema,
                "table": table,
                "rows": [dict(row) for row in rows],
                "primary_key": list(primary_key),
            }
        )
        keys = {tuple(row[column] for column in primary_key) for row in rows}
        current = self.rows_by_table.get((schema, table), [])
        self.rows_by_table[(schema, table)] = [
            row for row in current if tuple(row[column] for column in primary_key) not in keys
        ]
        return len(keys)

    def update_matching_rows(self, schema, table, rows, primary_key, values):
        self.updated_batches.append(
            {
                "schema": schema,
                "table": table,
                "rows": [dict(row) for row in rows],
                "primary_key": list(primary_key),
                "values": dict(values),
            }
        )
        keys = {tuple(row[column] for column in primary_key) for row in rows}
        updated = 0
        for row in self.rows_by_table.get((schema, table), []):
            if tuple(row[column] for column in primary_key) in keys:
                row.update(values)
                updated += 1
        return updated

    def copy_table_rows(self, source_schema, source_table, target_schema, target_table, columns):
        self.copied_tables.append(
            {
                "source_schema": source_schema,
                "source_table": source_table,
                "target_schema": target_schema,
                "target_table": target_table,
                "columns": list(columns),
            }
        )
        rows = [{column: row.get(column) for column in columns} for row in self.rows_by_table.get((source_schema, source_table), [])]
        self.rows_by_table.setdefault((target_schema, target_table), []).extend(rows)
        return len(rows)

    def drop_table(self, schema, table):
        self.dropped_tables.append((schema, table))
        self.rows_by_table.pop((schema, table), None)
        self.columns_by_table.pop((schema, table), None)


def _live_output_enabled() -> bool:
    return os.getenv("SYNCDB_TEST_LIVE_OUTPUT_DETAIL", "").strip().lower() in {"1", "true", "yes", "on"}


def _live_progress_mode() -> ProgressMode:
    value = os.getenv("SYNCDB_TEST_PROGRESS_MODE", ProgressMode.MULTI_LINE.value)
    return ProgressMode(value)


def make_sync(source, target, **kwargs) -> SyncDB:
    if _live_output_enabled():
        kwargs.setdefault("verbose", os.getenv("SYNCDB_TEST_VERBOSE", "standard"))
        progress_mode = kwargs.pop("progress_mode", _live_progress_mode())
    else:
        kwargs.setdefault("verbose", None)
        progress_mode = kwargs.pop("progress_mode", ProgressMode.NONE)
    return SyncDB(source_connector=source, target_connector=target, progress_mode=progress_mode, **kwargs)
