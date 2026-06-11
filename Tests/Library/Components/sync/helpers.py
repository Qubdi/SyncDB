from syncdb import Column, DatabaseConfig, ProgressMode, SyncDB
from syncdb.connectors.base import BaseConnector

from Tests.Library.test_env import live_output_enabled, live_progress_mode, live_verbose


class MemoryConnector(BaseConnector):
    def __init__(self, engine, default_schema, rows_by_table=None, columns_by_table=None):
        super().__init__(DatabaseConfig(engine=engine, connection_string="memory", default_schema=default_schema))
        self.engine = self.config.engine
        self.quote_char = {"mssql": "[", "mysql": "`"}.get(self.engine, '"')
        self.placeholder = "?" if self.engine in {"mssql", "sqlite"} else "%s"
        self.timestamp_type = {"mssql": "datetime2", "sqlite": "text"}.get(self.engine, "timestamp")
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

    def execute_query_batches(self, query, params=None, batch_size=5000):
        # The base implementation streams from a real DB-API cursor; the memory
        # connector has no connection, so chunk the canned execute_query result.
        rows = self.execute_query(query, params)
        for start in range(0, len(rows), batch_size):
            chunk = rows[start : start + batch_size]
            if chunk:
                yield chunk

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

    def apply_soft_deletes_sql(self, schema, table, pk_columns, seen_keys, deleted_at_value, batch_size=5000):
        primary_key = [col.name for col in pk_columns]
        count = 0
        for row in self.rows_by_table.get((schema, table), []):
            key = tuple(row.get(pk) for pk in primary_key)
            if key not in seen_keys and row.get("deleted_at") is None:
                row["deleted_at"] = deleted_at_value
                count += 1
        return count

    def init_seen_keys_table(self, schema, table, pk_columns, uid):
        keys_table = f"__syncdb_{table[:40]}_{uid}_keys"
        self.rows_by_table[(schema, keys_table)] = []
        self.columns_by_table[(schema, keys_table)] = list(pk_columns)
        return keys_table

    def apply_soft_deletes_from_keys_table(self, schema, table, keys_table, pk_columns, deleted_at_value):
        primary_key = [col.name for col in pk_columns]
        seen_key_rows = self.rows_by_table.get((schema, keys_table), [])
        seen_keys = {tuple(row.get(pk) for pk in primary_key) for row in seen_key_rows}
        count = 0
        for row in self.rows_by_table.get((schema, table), []):
            key = tuple(row.get(pk) for pk in primary_key)
            if key not in seen_keys and row.get("deleted_at") is None:
                row["deleted_at"] = deleted_at_value
                count += 1
        return count

    def reconnect(self):
        self.closed = False
        self.connected = True

    def drop_table(self, schema, table):
        self.dropped_tables.append((schema, table))
        self.rows_by_table.pop((schema, table), None)
        self.columns_by_table.pop((schema, table), None)


def make_sync(source, target, **kwargs) -> SyncDB:
    if live_output_enabled():
        kwargs.setdefault("verbose", live_verbose() or "standard")
        progress_mode = kwargs.pop("progress_mode", live_progress_mode())
    else:
        kwargs.setdefault("verbose", None)
        progress_mode = kwargs.pop("progress_mode", ProgressMode.NONE)
    return SyncDB(source=source, target=target, progress_mode=progress_mode, **kwargs)
