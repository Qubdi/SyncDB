from syncdb import Column, DatabaseConfig, ProgressMode, SyncDB
from syncdb.connectors.base import BaseConnector


class MemoryConnector(BaseConnector):
    def __init__(self, engine, default_schema, rows_by_table=None, columns_by_table=None):
        super().__init__(DatabaseConfig(engine=engine, connection_string="memory", default_schema=default_schema))
        self.engine = self.config.engine
        self.rows_by_table = rows_by_table or {}
        self.columns_by_table = columns_by_table or {}
        self.created_schemas = []
        self.truncated = []
        self.connected = False

    def connect(self):
        self.connected = True

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
        rows = self.rows_by_table.get((schema, table), [])
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            if columns:
                yield [{column: row[column] for column in columns} for row in batch]
            else:
                yield batch

    def insert_batch(self, schema, table, rows, columns):
        records = [dict(row) for row in rows]
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
        keys = {tuple(row[column] for column in primary_key) for row in rows}
        current = self.rows_by_table.get((schema, table), [])
        self.rows_by_table[(schema, table)] = [
            row for row in current if tuple(row[column] for column in primary_key) not in keys
        ]
        return len(keys)

    def update_matching_rows(self, schema, table, rows, primary_key, values):
        keys = {tuple(row[column] for column in primary_key) for row in rows}
        updated = 0
        for row in self.rows_by_table.get((schema, table), []):
            if tuple(row[column] for column in primary_key) in keys:
                row.update(values)
                updated += 1
        return updated

    def copy_table_rows(self, source_schema, source_table, target_schema, target_table, columns):
        rows = [{column: row.get(column) for column in columns} for row in self.rows_by_table.get((source_schema, source_table), [])]
        self.rows_by_table.setdefault((target_schema, target_table), []).extend(rows)
        return len(rows)

    def drop_table(self, schema, table):
        self.rows_by_table.pop((schema, table), None)
        self.columns_by_table.pop((schema, table), None)


def make_sync(source, target, **kwargs) -> SyncDB:
    kwargs.setdefault("verbose", None)
    return SyncDB(source_connector=source, target_connector=target, progress_mode=ProgressMode.NONE, **kwargs)
