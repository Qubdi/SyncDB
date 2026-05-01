import unittest

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
        return []

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


class SyncDBTests(unittest.TestCase):
    def test_sync_tables_creates_schema_table_and_batches_rows(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={
                ("dbo", "customers"): [
                    {"id": 1, "name": "Ana"},
                    {"id": 2, "name": "Gio"},
                    {"id": 3, "name": "Nino"},
                ]
            },
            columns_by_table={
                ("dbo", "customers"): [
                    Column("id", "int", nullable=False, is_primary_key=True),
                    Column("name", "nvarchar", char_length=50),
                ]
            },
        )
        target = MemoryConnector("postgresql", "public")
        sync = SyncDB(
            source_connector=source,
            target_connector=target,
            batch_size=2,
            progress_mode=ProgressMode.NONE,
        )

        results = sync.sync_tables(
            {
                "customers": {
                    "source": "dbo.customers",
                    "destination": "public.customers",
                    "mode": "append",
                }
            }
        )

        self.assertEqual(results[0].rows_written, 3)
        self.assertEqual(results[0].batches, 2)
        self.assertTrue(results[0].table_created)
        self.assertEqual(len(target.rows_by_table[("public", "customers")]), 3)
        self.assertEqual(target.columns_by_table[("public", "customers")][0].data_type, "integer")

    def test_full_refresh_truncates_existing_target(self):
        source = MemoryConnector(
            "postgresql",
            "public",
            rows_by_table={("public", "orders"): [{"id": 10}]},
            columns_by_table={("public", "orders"): [Column("id", "integer", is_primary_key=True)]},
        )
        target = MemoryConnector(
            "mysql",
            None,
            rows_by_table={(None, "orders"): [{"id": 1}]},
            columns_by_table={(None, "orders"): [Column("id", "int", is_primary_key=True)]},
        )
        sync = SyncDB(
            source_connector=source,
            target_connector=target,
            progress_mode=ProgressMode.NONE,
        )

        result = sync.sync_tables(
            {
                "orders": {
                    "source": "public.orders",
                    "destination": "orders",
                    "mode": "full_refresh",
                }
            }
        )[0]

        self.assertEqual(result.rows_written, 1)
        self.assertEqual(target.truncated, [(None, "orders")])
        self.assertEqual(target.rows_by_table[(None, "orders")], [{"id": 10}])

    def test_dry_run_reports_schema_changes_without_writing(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "products"): [{"id": 1}]},
            columns_by_table={("dbo", "products"): [Column("id", "int", is_primary_key=True)]},
        )
        target = MemoryConnector("postgresql", "public")
        sync = SyncDB(
            source_connector=source,
            target_connector=target,
            progress_mode=ProgressMode.NONE,
            dry_run=True,
        )

        result = sync.sync_tables({"products": {"source": "dbo.products", "destination": "public.products"}})[0]

        self.assertTrue(result.table_created)
        self.assertEqual(target.rows_by_table, {})
        self.assertEqual(target.columns_by_table, {})


if __name__ == "__main__":
    unittest.main()
