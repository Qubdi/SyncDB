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


def _make_sync(source, target, **kwargs) -> "SyncDB":
    return SyncDB(source_connector=source, target_connector=target, progress_mode=ProgressMode.NONE, **kwargs)


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
        sync = _make_sync(source, target, batch_size=2)

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
        sync = _make_sync(source, target)

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
        sync = _make_sync(source, target, dry_run=True)

        result = sync.sync_tables({"products": {"source": "dbo.products", "destination": "public.products"}})[0]

        self.assertTrue(result.table_created)
        self.assertEqual(target.rows_by_table, {})
        self.assertEqual(target.columns_by_table, {})

    def test_drop_extra_columns_removes_columns_not_in_source(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "items"): [{"id": 1}]},
            columns_by_table={("dbo", "items"): [Column("id", "int", is_primary_key=True)]},
        )
        target = MemoryConnector(
            "postgresql",
            "public",
            rows_by_table={("public", "items"): []},
            columns_by_table={
                ("public", "items"): [
                    Column("id", "integer", is_primary_key=True),
                    Column("legacy_col", "text"),
                ]
            },
        )
        sync = _make_sync(source, target, drop_extra_columns=True)

        result = sync.sync_tables({"items": {"source": "dbo.items", "destination": "public.items"}})[0]

        self.assertEqual(result.columns_dropped, ["legacy_col"])
        col_names = [c.name for c in target.columns_by_table[("public", "items")]]
        self.assertNotIn("legacy_col", col_names)

    def test_export_query_to_file_writes_rows(self):
        import tempfile
        from pathlib import Path

        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "t"): [{"id": 1}, {"id": 2}]},
            columns_by_table={("dbo", "t"): [Column("id", "int")]},
        )
        sync = SyncDB(source_connector=source, progress_mode=ProgressMode.NONE)

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            count = sync.export_query_to_file("SELECT id FROM t", path)

        self.assertEqual(count, 2)

    def test_import_file_to_table_creates_table_and_inserts(self):
        import io

        target = MemoryConnector("postgresql", "public")
        sync = SyncDB(target_connector=target, progress_mode=ProgressMode.NONE)

        csv_content = "id,name\n1,Ana\n2,Gio\n"
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
            f.write(csv_content)
            tmp_path = Path(f.name)

        try:
            count = sync.import_file_to_table(tmp_path, "public.people")
        finally:
            tmp_path.unlink(missing_ok=True)

        self.assertEqual(count, 2)
        self.assertIn(("public", "people"), target.rows_by_table)


if __name__ == "__main__":
    unittest.main()
