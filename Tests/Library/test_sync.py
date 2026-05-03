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

    def test_insert_only_keeps_existing_rows_even_with_primary_key(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={
                ("dbo", "events"): [
                    {"id": 1, "message": "duplicate fact"},
                    {"id": 2, "message": "new fact"},
                ]
            },
            columns_by_table={
                ("dbo", "events"): [
                    Column("id", "int", nullable=False, is_primary_key=True),
                    Column("message", "nvarchar", char_length=100),
                ]
            },
        )
        target = MemoryConnector(
            "postgresql",
            "public",
            rows_by_table={("public", "events"): [{"id": 1, "message": "existing fact"}]},
            columns_by_table={
                ("public", "events"): [
                    Column("id", "integer", nullable=False, is_primary_key=True),
                    Column("message", "varchar", char_length=100),
                ]
            },
        )
        sync = _make_sync(source, target)

        result = sync.sync_tables(
            {
                "events": {
                    "source": "dbo.events",
                    "destination": "public.events",
                    "mode": "insert_only",
                    "primary_key": ["id"],
                }
            }
        )[0]

        self.assertEqual(result.mode, "insert_only")
        self.assertEqual(result.rows_written, 2)
        self.assertEqual(
            target.rows_by_table[("public", "events")],
            [
                {"id": 1, "message": "existing fact"},
                {"id": 1, "message": "duplicate fact"},
                {"id": 2, "message": "new fact"},
            ],
        )

    def test_upsert_mode_replaces_matching_primary_key_rows(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "orders"): [{"id": 1, "status": "new"}]},
            columns_by_table={
                ("dbo", "orders"): [
                    Column("id", "int", nullable=False, is_primary_key=True),
                    Column("status", "nvarchar", char_length=20),
                ]
            },
        )
        target = MemoryConnector(
            "postgresql",
            "public",
            rows_by_table={("public", "orders"): [{"id": 1, "status": "old"}]},
            columns_by_table={
                ("public", "orders"): [
                    Column("id", "integer", nullable=False, is_primary_key=True),
                    Column("status", "varchar", char_length=20),
                ]
            },
        )
        sync = _make_sync(source, target)

        result = sync.sync_tables(
            {"orders": {"source": "dbo.orders", "destination": "public.orders", "mode": "upsert", "primary_key": ["id"]}}
        )[0]

        self.assertEqual(result.mode, "upsert")
        self.assertEqual(target.rows_by_table[("public", "orders")], [{"id": 1, "status": "new"}])

    def test_snapshot_adds_synced_at_and_preserves_existing_rows(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "orders"): [{"id": 1}]},
            columns_by_table={("dbo", "orders"): [Column("id", "int", is_primary_key=True)]},
        )
        target = MemoryConnector("postgresql", "public")
        sync = _make_sync(source, target)

        result = sync.sync_tables({"orders": {"source": "dbo.orders", "destination": "public.orders", "mode": "snapshot"}})[0]

        self.assertEqual(result.mode, "snapshot")
        self.assertIn("_synced_at", target.rows_by_table[("public", "orders")][0])
        self.assertIn("_synced_at", [column.name for column in target.columns_by_table[("public", "orders")]])

    def test_transform_rename_and_type_overrides_are_applied(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "customers"): [{"cust_id": 1, "email": "a@example.com"}]},
            columns_by_table={
                ("dbo", "customers"): [
                    Column("cust_id", "int", is_primary_key=True),
                    Column("email", "nvarchar", char_length=200),
                ]
            },
        )
        target = MemoryConnector("postgresql", "public")
        sync = _make_sync(source, target)

        sync.sync_tables(
            {
                "customers": {
                    "source": "dbo.customers",
                    "destination": "public.customers",
                    "rename": {"cust_id": "customer_id"},
                    "type_overrides": {"email": "text"},
                    "transform": lambda rows: [{**row, "email": "***@***.***"} for row in rows],
                }
            }
        )

        self.assertEqual(target.rows_by_table[("public", "customers")], [{"customer_id": 1, "email": "***@***.***"}])
        columns = {column.name: column.data_type for column in target.columns_by_table[("public", "customers")]}
        self.assertEqual(columns["email"], "text")

    def test_soft_delete_marks_missing_target_rows(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "orders"): [{"id": 1}]},
            columns_by_table={("dbo", "orders"): [Column("id", "int", is_primary_key=True)]},
        )
        target = MemoryConnector(
            "postgresql",
            "public",
            rows_by_table={("public", "orders"): [{"id": 1, "deleted_at": "old"}, {"id": 2, "deleted_at": None}]},
            columns_by_table={
                ("public", "orders"): [
                    Column("id", "integer", is_primary_key=True),
                    Column("deleted_at", "timestamp"),
                ]
            },
        )
        sync = _make_sync(source, target)

        result = sync.sync_tables(
            {"orders": {"source": "dbo.orders", "destination": "public.orders", "mode": "soft_delete", "primary_key": ["id"]}}
        )[0]

        self.assertEqual(result.rows_soft_deleted, 1)
        row_by_id = {row["id"]: row for row in target.rows_by_table[("public", "orders")]}
        self.assertIsNone(row_by_id[1]["deleted_at"])
        self.assertIsNotNone(row_by_id[2]["deleted_at"])

    def test_append_staging_replaces_live_table_after_loading_stage(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "items"): [{"id": 2}]},
            columns_by_table={("dbo", "items"): [Column("id", "int", is_primary_key=True)]},
        )
        target = MemoryConnector(
            "postgresql",
            "public",
            rows_by_table={("public", "items"): [{"id": 1}]},
            columns_by_table={("public", "items"): [Column("id", "integer", is_primary_key=True)]},
        )
        sync = _make_sync(source, target)

        sync.sync_tables({"items": {"source": "dbo.items", "destination": "public.items", "mode": "append_staging"}})

        self.assertEqual(target.rows_by_table[("public", "items")], [{"id": 2}])
        self.assertNotIn(("public", "__syncdb_items_staging"), target.rows_by_table)

    def test_expectations_fail_loudly(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "items"): [{"id": 1}, {"id": 1}]},
            columns_by_table={("dbo", "items"): [Column("id", "int")]},
        )
        target = MemoryConnector("postgresql", "public")
        sync = _make_sync(source, target)

        with self.assertRaises(ValueError):
            sync.sync_tables(
                {
                    "items": {
                        "source": "dbo.items",
                        "destination": "public.items",
                        "expect": {"unique": ["id"], "min_rows": 3},
                    }
                }
            )

    def test_sync_schema_builds_table_specs_from_source_schema(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "customers"): [{"id": 1}], ("dbo", "tmp_skip"): [{"id": 2}]},
            columns_by_table={
                ("dbo", "customers"): [Column("id", "int", is_primary_key=True)],
                ("dbo", "tmp_skip"): [Column("id", "int", is_primary_key=True)],
            },
        )
        target = MemoryConnector("postgresql", "public")
        sync = _make_sync(source, target)

        results = sync.sync_schema("dbo", "public", exclude=["tmp_*"])

        self.assertEqual([result.name for result in results], ["customers"])
        self.assertIn(("public", "customers"), target.rows_by_table)

    def test_from_job_config_builds_sync_instance(self):
        sync = SyncDB.from_job_config(
            {
                "source": {"engine": "mssql", "connection_string": "Driver=..."},
                "target": {"engine": "postgresql", "connection_string": "postgresql://example"},
                "settings": {"batch_size": 123, "verbose": "standard", "retry_count": 2},
                "tables": {"orders": {"source": "dbo.orders", "destination": "public.orders"}},
            }
        )

        self.assertEqual(sync.batch_size, 123)
        self.assertEqual(sync.verbose, "standard")
        self.assertEqual(sync.retry_count, 2)
        self.assertEqual(sync.source.config.engine, "mssql")
        self.assertEqual(sync.target.config.engine, "postgresql")

    def test_verbose_standard_prints_summary_after_sync(self):
        import io

        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "customers"): [{"id": 1}, {"id": 2}]},
            columns_by_table={("dbo", "customers"): [Column("id", "int", is_primary_key=True)]},
        )
        target = MemoryConnector("postgresql", "public")
        stream = io.StringIO()
        sync = _make_sync(source, target, verbose="standard", verbose_stream=stream)

        sync.sync_tables({"customers": {"source": "dbo.customers", "destination": "public.customers"}})

        output = stream.getvalue()
        self.assertIn("SyncDB summary (standard)", output)
        self.assertIn("public.customers", output)
        self.assertIn("total: 2 rows in 1 batches across 1 tables", output)

    def test_invalid_verbose_mode_is_rejected(self):
        source = MemoryConnector("mssql", "dbo")
        target = MemoryConnector("postgresql", "public")

        with self.assertRaises(ValueError):
            _make_sync(source, target, verbose="chatty")

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
