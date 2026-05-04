import unittest

from syncdb import Column, SyncDB

from .helpers import MemoryConnector, make_sync


class SyncFeaturesTests(unittest.TestCase):
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
        sync = make_sync(source, target)

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

    def test_expectations_fail_loudly(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "items"): [{"id": 1}, {"id": 1}]},
            columns_by_table={("dbo", "items"): [Column("id", "int")]},
        )
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target)

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

    def test_dry_run_reports_schema_changes_without_writing(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "products"): [{"id": 1}]},
            columns_by_table={("dbo", "products"): [Column("id", "int", is_primary_key=True)]},
        )
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target, dry_run=True)

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
        sync = make_sync(source, target, drop_extra_columns=True)

        result = sync.sync_tables({"items": {"source": "dbo.items", "destination": "public.items"}})[0]

        self.assertEqual(result.columns_dropped, ["legacy_col"])
        col_names = [c.name for c in target.columns_by_table[("public", "items")]]
        self.assertNotIn("legacy_col", col_names)

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
        sync = make_sync(source, target)

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


if __name__ == "__main__":
    unittest.main()
