import unittest
import warnings

from syncdb import Column

from .helpers import MemoryConnector, make_sync


class SyncModesTests(unittest.TestCase):
    def test_soft_delete_without_primary_key_warns_and_skips_deletion(self):
        # Without a PK the seen-keys pass cannot run; that must be loud, not silent.
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "logs"): [{"msg": "a"}]},
            columns_by_table={("dbo", "logs"): [Column("msg", "nvarchar", char_length=50)]},
        )
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = sync.sync_tables(
                {"logs": {"source": "dbo.logs", "destination": "public.logs", "mode": "soft_delete"}}
            )[0]

        self.assertEqual(result.rows_soft_deleted, 0)
        messages = [str(w.message) for w in caught if issubclass(w.category, RuntimeWarning)]
        self.assertTrue(any("SOFT_DELETE needs a primary key" in m for m in messages))

    def test_append_creates_schema_table_and_batches_rows(self):
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
        sync = make_sync(source, target, batch_size=2)

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
        sync = make_sync(source, target)

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
        sync = make_sync(source, target)

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

    def test_upsert_replaces_matching_primary_key_rows(self):
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
        sync = make_sync(source, target)

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
        sync = make_sync(source, target)

        result = sync.sync_tables({"orders": {"source": "dbo.orders", "destination": "public.orders", "mode": "snapshot"}})[0]

        self.assertEqual(result.mode, "snapshot")
        self.assertIn("_synced_at", target.rows_by_table[("public", "orders")][0])
        self.assertIn("_synced_at", [column.name for column in target.columns_by_table[("public", "orders")]])

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
        sync = make_sync(source, target)

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
        sync = make_sync(source, target)

        sync.sync_tables({"items": {"source": "dbo.items", "destination": "public.items", "mode": "append_staging"}})

        self.assertEqual(target.rows_by_table[("public", "items")], [{"id": 2}])
        self.assertNotIn(("public", "__syncdb_items_staging"), target.rows_by_table)


if __name__ == "__main__":
    unittest.main()
