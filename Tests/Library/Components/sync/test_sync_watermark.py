import json
import tempfile
import unittest
from pathlib import Path

from syncdb import Column

from .helpers import MemoryConnector, make_sync


class SyncWatermarkTests(unittest.TestCase):
    def test_incremental_column_saves_high_watermark_after_successful_sync(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={
                ("dbo", "orders"): [
                    {"id": 1, "updated_at": "2026-01-01T10:00:00"},
                    {"id": 2, "updated_at": "2026-01-01T11:00:00"},
                ]
            },
            columns_by_table={
                ("dbo", "orders"): [
                    Column("id", "int", is_primary_key=True),
                    Column("updated_at", "datetime2"),
                ]
            },
        )
        target = MemoryConnector("postgresql", "public")

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
            path = Path(handle.name)
        path.unlink(missing_ok=True)

        try:
            sync = make_sync(source, target)
            result = sync.sync_tables(
                {
                    "orders": {
                        "source": "dbo.orders",
                        "destination": "public.orders",
                        "incremental_column": "updated_at",
                        "watermark_store": str(path),
                    }
                }
            )[0]
            values = json.loads(path.read_text(encoding="utf-8"))
        finally:
            path.unlink(missing_ok=True)

        self.assertEqual(result.watermark_value, "2026-01-01T11:00:00")
        self.assertEqual(values["dbo.orders->public.orders:updated_at"], "2026-01-01T11:00:00")

    def test_existing_watermark_is_added_to_fetch_filter(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "orders"): [{"id": 2, "updated_at": "2026-01-01T11:00:00"}]},
            columns_by_table={
                ("dbo", "orders"): [
                    Column("id", "int", is_primary_key=True),
                    Column("updated_at", "datetime2"),
                ]
            },
        )
        target = MemoryConnector("postgresql", "public")

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w", encoding="utf-8", delete=False) as handle:
            json.dump({"orders_cursor": "2026-01-01T10:00:00"}, handle)
            path = Path(handle.name)

        try:
            sync = make_sync(source, target)
            sync.sync_tables(
                {
                    "orders": {
                        "source": "dbo.orders",
                        "destination": "public.orders",
                        "incremental_column": "updated_at",
                        "watermark_store": str(path),
                        "watermark_key": "orders_cursor",
                    }
                }
            )
        finally:
            path.unlink(missing_ok=True)

        call = source.fetch_calls[0]
        self.assertIn("[updated_at] > ?", call["where"])
        self.assertEqual(call["params"], ["2026-01-01T10:00:00"])

    def test_existing_filter_and_watermark_are_combined(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "orders"): [{"id": 2, "status": "active", "updated_at": "2026-01-01T11:00:00"}]},
            columns_by_table={
                ("dbo", "orders"): [
                    Column("id", "int", is_primary_key=True),
                    Column("status", "nvarchar", char_length=20),
                    Column("updated_at", "datetime2"),
                ]
            },
        )
        target = MemoryConnector("postgresql", "public")

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w", encoding="utf-8", delete=False) as handle:
            json.dump({"orders_cursor": "2026-01-01T10:00:00"}, handle)
            path = Path(handle.name)

        try:
            sync = make_sync(source, target)
            sync.sync_tables(
                {
                    "orders": {
                        "source": "dbo.orders",
                        "destination": "public.orders",
                        "filter": {"where": "status = ?", "params": ["active"]},
                        "incremental_column": "updated_at",
                        "watermark_store": str(path),
                        "watermark_key": "orders_cursor",
                    }
                }
            )
        finally:
            path.unlink(missing_ok=True)

        call = source.fetch_calls[0]
        self.assertEqual(call["where"], " WHERE (status = ?) AND ([updated_at] > ?) ")
        self.assertEqual(call["params"], ["active", "2026-01-01T10:00:00"])


if __name__ == "__main__":
    unittest.main()
