import io
import tempfile
import unittest
from pathlib import Path

from syncdb import Column, ProgressMode, SyncDB

from .helpers import MemoryConnector, make_sync


class SyncVerboseTests(unittest.TestCase):
    def test_verbose_standard_prints_summary_after_sync(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "customers"): [{"id": 1}, {"id": 2}]},
            columns_by_table={("dbo", "customers"): [Column("id", "int", is_primary_key=True)]},
        )
        target = MemoryConnector("postgresql", "public")
        stream = io.StringIO()
        sync = make_sync(source, target, verbose="standard", verbose_stream=stream)

        sync.sync_tables({"customers": {"source": "dbo.customers", "destination": "public.customers"}})

        output = stream.getvalue()
        self.assertIn("SyncDB summary (standard)", output)
        self.assertIn("public.customers", output)
        self.assertIn("total: 2 rows in 1 batches across 1 tables", output)

    def test_invalid_verbose_mode_is_rejected(self):
        source = MemoryConnector("mssql", "dbo")
        target = MemoryConnector("postgresql", "public")

        with self.assertRaises(ValueError):
            make_sync(source, target, verbose="chatty")

    def test_verbose_detailed_prints_schema_expectation_and_watermark_fields(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "orders"): [{"id": 1, "updated_at": "2026-01-01T10:00:00"}]},
            columns_by_table={
                ("dbo", "orders"): [
                    Column("id", "int", is_primary_key=True),
                    Column("updated_at", "datetime2"),
                ]
            },
        )
        target = MemoryConnector("postgresql", "public")
        stream = io.StringIO()
        sync = make_sync(source, target, verbose="detailed", verbose_stream=stream)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
            watermark_path = Path(handle.name)
        watermark_path.unlink(missing_ok=True)

        try:
            sync.sync_tables(
                {
                    "orders": {
                        "source": "dbo.orders",
                        "destination": "public.orders",
                        "incremental_column": "updated_at",
                        "watermark_store": str(watermark_path),
                    }
                }
            )
        finally:
            watermark_path.unlink(missing_ok=True)

        output = stream.getvalue()
        self.assertIn("SyncDB summary (detailed)", output)
        self.assertIn("soft deleted", output)
        self.assertIn("watermark", output)
        self.assertIn("2026-01-01T10:00:00", output)


class SyncFileIOTests(unittest.TestCase):
    def test_export_query_to_file_writes_rows(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "t"): [{"id": 1}, {"id": 2}]},
            columns_by_table={("dbo", "t"): [Column("id", "int")]},
        )
        sync = SyncDB(source=source, progress_mode=ProgressMode.NONE, verbose=None)

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            count = sync.export_query_to_file("SELECT id FROM t", path)

        self.assertEqual(count, 2)

    def test_export_query_can_read_sql_from_file_path(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "t"): [{"id": 1}]},
            columns_by_table={("dbo", "t"): [Column("id", "int")]},
        )
        sync = SyncDB(source=source, progress_mode=ProgressMode.NONE, verbose=None)

        with tempfile.TemporaryDirectory() as tmp:
            query_path = Path(tmp) / "query.sql"
            output_path = Path(tmp) / "out.csv"
            query_path.write_text("SELECT id FROM t", encoding="utf-8")
            count = sync.export_query_to_file(query_path, output_path)

        self.assertEqual(count, 1)

    def test_import_file_to_table_creates_table_and_inserts(self):
        target = MemoryConnector("postgresql", "public")
        sync = SyncDB(target=target, progress_mode=ProgressMode.NONE, verbose=None)

        csv_content = "id,name\n1,Ana\n2,Gio\n"
        with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
            f.write(csv_content)
            tmp_path = Path(f.name)

        try:
            count = sync.import_file_to_table(tmp_path, "public.people")
        finally:
            tmp_path.unlink(missing_ok=True)

        self.assertEqual(count, 2)
        self.assertIn(("public", "people"), target.rows_by_table)

    def test_import_file_to_table_fresh_insert_truncates_existing_rows(self):
        target = MemoryConnector(
            "postgresql",
            "public",
            rows_by_table={("public", "people"): [{"id": "old", "name": "Old"}]},
            columns_by_table={
                ("public", "people"): [
                    Column("id", "text"),
                    Column("name", "text"),
                ]
            },
        )
        sync = SyncDB(target=target, progress_mode=ProgressMode.NONE, verbose=None)

        csv_content = "id,name\n1,Ana\n"
        with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
            f.write(csv_content)
            tmp_path = Path(f.name)

        try:
            count = sync.import_file_to_table(tmp_path, "public.people", fresh_insert=True)
        finally:
            tmp_path.unlink(missing_ok=True)

        self.assertEqual(count, 1)
        self.assertEqual(target.truncated, [("public", "people")])
        self.assertEqual(target.rows_by_table[("public", "people")], [{"id": "1", "name": "Ana"}])

    def test_import_empty_file_cannot_infer_schema_for_new_table(self):
        target = MemoryConnector("postgresql", "public")
        sync = SyncDB(target=target, progress_mode=ProgressMode.NONE, verbose=None)

        with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
            f.write("")
            tmp_path = Path(f.name)

        try:
            with self.assertRaisesRegex(ValueError, "empty file"):
                sync.import_file_to_table(tmp_path, "public.empty_table")
        finally:
            tmp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
