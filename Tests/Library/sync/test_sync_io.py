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


class SyncFileIOTests(unittest.TestCase):
    def test_export_query_to_file_writes_rows(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "t"): [{"id": 1}, {"id": 2}]},
            columns_by_table={("dbo", "t"): [Column("id", "int")]},
        )
        sync = SyncDB(source_connector=source, progress_mode=ProgressMode.NONE, verbose=None)

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            count = sync.export_query_to_file("SELECT id FROM t", path)

        self.assertEqual(count, 2)

    def test_import_file_to_table_creates_table_and_inserts(self):
        target = MemoryConnector("postgresql", "public")
        sync = SyncDB(target_connector=target, progress_mode=ProgressMode.NONE, verbose=None)

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


if __name__ == "__main__":
    unittest.main()
