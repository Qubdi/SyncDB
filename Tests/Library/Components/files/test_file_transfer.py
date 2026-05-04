import unittest
from pathlib import Path

from syncdb import FileTransfer


class FileTransferTests(unittest.TestCase):
    temp_root = Path(__file__).resolve().parent / ".tmp"

    @classmethod
    def setUpClass(cls):
        cls.temp_root.mkdir(exist_ok=True)

    def _tmp(self, filename: str) -> Path:
        return self.temp_root / filename

    def test_writes_and_reads_csv_records(self):
        transfer = FileTransfer()
        rows = [{"id": 1, "name": "Ana"}, {"id": 2, "name": "Gio"}]
        path = self._tmp("customers.csv")

        try:
            written = transfer.write(rows, path)
            loaded = transfer.read(path)
        finally:
            path.unlink(missing_ok=True)

        # CSV reads all values as strings — callers must cast as needed.
        self.assertEqual(written, 2)
        self.assertEqual(loaded, [{"id": "1", "name": "Ana"}, {"id": "2", "name": "Gio"}])

    def test_writes_and_reads_pickle_records(self):
        transfer = FileTransfer()
        rows = [{"id": 1, "active": True}]
        path = self._tmp("customers.pickle")

        try:
            transfer.write(rows, path)
            loaded = transfer.read(path)
        finally:
            path.unlink(missing_ok=True)

        self.assertEqual(loaded, rows)

    def test_writes_empty_csv_produces_header_only_file(self):
        transfer = FileTransfer()
        path = self._tmp("empty.csv")

        try:
            written = transfer.write([], path)
            content = path.read_text(encoding="utf-8")
        finally:
            path.unlink(missing_ok=True)

        self.assertEqual(written, 0)
        # No header row when fieldnames list is empty.
        self.assertEqual(content.strip(), "")

    def test_unknown_extension_raises_value_error(self):
        transfer = FileTransfer()

        with self.assertRaises(ValueError) as ctx:
            transfer.read(self._tmp("data.json"))

        self.assertIn("json", str(ctx.exception))
        self.assertIn("Supported", str(ctx.exception))

    def test_explicit_format_overrides_extension(self):
        transfer = FileTransfer()
        rows = [{"x": "1"}]
        # Write a CSV but give it a .txt extension; explicit format must override.
        path = self._tmp("export.txt")

        try:
            written = transfer.write(rows, path, file_format="csv")
            loaded = transfer.read(path, file_format="csv")
        finally:
            path.unlink(missing_ok=True)

        self.assertEqual(written, 1)
        self.assertEqual(loaded, [{"x": "1"}])


if __name__ == "__main__":
    unittest.main()
