import unittest
from pathlib import Path

from syncdb import FileTransfer


class FileTransferTests(unittest.TestCase):
    temp_root = Path(__file__).resolve().parent / ".tmp"

    @classmethod
    def setUpClass(cls):
        cls.temp_root.mkdir(exist_ok=True)

    def test_writes_and_reads_csv_records(self):
        transfer = FileTransfer()
        rows = [{"id": 1, "name": "Ana"}, {"id": 2, "name": "Gio"}]
        path = self.temp_root / "customers.csv"

        try:
            written = transfer.write(rows, path)
            loaded = transfer.read(path)
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(written, 2)
        self.assertEqual(loaded, [{"id": "1", "name": "Ana"}, {"id": "2", "name": "Gio"}])

    def test_writes_and_reads_pickle_records(self):
        transfer = FileTransfer()
        rows = [{"id": 1, "active": True}]
        path = self.temp_root / "customers.pickle"

        try:
            transfer.write(rows, path)
            loaded = transfer.read(path)
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(loaded, rows)


if __name__ == "__main__":
    unittest.main()
