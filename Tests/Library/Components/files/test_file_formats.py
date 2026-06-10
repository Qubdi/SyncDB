import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from syncdb import FileTransfer


class ParquetExcelTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_parquet_round_trip(self):
        transfer = FileTransfer()
        rows = [{"id": 1, "name": "Ana"}, {"id": 2, "name": "Gio"}]
        path = self.dir / "data.parquet"
        written = transfer.write(rows, path)
        loaded = transfer.read(path)
        self.assertEqual(written, 2)
        self.assertEqual(loaded, rows)

    def test_excel_round_trip(self):
        transfer = FileTransfer()
        rows = [{"id": 1, "name": "Ana"}]
        path = self.dir / "data.xlsx"
        transfer.write(rows, path)
        loaded = transfer.read(path)
        self.assertEqual(loaded, rows)

    def test_streaming_parquet(self):
        transfer = FileTransfer()
        batches = iter([[{"id": 1}], [{"id": 2}, {"id": 3}]])
        path = self.dir / "stream.parquet"
        count = transfer.write_streaming(batches, path)
        self.assertEqual(count, 3)
        self.assertEqual(len(transfer.read(path)), 3)

    def test_streaming_csv(self):
        transfer = FileTransfer()
        batches = iter([[{"id": 1, "v": "a"}], [{"id": 2, "v": "b"}]])
        path = self.dir / "stream.csv"
        count = transfer.write_streaming(batches, path)
        self.assertEqual(count, 2)

    def test_streaming_excel_materialises(self):
        transfer = FileTransfer()
        batches = iter([[{"id": 1}], [{"id": 2}]])
        path = self.dir / "stream.xlsx"
        count = transfer.write_streaming(batches, path)
        self.assertEqual(count, 2)

    def test_pickle_dataframe_payload_normalised(self):
        import pandas as pd
        import pickle
        path = self.dir / "frame.pickle"
        with path.open("wb") as fh:
            pickle.dump(pd.DataFrame([{"id": 1}]), fh)
        loaded = FileTransfer().read(path, hmac_key=None)
        self.assertEqual(loaded, [{"id": 1}])

    def test_explicit_xls_extension_detected_as_excel(self):
        transfer = FileTransfer()
        path = self.dir / "legacy.xls"
        transfer.write([{"id": 1}], path)
        self.assertEqual(transfer.read(path), [{"id": 1}])

    def test_pkl_extension_detected_as_pickle(self):
        transfer = FileTransfer()
        path = self.dir / "data.pkl"
        transfer.write([{"id": 1}], path)
        self.assertEqual(transfer.read(path), [{"id": 1}])


if __name__ == "__main__":
    unittest.main()
