import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from syncdb.sync import watermark as wm


class WatermarkUnitTests(unittest.TestCase):
    def test_load_watermark_returns_none_without_column(self):
        self.assertIsNone(wm.load_watermark({"source": "a", "destination": "b"}))

    def test_load_watermark_validates_column(self):
        with self.assertRaises(ValueError):
            wm.load_watermark({"source": "a", "destination": "b", "incremental_column": "bad; DROP"})

    def test_load_and_save_round_trip(self):
        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "wm.json"
            spec = {
                "source": "dbo.t", "destination": "public.t",
                "incremental_column": "updated_at", "watermark_store": str(store),
            }
            cfg = wm.load_watermark(spec)
            self.assertIsNone(cfg["value"])
            wm.save_watermark(cfg, "2026-01-01T00:00:00")
            reloaded = wm.load_watermark(spec)
            self.assertEqual(reloaded["value"], "2026-01-01T00:00:00")

    def test_resolve_path_rejects_relative_dotdot(self):
        with self.assertRaises(ValueError):
            wm._resolve_watermark_path("../escape.json")

    def test_apply_filter_without_existing_where(self):
        sql, params = wm.apply_watermark_filter("", [], "ts", 5, '"', "%s")
        self.assertEqual(sql, ' WHERE "ts" > %s ')
        self.assertEqual(params, [5])

    def test_apply_filter_merges_existing_where(self):
        sql, params = wm.apply_watermark_filter(" WHERE active = %s ", [True], "ts", 5, '"', "%s")
        self.assertIn("(active = %s)", sql)
        self.assertIn('("ts" > %s)', sql)
        self.assertEqual(params, [True, 5])

    def test_apply_filter_noop_for_empty_value(self):
        sql, params = wm.apply_watermark_filter(" WHERE x ", [1], "ts", None, '"', "%s")
        self.assertEqual(sql, " WHERE x ")
        self.assertEqual(params, [1])

    def test_max_watermark_value_tracks_maximum(self):
        self.assertEqual(wm.max_watermark_value(None, [{"c": 3}, {"c": 7}], "c"), 7)
        self.assertEqual(wm.max_watermark_value(10, [{"c": 3}], "c"), 10)
        self.assertEqual(wm.max_watermark_value(5, [{"c": None}], "c"), 5)

    def test_read_watermark_file_missing_returns_empty(self):
        with TemporaryDirectory() as tmp:
            self.assertEqual(wm.read_watermark_file(Path(tmp) / "nope.json"), {})

    def test_save_watermark_serialises_isoformat(self):
        import datetime
        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "wm.json"
            cfg = {"path": store, "key": "k", "column": "c", "value": None}
            wm.save_watermark(cfg, datetime.datetime(2026, 6, 10, 12, 0, 0))
            self.assertIn("2026-06-10T12:00:00", wm.read_watermark_file(store)["k"])


if __name__ == "__main__":
    unittest.main()
