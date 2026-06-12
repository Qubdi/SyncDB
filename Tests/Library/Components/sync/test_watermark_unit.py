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

    def test_max_watermark_value_mixed_types_raise_named_error(self):
        # A bare TypeError from max() would not name the offending column.
        with self.assertRaisesRegex(ValueError, "updated_at.*incomparable"):
            wm.max_watermark_value(None, [{"updated_at": 1}, {"updated_at": "2026"}], "updated_at")
        import datetime
        with self.assertRaisesRegex(ValueError, "updated_at"):
            wm.max_watermark_value("2026-01-01", [{"updated_at": datetime.datetime(2026, 1, 2)}], "updated_at")

    def test_read_watermark_file_missing_returns_empty(self):
        with TemporaryDirectory() as tmp:
            self.assertEqual(wm.read_watermark_file(Path(tmp) / "nope.json"), {})

    def test_save_watermark_round_trips_datetime_as_datetime(self):
        import datetime
        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "wm.json"
            cfg = {"path": store, "key": "k", "column": "c", "value": None}
            saved = datetime.datetime(2026, 6, 10, 12, 0, 0)
            wm.save_watermark(cfg, saved)
            spec = {
                "source": "s", "destination": "d",
                "incremental_column": "c", "watermark_store": str(store),
                "watermark_key": "k",
            }
            # The next run must get a real datetime back, not an ISO string —
            # string-vs-datetime filter params are driver-dependent.
            self.assertEqual(wm.load_watermark(spec)["value"], saved)

    def test_save_watermark_round_trips_date_and_decimal(self):
        import datetime
        from decimal import Decimal
        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "wm.json"
            wm.save_watermark({"path": store, "key": "d"}, datetime.date(2026, 6, 10))
            wm.save_watermark({"path": store, "key": "n"}, Decimal("12.50"))
            spec = {"source": "s", "destination": "d", "incremental_column": "c",
                    "watermark_store": str(store)}
            self.assertEqual(
                wm.load_watermark({**spec, "watermark_key": "d"})["value"], datetime.date(2026, 6, 10)
            )
            self.assertEqual(
                wm.load_watermark({**spec, "watermark_key": "n"})["value"], Decimal("12.50")
            )

    def test_legacy_plain_string_watermarks_pass_through(self):
        import json
        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "wm.json"
            # Simulate a pre-2.x store holding a bare ISO string.
            store.write_text(json.dumps({"k": "2026-01-01T00:00:00"}), encoding="utf-8")
            spec = {"source": "s", "destination": "d", "incremental_column": "c",
                    "watermark_store": str(store), "watermark_key": "k"}
            self.assertEqual(wm.load_watermark(spec)["value"], "2026-01-01T00:00:00")


if __name__ == "__main__":
    unittest.main()
