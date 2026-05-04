"""Live tests: incremental / watermark mode — PostgreSQL → MySQL.

Verifies that the watermark file is written after run 1, that a second run
with the same store fetches zero new rows, that custom watermark keys are
saved correctly, and that a base filter and watermark condition are ANDed.

Run:
    pytest "Tests/Library/PGSQL to MySQL/Tables/test_watermark.py" -v
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from .helpers import LiveBase, count, make_sync

_SRC = "public.customers"
_BASE = {
    "source": _SRC,
    "mode": "append",
    "primary_key": ["customer_id"],
    "order_by": ["customer_id"],
    "filter": "customer_id <= 500",
    "incremental_column": "created_at",
}


def _tmp_store() -> Path:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        p = Path(f.name)
    p.unlink(missing_ok=True)
    return p


class TestWatermarkFirstRun(LiveBase):
    """First run must write rows and persist the watermark."""

    tables = ["t_wm_first"]

    def test_file_created_with_watermark_value(self):
        store = _tmp_store()
        try:
            spec = {"t": {**_BASE, "destination": "t_wm_first",
                          "watermark_store": str(store)}}
            r = make_sync().sync_tables(spec)[0]
            self.assertEqual(r.rows_written, 500)
            self.assertIsNotNone(r.watermark_value)
            self.assertTrue(store.exists())
            saved = json.loads(store.read_text(encoding="utf-8"))
            self.assertGreater(len(saved), 0)
        finally:
            store.unlink(missing_ok=True)


class TestWatermarkSecondRun(LiveBase):
    """Second run with unchanged source must write 0 rows."""

    tables = ["t_wm_second"]

    def test_second_run_writes_zero_rows(self):
        store = _tmp_store()
        try:
            spec = {"t": {**_BASE, "destination": "t_wm_second",
                          "watermark_store": str(store)}}
            make_sync().sync_tables(spec)
            count_run1 = count("t_wm_second")

            r2 = make_sync().sync_tables(spec)[0]
            self.assertEqual(count("t_wm_second"), count_run1,
                             "row count must not change on second run")
            self.assertEqual(r2.rows_written, 0)
        finally:
            store.unlink(missing_ok=True)


class TestWatermarkCustomKey(LiveBase):
    """Custom watermark_key must be used as the JSON key in the store file."""

    tables = ["t_wm_key"]

    def test_custom_key_saved_in_store(self):
        store = _tmp_store()
        try:
            spec = {
                "t": {
                    **_BASE,
                    "destination": "t_wm_key",
                    "watermark_store": str(store),
                    "watermark_key": "customers_cursor",
                }
            }
            r = make_sync().sync_tables(spec)[0]
            saved = json.loads(store.read_text(encoding="utf-8"))
            self.assertIn("customers_cursor", saved)
            self.assertEqual(r.watermark_value, saved["customers_cursor"])
        finally:
            store.unlink(missing_ok=True)


class TestWatermarkCombinedWithFilter(LiveBase):
    """Parameterised base filter and watermark condition must be ANDed."""

    tables = ["t_wm_combo"]

    def test_combined_filter_and_watermark(self):
        store = _tmp_store()
        try:
            spec = {
                "t": {
                    "source": _SRC,
                    "destination": "t_wm_combo",
                    "mode": "append",
                    "primary_key": ["customer_id"],
                    "order_by": ["customer_id"],
                    "filter": {"where": "customer_id <= %s", "params": [500]},
                    "incremental_column": "created_at",
                    "watermark_store": str(store),
                }
            }
            r1 = make_sync().sync_tables(spec)[0]
            self.assertEqual(r1.rows_written, 500)
            r2 = make_sync().sync_tables(spec)[0]
            self.assertEqual(r2.rows_written, 0, "combined filter + watermark")
        finally:
            store.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
