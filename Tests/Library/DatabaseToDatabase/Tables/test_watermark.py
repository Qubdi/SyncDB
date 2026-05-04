"""Common live tests for incremental watermark mode."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from ..helpers import LiveBase, count, materialize_scenario_classes, parameterized_filter


def _tmp_store() -> Path:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
        path = Path(handle.name)
    path.unlink(missing_ok=True)
    return path


def _base(source: str) -> dict:
    return {
        "source": source,
        "mode": "append",
        "primary_key": ["customer_id"],
        "order_by": ["customer_id"],
        "filter": "customer_id <= 500",
        "incremental_column": "signup_ts",
    }


class _WatermarkFirstRun(LiveBase):
    tables = ["t_wm_first"]

    def test_file_created_with_watermark_value(self):
        store = _tmp_store()
        try:
            spec = {"t": {**_base(self.source_customers), "destination": "t_wm_first", "watermark_store": str(store)}}
            result = self.make_sync().sync_tables(spec)[0]
            self.assertEqual(result.rows_written, 500)
            self.assertIsNotNone(result.watermark_value)
            self.assertTrue(store.exists())
            saved = json.loads(store.read_text(encoding="utf-8"))
            self.assertGreater(len(saved), 0)
        finally:
            store.unlink(missing_ok=True)


class _WatermarkSecondRun(LiveBase):
    tables = ["t_wm_second"]

    def test_second_run_writes_zero_rows(self):
        store = _tmp_store()
        try:
            spec = {"t": {**_base(self.source_customers), "destination": "t_wm_second", "watermark_store": str(store)}}
            self.make_sync().sync_tables(spec)
            count_run1 = count(self.scenario, "t_wm_second")

            second = self.make_sync().sync_tables(spec)[0]
            self.assertEqual(count(self.scenario, "t_wm_second"), count_run1)
            self.assertEqual(second.rows_written, 0)
        finally:
            store.unlink(missing_ok=True)


class _WatermarkCustomKey(LiveBase):
    tables = ["t_wm_key"]

    def test_custom_key_saved_in_store(self):
        store = _tmp_store()
        try:
            spec = {"t": {
                **_base(self.source_customers),
                "destination": "t_wm_key",
                "watermark_store": str(store),
                "watermark_key": "customers_cursor",
            }}
            result = self.make_sync().sync_tables(spec)[0]
            saved = json.loads(store.read_text(encoding="utf-8"))
            self.assertIn("customers_cursor", saved)
            self.assertEqual(result.watermark_value.isoformat(), saved["customers_cursor"])
        finally:
            store.unlink(missing_ok=True)


class _WatermarkCombinedWithFilter(LiveBase):
    tables = ["t_wm_combo"]

    def test_combined_filter_and_watermark(self):
        store = _tmp_store()
        try:
            spec = {"t": {
                "source": self.source_customers,
                "destination": "t_wm_combo",
                "mode": "append",
                "primary_key": ["customer_id"],
                "order_by": ["customer_id"],
                "filter": parameterized_filter(self.scenario, "customer_id <= {p}", [500]),
                "incremental_column": "signup_ts",
                "watermark_store": str(store),
            }}
            first = self.make_sync().sync_tables(spec)[0]
            self.assertEqual(first.rows_written, 500)
            second = self.make_sync().sync_tables(spec)[0]
            self.assertEqual(second.rows_written, 0)
        finally:
            store.unlink(missing_ok=True)


materialize_scenario_classes(
    globals(),
    _WatermarkFirstRun,
    _WatermarkSecondRun,
    _WatermarkCustomKey,
    _WatermarkCombinedWithFilter,
)
