"""Live tests: sync parameters — batch_size, filter, order_by.

Covers every variant of batch_size (int, percent, per-table override),
every filter form (string, parameterised, multi-condition, zero rows),
and single vs. multi-column order_by.  All tests run 2–3 times to
confirm the parameters produce stable, idempotent results.

Run:
    pytest "Tests/Library/PGSQL to MySQL/Tables/test_parameters.py" -v
"""
from __future__ import annotations

import unittest

from .helpers import LiveBase, count, fetch_rows, make_sync

_SRC = "public.customers"
_BASE = {
    "source": _SRC,
    "mode": "full_refresh",
    "primary_key": ["customer_id"],
    "order_by": ["customer_id"],
    "filter": "customer_id <= 500",
}


# ── batch_size ─────────────────────────────────────────────────────────────────

class TestBatchSizeInteger(LiveBase):
    tables = ["t_bs_int"]

    def test_integer_50_two_runs(self):
        spec = {"t": {**_BASE, "destination": "t_bs_int"}}
        for run in range(1, 3):
            make_sync(batch_size=50).sync_tables(spec)
            self.assertEqual(count("t_bs_int"), 500, f"run {run}")


class TestBatchSizePercentage(LiveBase):
    tables = ["t_bs_1pct", "t_bs_10pct", "t_bs_50pct"]

    def test_1_percent(self):
        spec = {"t": {**_BASE, "destination": "t_bs_1pct"}}
        for run in range(1, 3):
            make_sync(batch_size="1%").sync_tables(spec)
            self.assertEqual(count("t_bs_1pct"), 500, f"run {run}")

    def test_10_percent(self):
        spec = {"t": {**_BASE, "destination": "t_bs_10pct"}}
        for run in range(1, 3):
            make_sync(batch_size="10%").sync_tables(spec)
            self.assertEqual(count("t_bs_10pct"), 500, f"run {run}")

    def test_50_percent(self):
        spec = {"t": {**_BASE, "destination": "t_bs_50pct"}}
        for run in range(1, 3):
            make_sync(batch_size="50%").sync_tables(spec)
            self.assertEqual(count("t_bs_50pct"), 500, f"run {run}")


class TestBatchSizeLargerThanData(LiveBase):
    tables = ["t_bs_big"]

    def test_larger_than_row_count_is_single_batch(self):
        spec = {"t": {**_BASE, "destination": "t_bs_big"}}
        r = make_sync(batch_size=10_000).sync_tables(spec)[0]
        self.assertEqual(r.batches, 1)
        self.assertEqual(count("t_bs_big"), 500)


class TestBatchSizePerTableOverride(LiveBase):
    tables = ["t_bs_override"]

    def test_per_table_overrides_global(self):
        spec = {"t": {**_BASE, "destination": "t_bs_override", "batch_size": "10%"}}
        r = make_sync(batch_size=50).sync_tables(spec)[0]
        self.assertEqual(r.rows_written, 500)
        self.assertEqual(count("t_bs_override"), 500)


# ── filter ─────────────────────────────────────────────────────────────────────

class TestFilterString(LiveBase):
    tables = ["t_flt_s500", "t_flt_s1k", "t_flt_s0"]

    _BASE = {"source": _SRC, "mode": "full_refresh",
             "primary_key": ["customer_id"], "order_by": ["customer_id"]}

    def test_500_rows(self):
        spec = {"t": {**self._BASE, "destination": "t_flt_s500",
                      "filter": "customer_id <= 500"}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_flt_s500"), 500, f"run {run}")

    def test_1000_rows(self):
        spec = {"t": {**self._BASE, "destination": "t_flt_s1k",
                      "filter": "customer_id <= 1000"}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_flt_s1k"), 1000, f"run {run}")

    def test_zero_rows_produces_empty_table(self):
        spec = {"t": {**self._BASE, "destination": "t_flt_s0",
                      "filter": "customer_id < 0"}}
        for run in range(1, 3):
            r = make_sync().sync_tables(spec)[0]
            self.assertEqual(r.rows_written, 0, f"run {run}")
            self.assertEqual(count("t_flt_s0"), 0, f"run {run}")


class TestFilterParameterised(LiveBase):
    tables = ["t_flt_p500", "t_flt_pmulti"]

    _BASE = {"source": _SRC, "mode": "full_refresh",
             "primary_key": ["customer_id"], "order_by": ["customer_id"]}

    def test_single_param_500_rows(self):
        spec = {"t": {**self._BASE, "destination": "t_flt_p500",
                      "filter": {"where": "customer_id <= %s", "params": [500]}}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_flt_p500"), 500, f"run {run}")
            self.assertEqual(fetch_rows("t_flt_p500", limit=1)[0]["customer_id"], 1)

    def test_multi_condition_range(self):
        spec = {"t": {**self._BASE, "destination": "t_flt_pmulti",
                      "filter": {"where": "customer_id >= %s AND customer_id <= %s",
                                 "params": [100, 200]}}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_flt_pmulti"), 101, f"run {run}")
            self.assertEqual(fetch_rows("t_flt_pmulti", limit=1)[0]["customer_id"], 100)


# ── order_by ───────────────────────────────────────────────────────────────────

class TestOrderBy(LiveBase):
    tables = ["t_ord_single", "t_ord_multi"]

    _BASE = {"source": _SRC, "mode": "full_refresh",
             "primary_key": ["customer_id"], "filter": "customer_id <= 500"}

    def test_single_column(self):
        spec = {"t": {**self._BASE, "destination": "t_ord_single",
                      "order_by": ["customer_id"]}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_ord_single"), 500, f"run {run}")

    def test_multiple_columns(self):
        spec = {"t": {**self._BASE, "destination": "t_ord_multi",
                      "order_by": ["country", "customer_id"]}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_ord_multi"), 500, f"run {run}")


if __name__ == "__main__":
    unittest.main()
