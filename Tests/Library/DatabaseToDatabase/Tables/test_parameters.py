"""Common live tests for sync parameters."""

from __future__ import annotations

from ..helpers import (
    LiveBase,
    count,
    fetch_rows,
    materialize_scenario_classes,
    parameterized_filter,
)


def _base(source: str) -> dict:
    return {
        "source": source,
        "mode": "full_refresh",
        "primary_key": ["customer_id"],
        "order_by": ["customer_id"],
        "filter": "customer_id <= 500",
    }


class _BatchSizeInteger(LiveBase):
    tables = ["t_bs_int"]

    def test_integer_50_two_runs(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_bs_int"}}
        for run in range(1, 3):
            self.make_sync(batch_size=50).sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_bs_int"), 500, f"run {run}")


class _BatchSizePercentage(LiveBase):
    tables = ["t_bs_1pct", "t_bs_10pct", "t_bs_50pct"]

    def test_percentages(self):
        for value, table in (("1%", "t_bs_1pct"), ("10%", "t_bs_10pct"), ("50%", "t_bs_50pct")):
            with self.subTest(batch_size=value):
                spec = {"t": {**_base(self.source_customers), "destination": table}}
                for run in range(1, 3):
                    self.make_sync(batch_size=value).sync_tables(spec)
                    self.assertEqual(count(self.scenario, table), 500, f"run {run}")


class _BatchSizeLargerThanData(LiveBase):
    tables = ["t_bs_big"]

    def test_larger_than_row_count_is_single_batch(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_bs_big"}}
        result = self.make_sync(batch_size=10_000).sync_tables(spec)[0]
        self.assertEqual(result.batches, 1)
        self.assertEqual(count(self.scenario, "t_bs_big"), 500)


class _BatchSizePerTableOverride(LiveBase):
    tables = ["t_bs_override"]

    def test_per_table_overrides_global(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_bs_override", "batch_size": "10%"}}
        result = self.make_sync(batch_size=50).sync_tables(spec)[0]
        self.assertEqual(result.rows_written, 500)
        self.assertEqual(count(self.scenario, "t_bs_override"), 500)


class _FilterString(LiveBase):
    tables = ["t_flt_s500", "t_flt_s1k", "t_flt_s0"]

    def _common(self) -> dict:
        return {
            "source": self.source_customers,
            "mode": "full_refresh",
            "primary_key": ["customer_id"],
            "order_by": ["customer_id"],
        }

    def test_500_rows(self):
        spec = {"t": {**self._common(), "destination": "t_flt_s500", "filter": "customer_id <= 500"}}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_flt_s500"), 500, f"run {run}")

    def test_1000_rows(self):
        spec = {"t": {**self._common(), "destination": "t_flt_s1k", "filter": "customer_id <= 1000"}}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_flt_s1k"), 1000, f"run {run}")

    def test_zero_rows_produces_empty_table(self):
        spec = {"t": {**self._common(), "destination": "t_flt_s0", "filter": "customer_id < 0"}}
        for run in range(1, 3):
            result = self.make_sync().sync_tables(spec)[0]
            self.assertEqual(result.rows_written, 0, f"run {run}")
            self.assertEqual(count(self.scenario, "t_flt_s0"), 0, f"run {run}")


class _FilterParameterised(LiveBase):
    tables = ["t_flt_p500", "t_flt_pmulti"]

    def _common(self) -> dict:
        return {
            "source": self.source_customers,
            "mode": "full_refresh",
            "primary_key": ["customer_id"],
            "order_by": ["customer_id"],
        }

    def test_single_param_500_rows(self):
        spec = {"t": {
            **self._common(),
            "destination": "t_flt_p500",
            "filter": parameterized_filter(self.scenario, "customer_id <= {p}", [500]),
        }}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_flt_p500"), 500, f"run {run}")
            self.assertEqual(fetch_rows(self.scenario, "t_flt_p500", limit=1)[0]["customer_id"], 1)

    def test_multi_condition_range(self):
        spec = {"t": {
            **self._common(),
            "destination": "t_flt_pmulti",
            "filter": parameterized_filter(
                self.scenario,
                "customer_id >= {p} AND customer_id <= {p}",
                [100, 200],
            ),
        }}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_flt_pmulti"), 101, f"run {run}")
            self.assertEqual(fetch_rows(self.scenario, "t_flt_pmulti", limit=1)[0]["customer_id"], 100)


class _OrderBy(LiveBase):
    tables = ["t_ord_single", "t_ord_multi"]

    def _common(self) -> dict:
        return {
            "source": self.source_customers,
            "mode": "full_refresh",
            "primary_key": ["customer_id"],
            "filter": "customer_id <= 500",
        }

    def test_single_column(self):
        spec = {"t": {**self._common(), "destination": "t_ord_single", "order_by": ["customer_id"]}}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_ord_single"), 500, f"run {run}")

    def test_multiple_columns(self):
        spec = {"t": {**self._common(), "destination": "t_ord_multi", "order_by": ["country", "customer_id"]}}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_ord_multi"), 500, f"run {run}")


materialize_scenario_classes(
    globals(),
    _BatchSizeInteger,
    _BatchSizePercentage,
    _BatchSizeLargerThanData,
    _BatchSizePerTableOverride,
    _FilterString,
    _FilterParameterised,
    _OrderBy,
)
