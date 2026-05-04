"""Common live tests for data-quality features."""

from __future__ import annotations

from ..helpers import (
    LiveBase,
    column_names,
    column_type,
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


class _ExpectPassing(LiveBase):
    tables = ["t_exp_pass"]

    def test_min_rows(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_exp_pass", "expect": {"min_rows": 100}}}
        for run in range(1, 3):
            result = self.make_sync().sync_tables(spec)[0]
            self.assertEqual(len(result.expectations_failed), 0, f"run {run}")

    def test_not_null(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_exp_pass",
            "expect": {"not_null": ["customer_id", "email"]},
        }}
        result = self.make_sync().sync_tables(spec)[0]
        self.assertEqual(len(result.expectations_failed), 0)

    def test_unique(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_exp_pass",
            "expect": {"unique": ["customer_id", "email"]},
        }}
        result = self.make_sync().sync_tables(spec)[0]
        self.assertEqual(len(result.expectations_failed), 0)

    def test_range(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_exp_pass",
            "expect": {"range": {"customer_id": {"min": 1, "max": 500}}},
        }}
        for run in range(1, 3):
            result = self.make_sync().sync_tables(spec)[0]
            self.assertEqual(len(result.expectations_failed), 0, f"run {run}")

    def test_all_checks_combined(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_exp_pass",
            "expect": {
                "min_rows": 100,
                "not_null": ["customer_id", "email"],
                "unique": ["customer_id", "email"],
                "range": {"customer_id": {"min": 1, "max": 500}},
            },
        }}
        for run in range(1, 3):
            result = self.make_sync().sync_tables(spec)[0]
            self.assertEqual(len(result.expectations_failed), 0, f"run {run}")
            self.assertEqual(count(self.scenario, "t_exp_pass"), 500)


class _ExpectViolations(LiveBase):
    tables = ["t_exp_fail_min", "t_exp_fail_uniq"]

    def test_min_rows_violation(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_exp_fail_min",
            "filter": "customer_id <= 5",
            "expect": {"min_rows": 1000},
        }}
        with self.assertRaises(ValueError):
            self.make_sync().sync_tables(spec)

    def test_unique_violation_on_second_insert_only_run(self):
        spec = {"t": {
            "source": self.source_customers,
            "destination": "t_exp_fail_uniq",
            "mode": "snapshot",
            "order_by": ["customer_id"],
            "filter": "customer_id <= 5",
            "expect": {"unique": ["customer_id"]},
        }}
        self.make_sync().sync_tables(spec)
        with self.assertRaises(ValueError):
            self.make_sync().sync_tables(spec)


class _Transform(LiveBase):
    tables = ["t_trx_upper", "t_trx_const"]

    def _small_base(self) -> dict:
        return {**_base(self.source_customers), "filter": "customer_id <= 100"}

    def test_uppercase_name_applied_every_run(self):
        spec = {"t": {
            **self._small_base(),
            "destination": "t_trx_upper",
            "transform": lambda batch: [{**row, "full_name": row["full_name"].upper()} for row in batch],
        }}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            for row in fetch_rows(self.scenario, "t_trx_upper", limit=3):
                self.assertEqual(row["full_name"], row["full_name"].upper(), f"run {run}")

    def test_constant_field_override(self):
        spec = {"t": {
            **self._small_base(),
            "destination": "t_trx_const",
            "transform": lambda batch: [{**row, "country": "TEST"} for row in batch],
        }}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            for row in fetch_rows(self.scenario, "t_trx_const", limit=5):
                self.assertEqual(row["country"], "TEST", f"run {run}")


class _OnBatch(LiveBase):
    tables = ["t_ob"]

    def test_callback_receives_incremental_totals(self):
        seen: list[tuple[int, int]] = []
        spec = {"t": {
            "source": self.source_customers,
            "destination": "t_ob",
            "mode": "full_refresh",
            "primary_key": ["customer_id"],
            "order_by": ["customer_id"],
            "filter": "customer_id <= 100",
            "on_batch": lambda result: seen.append((result.rows_written, result.batches)),
        }}
        for run in range(1, 3):
            seen.clear()
            self.make_sync(batch_size=50).sync_tables(spec)
            self.assertEqual(len(seen), 2, f"run {run}: 2 batches of 50")
            self.assertEqual(seen[-1][0], 100, f"run {run}: final rows_written")
            self.assertEqual(seen[-1][1], 2, f"run {run}: final batches")


class _CombinedParameters(LiveBase):
    tables = ["t_combo"]

    def test_all_parameters_three_runs(self):
        batches_seen: list[int] = []
        spec = {"t": {
            "source": self.source_customers,
            "destination": "t_combo",
            "mode": "full_refresh",
            "primary_key": ["customer_id"],
            "order_by": ["customer_id"],
            "filter": parameterized_filter(self.scenario, "customer_id <= {p}", [500]),
            "rename": {"full_name": "name"},
            "type_overrides": {"country": "char(80)"},
            "transform": lambda batch: [{**row, "full_name": row["full_name"].upper()} for row in batch],
            "expect": {
                "min_rows": 100,
                "not_null": ["customer_id"],
                "unique": ["customer_id"],
                "range": {"customer_id": {"min": 1, "max": 500}},
            },
            "on_batch": lambda result: batches_seen.append(result.batches),
        }}
        for run in range(1, 4):
            batches_seen.clear()
            result = self.make_sync(batch_size=100).sync_tables(spec)[0]

            self.assertEqual(result.rows_written, 500, f"run {run}: rows")
            self.assertEqual(count(self.scenario, "t_combo"), 500, f"run {run}: db count")
            self.assertEqual(len(result.expectations_failed), 0, f"run {run}: expect")
            self.assertGreater(len(batches_seen), 0, f"run {run}: on_batch fired")

            cols = column_names(self.scenario, "t_combo")
            self.assertIn("name", cols, f"run {run}: rename applied")
            self.assertNotIn("full_name", cols, f"run {run}: old col gone")
            self.assertIn("char", column_type(self.scenario, "t_combo", "country").lower(), f"run {run}: type override")

            for row in fetch_rows(self.scenario, "t_combo", limit=3):
                self.assertEqual(row["name"], row["name"].upper(), f"run {run}: transform uppercase")


materialize_scenario_classes(
    globals(),
    _ExpectPassing,
    _ExpectViolations,
    _Transform,
    _OnBatch,
    _CombinedParameters,
)
