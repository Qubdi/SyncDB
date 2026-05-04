"""Live tests: data-quality features — expect, transform, on_batch, combined.

expect: verifies all four checks (min_rows, not_null, unique, range) pass
on clean data and raise ValueError on violations.

transform: verifies row-level mutations are applied and survive re-runs.

on_batch: verifies the callback receives correct incremental totals.

combined stress: runs all major parameters together for 3 runs and asserts
every detail of the MySQL state.

Run:
    pytest "Tests/Library/PGSQL to MySQL/Tables/test_data_quality.py" -v
"""
from __future__ import annotations

import unittest

from .helpers import LiveBase, column_names, column_type, count, fetch_rows, make_sync

_SRC = "public.customers"
_BASE = {
    "source": _SRC,
    "mode": "full_refresh",
    "primary_key": ["customer_id"],
    "order_by": ["customer_id"],
    "filter": "customer_id <= 500",
}


# ── expect: passing checks ─────────────────────────────────────────────────────

class TestExpectPassing(LiveBase):
    tables = ["t_exp_pass"]

    def test_min_rows(self):
        spec = {"t": {**_BASE, "destination": "t_exp_pass",
                      "expect": {"min_rows": 100}}}
        for run in range(1, 3):
            r = make_sync().sync_tables(spec)[0]
            self.assertEqual(len(r.expectations_failed), 0, f"run {run}")

    def test_not_null(self):
        spec = {"t": {**_BASE, "destination": "t_exp_pass",
                      "expect": {"not_null": ["customer_id", "email"]}}}
        r = make_sync().sync_tables(spec)[0]
        self.assertEqual(len(r.expectations_failed), 0)

    def test_unique(self):
        spec = {"t": {**_BASE, "destination": "t_exp_pass",
                      "expect": {"unique": ["customer_id", "email"]}}}
        r = make_sync().sync_tables(spec)[0]
        self.assertEqual(len(r.expectations_failed), 0)

    def test_range(self):
        spec = {"t": {**_BASE, "destination": "t_exp_pass",
                      "expect": {"range": {"customer_id": {"min": 1, "max": 500}}}}}
        for run in range(1, 3):
            r = make_sync().sync_tables(spec)[0]
            self.assertEqual(len(r.expectations_failed), 0, f"run {run}")

    def test_all_checks_combined(self):
        spec = {
            "t": {
                **_BASE,
                "destination": "t_exp_pass",
                "expect": {
                    "min_rows": 100,
                    "not_null": ["customer_id", "email"],
                    "unique": ["customer_id", "email"],
                    "range": {"customer_id": {"min": 1, "max": 500}},
                },
            }
        }
        for run in range(1, 3):
            r = make_sync().sync_tables(spec)[0]
            self.assertEqual(len(r.expectations_failed), 0, f"run {run}")
            self.assertEqual(count("t_exp_pass"), 500)


# ── expect: failing checks raise ValueError ────────────────────────────────────

class TestExpectViolations(LiveBase):
    tables = ["t_exp_fail_min", "t_exp_fail_uniq"]

    def test_min_rows_violation(self):
        spec = {"t": {**_BASE, "destination": "t_exp_fail_min",
                      "filter": "customer_id <= 5",
                      "expect": {"min_rows": 1000}}}
        with self.assertRaises(ValueError):
            make_sync().sync_tables(spec)

    def test_unique_violation_on_second_insert_only_run(self):
        spec = {
            "t": {
                "source": _SRC,
                "destination": "t_exp_fail_uniq",
                "mode": "insert_only",
                "order_by": ["customer_id"],
                "filter": "customer_id <= 5",
                "expect": {"unique": ["customer_id"]},
            }
        }
        make_sync().sync_tables(spec)
        with self.assertRaises(ValueError):
            make_sync().sync_tables(spec)


# ── transform ──────────────────────────────────────────────────────────────────

class TestTransform(LiveBase):
    tables = ["t_trx_upper", "t_trx_const"]

    _BASE = {**_BASE, "filter": "customer_id <= 100"}

    def test_uppercase_name_applied_every_run(self):
        spec = {
            "t": {
                **self._BASE,
                "destination": "t_trx_upper",
                "transform": lambda batch: [
                    {**row, "full_name": row["full_name"].upper()} for row in batch
                ],
            }
        }
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            for row in fetch_rows("t_trx_upper", limit=3):
                self.assertEqual(row["full_name"], row["full_name"].upper(),
                                 f"run {run}")

    def test_constant_field_override(self):
        spec = {
            "t": {
                **self._BASE,
                "destination": "t_trx_const",
                "transform": lambda batch: [{**row, "country": "TEST"} for row in batch],
            }
        }
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            for row in fetch_rows("t_trx_const", limit=5):
                self.assertEqual(row["country"], "TEST", f"run {run}")


# ── on_batch ───────────────────────────────────────────────────────────────────

class TestOnBatch(LiveBase):
    tables = ["t_ob"]

    def test_callback_receives_incremental_totals(self):
        seen: list[tuple[int, int]] = []
        spec = {
            "t": {
                "source": _SRC,
                "destination": "t_ob",
                "mode": "full_refresh",
                "primary_key": ["customer_id"],
                "order_by": ["customer_id"],
                "filter": "customer_id <= 100",
                "on_batch": lambda r: seen.append((r.rows_written, r.batches)),
            }
        }
        for run in range(1, 3):
            seen.clear()
            make_sync(batch_size=50).sync_tables(spec)
            self.assertEqual(len(seen), 2, f"run {run}: 2 batches of 50")
            self.assertEqual(seen[-1][0], 100, f"run {run}: final rows_written")
            self.assertEqual(seen[-1][1], 2, f"run {run}: final batches")


# ── combined stress ────────────────────────────────────────────────────────────

class TestCombinedParameters(LiveBase):
    """All major parameters exercised together for 3 full runs."""

    tables = ["t_combo"]

    def test_all_parameters_three_runs(self):
        batches_seen: list[int] = []
        spec = {
            "t": {
                "source": _SRC,
                "destination": "t_combo",
                "mode": "full_refresh",
                "primary_key": ["customer_id"],
                "order_by": ["customer_id"],
                "filter": {"where": "customer_id <= %s", "params": [500]},
                "rename": {"full_name": "name"},
                "type_overrides": {"country": "char(80)"},
                "transform": lambda batch: [
                    {**row, "name": row["name"].upper()} for row in batch
                ],
                "expect": {
                    "min_rows": 100,
                    "not_null": ["customer_id"],
                    "unique": ["customer_id"],
                    "range": {"customer_id": {"min": 1, "max": 500}},
                },
                "on_batch": lambda r: batches_seen.append(r.batches),
            }
        }
        for run in range(1, 4):
            batches_seen.clear()
            r = make_sync(batch_size=100).sync_tables(spec)[0]

            self.assertEqual(r.rows_written, 500, f"run {run}: rows")
            self.assertEqual(count("t_combo"), 500, f"run {run}: db count")
            self.assertEqual(len(r.expectations_failed), 0, f"run {run}: expect")
            self.assertGreater(len(batches_seen), 0, f"run {run}: on_batch fired")

            cols = column_names("t_combo")
            self.assertIn("name", cols, f"run {run}: rename applied")
            self.assertNotIn("full_name", cols, f"run {run}: old col gone")
            self.assertIn("char", column_type("t_combo", "country").lower(),
                          f"run {run}: type override")

            for row in fetch_rows("t_combo", limit=3):
                self.assertEqual(row["name"], row["name"].upper(),
                                 f"run {run}: transform uppercase")


if __name__ == "__main__":
    unittest.main()
