"""Live tests: all sync modes — PostgreSQL → MySQL.

Tests every TransferMode (full_refresh, append, insert_only, upsert,
snapshot, soft_delete, append_staging) and verifies each is idempotent
(or accumulates predictably) across 2–3 runs.

Run:
    pytest "Tests/Library/PGSQL to MySQL/Tables/test_modes.py" -v
"""
from __future__ import annotations

import unittest

from syncdb.connections import create_connector

from .helpers import LiveBase, MY, count, drop, fetch_rows, column_names, make_sync

_SRC = "public.customers"
_FILTER = "customer_id <= 500"
_BASE = {
    "source": _SRC,
    "primary_key": ["customer_id"],
    "order_by": ["customer_id"],
    "filter": _FILTER,
}


class TestFullRefresh(LiveBase):
    """full_refresh truncates and reloads — result must be identical every run."""

    tables = ["t_fr"]

    def test_three_runs_same_count_and_values(self):
        spec = {"t": {**_BASE, "destination": "t_fr", "mode": "full_refresh"}}
        for run in range(1, 4):
            with self.subTest(run=run):
                r = make_sync().sync_tables(spec)[0]
                self.assertEqual(r.rows_written, 500)
                self.assertEqual(count("t_fr"), 500)
                first = fetch_rows("t_fr", limit=1)[0]
                last = fetch_rows("t_fr", order_by="customer_id DESC", limit=1)[0]
                self.assertEqual(first["customer_id"], 1)
                self.assertEqual(last["customer_id"], 500)


class TestAppend(LiveBase):
    """append (delete-then-insert on PK) stays stable when source doesn't change."""

    tables = ["t_app_stable", "t_app_wider"]

    _BASE = {**_BASE, "mode": "append"}

    def test_same_filter_three_runs_stable(self):
        spec = {"t": {**self._BASE, "destination": "t_app_stable"}}
        for run in range(1, 4):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_app_stable"), 500, f"run {run}")

    def test_wider_filter_second_run_adds_rows(self):
        narrow = {"t": {**self._BASE, "destination": "t_app_wider",
                        "filter": "customer_id <= 300"}}
        wide = {"t": {**self._BASE, "destination": "t_app_wider",
                      "filter": "customer_id <= 500"}}
        make_sync().sync_tables(narrow)
        self.assertEqual(count("t_app_wider"), 300, "after narrow run")
        make_sync().sync_tables(wide)
        self.assertEqual(count("t_app_wider"), 500, "after wide run")
        make_sync().sync_tables(wide)
        self.assertEqual(count("t_app_wider"), 500, "after repeat wide run")


class TestInsertOnly(LiveBase):
    """insert_only never removes rows — every run appends the full source slice."""

    tables = ["t_ins_accum", "t_ins_nopk"]

    def test_each_run_accumulates_rows(self):
        spec = {"t": {
            "source": _SRC, "destination": "t_ins_accum", "mode": "insert_only",
            "order_by": ["customer_id"], "filter": "customer_id <= 100",
        }}
        for run in range(1, 4):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_ins_accum"), 100 * run, f"after run {run}")

    def test_works_without_order_by(self):
        spec = {"t": {
            "source": _SRC, "destination": "t_ins_nopk", "mode": "insert_only",
            "filter": "customer_id <= 50",
        }}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_ins_nopk"), 50 * run, f"run {run}")


class TestUpsert(LiveBase):
    """upsert replaces PK-matching rows — idempotent with unchanged source."""

    tables = ["t_ups"]

    def test_three_runs_idempotent(self):
        spec = {"t": {**_BASE, "destination": "t_ups", "mode": "upsert"}}
        for run in range(1, 4):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_ups"), 500, f"run {run}")
            self.assertEqual(fetch_rows("t_ups", limit=1)[0]["customer_id"], 1)


class TestSnapshot(LiveBase):
    """snapshot appends all rows with _synced_at each run — count multiplies."""

    tables = ["t_snap"]

    def test_three_runs_accumulate_and_synced_at_is_set(self):
        spec = {"t": {
            "source": _SRC, "destination": "t_snap", "mode": "snapshot",
            "primary_key": ["customer_id"], "order_by": ["customer_id"],
            "filter": "customer_id <= 100",
        }}
        for run in range(1, 4):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_snap"), 100 * run, f"run {run}")

        self.assertIn("_synced_at", column_names("t_snap"))
        row = fetch_rows("t_snap", limit=1)[0]
        self.assertIsNotNone(row["_synced_at"])


class TestSoftDelete(LiveBase):
    """soft_delete marks rows absent from source with deleted_at; idempotent."""

    tables = ["t_sdel"]

    def test_two_runs_idempotent_no_rows_marked_deleted(self):
        spec = {"t": {**_BASE, "destination": "t_sdel", "mode": "soft_delete"}}
        for run in range(1, 3):
            r = make_sync().sync_tables(spec)[0]
            self.assertEqual(count("t_sdel"), 500, f"run {run}")
            self.assertEqual(r.rows_soft_deleted, 0, f"run {run}")

        self.assertIn("deleted_at", column_names("t_sdel"))

        c = create_connector(MY)
        c.connect()
        try:
            n = int(c.execute_query(
                "SELECT COUNT(*) AS n FROM `t_sdel` WHERE `deleted_at` IS NOT NULL"
            )[0]["n"])
        finally:
            c.close()
        self.assertEqual(n, 0)


class TestAppendStaging(LiveBase):
    """append_staging swaps a staging table into live — idempotent."""

    tables = ["t_stg"]

    def test_three_runs_idempotent(self):
        spec = {"t": {**_BASE, "destination": "t_stg", "mode": "append_staging"}}
        for run in range(1, 4):
            make_sync().sync_tables(spec)
            self.assertEqual(count("t_stg"), 500, f"run {run}")


if __name__ == "__main__":
    unittest.main()
