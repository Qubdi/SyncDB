"""Common live tests for all table sync modes."""

from __future__ import annotations

from ..helpers import (
    LiveBase,
    column_names,
    count,
    count_where_not_null,
    fetch_rows,
    materialize_scenario_classes,
)

_FILTER = "customer_id <= 500"


def _base(source: str) -> dict:
    return {
        "source": source,
        "primary_key": ["customer_id"],
        "order_by": ["customer_id"],
        "filter": _FILTER,
    }


class _FullRefresh(LiveBase):
    tables = ["t_fr"]

    def test_three_runs_same_count_and_values(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_fr", "mode": "full_refresh"}}
        for run in range(1, 4):
            with self.subTest(run=run):
                result = self.make_sync().sync_tables(spec)[0]
                self.assertEqual(result.rows_written, 500)
                self.assertEqual(count(self.scenario, "t_fr"), 500)
                first = fetch_rows(self.scenario, "t_fr", limit=1)[0]
                last = fetch_rows(self.scenario, "t_fr", order_by="customer_id DESC", limit=1)[0]
                self.assertEqual(first["customer_id"], 1)
                self.assertEqual(last["customer_id"], 500)


class _Append(LiveBase):
    tables = ["t_app_stable", "t_app_wider"]

    def test_same_filter_three_runs_stable(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_app_stable", "mode": "append"}}
        for run in range(1, 4):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_app_stable"), 500, f"run {run}")

    def test_wider_filter_second_run_adds_rows(self):
        common = {**_base(self.source_customers), "mode": "append"}
        narrow = {"t": {**common, "destination": "t_app_wider", "filter": "customer_id <= 300"}}
        wide = {"t": {**common, "destination": "t_app_wider", "filter": "customer_id <= 500"}}
        self.make_sync().sync_tables(narrow)
        self.assertEqual(count(self.scenario, "t_app_wider"), 300, "after narrow run")
        self.make_sync().sync_tables(wide)
        self.assertEqual(count(self.scenario, "t_app_wider"), 500, "after wide run")
        self.make_sync().sync_tables(wide)
        self.assertEqual(count(self.scenario, "t_app_wider"), 500, "after repeat wide run")


class _InsertOnly(LiveBase):
    tables = ["t_ins_accum", "t_ins_nopk"]

    def test_same_primary_keys_raise_on_second_run(self):
        spec = {"t": {
            "source": self.source_customers,
            "destination": "t_ins_accum",
            "mode": "insert_only",
            "order_by": ["customer_id"],
            "filter": "customer_id <= 100",
        }}
        self.make_sync().sync_tables(spec)
        self.assertEqual(count(self.scenario, "t_ins_accum"), 100)
        with self.assertRaises(Exception):
            self.make_sync().sync_tables(spec)

    def test_works_without_order_by(self):
        spec = {"t": {
            "source": self.source_customers,
            "destination": "t_ins_nopk",
            "mode": "insert_only",
            "filter": "customer_id <= 50",
        }}
        self.make_sync().sync_tables(spec)
        self.assertEqual(count(self.scenario, "t_ins_nopk"), 50)


class _Upsert(LiveBase):
    tables = ["t_ups"]

    def test_three_runs_idempotent(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_ups", "mode": "upsert"}}
        for run in range(1, 4):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_ups"), 500, f"run {run}")
            self.assertEqual(fetch_rows(self.scenario, "t_ups", limit=1)[0]["customer_id"], 1)


class _Snapshot(LiveBase):
    tables = ["t_snap"]

    def test_three_runs_accumulate_and_synced_at_is_set(self):
        spec = {"t": {
            "source": self.source_customers,
            "destination": "t_snap",
            "mode": "snapshot",
            "primary_key": ["customer_id"],
            "order_by": ["customer_id"],
            "filter": "customer_id <= 100",
        }}
        for run in range(1, 4):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_snap"), 100 * run, f"run {run}")

        self.assertIn("_synced_at", column_names(self.scenario, "t_snap"))
        row = fetch_rows(self.scenario, "t_snap", limit=1)[0]
        self.assertIsNotNone(row["_synced_at"])


class _SoftDelete(LiveBase):
    tables = ["t_sdel"]

    def test_two_runs_idempotent_no_rows_marked_deleted(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_sdel", "mode": "soft_delete"}}
        for run in range(1, 3):
            result = self.make_sync().sync_tables(spec)[0]
            self.assertEqual(count(self.scenario, "t_sdel"), 500, f"run {run}")
            self.assertEqual(result.rows_soft_deleted, 0, f"run {run}")

        self.assertIn("deleted_at", column_names(self.scenario, "t_sdel"))
        self.assertEqual(count_where_not_null(self.scenario, "t_sdel", "deleted_at"), 0)


class _AppendStaging(LiveBase):
    tables = ["t_stg"]

    def test_three_runs_idempotent(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_stg", "mode": "append_staging"}}
        for run in range(1, 4):
            self.make_sync().sync_tables(spec)
            self.assertEqual(count(self.scenario, "t_stg"), 500, f"run {run}")


materialize_scenario_classes(
    globals(),
    _FullRefresh,
    _Append,
    _InsertOnly,
    _Upsert,
    _Snapshot,
    _SoftDelete,
    _AppendStaging,
)
