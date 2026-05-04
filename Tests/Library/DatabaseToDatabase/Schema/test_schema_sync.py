"""Common live tests for schema-level sync."""

from __future__ import annotations

from ..helpers import SchemaLiveBase, count, materialize_scenario_classes

_EXCLUDED_TABLES = ["customers", "products", "orders", "payments", "datatype_samples"]
_TARGET_PREFIX = "syncdb_test_"


class _SchemaSyncExclude(SchemaLiveBase):
    def test_excluded_tables_not_created_in_target(self):
        results = self.make_sync().sync_schema(
            source_schema=self.scenario.source_schema,
            destination_schema=self.scenario.target_schema,
            exclude=_EXCLUDED_TABLES,
            table_prefix=_TARGET_PREFIX,
        )
        synced_names = {result.name for result in results}
        for excluded in _EXCLUDED_TABLES:
            self.assertNotIn(excluded, synced_names)

    def test_small_tables_are_synced(self):
        results = self.make_sync().sync_schema(
            source_schema=self.scenario.source_schema,
            destination_schema=self.scenario.target_schema,
            exclude=_EXCLUDED_TABLES,
            table_prefix=_TARGET_PREFIX,
        )
        synced_names = {result.name for result in results}
        self.assertIn("sync_audit", synced_names)


class _SchemaSyncRowCounts(SchemaLiveBase):
    def test_sync_audit_row_count(self):
        self.make_sync().sync_schema(
            source_schema=self.scenario.source_schema,
            destination_schema=self.scenario.target_schema,
            exclude=_EXCLUDED_TABLES,
            table_prefix=_TARGET_PREFIX,
        )
        self.assertEqual(count(self.scenario, f"{_TARGET_PREFIX}sync_audit"), 500)


class _SchemaSyncIdempotency(SchemaLiveBase):
    def test_two_runs_same_row_counts(self):
        kwargs = {
            "source_schema": self.scenario.source_schema,
            "destination_schema": self.scenario.target_schema,
            "exclude": _EXCLUDED_TABLES,
            "mode": "full_refresh",
            "table_prefix": _TARGET_PREFIX,
        }
        self.make_sync().sync_schema(**kwargs)
        counts_run1 = {
            "sync_audit": count(self.scenario, f"{_TARGET_PREFIX}sync_audit"),
        }

        self.make_sync().sync_schema(**kwargs)
        self.assertEqual(count(self.scenario, f"{_TARGET_PREFIX}sync_audit"), counts_run1["sync_audit"])


class _SchemaSyncWildcardExclude(SchemaLiveBase):
    def test_exclude_glob_skips_matching_tables(self):
        results = self.make_sync().sync_schema(
            source_schema=self.scenario.source_schema,
            destination_schema=self.scenario.target_schema,
            exclude=["customers*", "orders*", "payments*", "products*", "datatype_samples"],
            table_prefix=_TARGET_PREFIX,
        )
        synced_names = {result.name for result in results}
        for name in synced_names:
            self.assertFalse(
                name.startswith(("customers", "orders", "payments", "products")),
                f"table '{name}' should have been excluded by glob",
            )


materialize_scenario_classes(
    globals(),
    _SchemaSyncExclude,
    _SchemaSyncRowCounts,
    _SchemaSyncIdempotency,
    _SchemaSyncWildcardExclude,
)
