"""Live tests: schema sync — PostgreSQL public schema → MySQL.

sync_schema() discovers every table in the source schema and syncs them all
in one call.  These tests use exclude to skip the large seed tables
(customers, orders, payments) so runs finish quickly while still exercising
the full schema-discovery and multi-table orchestration path.

Only small seed tables are used:
  sync_audit    500 rows
  datatype_samples  25 rows

Run:
    pytest "Tests/Library/PGSQL to MySQL/Tables/Schema/test_schema_sync.py" -v
"""
from __future__ import annotations

import unittest

from syncdb import DatabaseConfig, ProgressMode, SyncDB
from syncdb.connections import create_connector

PG = DatabaseConfig(
    engine="postgresql",
    connection_string="postgresql://admin:admin@localhost:15432/syncdb_test",
)
MY = DatabaseConfig(
    engine="mysql",
    host="localhost",
    port=13306,
    database="syncdb_test",
    user="admin",
    password="admin",
)

SKIP_MSG = "Docker DB stack not running — start with: docker compose up -d --build"

_LARGE_TABLES = ["customers", "products", "orders", "payments"]


def _databases_reachable() -> bool:
    for cfg in (PG, MY):
        try:
            c = create_connector(cfg)
            c.connect()
            c.close()
        except Exception:
            return False
    return True


LIVE = _databases_reachable()


def _count(table: str) -> int:
    c = create_connector(MY)
    c.connect()
    try:
        return int(c.execute_query(f"SELECT COUNT(*) AS n FROM `{table}`")[0]["n"])
    finally:
        c.close()


def _drop_schema_tables(*extra: str) -> None:
    c = create_connector(MY)
    c.connect()
    try:
        rows = c.execute_query(
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = 'syncdb_test'"
        )
        all_tables = [r["table_name"] for r in rows] + list(extra)
        for t in all_tables:
            c.execute_query(f"DROP TABLE IF EXISTS `{t}`")
    finally:
        c.close()


def _make_sync(**kwargs) -> SyncDB:
    return SyncDB(
        source=PG,
        target=MY,
        progress_mode=ProgressMode.NONE,
        verbose=None,
        **kwargs,
    )


class SchemaLiveBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not LIVE:
            raise unittest.SkipTest(SKIP_MSG)

    def setUp(self):
        _drop_schema_tables()

    def tearDown(self):
        _drop_schema_tables()


class TestSchemaSyncExclude(SchemaLiveBase):
    """sync_schema respects the exclude list."""

    def test_excluded_tables_not_created_in_mysql(self):
        results = _make_sync().sync_schema(
            source_schema="public",
            destination_schema=None,
            exclude=_LARGE_TABLES,
        )
        synced_names = {r.name for r in results}
        for large in _LARGE_TABLES:
            self.assertNotIn(large, synced_names)

    def test_small_tables_are_synced(self):
        results = _make_sync().sync_schema(
            source_schema="public",
            destination_schema=None,
            exclude=_LARGE_TABLES,
        )
        synced_names = {r.name for r in results}
        self.assertIn("sync_audit", synced_names)
        self.assertIn("datatype_samples", synced_names)


class TestSchemaSyncRowCounts(SchemaLiveBase):
    """Row counts in MySQL must match PostgreSQL seed counts for small tables."""

    def test_sync_audit_and_datatype_samples_row_counts(self):
        _make_sync().sync_schema(
            source_schema="public",
            destination_schema=None,
            exclude=_LARGE_TABLES,
        )
        self.assertEqual(_count("sync_audit"), 500)
        self.assertEqual(_count("datatype_samples"), 25)


class TestSchemaSyncIdempotency(SchemaLiveBase):
    """Running sync_schema twice must produce the same result."""

    def test_two_runs_same_row_counts(self):
        kwargs = {
            "source_schema": "public",
            "destination_schema": None,
            "exclude": _LARGE_TABLES,
            "mode": "full_refresh",
        }
        _make_sync().sync_schema(**kwargs)
        counts_run1 = {
            "sync_audit": _count("sync_audit"),
            "datatype_samples": _count("datatype_samples"),
        }

        _make_sync().sync_schema(**kwargs)
        self.assertEqual(_count("sync_audit"), counts_run1["sync_audit"])
        self.assertEqual(_count("datatype_samples"), counts_run1["datatype_samples"])


class TestSchemaSyncWildcardExclude(SchemaLiveBase):
    """Glob patterns in exclude filter tables by name prefix/suffix."""

    def test_exclude_glob_skips_matching_tables(self):
        results = _make_sync().sync_schema(
            source_schema="public",
            destination_schema=None,
            exclude=["customers*", "orders*", "payments*", "products*"],
        )
        synced_names = {r.name for r in results}
        for name in synced_names:
            self.assertFalse(
                name.startswith(("customers", "orders", "payments", "products")),
                f"table '{name}' should have been excluded by glob",
            )


if __name__ == "__main__":
    unittest.main()
