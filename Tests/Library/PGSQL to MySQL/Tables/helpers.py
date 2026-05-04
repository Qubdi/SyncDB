"""Shared helpers for live PostgreSQL → MySQL table-sync tests."""
from __future__ import annotations

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


def databases_reachable() -> bool:
    for cfg in (PG, MY):
        try:
            c = create_connector(cfg)
            c.connect()
            c.close()
        except Exception:
            return False
    return True


LIVE = databases_reachable()


# ── MySQL query helpers ────────────────────────────────────────────────────────

def drop(*tables: str) -> None:
    c = create_connector(MY)
    c.connect()
    try:
        for t in tables:
            c.execute_query(f"DROP TABLE IF EXISTS `{t}`")
    finally:
        c.close()


def count(table: str) -> int:
    c = create_connector(MY)
    c.connect()
    try:
        return int(c.execute_query(f"SELECT COUNT(*) AS n FROM `{table}`")[0]["n"])
    finally:
        c.close()


def fetch_rows(table: str, order_by: str = "customer_id", limit: int = 5) -> list[dict]:
    c = create_connector(MY)
    c.connect()
    try:
        return c.execute_query(
            f"SELECT * FROM `{table}` ORDER BY `{order_by}` LIMIT {limit}"
        )
    finally:
        c.close()


def column_names(table: str) -> list[str]:
    c = create_connector(MY)
    c.connect()
    try:
        rows = c.execute_query(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = 'syncdb_test' AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (table,),
        )
        return [r["column_name"] for r in rows]
    finally:
        c.close()


def column_type(table: str, column: str) -> str:
    c = create_connector(MY)
    c.connect()
    try:
        rows = c.execute_query(
            "SELECT COLUMN_TYPE FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = 'syncdb_test' AND TABLE_NAME = %s "
            "AND COLUMN_NAME = %s",
            (table, column),
        )
        return rows[0]["column_type"] if rows else ""
    finally:
        c.close()


def make_sync(**kwargs) -> SyncDB:
    return SyncDB(
        source=PG,
        target=MY,
        progress_mode=ProgressMode.NONE,
        verbose=None,
        **kwargs,
    )


# ── Base test class ────────────────────────────────────────────────────────────

import unittest


class LiveBase(unittest.TestCase):
    """Skip the entire class when Docker DBs are not reachable; drop/restore tables."""

    tables: list[str] = []

    @classmethod
    def setUpClass(cls):
        if not LIVE:
            raise unittest.SkipTest(SKIP_MSG)

    def setUp(self):
        drop(*self.tables)

    def tearDown(self):
        drop(*self.tables)
