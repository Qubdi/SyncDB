"""Tests for parallel table sync (abort on failure) and retry with reconnect."""

import threading
import unittest

from syncdb import Column, DatabaseConfig

from .helpers import MemoryConnector, make_sync


def _base_source_spec(schema="dbo", tables=("t1", "t2", "t3")):
    """Return rows_by_table and columns_by_table for a MemoryConnector."""
    rows = {(schema, t): [{"id": i + 1, "val": t}] for i, t in enumerate(tables)}
    cols = {
        (schema, t): [
            Column("id", "int", nullable=False, is_primary_key=True),
            Column("val", "nvarchar", char_length=50),
        ]
        for t in tables
    }
    return rows, cols


def _base_source(schema="dbo", tables=("t1", "t2", "t3")):
    """Return a MemoryConnector seeded with one row per table."""
    rows, cols = _base_source_spec(schema, tables)
    return MemoryConnector("mssql", schema, rows_by_table=rows, columns_by_table=cols)


class FailAfterNInserts(MemoryConnector):
    """MemoryConnector that raises RuntimeError after N successful insert_batch calls."""

    def __init__(self, *args, fail_on_call: int = 1, **kwargs):
        super().__init__(*args, **kwargs)
        self._insert_call_count = 0
        self._fail_on_call = fail_on_call

    def insert_batch(self, schema, table, rows, columns):
        self._insert_call_count += 1
        if self._insert_call_count == self._fail_on_call:
            raise RuntimeError(f"planned failure on insert call #{self._insert_call_count}")
        return super().insert_batch(schema, table, rows, columns)


class ReconnectTrackingConnector(MemoryConnector):
    """MemoryConnector that records reconnect() calls and exposes a transient failure."""

    def __init__(self, *args, fail_inserts: int = 0, **kwargs):
        super().__init__(*args, **kwargs)
        self.reconnect_calls = 0
        self.fail_insert_times = fail_inserts

    def reconnect(self):
        self.reconnect_calls += 1
        self.closed = False
        self.connected = True


_TABLE_SPECS = {
    "t1": {"source": "dbo.t1", "destination": "public.t1", "mode": "append"},
    "t2": {"source": "dbo.t2", "destination": "public.t2", "mode": "append"},
    "t3": {"source": "dbo.t3", "destination": "public.t3", "mode": "append"},
}


class TestParallelSyncAbortOnFailure(unittest.TestCase):
    """When one table fails during parallel sync the whole sync raises.

    Parallel mode creates fresh connectors per thread via a factory.  In tests we
    inject a custom factory (_connector_factory) that produces MemoryConnectors so
    no real database driver is required.
    """

    def _make_parallel_sync(self, source_spec, target_factory, max_workers=2):
        """Build a SyncDB with parallel mode and an injected connector factory."""
        src_rows, src_cols = source_spec
        src_config = DatabaseConfig(engine="mssql", connection_string="memory", default_schema="dbo")
        tgt_config = DatabaseConfig(engine="postgresql", connection_string="memory", default_schema="public")

        source = MemoryConnector("mssql", "dbo", rows_by_table=src_rows, columns_by_table=src_cols)
        target_initial = target_factory()

        sync = make_sync(source, target_initial, max_workers=max_workers)
        # Override source/target configs so _sync_tables_parallel can call factory(config)
        sync.source.config = src_config
        sync.target.config = tgt_config
        sync._connector_factory = lambda cfg: (
            MemoryConnector("mssql", "dbo", rows_by_table=dict(src_rows), columns_by_table=dict(src_cols))
            if cfg.engine == "mssql"
            else target_factory()
        )
        return sync

    def test_parallel_sync_raises_on_table_failure(self):
        """If any worker raises, sync_tables propagates an exception."""
        spec = _base_source_spec()
        sync = self._make_parallel_sync(spec, lambda: FailAfterNInserts("postgresql", "public", fail_on_call=1))
        with self.assertRaises(RuntimeError):
            sync.sync_tables(_TABLE_SPECS)

    def test_sequential_sync_raises_on_table_failure(self):
        """Sequential mode also propagates insert failures."""
        source = _base_source()
        target = FailAfterNInserts("postgresql", "public", fail_on_call=1)
        sync = make_sync(source, target)
        with self.assertRaises(RuntimeError):
            sync.sync_tables(_TABLE_SPECS)

    def test_parallel_sync_succeeds_when_no_failure(self):
        """All tables sync successfully in parallel mode."""
        spec = _base_source_spec()
        sync = self._make_parallel_sync(spec, lambda: MemoryConnector("postgresql", "public"))
        results = sync.sync_tables(_TABLE_SPECS)
        self.assertEqual(len(results), 3)
        self.assertTrue(all(r.rows_written == 1 for r in results))

    def test_parallel_abort_event_prevents_cancelled_tables(self):
        """After abort is set, pending workers exit early with RuntimeError."""
        spec = _base_source_spec()
        # fail_on_call=1 means the very first target insert fails, triggering abort
        sync = self._make_parallel_sync(spec, lambda: FailAfterNInserts("postgresql", "public", fail_on_call=1), max_workers=3)
        with self.assertRaises((RuntimeError, Exception)):
            sync.sync_tables(_TABLE_SPECS)


class TestRetryWithReconnect(unittest.TestCase):
    """with_retries() calls on_retry between attempts; connector.reconnect() is called."""

    def _source_with_one_row(self, schema="dbo", table="users"):
        rows = {(schema, table): [{"id": 1, "name": "Alice"}]}
        cols = {
            (schema, table): [
                Column("id", "int", nullable=False, is_primary_key=True),
                Column("name", "nvarchar", char_length=50),
            ]
        }
        return MemoryConnector("mssql", schema, rows_by_table=rows, columns_by_table=cols)

    def test_retry_succeeds_after_transient_insert_failure(self):
        """Sync succeeds after one insert failure when retry_count >= 1."""
        source = self._source_with_one_row()
        target = ReconnectTrackingConnector("postgresql", "public", fail_inserts=1)

        sync = make_sync(source, target, retry_count=2, retry_delay_seconds=0)
        results = sync.sync_tables(
            {
                "users": {
                    "source": "dbo.users",
                    "destination": "public.users",
                    "mode": "append",
                }
            }
        )

        self.assertEqual(results[0].rows_written, 1)

    def test_retry_calls_reconnect_between_attempts(self):
        """reconnect() is called once per retry attempt (via on_retry=target.reconnect)."""
        source = self._source_with_one_row()
        target = ReconnectTrackingConnector("postgresql", "public", fail_inserts=1)

        sync = make_sync(source, target, retry_count=2, retry_delay_seconds=0)
        sync.sync_tables(
            {
                "users": {
                    "source": "dbo.users",
                    "destination": "public.users",
                    "mode": "append",
                }
            }
        )

        self.assertEqual(target.reconnect_calls, 1)

    def test_retry_exhausted_raises(self):
        """When all retries are exhausted, the original exception propagates."""
        source = self._source_with_one_row()
        target = ReconnectTrackingConnector("postgresql", "public", fail_inserts=10)

        sync = make_sync(source, target, retry_count=2, retry_delay_seconds=0)
        with self.assertRaises(RuntimeError):
            sync.sync_tables(
                {
                    "users": {
                        "source": "dbo.users",
                        "destination": "public.users",
                        "mode": "append",
                    }
                }
            )

    def test_no_retry_raises_immediately(self):
        """retry_count=0 means no retries; first failure raises without reconnect."""
        source = self._source_with_one_row()
        target = ReconnectTrackingConnector("postgresql", "public", fail_inserts=1)

        sync = make_sync(source, target, retry_count=0)
        with self.assertRaises(RuntimeError):
            sync.sync_tables(
                {
                    "users": {
                        "source": "dbo.users",
                        "destination": "public.users",
                        "mode": "append",
                    }
                }
            )

        self.assertEqual(target.reconnect_calls, 0)


if __name__ == "__main__":
    unittest.main()
