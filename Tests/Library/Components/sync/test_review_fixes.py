"""Regression tests for the 2.1.0 reliability/security fixes.

Each test pins one of the fixed behaviors:
  - FULL_REFRESH truncate participates in the transaction (rollback restores rows)
  - APPEND delete+insert is atomic per batch outside an explicit transaction
  - watermark_comparison '>=' is honoured
  - SOFT_DELETE + filter warns
  - import_file_to_table chunks inserts by batch_size
  - parallel failures raise ParallelSyncError carrying partial results
  - delete_matching_rows uses a keys-table anti-join for large batches
  - WHERE deny-list catches parenthesised keywords and ignores string literals
"""

import csv
import tempfile
import unittest
import warnings
from pathlib import Path

from syncdb import Column, DatabaseConfig, ParallelSyncError
from syncdb.connectors.sqlite import SQLiteConnector
from syncdb.sql import validate_where_clause
from syncdb.sync.watermark import apply_watermark_filter, load_watermark

from .helpers import MemoryConnector, make_sync


def _sqlite(database: str = ":memory:") -> SQLiteConnector:
    connector = SQLiteConnector(DatabaseConfig(engine="sqlite", database=database))
    connector.connect()
    return connector


class FullRefreshTransactionTests(unittest.TestCase):
    def test_failed_full_refresh_with_transaction_keeps_existing_rows(self):
        """TRUNCATE must roll back with the rest of the sync on failure."""
        source = MemoryConnector(
            "sqlite", None,
            rows_by_table={(None, "src"): [{"id": 1, "v": "new"}]},
            columns_by_table={(None, "src"): [
                Column("id", "integer", nullable=False, is_primary_key=True),
                Column("v", "text"),
            ]},
        )
        # File-backed DB: the sync closes the connection at the end, which would
        # discard an in-memory database before we can assert on its contents.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        target = _sqlite(str(Path(tmp.name) / "t.db"))
        # Cleanups run LIFO: close the connection before the dir is removed
        # (Windows cannot delete a file with an open handle).
        self.addCleanup(target.close)
        target.create_table(None, "dst", [
            Column("id", "integer", nullable=False, is_primary_key=True),
            Column("v", "text"),
        ])
        target.insert_batch(None, "dst", [{"id": 99, "v": "old"}], ["id", "v"])

        sync = make_sync(source, target, use_transaction=True)
        # The impossible min_rows expectation fails the quality gate AFTER the
        # truncate+load but BEFORE commit, forcing a rollback.
        with self.assertRaises(ValueError):
            sync.sync_tables({
                "dst": {
                    "source": "src",
                    "destination": "dst",
                    "mode": "full_refresh",
                    "expect": {"min_rows": 10_000},
                }
            })

        target.connect()
        rows = target.execute_query('SELECT id, v FROM "dst"')
        self.assertEqual(rows, [{"id": 99, "v": "old"}],
                         "rollback must restore the pre-sync contents")


class AppendBatchAtomicityTests(unittest.TestCase):
    def test_append_failure_after_delete_does_not_lose_existing_rows(self):
        """delete+insert run in a per-batch transaction outside an outer one."""

        class FailingInsertConnector(SQLiteConnector):
            fail_inserts = True

            def insert_batch(self, schema, table, rows, columns):
                if self.fail_inserts and table == "dst":
                    raise RuntimeError("planned insert failure")
                return super().insert_batch(schema, table, rows, columns)

        source = MemoryConnector(
            "sqlite", None,
            rows_by_table={(None, "src"): [{"id": 1, "v": "new"}]},
            columns_by_table={(None, "src"): [
                Column("id", "integer", nullable=False, is_primary_key=True),
                Column("v", "text"),
            ]},
        )
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        target = FailingInsertConnector(
            DatabaseConfig(engine="sqlite", database=str(Path(tmp.name) / "t.db"))
        )
        self.addCleanup(target.close)
        target.connect()
        target.create_table(None, "dst", [
            Column("id", "integer", nullable=False, is_primary_key=True),
            Column("v", "text"),
        ])
        target.fail_inserts = False
        target.insert_batch(None, "dst", [{"id": 1, "v": "old"}], ["id", "v"])
        target.fail_inserts = True

        sync = make_sync(source, target)
        with self.assertRaises(RuntimeError):
            sync.sync_tables({
                "dst": {"source": "src", "destination": "dst", "mode": "append",
                        "primary_key": ["id"]}
            })

        target.connect()
        rows = target.execute_query('SELECT id, v FROM "dst"')
        self.assertEqual(rows, [{"id": 1, "v": "old"}],
                         "the deleted row must be restored when the insert fails")


class WatermarkComparisonTests(unittest.TestCase):
    def test_default_comparison_is_strict(self):
        where, params = apply_watermark_filter("", [], "updated_at", "2026-01-01", '"', "?")
        self.assertIn('"updated_at" > ?', where)
        self.assertEqual(params, ["2026-01-01"])

    def test_inclusive_comparison_emits_gte(self):
        where, params = apply_watermark_filter(
            "", [], "updated_at", "2026-01-01", '"', "?", comparison=">="
        )
        self.assertIn('"updated_at" >= ?', where)

    def test_invalid_comparison_rejected(self):
        with self.assertRaises(ValueError):
            apply_watermark_filter("", [], "c", "v", '"', "?", comparison="<")
        with self.assertRaises(ValueError):
            load_watermark({
                "source": "s", "destination": "d",
                "incremental_column": "c", "watermark_comparison": "like",
            })

    def test_load_watermark_passes_comparison_through(self):
        cfg = load_watermark({
            "source": "s", "destination": "d",
            "incremental_column": "c", "watermark_comparison": ">=",
        })
        self.assertEqual(cfg["comparison"], ">=")


class SoftDeleteFilterWarningTests(unittest.TestCase):
    def test_soft_delete_with_filter_warns(self):
        source = MemoryConnector(
            "sqlite", None,
            rows_by_table={(None, "src"): [{"id": 1, "v": "a"}]},
            columns_by_table={(None, "src"): [
                Column("id", "integer", nullable=False, is_primary_key=True),
                Column("v", "text"),
            ]},
        )
        target = MemoryConnector("sqlite", None)
        sync = make_sync(source, target)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            sync.sync_tables({
                "src": {"source": "src", "destination": "dst", "mode": "soft_delete",
                        "primary_key": ["id"], "filter": "v = 'a'"}
            })
        self.assertTrue(
            any("SOFT_DELETE" in str(w.message) and issubclass(w.category, RuntimeWarning)
                for w in caught),
            "soft_delete + filter must emit a RuntimeWarning",
        )


class ImportChunkingTests(unittest.TestCase):
    def test_import_file_inserts_in_batch_size_chunks(self):
        import tempfile
        from pathlib import Path
        target = MemoryConnector("sqlite", None)
        sync = make_sync(None, target, batch_size=10)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "data.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["id", "v"])
                writer.writeheader()
                writer.writerows({"id": i, "v": f"r{i}"} for i in range(25))
            written = sync.import_file_to_table(path, "dst")
        self.assertEqual(written, 25)
        self.assertEqual([len(b["rows"]) for b in target.insert_batches], [10, 10, 5])


class ParallelSyncErrorTests(unittest.TestCase):
    def test_parallel_failure_raises_parallel_sync_error_with_results(self):
        rows = {("dbo", t): [{"id": 1}] for t in ("t1", "t2")}
        cols = {
            ("dbo", t): [Column("id", "int", nullable=False, is_primary_key=True)]
            for t in ("t1", "t2")
        }

        class FailingTarget(MemoryConnector):
            def insert_batch(self, schema, table, batch, columns):
                if table == "t2":
                    raise RuntimeError("planned failure")
                return super().insert_batch(schema, table, batch, columns)

        source = MemoryConnector("mssql", "dbo", rows_by_table=rows, columns_by_table=cols)
        target = FailingTarget("postgresql", "public")
        sync = make_sync(source, target, max_workers=2)
        sync.source.config = DatabaseConfig(engine="mssql", connection_string="m", default_schema="dbo")
        sync.target.config = DatabaseConfig(engine="postgresql", connection_string="m", default_schema="public")
        sync._connector_factory = lambda cfg: (
            MemoryConnector("mssql", "dbo", rows_by_table=dict(rows), columns_by_table=dict(cols))
            if cfg.engine == "mssql"
            else FailingTarget("postgresql", "public")
        )

        with self.assertRaises(ParallelSyncError) as ctx:
            sync.sync_tables({
                "t1": {"source": "dbo.t1", "destination": "public.t1"},
                "t2": {"source": "dbo.t2", "destination": "public.t2"},
            })
        exc = ctx.exception
        self.assertEqual(len(exc.errors), 1)
        self.assertIsInstance(exc, RuntimeError)  # backwards compatible
        self.assertTrue(all(r.rows_written == 1 for r in exc.results))


class KeysTableDeleteTests(unittest.TestCase):
    def test_large_delete_uses_keys_table_and_removes_rows(self):
        connector = _sqlite()
        connector.create_table(None, "t", [Column("id", "integer", nullable=False, is_primary_key=True)])
        all_rows = [{"id": i} for i in range(1500)]
        connector.insert_batch(None, "t", all_rows, ["id"])

        deleted = connector.delete_matching_rows(None, "t", [{"id": i} for i in range(1200)], ["id"])

        self.assertEqual(deleted, 1200)
        self.assertEqual(connector.get_row_count(None, "t"), 300)
        # The temp keys table must be gone afterwards.
        self.assertEqual(
            [t for t in connector.list_tables() if t.startswith("__syncdb_")], []
        )

    def test_execute_update_returns_rowcount(self):
        connector = _sqlite()
        connector.create_table(None, "t", [Column("id", "integer", nullable=False, is_primary_key=True)])
        connector.insert_batch(None, "t", [{"id": 1}, {"id": 2}], ["id"])
        affected = connector.execute_update('DELETE FROM "t" WHERE id > ?', [0])
        self.assertEqual(affected, 2)


class WatermarkLockingTests(unittest.TestCase):
    def test_concurrent_saves_to_same_store_preserve_all_keys(self):
        """The cross-process/thread lock must serialise read-modify-write."""
        import threading

        from syncdb.sync.watermark import read_watermark_file, save_watermark

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = Path(tmp.name) / "wm.json"

        def writer(key: str) -> None:
            for i in range(20):
                save_watermark({"path": store, "key": key}, i)

        threads = [threading.Thread(target=writer, args=(f"k{n}",)) for n in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        values = read_watermark_file(store)
        self.assertEqual(sorted(values), ["k0", "k1", "k2", "k3"],
                         "no writer's key may be lost to an interleaved write")
        self.assertTrue(all(v == 19 for v in values.values()))
        self.assertTrue(store.with_suffix(store.suffix + ".lock").exists())


class StreamingImportTests(unittest.TestCase):
    def _write_csv(self, path: Path, count: int) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["id", "v"])
            writer.writeheader()
            writer.writerows({"id": i, "v": f"r{i}"} for i in range(count))

    def test_read_streaming_csv_yields_batches(self):
        from syncdb import FileTransfer

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "data.csv"
        self._write_csv(path, 25)
        batches = list(FileTransfer().read_streaming(path, batch_size=10))
        self.assertEqual([len(b) for b in batches], [10, 10, 5])
        self.assertEqual(batches[0][0], {"id": "0", "v": "r0"})

    def test_read_streaming_excel_yields_batches(self):
        from syncdb import FileTransfer

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "data.xlsx"
        transfer = FileTransfer()
        transfer.write([{"id": i, "v": f"r{i}"} for i in range(25)], path)
        batches = list(transfer.read_streaming(path, batch_size=10))
        self.assertEqual([len(b) for b in batches], [10, 10, 5])
        self.assertEqual(batches[0][0], {"id": 0, "v": "r0"})

    def test_read_streaming_parquet_yields_batches(self):
        from syncdb import FileTransfer

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "data.parquet"
        transfer = FileTransfer()
        transfer.write([{"id": i, "v": f"r{i}"} for i in range(25)], path)
        batches = list(transfer.read_streaming(path, batch_size=10))
        self.assertEqual(sum(len(b) for b in batches), 25)
        self.assertTrue(all(len(b) <= 10 for b in batches))

    def test_import_empty_csv_into_missing_table_raises(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "empty.csv"
        self._write_csv(path, 0)
        target = MemoryConnector("sqlite", None)
        sync = make_sync(None, target)
        with self.assertRaises(ValueError):
            sync.import_file_to_table(path, "dst")

    def test_import_empty_csv_into_existing_table_returns_zero(self):
        from syncdb import Column as Col

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "empty.csv"
        self._write_csv(path, 0)
        target = MemoryConnector(
            "sqlite", None,
            columns_by_table={(None, "dst"): [Col("id", "text")]},
        )
        sync = make_sync(None, target)
        self.assertEqual(sync.import_file_to_table(path, "dst"), 0)


class DatabaseWatermarkStoreTests(unittest.TestCase):
    def _seed_source(self, tmp: Path) -> SQLiteConnector:
        source = _sqlite(str(tmp / "src.db"))
        source.create_table(None, "src", [
            Column("id", "integer", nullable=False, is_primary_key=True),
            Column("updated_at", "text"),
        ])
        source.insert_batch(
            None, "src",
            [{"id": 1, "updated_at": "2026-01-01"}, {"id": 2, "updated_at": "2026-01-02"}],
            ["id", "updated_at"],
        )
        return source

    def test_watermark_persists_in_target_table_and_filters_second_run(self):
        tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tmp_dir.cleanup)
        tmp = Path(tmp_dir.name)
        spec = {
            "src": {
                "source": "src", "destination": "dst", "mode": "append",
                "primary_key": ["id"],
                "incremental_column": "updated_at",
                "watermark_storage": "database",
            }
        }

        source = self._seed_source(tmp)
        self.addCleanup(source.close)
        target = _sqlite(str(tmp / "dst.db"))
        self.addCleanup(target.close)

        first = make_sync(source, target).sync_tables(spec)[0]
        self.assertEqual(first.rows_written, 2)

        # The watermark must live in a table on the target, not a local file.
        target.connect()
        wm_rows = target.execute_query('SELECT wm_key, wm_value FROM "__syncdb_watermarks"')
        self.assertEqual(len(wm_rows), 1)
        self.assertIn("2026-01-02", wm_rows[0]["wm_value"])
        target.close()

        # Second run: nothing newer than the stored watermark → zero rows read.
        source.connect()
        second = make_sync(source, target).sync_tables(spec)[0]
        self.assertEqual(second.rows_read, 0)

    def test_database_storage_requires_target(self):
        from syncdb.sync.watermark import load_watermark

        with self.assertRaises(ValueError):
            load_watermark({
                "source": "s", "destination": "d",
                "incremental_column": "c", "watermark_storage": "database",
            })

    def test_unknown_storage_rejected(self):
        from syncdb.sync.watermark import load_watermark

        with self.assertRaises(ValueError):
            load_watermark({
                "source": "s", "destination": "d",
                "incremental_column": "c", "watermark_storage": "redis",
            })


class WhereClauseHardeningTests(unittest.TestCase):
    def test_parenthesised_keywords_blocked(self):
        for clause in (
            "id IN(SELECT password FROM users)",
            "id=1 UNION(SELECT 1)",
            "id=1\nUNION\nSELECT 1",
            "a=1/**/OR/**/1=1",
        ):
            with self.assertRaises(ValueError, msg=clause):
                validate_where_clause(clause)

    def test_string_literals_are_not_false_positives(self):
        for clause in (
            "hex_val = '0x1f'",
            "note = 'it''s fine' AND selected_id = 5",
            "status = 'active' AND deleted_flag = 0",
        ):
            self.assertEqual(validate_where_clause(clause), clause)

    def test_unbalanced_or_escaped_quotes_rejected(self):
        with self.assertRaises(ValueError):
            validate_where_clause("x = '")
        with self.assertRaises(ValueError):
            validate_where_clause("name = 'it\\'s'")


if __name__ == "__main__":
    unittest.main()
