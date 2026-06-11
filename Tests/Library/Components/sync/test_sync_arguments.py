import json
import tempfile
import unittest
import warnings
from pathlib import Path

from syncdb import Column, ProgressMode, SyncDB

from .helpers import MemoryConnector, make_sync


class SyncArgumentTests(unittest.TestCase):
    def test_legacy_connector_kwargs_emit_deprecation_warning(self):
        source = MemoryConnector("mssql", "dbo")
        target = MemoryConnector("postgresql", "public")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            SyncDB(
                source_connector=source,
                target_connector=target,
                progress_mode=ProgressMode.NONE,
                verbose=None,
            )
        messages = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertTrue(any("source_connector is deprecated" in m for m in messages))
        self.assertTrue(any("target_connector is deprecated" in m for m in messages))

    def test_constructor_rejects_invalid_batch_and_retry_arguments(self):
        source = MemoryConnector("mssql", "dbo")
        target = MemoryConnector("postgresql", "public")

        invalid_cases = [
            {"batch_size": 0},
            {"batch_size": "0%"},
            {"batch_size": "101%"},
            {"batch_size": "ten"},
            {"retry_count": -1},
            {"retry_delay_seconds": -0.1},
        ]
        for kwargs in invalid_cases:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                make_sync(source, target, **kwargs)

    def test_missing_required_connectors_raise_clear_errors(self):
        sync = SyncDB(progress_mode=ProgressMode.NONE, verbose=None)

        with self.assertRaisesRegex(ValueError, "source and target"):
            sync.sync_tables({"items": {"source": "dbo.items", "destination": "public.items"}})
        with self.assertRaisesRegex(ValueError, "source connector"):
            sync.export_query_to_file("SELECT 1", "out.csv")
        with self.assertRaisesRegex(ValueError, "target connector"):
            sync.import_file_to_table("in.csv", "public.items")

    def test_table_spec_requires_source_and_destination(self):
        source = MemoryConnector("mssql", "dbo")
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target)

        with self.assertRaisesRegex(ValueError, "must include source and destination"):
            sync.sync_tables({"bad": {"source": "dbo.items"}})

    def test_constructor_closes_connections_after_successful_sync(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "items"): [{"id": 1}]},
            columns_by_table={("dbo", "items"): [Column("id", "int")]},
        )
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target)

        sync.sync_tables({"items": {"source": "dbo.items", "destination": "public.items"}})

        self.assertTrue(source.connected)
        self.assertTrue(target.connected)
        self.assertTrue(source.closed)
        self.assertTrue(target.closed)

    def test_percentage_batch_size_resolves_against_source_row_count(self):
        rows = [{"id": value} for value in range(10)]
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "items"): rows},
            columns_by_table={("dbo", "items"): [Column("id", "int")]},
        )
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target, batch_size="30%")

        result = sync.sync_tables({"items": {"source": "dbo.items", "destination": "public.items"}})[0]

        self.assertEqual(result.rows_written, 10)
        self.assertEqual(result.batches, 4)
        self.assertEqual(source.fetch_calls[0]["batch_size"], 3)
        self.assertEqual([len(batch["rows"]) for batch in target.insert_batches], [3, 3, 3, 1])

    def test_order_by_filter_and_params_are_forwarded_to_connector(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "items"): [{"id": 1}]},
            columns_by_table={("dbo", "items"): [Column("id", "int")]},
        )
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target)

        sync.sync_tables(
            {
                "items": {
                    "source": "dbo.items",
                    "destination": "public.items",
                    "filter": {"where": "id > ?", "params": [0]},
                    "order_by": ["id"],
                }
            }
        )

        call = source.fetch_calls[0]
        self.assertEqual(call["where"], " WHERE id > ? ")
        self.assertEqual(call["params"], [0])
        self.assertEqual(call["order_by"], " ORDER BY [id]")

    def test_on_batch_callback_receives_running_result(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "items"): [{"id": 1}, {"id": 2}, {"id": 3}]},
            columns_by_table={("dbo", "items"): [Column("id", "int")]},
        )
        target = MemoryConnector("postgresql", "public")
        seen = []
        sync = make_sync(source, target, batch_size=2)

        sync.sync_tables(
            {
                "items": {
                    "source": "dbo.items",
                    "destination": "public.items",
                    "on_batch": lambda result: seen.append((result.rows_written, result.batches)),
                }
            }
        )

        self.assertEqual(seen, [(2, 1), (3, 2)])

    def test_retry_count_retries_transient_write_failures(self):
        source = MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "items"): [{"id": 1}]},
            columns_by_table={("dbo", "items"): [Column("id", "int")]},
        )
        target = MemoryConnector("postgresql", "public")
        target.fail_insert_times = 1
        sync = make_sync(source, target, retry_count=1, retry_delay_seconds=0)

        result = sync.sync_tables({"items": {"source": "dbo.items", "destination": "public.items"}})[0]

        self.assertEqual(result.rows_written, 1)
        self.assertEqual(target.rows_by_table[("public", "items")], [{"id": 1}])

    def test_json_config_file_runs_table_sync(self):
        config = {
            "source": {"engine": "sqlite", "database": ":memory:"},
            "target": {"engine": "sqlite", "database": ":memory:"},
            "settings": {"progress_mode": "none", "verbose": "none"},
            "tables": {},
        }

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w", encoding="utf-8", delete=False) as handle:
            json.dump(config, handle)
            path = Path(handle.name)

        try:
            results = SyncDB.run_config_file(path)
        finally:
            path.unlink(missing_ok=True)

        self.assertEqual(results, [])

    def test_unsupported_config_file_extension_is_rejected(self):
        with tempfile.NamedTemporaryFile(suffix=".txt", mode="w", encoding="utf-8", delete=False) as handle:
            handle.write("{}")
            path = Path(handle.name)

        try:
            with self.assertRaisesRegex(ValueError, "json, .yaml, or .yml"):
                SyncDB.run_config_file(path)
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
