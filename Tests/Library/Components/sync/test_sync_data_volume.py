import unittest

from syncdb import Column

from .helpers import MemoryConnector, make_sync


class SyncDataVolumeTests(unittest.TestCase):
    def _make_source(self, rows):
        return MemoryConnector(
            "mssql",
            "dbo",
            rows_by_table={("dbo", "big_table"): rows},
            columns_by_table={
                ("dbo", "big_table"): [
                    Column("id", "int", nullable=False, is_primary_key=True),
                    Column("payload", "nvarchar", char_length=100),
                ]
            },
        )

    def test_empty_source_creates_table_without_inserting_batches(self):
        source = self._make_source([])
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target, batch_size=10)

        result = sync.sync_tables({"big_table": {"source": "dbo.big_table", "destination": "public.big_table"}})[0]

        self.assertEqual(result.rows_read, 0)
        self.assertEqual(result.rows_written, 0)
        self.assertEqual(result.batches, 0)
        self.assertTrue(result.table_created)
        self.assertEqual(target.rows_by_table[("public", "big_table")], [])
        self.assertEqual(target.insert_batches, [])

    def test_single_row_source_writes_one_batch(self):
        source = self._make_source([{"id": 1, "payload": "one"}])
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target, batch_size=5000)

        result = sync.sync_tables({"big_table": {"source": "dbo.big_table", "destination": "public.big_table"}})[0]

        self.assertEqual(result.rows_read, 1)
        self.assertEqual(result.rows_written, 1)
        self.assertEqual(result.batches, 1)
        self.assertEqual([len(batch["rows"]) for batch in target.insert_batches], [1])

    def test_exact_chunk_boundary_does_not_create_empty_final_batch(self):
        rows = [{"id": value, "payload": f"row-{value}"} for value in range(1, 101)]
        source = self._make_source(rows)
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target, batch_size=25)

        result = sync.sync_tables({"big_table": {"source": "dbo.big_table", "destination": "public.big_table"}})[0]

        self.assertEqual(result.rows_written, 100)
        self.assertEqual(result.batches, 4)
        self.assertEqual([len(batch["rows"]) for batch in target.insert_batches], [25, 25, 25, 25])

    def test_remainder_chunk_writes_partial_final_batch(self):
        rows = [{"id": value, "payload": f"row-{value}"} for value in range(1, 104)]
        source = self._make_source(rows)
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target, batch_size=25)

        result = sync.sync_tables({"big_table": {"source": "dbo.big_table", "destination": "public.big_table"}})[0]

        self.assertEqual(result.rows_written, 103)
        self.assertEqual(result.batches, 5)
        self.assertEqual([len(batch["rows"]) for batch in target.insert_batches], [25, 25, 25, 25, 3])

    def test_large_bulk_sync_preserves_order_and_row_count_across_many_chunks(self):
        rows = [{"id": value, "payload": f"row-{value}"} for value in range(1, 5001)]
        source = self._make_source(rows)
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target, batch_size=777)

        result = sync.sync_tables({"big_table": {"source": "dbo.big_table", "destination": "public.big_table"}})[0]

        written = target.rows_by_table[("public", "big_table")]
        self.assertEqual(result.rows_read, 5000)
        self.assertEqual(result.rows_written, 5000)
        self.assertEqual(result.batches, 7)
        self.assertEqual([len(batch["rows"]) for batch in target.insert_batches], [777, 777, 777, 777, 777, 777, 338])
        self.assertEqual(written[0], {"id": 1, "payload": "row-1"})
        self.assertEqual(written[-1], {"id": 5000, "payload": "row-5000"})

    def test_bulk_upsert_deletes_per_chunk_before_insert(self):
        source_rows = [{"id": value, "payload": f"new-{value}"} for value in range(1, 6)]
        target_rows = [{"id": value, "payload": f"old-{value}"} for value in range(1, 6)]
        source = self._make_source(source_rows)
        target = MemoryConnector(
            "postgresql",
            "public",
            rows_by_table={("public", "big_table"): target_rows},
            columns_by_table={
                ("public", "big_table"): [
                    Column("id", "integer", nullable=False, is_primary_key=True),
                    Column("payload", "varchar", char_length=100),
                ]
            },
        )
        sync = make_sync(source, target, batch_size=2)

        result = sync.sync_tables(
            {
                "big_table": {
                    "source": "dbo.big_table",
                    "destination": "public.big_table",
                    "mode": "upsert",
                    "primary_key": ["id"],
                }
            }
        )[0]

        self.assertEqual(result.rows_written, 5)
        self.assertEqual([len(batch["rows"]) for batch in target.deleted_batches], [2, 2, 1])
        self.assertEqual(target.rows_by_table[("public", "big_table")], source_rows)


if __name__ == "__main__":
    unittest.main()
