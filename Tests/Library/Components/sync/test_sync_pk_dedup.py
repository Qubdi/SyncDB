"""Duplicate primary keys within one batch must be de-duplicated before writing.

The engines reject duplicate keys inside a single statement: PostgreSQL
ON CONFLICT raises "command cannot affect row a second time", MSSQL MERGE
errors on duplicate source rows, and the APPEND delete+insert pair would hit a
PK violation on the second insert.  _write_batch keeps the LAST occurrence so
CDC-style feeds (updates ordered oldest-to-newest) land their newest version.
"""

import unittest

from syncdb import Column

from .helpers import MemoryConnector, make_sync


def _source_with_duplicate_ids(schema="dbo", table="events"):
    rows = {
        (schema, table): [
            {"id": 1, "v": "old"},
            {"id": 2, "v": "b"},
            {"id": 1, "v": "new"},
        ]
    }
    cols = {
        (schema, table): [
            Column("id", "int", nullable=False, is_primary_key=True),
            Column("v", "nvarchar", char_length=50),
        ]
    }
    return MemoryConnector("mssql", schema, rows_by_table=rows, columns_by_table=cols)


class TestDuplicatePkDedup(unittest.TestCase):
    def _sync_one(self, mode):
        source = _source_with_duplicate_ids()
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target)
        results = sync.sync_tables(
            {
                "events": {
                    "source": "dbo.events",
                    "destination": "public.events",
                    "mode": mode,
                    "primary_key": ["id"],
                }
            }
        )
        return results[0], target

    def test_upsert_batch_receives_unique_keys_keep_last(self):
        result, target = self._sync_one("upsert")
        # MemoryConnector inherits the base delete+insert upsert; the insert
        # must only ever see one row per PK, carrying the LAST occurrence.
        written = target.insert_batches[-1]["rows"]
        self.assertEqual(len(written), 2)
        by_id = {row["id"]: row["v"] for row in written}
        self.assertEqual(by_id, {1: "new", 2: "b"})
        self.assertEqual(result.rows_written, 2)
        self.assertEqual(result.rows_read, 3)

    def test_append_mode_dedupes_before_delete_insert(self):
        result, target = self._sync_one("append")
        written = target.insert_batches[-1]["rows"]
        self.assertEqual(len(written), 2)
        self.assertEqual({row["id"]: row["v"] for row in written}, {1: "new", 2: "b"})
        # The delete that precedes the insert also operates on the deduped batch.
        self.assertEqual(len(target.deleted_batches[-1]["rows"]), 2)
        self.assertEqual(result.rows_written, 2)

    def test_insert_only_mode_keeps_duplicates(self):
        # No PK-driven statement is involved, so every source row passes through.
        result, target = self._sync_one("insert_only")
        self.assertEqual(len(target.insert_batches[-1]["rows"]), 3)
        self.assertEqual(result.rows_written, 3)


if __name__ == "__main__":
    unittest.main()
