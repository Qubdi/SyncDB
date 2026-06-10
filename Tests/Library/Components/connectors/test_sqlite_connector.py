import unittest

from syncdb import Column, DatabaseConfig
from syncdb.connectors.sqlite import SQLiteConnector


def _mem() -> SQLiteConnector:
    connector = SQLiteConnector(DatabaseConfig(engine="sqlite", database=":memory:"))
    connector.connect()
    return connector


class SQLiteConnectorTests(unittest.TestCase):
    def test_sqlite_connector_uses_stdlib_database(self):
        connector = SQLiteConnector(DatabaseConfig(engine="sqlite", connection_string="sqlite://"))
        try:
            connector.create_table(None, "items", [Column("id", "integer", is_primary_key=True), Column("name", "text")])
            connector.insert_batch(None, "items", [{"id": 1, "name": "Ana"}], ["id", "name"])

            self.assertEqual(connector.list_tables(), ["items"])
            self.assertEqual(connector.get_primary_keys(None, "items"), ["id"])
            self.assertEqual(connector.execute_query('SELECT name FROM "items"'), [{"name": "Ana"}])
        finally:
            connector.close()

    def test_upsert_updates_existing_and_inserts_new(self):
        connector = _mem()
        try:
            connector.create_table(
                None, "t",
                [Column("id", "integer", is_primary_key=True), Column("v", "text")],
            )
            connector.insert_batch(None, "t", [{"id": 1, "v": "old"}], ["id", "v"])
            connector.upsert_batch(None, "t", [{"id": 1, "v": "new"}, {"id": 2, "v": "two"}], ["id", "v"], ["id"])
            rows = connector.execute_query('SELECT id, v FROM "t" ORDER BY id')
            self.assertEqual(rows, [{"id": 1, "v": "new"}, {"id": 2, "v": "two"}])
        finally:
            connector.close()

    def test_upsert_pk_only_uses_insert_or_replace(self):
        connector = _mem()
        try:
            connector.create_table(None, "t", [Column("id", "integer", is_primary_key=True)])
            connector.upsert_batch(None, "t", [{"id": 1}, {"id": 1}], ["id"], ["id"])
            self.assertEqual(connector.get_row_count(None, "t"), 1)
        finally:
            connector.close()

    def test_fetch_batches_streams(self):
        connector = _mem()
        try:
            connector.create_table(None, "t", [Column("id", "integer", is_primary_key=True)])
            connector.insert_batch(None, "t", [{"id": i} for i in range(5)], ["id"])
            batches = list(connector.fetch_batches(None, "t", columns=["id"], batch_size=2))
            self.assertEqual([len(b) for b in batches], [2, 2, 1])
        finally:
            connector.close()

    def test_truncate_deletes_all_rows(self):
        connector = _mem()
        try:
            connector.create_table(None, "t", [Column("id", "integer", is_primary_key=True)])
            connector.insert_batch(None, "t", [{"id": 1}, {"id": 2}], ["id"])
            connector.truncate_table(None, "t")
            self.assertEqual(connector.get_row_count(None, "t"), 0)
        finally:
            connector.close()

    def test_add_and_drop_column(self):
        connector = _mem()
        try:
            connector.create_table(None, "t", [Column("id", "integer", is_primary_key=True)])
            connector.add_column(None, "t", Column("extra", "text"))
            self.assertIn("extra", [c.name for c in connector.get_columns(None, "t")])
            connector.drop_column(None, "t", "extra")
            self.assertNotIn("extra", [c.name for c in connector.get_columns(None, "t")])
        finally:
            connector.close()

    def test_table_exists(self):
        connector = _mem()
        try:
            self.assertFalse(connector.table_exists(None, "t"))
            connector.create_table(None, "t", [Column("id", "integer", is_primary_key=True)])
            self.assertTrue(connector.table_exists(None, "t"))
        finally:
            connector.close()

    def test_execute_query_batches_streams(self):
        connector = _mem()
        try:
            connector.create_table(None, "t", [Column("id", "integer", is_primary_key=True)])
            connector.insert_batch(None, "t", [{"id": i} for i in range(3)], ["id"])
            batches = list(connector.execute_query_batches('SELECT id FROM "t"', batch_size=2))
            self.assertEqual([len(b) for b in batches], [2, 1])
        finally:
            connector.close()

    def test_memory_path_for_blank_connection_string(self):
        connector = SQLiteConnector(DatabaseConfig(engine="sqlite", connection_string="sqlite://"))
        self.assertEqual(connector._database_path(), ":memory:")

    def test_bad_connection_string_scheme_raises(self):
        connector = SQLiteConnector(DatabaseConfig(engine="sqlite", connection_string="mysql://x/y"))
        with self.assertRaises(ValueError):
            connector._database_path()


if __name__ == "__main__":
    unittest.main()
