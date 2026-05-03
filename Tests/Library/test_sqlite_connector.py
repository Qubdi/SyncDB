import unittest

from syncdb import Column, DatabaseConfig
from syncdb.connectors.sqlite import SQLiteConnector


class SQLiteConnectorTests(unittest.TestCase):
    def test_sqlite_connector_uses_stdlib_database(self):
        connector = SQLiteConnector(DatabaseConfig(engine="sqlite", connection_string="sqlite://"))
        connector.create_table(None, "items", [Column("id", "integer", is_primary_key=True), Column("name", "text")])
        connector.insert_batch(None, "items", [{"id": 1, "name": "Ana"}], ["id", "name"])

        self.assertEqual(connector.list_tables(), ["items"])
        self.assertEqual(connector.get_primary_keys(None, "items"), ["id"])
        self.assertEqual(connector.execute_query('SELECT name FROM "items"'), [{"name": "Ana"}])


if __name__ == "__main__":
    unittest.main()
