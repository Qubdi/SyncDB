import unittest

from syncdb import DatabaseConfig
from syncdb.connectors.mysql import MySQLConnector


class DatabaseConfigTests(unittest.TestCase):
    def test_accepts_connection_string_and_normalizes_engine(self):
        config = DatabaseConfig(engine="postgres", connection_string="postgresql://example")

        self.assertEqual(config.engine, "postgresql")
        self.assertEqual(config.port, 5432)
        self.assertEqual(config.default_schema, "public")

    def test_requires_connection_details(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="mysql", host="localhost", database="syncdb")

    def test_rejects_unknown_engine(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="sqlite", connection_string="sqlite://")

    def test_mysql_connector_parses_url_connection_string(self):
        config = DatabaseConfig(engine="mysql", connection_string="mysql://admin:secret@localhost:13306/syncdb_test")
        connector = MySQLConnector(config)

        kwargs = connector._connection_kwargs()

        self.assertEqual(kwargs["host"], "localhost")
        self.assertEqual(kwargs["port"], 13306)
        self.assertEqual(kwargs["database"], "syncdb_test")
        self.assertEqual(kwargs["user"], "admin")
        self.assertEqual(kwargs["password"], "secret")


if __name__ == "__main__":
    unittest.main()
