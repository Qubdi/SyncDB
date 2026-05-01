import unittest

from syncdb import DatabaseConfig
from syncdb.connectors.mysql import MySQLConnector


class DatabaseConfigTests(unittest.TestCase):
    def test_accepts_connection_string_and_normalizes_engine(self):
        config = DatabaseConfig(engine="postgres", connection_string="postgresql://example")

        self.assertEqual(config.engine, "postgresql")
        self.assertEqual(config.port, 5432)
        self.assertEqual(config.default_schema, "public")

    def test_engine_aliases_resolve_to_canonical_values(self):
        self.assertEqual(DatabaseConfig(engine="sqlserver", connection_string="x").engine, "mssql")
        self.assertEqual(DatabaseConfig(engine="sql_server", connection_string="x").engine, "mssql")
        self.assertEqual(DatabaseConfig(engine="pg", connection_string="x").engine, "postgresql")
        self.assertEqual(DatabaseConfig(engine="mysql", connection_string="x").engine, "mysql")

    def test_default_ports_are_set_per_engine(self):
        self.assertEqual(DatabaseConfig(engine="mssql", connection_string="x").port, 1433)
        self.assertEqual(DatabaseConfig(engine="postgresql", connection_string="x").port, 5432)
        self.assertEqual(DatabaseConfig(engine="mysql", connection_string="x").port, 3306)

    def test_requires_connection_details(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="mysql", host="localhost", database="syncdb")

    def test_rejects_unknown_engine(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="sqlite", connection_string="sqlite://")

    def test_rejects_invalid_pool_range(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="mssql", connection_string="x", pool_min=5, pool_max=2)

    def test_rejects_zero_connect_timeout(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="mssql", connection_string="x", connect_timeout=0)

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
