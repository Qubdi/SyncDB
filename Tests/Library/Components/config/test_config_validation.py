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
        self.assertEqual(DatabaseConfig(engine="sqlite3", database="local.db").engine, "sqlite")

    def test_default_ports_are_set_per_engine(self):
        self.assertEqual(DatabaseConfig(engine="mssql", connection_string="x").port, 1433)
        self.assertEqual(DatabaseConfig(engine="postgresql", connection_string="x").port, 5432)
        self.assertEqual(DatabaseConfig(engine="mysql", connection_string="x").port, 3306)
        self.assertIsNone(DatabaseConfig(engine="sqlite", database="local.db").port)

    def test_requires_connection_details(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="mysql", host="localhost", database="syncdb")

    def test_repr_never_exposes_credentials(self):
        # password is a dedicated field, but a DSN routinely embeds one too —
        # both must stay out of repr/tracebacks/logs.
        config = DatabaseConfig(
            engine="postgresql",
            connection_string="postgresql://etl_user:S3cretPW@db.example.com/prod",
        )
        self.assertNotIn("S3cretPW", repr(config))
        config = DatabaseConfig(
            engine="postgresql", host="h", database="d", user="u", password="S3cretPW"
        )
        self.assertNotIn("S3cretPW", repr(config))


if __name__ == "__main__":
    unittest.main()
