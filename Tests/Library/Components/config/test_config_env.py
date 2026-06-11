import unittest
from unittest import mock

from syncdb import DatabaseConfig
from syncdb.config import Engine, normalize_engine


class FromEnvTests(unittest.TestCase):
    def test_from_env_reads_discrete_fields(self):
        env = {
            "SYNCDB_ENGINE": "postgresql",
            "SYNCDB_HOST": "db.example.com",
            "SYNCDB_DATABASE": "mydb",
            "SYNCDB_USER": "etl",
            "SYNCDB_PASSWORD": "secret",
            "SYNCDB_PORT": "6543",
            "SYNCDB_DEFAULT_SCHEMA": "analytics",
            "SYNCDB_CONNECT_TIMEOUT": "45",
            "SYNCDB_QUERY_TIMEOUT": "120",
        }
        with mock.patch.dict("os.environ", env, clear=True):
            config = DatabaseConfig.from_env()
        self.assertEqual(config.engine, "postgresql")
        self.assertEqual(config.host, "db.example.com")
        self.assertEqual(config.port, 6543)
        self.assertEqual(config.default_schema, "analytics")
        self.assertEqual(config.connect_timeout, 45)
        self.assertEqual(config.query_timeout, 120)

    def test_from_env_connection_string_takes_precedence(self):
        env = {
            "SYNCDB_ENGINE": "mysql",
            "SYNCDB_CONNECTION_STRING": "mysql://u:p@h/db",
            "SYNCDB_HOST": "ignored",
        }
        with mock.patch.dict("os.environ", env, clear=True):
            config = DatabaseConfig.from_env()
        self.assertEqual(config.connection_string, "mysql://u:p@h/db")
        self.assertIsNone(config.host)

    def test_from_env_custom_prefix(self):
        with mock.patch.dict("os.environ", {"WH_ENGINE": "sqlite", "WH_DATABASE": "x.db"}, clear=True):
            config = DatabaseConfig.from_env(prefix="WH")
        self.assertEqual(config.engine, "sqlite")

    def test_from_env_requires_engine(self):
        with mock.patch.dict("os.environ", {}, clear=True), self.assertRaises(ValueError):
            DatabaseConfig.from_env()


class ConnectionKwargsTests(unittest.TestCase):
    def test_strips_none_and_merges_options(self):
        config = DatabaseConfig(
            engine="postgresql", host="h", database="d", user="u", password="p",
            options={"sslmode": "require"},
        )
        kwargs = config.as_connection_kwargs()
        self.assertEqual(kwargs["host"], "h")
        self.assertEqual(kwargs["sslmode"], "require")
        self.assertNotIn("default_schema", kwargs)
        self.assertNotIn("engine", kwargs)

    def test_options_override_defaults(self):
        config = DatabaseConfig(
            engine="postgresql", host="h", database="d", user="u", password="p",
            connect_timeout=30, options={"connect_timeout": 5},
        )
        self.assertEqual(config.as_connection_kwargs()["connect_timeout"], 5)


class ValidationTests(unittest.TestCase):
    def test_rejects_nonpositive_connect_timeout(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="sqlite", database="x.db", connect_timeout=0)

    def test_rejects_nonpositive_query_timeout(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="sqlite", database="x.db", query_timeout=0)

    def test_sqlite_requires_database(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="sqlite")

    def test_unsupported_engine_raises(self):
        with self.assertRaises(ValueError):
            DatabaseConfig(engine="oracle", connection_string="x")

    def test_normalized_engine_property(self):
        config = DatabaseConfig(engine="pg", connection_string="x")
        self.assertEqual(config.normalized_engine, Engine.POSTGRESQL)

    def test_normalize_engine_passthrough_enum(self):
        self.assertEqual(normalize_engine(Engine.MYSQL), Engine.MYSQL)


if __name__ == "__main__":
    unittest.main()
