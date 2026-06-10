import unittest

from syncdb import Column, DatabaseConfig
from syncdb.connectors import MySQLConnector

from .fakedb import FakeConnection


def make_connector(results=None) -> MySQLConnector:
    config = DatabaseConfig(engine="mysql", host="h", database="db", user="u", password="p")
    connector = MySQLConnector(config)
    connector.connection = FakeConnection(results=results or [])
    return connector


class MySQLConnectorTests(unittest.TestCase):
    def test_create_table_uses_backticks(self):
        connector = make_connector()
        connector.create_table(
            None, "orders",
            [Column("id", "int", nullable=False, is_primary_key=True), Column("name", "varchar(50)")],
        )
        sql = connector.connection.last_query()
        self.assertIn("CREATE TABLE `orders`", sql)
        self.assertIn("`id` int NOT NULL", sql)
        self.assertIn("PRIMARY KEY (`id`)", sql)

    def test_insert_batch_parameterised_and_commits(self):
        connector = make_connector()
        n = connector.insert_batch(None, "t", [{"id": 1, "v": "a"}], ["id", "v"])
        self.assertEqual(n, 1)
        query, values = connector.connection.executed[-1]
        self.assertIn("INSERT INTO `t` (`id`, `v`) VALUES (%s, %s)", query)
        self.assertEqual(values, [(1, "a")])
        self.assertEqual(connector.connection.commits, 1)

    def test_upsert_batch_on_duplicate_key_update(self):
        connector = make_connector()
        connector.upsert_batch(None, "orders", [{"id": 1, "status": "new"}], ["id", "status"], ["id"])
        sql = connector.connection.last_query()
        self.assertIn("ON DUPLICATE KEY UPDATE `status` = VALUES(`status`)", sql)

    def test_upsert_batch_pk_only_updates_pk_to_itself(self):
        connector = make_connector()
        connector.upsert_batch(None, "t", [{"id": 1}], ["id"], ["id"])
        sql = connector.connection.last_query()
        self.assertIn("ON DUPLICATE KEY UPDATE `id` = VALUES(`id`)", sql)

    def test_get_columns_detects_unsigned(self):
        results = [
            (
                "information_schema.columns",
                ["column_name", "data_type", "character_maximum_length",
                 "numeric_precision", "numeric_scale", "is_nullable", "column_type"],
                [("id", "bigint", None, 20, 0, "NO", "bigint(20) unsigned")],
            ),
            ("key_column_usage", ["column_name"], [("id",)]),
        ]
        connector = make_connector(results)
        cols = connector.get_columns(None, "t")
        self.assertTrue(cols[0].unsigned)
        self.assertTrue(cols[0].is_primary_key)

    def test_connection_kwargs_parses_url(self):
        config = DatabaseConfig(
            engine="mysql",
            connection_string="mysql://user:p%40ss@dbhost:3307/mydb",
        )
        connector = MySQLConnector(config)
        kwargs = connector._connection_kwargs()
        self.assertEqual(kwargs["host"], "dbhost")
        self.assertEqual(kwargs["port"], 3307)
        self.assertEqual(kwargs["database"], "mydb")
        self.assertEqual(kwargs["user"], "user")
        self.assertEqual(kwargs["password"], "p@ss")  # percent-decoded

    def test_connection_kwargs_rejects_bad_scheme(self):
        config = DatabaseConfig(engine="mysql", connection_string="postgres://x/y")
        connector = MySQLConnector(config)
        with self.assertRaises(ValueError):
            connector._connection_kwargs()

    def test_fetch_batches_streams(self):
        connector = make_connector([("SELECT", ["id"], [(i,) for i in range(5)])])
        batches = list(connector.fetch_batches(None, "t", columns=["id"], batch_size=2))
        self.assertEqual([len(b) for b in batches], [2, 2, 1])

    def test_execute_query_batches_streams(self):
        connector = make_connector([("SELECT", ["id"], [(1,), (2,), (3,)])])
        batches = list(connector.execute_query_batches("SELECT id FROM t", batch_size=2))
        self.assertEqual([len(b) for b in batches], [2, 1])

    def test_execute_query_select_returns_dicts(self):
        connector = make_connector([("SELECT", ["id", "v"], [(1, "a")])])
        self.assertEqual(connector.execute_query("SELECT id, v FROM t"), [{"id": 1, "v": "a"}])

    def test_execute_query_dml_commits(self):
        connector = make_connector()
        connector.execute_query("DELETE FROM t")
        self.assertEqual(connector.connection.commits, 1)

    def test_add_drop_truncate_sql(self):
        connector = make_connector()
        connector.add_column(None, "t", Column("c", "int"))
        self.assertIn("ALTER TABLE `t` ADD COLUMN `c` int", connector.connection.last_query())
        connector.drop_column(None, "t", "c")
        self.assertIn("DROP COLUMN `c`", connector.connection.last_query())
        connector.truncate_table(None, "t")
        self.assertIn("TRUNCATE TABLE `t`", connector.connection.last_query())

    def test_create_schema_creates_database(self):
        connector = make_connector()
        connector.create_schema("warehouse")
        self.assertIn("CREATE DATABASE IF NOT EXISTS `warehouse`", connector.connection.last_query())

    def test_apply_query_timeout_sets_session_variable(self):
        config = DatabaseConfig(engine="mysql", host="h", database="db", user="u", password="p", query_timeout=10)
        connector = MySQLConnector(config)
        connector.connection = FakeConnection()
        connector._apply_query_timeout()
        self.assertIn("SET SESSION max_execution_time = 10000", connector.connection.last_query())

    def test_apply_query_timeout_tolerates_unknown_variable(self):
        config = DatabaseConfig(engine="mysql", host="h", database="db", user="u", password="p", query_timeout=10)
        connector = MySQLConnector(config)

        class OldMySQL(FakeConnection):
            def cursor(self):
                raise RuntimeError("Unknown system variable 'max_execution_time'")

        connector.connection = OldMySQL()
        connector._apply_query_timeout()  # must not raise

    def test_apply_query_timeout_reraises_other_errors(self):
        config = DatabaseConfig(engine="mysql", host="h", database="db", user="u", password="p", query_timeout=10)
        connector = MySQLConnector(config)

        class BrokenMySQL(FakeConnection):
            def cursor(self):
                raise RuntimeError("Access denied for user")

        connector.connection = BrokenMySQL()
        with self.assertRaises(RuntimeError):
            connector._apply_query_timeout()

    def test_upsert_batch_empty_is_noop(self):
        connector = make_connector()
        self.assertEqual(connector.upsert_batch(None, "t", [], ["id"], ["id"]), 0)


if __name__ == "__main__":
    unittest.main()
