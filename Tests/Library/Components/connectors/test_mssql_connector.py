import unittest

from syncdb import Column, DatabaseConfig
from syncdb.connectors import MSSQLConnector

from .fakedb import FakeConnection


def make_connector(results=None) -> MSSQLConnector:
    config = DatabaseConfig(engine="mssql", host="h", database="db", user="u", password="p")
    connector = MSSQLConnector(config)
    # Pre-setting connection makes connect() a no-op, so no pyodbc socket is opened.
    connector.connection = FakeConnection(results=results or [])
    return connector


class MSSQLConnectorTests(unittest.TestCase):
    def test_odbc_escape_wraps_special_characters(self):
        # Values with ';' or '=' must be braced so they can't inject ODBC attributes.
        self.assertEqual(MSSQLConnector._odbc_escape("pa;ss"), "{pa;ss}")
        self.assertEqual(MSSQLConnector._odbc_escape("a=b"), "{a=b}")
        self.assertEqual(MSSQLConnector._odbc_escape("clo}se"), "{clo}}se}")
        self.assertEqual(MSSQLConnector._odbc_escape("plain"), "plain")

    def test_create_table_emits_brackets_and_primary_key(self):
        connector = make_connector()
        connector.create_table(
            "dbo", "orders",
            [
                Column("id", "int", nullable=False, is_primary_key=True),
                Column("name", "nvarchar(50)", nullable=True),
            ],
        )
        sql = connector.connection.last_query()
        self.assertIn("CREATE TABLE [dbo].[orders]", sql)
        self.assertIn("[id] int NOT NULL", sql)
        self.assertIn("[name] nvarchar(50) NULL", sql)
        self.assertIn("PRIMARY KEY ([id])", sql)

    def test_insert_batch_generates_parameterised_insert_and_commits(self):
        connector = make_connector()
        n = connector.insert_batch("dbo", "t", [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}], ["id", "v"])
        self.assertEqual(n, 2)
        query, values = connector.connection.executed[-1]
        self.assertIn("INSERT INTO [dbo].[t] ([id], [v]) VALUES (?, ?)", query)
        self.assertEqual(values, [(1, "a"), (2, "b")])
        self.assertEqual(connector.connection.commits, 1)

    def test_insert_batch_in_transaction_does_not_commit(self):
        connector = make_connector()
        connector.begin()
        connector.insert_batch("dbo", "t", [{"id": 1}], ["id"])
        self.assertEqual(connector.connection.commits, 0)

    def test_upsert_batch_builds_merge_with_update_and_insert(self):
        connector = make_connector()
        connector.upsert_batch(
            "dbo", "orders",
            [{"id": 1, "status": "new"}],
            ["id", "status"],
            ["id"],
        )
        sql = connector.connection.last_query()
        self.assertIn("MERGE INTO [dbo].[orders] AS target", sql)
        self.assertIn("ON (target.[id] = source.[id])", sql)
        self.assertIn("WHEN MATCHED THEN UPDATE SET target.[status] = source.[status]", sql)
        self.assertIn("WHEN NOT MATCHED THEN INSERT", sql)

    def test_upsert_batch_pk_only_table_omits_update_clause(self):
        connector = make_connector()
        connector.upsert_batch("dbo", "t", [{"id": 1}], ["id"], ["id"])
        sql = connector.connection.last_query()
        self.assertNotIn("WHEN MATCHED", sql)
        self.assertIn("WHEN NOT MATCHED THEN INSERT", sql)

    def test_upsert_batch_sub_batches_to_stay_under_param_limit(self):
        connector = make_connector()
        # 3 columns → sub_size = 2000 // 3 = 666 rows per MERGE statement.
        rows = [{"a": i, "b": i, "c": i} for i in range(1500)]
        total = connector.upsert_batch("dbo", "t", rows, ["a", "b", "c"], ["a"])
        self.assertEqual(total, 1500)
        merges = [q for q in connector.connection.queries() if q.startswith("MERGE")]
        self.assertEqual(len(merges), 3)  # 666 + 666 + 168

    def test_get_columns_parses_metadata_and_primary_keys(self):
        results = [
            (
                "FROM INFORMATION_SCHEMA.COLUMNS",
                ["COLUMN_NAME", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH",
                 "NUMERIC_PRECISION", "NUMERIC_SCALE", "IS_NULLABLE"],
                [("id", "int", None, 10, 0, "NO"), ("name", "nvarchar", 50, None, None, "YES")],
            ),
            ("CONSTRAINT_TYPE = 'PRIMARY KEY'", ["COLUMN_NAME"], [("id",)]),
        ]
        connector = make_connector(results)
        cols = connector.get_columns("dbo", "t")
        self.assertEqual([c.name for c in cols], ["id", "name"])
        self.assertTrue(cols[0].is_primary_key)
        self.assertFalse(cols[0].nullable)
        self.assertTrue(cols[1].nullable)
        self.assertEqual(cols[1].char_length, 50)

    def test_table_exists_true_when_row_returned(self):
        connector = make_connector([("INFORMATION_SCHEMA.TABLES", ["exists_flag"], [(1,)])])
        self.assertTrue(connector.table_exists("dbo", "t"))

    def test_table_exists_false_when_no_rows(self):
        connector = make_connector()
        self.assertFalse(connector.table_exists("dbo", "missing"))

    def test_fetch_batches_streams_in_chunks(self):
        rows = [(i,) for i in range(5)]
        connector = make_connector([("SELECT", ["id"], rows)])
        batches = list(connector.fetch_batches("dbo", "t", columns=["id"], batch_size=2))
        self.assertEqual([len(b) for b in batches], [2, 2, 1])
        self.assertEqual(batches[0], [{"id": 0}, {"id": 1}])

    def test_create_schema_is_idempotent_guarded(self):
        connector = make_connector()
        connector.create_schema("analytics")
        sql = connector.connection.last_query()
        self.assertIn("SCHEMA_ID(N'analytics') IS NULL", sql)
        self.assertIn("CREATE SCHEMA [analytics]", sql)

    def test_create_schema_rejects_unsafe_name(self):
        connector = make_connector()
        with self.assertRaises(ValueError):
            connector.create_schema("bad; DROP")

    def test_execute_query_batches_streams(self):
        connector = make_connector([("SELECT", ["id"], [(1,), (2,), (3,)])])
        batches = list(connector.execute_query_batches("SELECT id FROM t", batch_size=2))
        self.assertEqual([len(b) for b in batches], [2, 1])

    def test_execute_query_dml_commits(self):
        connector = make_connector()
        connector.execute_query("DELETE FROM [dbo].[t]")
        self.assertEqual(connector.connection.commits, 1)

    def test_add_drop_truncate_sql(self):
        connector = make_connector()
        connector.add_column("dbo", "t", Column("c", "int"))
        self.assertIn("ALTER TABLE [dbo].[t] ADD [c] int", connector.connection.last_query())
        connector.drop_column("dbo", "t", "c")
        self.assertIn("DROP COLUMN [c]", connector.connection.last_query())
        connector.truncate_table("dbo", "t")
        self.assertIn("TRUNCATE TABLE [dbo].[t]", connector.connection.last_query())

    def test_get_primary_keys(self):
        connector = make_connector([("CONSTRAINT_TYPE = 'PRIMARY KEY'", ["COLUMN_NAME"], [("id",)])])
        self.assertEqual(connector.get_primary_keys("dbo", "t"), ["id"])

    def test_insert_batch_empty_is_noop(self):
        connector = make_connector()
        self.assertEqual(connector.insert_batch("dbo", "t", [], ["id"]), 0)
        self.assertEqual(connector.connection.executed, [])

    def test_upsert_batch_empty_is_noop(self):
        connector = make_connector()
        self.assertEqual(connector.upsert_batch("dbo", "t", [], ["id"], ["id"]), 0)

    def test_upsert_batch_without_pk_falls_back_to_insert(self):
        connector = make_connector()
        connector.upsert_batch("dbo", "t", [{"id": 1}], ["id"], [])
        self.assertIn("INSERT INTO [dbo].[t]", connector.connection.last_query())

    def test_create_schema_none_is_noop(self):
        connector = make_connector()
        connector.create_schema(None)
        self.assertEqual(connector.connection.executed, [])

    def test_list_tables_uses_information_schema(self):
        connector = make_connector(
            [("information_schema.tables", ["TABLE_NAME"], [("orders",), ("customers",)])]
        )
        self.assertEqual(connector.list_tables("dbo"), ["orders", "customers"])


if __name__ == "__main__":
    unittest.main()
