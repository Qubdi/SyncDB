import unittest
from unittest import mock

from syncdb import Column, DatabaseConfig
from syncdb.connectors import PostgresConnector

from .fakedb import FakeConnection


def make_connector(results=None) -> PostgresConnector:
    config = DatabaseConfig(engine="postgresql", host="h", database="db", user="u", password="p")
    connector = PostgresConnector(config)
    connector.connection = FakeConnection(results=results or [])
    return connector


class PostgresConnectorTests(unittest.TestCase):
    def test_create_table_uses_double_quotes_and_primary_key(self):
        connector = make_connector()
        connector.create_table(
            "public", "orders",
            [
                Column("id", "integer", nullable=False, is_primary_key=True),
                Column("name", "varchar(50)", nullable=True),
            ],
        )
        sql = connector.connection.last_query()
        self.assertIn('CREATE TABLE "public"."orders"', sql)
        self.assertIn('"id" integer NOT NULL', sql)
        self.assertIn('PRIMARY KEY ("id")', sql)

    def test_insert_batch_uses_execute_values_with_capped_page_size(self):
        connector = make_connector()
        with mock.patch("psycopg2.extras.execute_values") as ev:
            n = connector.insert_batch("public", "t", [{"id": 1}, {"id": 2}], ["id"])
        self.assertEqual(n, 2)
        self.assertEqual(ev.call_count, 1)
        _, kwargs = ev.call_args
        # page_size must be the fixed cap, never len(values) (the OOM-risk default).
        self.assertEqual(kwargs["page_size"], PostgresConnector._EXECUTE_VALUES_PAGE_SIZE)
        self.assertEqual(connector.connection.commits, 1)

    def test_upsert_batch_builds_on_conflict_do_update(self):
        connector = make_connector()
        with mock.patch("psycopg2.extras.execute_values") as ev:
            connector.upsert_batch(
                "public", "orders",
                [{"id": 1, "status": "new"}],
                ["id", "status"],
                ["id"],
            )
        query = ev.call_args[0][1]
        self.assertIn('INSERT INTO "public"."orders" ("id", "status") VALUES %s', query)
        self.assertIn('ON CONFLICT ("id") DO UPDATE SET "status" = EXCLUDED."status"', query)

    def test_upsert_batch_pk_only_uses_do_nothing(self):
        connector = make_connector()
        with mock.patch("psycopg2.extras.execute_values") as ev:
            connector.upsert_batch("public", "t", [{"id": 1}], ["id"], ["id"])
        query = ev.call_args[0][1]
        self.assertIn("DO NOTHING", query)

    def test_resolve_data_type_handles_arrays_and_varchar(self):
        self.assertEqual(PostgresConnector._resolve_data_type("ARRAY", "_text"), "text[]")
        self.assertEqual(PostgresConnector._resolve_data_type("ARRAY", ""), "text[]")
        self.assertEqual(PostgresConnector._resolve_data_type("character varying", ""), "varchar")
        self.assertEqual(PostgresConnector._resolve_data_type("integer", ""), "integer")

    def test_get_columns_parses_metadata(self):
        results = [
            (
                "information_schema.columns",
                ["column_name", "data_type", "udt_name", "character_maximum_length",
                 "numeric_precision", "numeric_scale", "is_nullable"],
                [("id", "integer", "int4", None, 32, 0, "NO"),
                 ("tags", "ARRAY", "_text", None, None, None, "YES")],
            ),
            ("constraint_type = 'PRIMARY KEY'", ["column_name"], [("id",)]),
        ]
        connector = make_connector(results)
        cols = connector.get_columns("public", "t")
        self.assertEqual(cols[0].name, "id")
        self.assertTrue(cols[0].is_primary_key)
        self.assertEqual(cols[1].data_type, "text[]")

    def test_truncate_and_drop_emit_expected_sql(self):
        connector = make_connector()
        connector.truncate_table("public", "t")
        self.assertIn('TRUNCATE TABLE "public"."t"', connector.connection.last_query())
        connector.drop_table("public", "t")
        self.assertIn('DROP TABLE IF EXISTS "public"."t"', connector.connection.last_query())

    def test_delete_matching_rows_sub_batches_under_param_limit(self):
        connector = make_connector()
        # 2-column PK → sub_size = 500 // 2 = 250 rows per DELETE.
        rows = [{"a": i, "b": i} for i in range(600)]
        deleted = connector.delete_matching_rows("public", "t", rows, ["a", "b"])
        self.assertEqual(deleted, 600)
        deletes = [q for q in connector.connection.queries() if q.startswith("DELETE")]
        self.assertEqual(len(deletes), 3)  # 250 + 250 + 100

    def test_update_matching_rows_sub_batches_under_param_limit(self):
        connector = make_connector()
        rows = [{"id": i} for i in range(600)]
        updated = connector.update_matching_rows("public", "t", rows, ["id"], {"deleted_at": "x"})
        self.assertEqual(updated, 600)
        updates = [q for q in connector.connection.queries() if q.startswith("UPDATE")]
        self.assertGreater(len(updates), 1)

    def test_fetch_batches_streams(self):
        connector = make_connector([("SELECT", ["id"], [(i,) for i in range(5)])])
        batches = list(connector.fetch_batches("public", "t", columns=["id"], batch_size=2))
        self.assertEqual([len(b) for b in batches], [2, 2, 1])

    def test_execute_query_batches_streams(self):
        connector = make_connector([("SELECT", ["id"], [(1,), (2,), (3,)])])
        batches = list(connector.execute_query_batches("SELECT id FROM t", batch_size=2))
        self.assertEqual([len(b) for b in batches], [2, 1])

    def test_execute_query_dml_commits(self):
        connector = make_connector()
        connector.execute_query("DELETE FROM t")
        self.assertEqual(connector.connection.commits, 1)

    def test_add_drop_create_schema_sql(self):
        connector = make_connector()
        connector.add_column("public", "t", Column("c", "integer"))
        self.assertIn('ALTER TABLE "public"."t" ADD COLUMN "c" integer', connector.connection.last_query())
        connector.drop_column("public", "t", "c")
        self.assertIn('DROP COLUMN "c"', connector.connection.last_query())
        connector.create_schema("analytics")
        self.assertIn('CREATE SCHEMA IF NOT EXISTS "analytics"', connector.connection.last_query())

    def test_get_primary_keys(self):
        connector = make_connector([("constraint_type = 'PRIMARY KEY'", ["column_name"], [("id",), ("sub",)])])
        self.assertEqual(connector.get_primary_keys("public", "t"), ["id", "sub"])


if __name__ == "__main__":
    unittest.main()
