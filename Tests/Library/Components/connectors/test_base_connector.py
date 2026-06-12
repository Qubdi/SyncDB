import unittest

from syncdb import Column, DatabaseConfig
from syncdb.connectors import MSSQLConnector, SQLiteConnector

from .fakedb import FakeConnection


class CloseResilienceTests(unittest.TestCase):
    def test_close_resets_connection_even_when_driver_close_raises(self):
        connector = MSSQLConnector(DatabaseConfig(engine="mssql", host="h", database="d", user="u", password="p"))
        conn = FakeConnection()
        conn.fail_close = True
        connector.connection = conn

        # close() must clear the handle despite the driver raising, otherwise the
        # idempotency guard in connect() would treat the dead connection as live.
        with self.assertRaises(RuntimeError):
            connector.close()
        self.assertIsNone(connector.connection)

    def test_close_is_safe_when_no_connection(self):
        connector = MSSQLConnector(DatabaseConfig(engine="mssql", host="h", database="d", user="u", password="p"))
        connector.close()  # no exception
        self.assertIsNone(connector.connection)


class TransactionStateTests(unittest.TestCase):
    def test_is_in_transaction_reflects_begin_commit_rollback(self):
        connector = SQLiteConnector(DatabaseConfig(engine="sqlite", database=":memory:"))
        connector.connect()
        self.addCleanup(connector.close)
        self.assertFalse(connector.is_in_transaction)
        connector.begin()
        self.assertTrue(connector.is_in_transaction)
        connector.commit()
        self.assertFalse(connector.is_in_transaction)
        connector.begin()
        connector.rollback()
        self.assertFalse(connector.is_in_transaction)


class DeprecatedSoftDeleteTests(unittest.TestCase):
    def _seed(self):
        connector = SQLiteConnector(DatabaseConfig(engine="sqlite", database=":memory:"))
        connector.connect()
        self.addCleanup(connector.close)
        connector.create_table(
            None, "t",
            [Column("id", "integer", is_primary_key=True), Column("deleted_at", "text")],
        )
        connector.insert_batch(
            None, "t",
            [{"id": 1, "deleted_at": None}, {"id": 2, "deleted_at": None}],
            ["id", "deleted_at"],
        )
        return connector

    def test_apply_soft_deletes_sql_warns_deprecation(self):
        connector = self._seed()
        pk = [Column("id", "integer", is_primary_key=True)]
        with self.assertWarns(DeprecationWarning):
            connector.apply_soft_deletes_sql(None, "t", pk, seen_keys=set(), deleted_at_value="2026-01-01")

    def test_apply_soft_deletes_sql_marks_missing_rows(self):
        connector = self._seed()
        pk = [Column("id", "integer", is_primary_key=True)]
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # Only id=1 was seen, so id=2 must be soft-deleted.
            marked = connector.apply_soft_deletes_sql(None, "t", pk, {(1,)}, "2026-01-01")
        self.assertEqual(marked, 1)


class SharedHelperTests(unittest.TestCase):
    def _mem(self):
        connector = SQLiteConnector(DatabaseConfig(engine="sqlite", database=":memory:"))
        connector.connect()
        self.addCleanup(connector.close)
        return connector

    def test_ping_true_when_reachable(self):
        self.assertTrue(self._mem().ping())

    def test_ping_false_when_connect_fails(self):
        connector = SQLiteConnector(DatabaseConfig(engine="sqlite", connection_string="badscheme://x"))
        self.assertFalse(connector.ping())

    def test_transaction_helpers_toggle_state(self):
        connector = self._mem()
        connector.begin()
        connector.insert_into_scratch = True  # arbitrary attribute write to ensure access is fine
        connector.commit()
        self.assertFalse(connector.is_in_transaction)
        connector.begin()
        connector.rollback()
        self.assertFalse(connector.is_in_transaction)

    def test_context_manager_opens_and_closes(self):
        connector = SQLiteConnector(DatabaseConfig(engine="sqlite", database=":memory:"))
        with connector as c:
            self.assertIsNotNone(c.connection)
        self.assertIsNone(connector.connection)

    def test_copy_table_rows(self):
        connector = self._mem()
        cols = [Column("id", "integer", is_primary_key=True), Column("v", "text")]
        connector.create_table(None, "src", cols)
        connector.create_table(None, "dst", cols)
        connector.insert_batch(None, "src", [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}], ["id", "v"])
        copied = connector.copy_table_rows(None, "src", None, "dst", ["id", "v"])
        self.assertEqual(copied, 2)
        self.assertEqual(connector.get_row_count(None, "dst"), 2)

    def test_update_matching_rows_changes_values(self):
        connector = self._mem()
        connector.create_table(None, "t", [Column("id", "integer", is_primary_key=True), Column("s", "text")])
        connector.insert_batch(None, "t", [{"id": 1, "s": "x"}, {"id": 2, "s": "y"}], ["id", "s"])
        updated = connector.update_matching_rows(None, "t", [{"id": 1}], ["id"], {"s": "z"})
        self.assertEqual(updated, 1)
        rows = {r["id"]: r["s"] for r in connector.execute_query('SELECT id, s FROM "t"')}
        self.assertEqual(rows[1], "z")
        self.assertEqual(rows[2], "y")

    def test_delete_matching_rows_removes_keys(self):
        connector = self._mem()
        connector.create_table(None, "t", [Column("id", "integer", is_primary_key=True)])
        connector.insert_batch(None, "t", [{"id": 1}, {"id": 2}, {"id": 3}], ["id"])
        deleted = connector.delete_matching_rows(None, "t", [{"id": 2}], ["id"])
        self.assertEqual(deleted, 1)
        self.assertEqual(connector.get_row_count(None, "t"), 2)


if __name__ == "__main__":
    unittest.main()
