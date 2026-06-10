import unittest
import warnings

from syncdb.sync.staging import create_staging_table, replace_from_staging


class FakeStagingConnector:
    def __init__(self, engine="postgresql", fail_copy=False):
        self.engine = engine
        self._in_transaction = False
        self.fail_copy = fail_copy
        self.events = []
        self.created = []
        self.dropped = []

    @property
    def is_in_transaction(self):
        return self._in_transaction

    def begin(self):
        self._in_transaction = True
        self.events.append("begin")

    def commit(self):
        self._in_transaction = False
        self.events.append("commit")

    def rollback(self):
        self._in_transaction = False
        self.events.append("rollback")

    def drop_table(self, schema, table):
        self.dropped.append(table)

    def create_table(self, schema, table, columns):
        self.created.append(table)

    def truncate_table(self, schema, table):
        self.events.append("truncate")

    def copy_table_rows(self, ss, st, ts, tt, cols):
        self.events.append("copy")
        if self.fail_copy:
            raise RuntimeError("copy failed")
        return 1


def _retry(op):
    op()


class StagingTests(unittest.TestCase):
    def test_create_staging_table_drops_then_creates_with_uid(self):
        connector = FakeStagingConnector()
        name = create_staging_table(connector, "public", "orders", [], uid="abc123")
        self.assertEqual(name, "__syncdb_orders_abc123_stg")
        self.assertIn("__syncdb_orders_abc123_stg", connector.dropped)
        self.assertIn("__syncdb_orders_abc123_stg", connector.created)

    def test_replace_from_staging_truncates_and_copies_in_transaction(self):
        connector = FakeStagingConnector()
        replace_from_staging(connector, "public", "orders", "stg", ["id"], _retry)
        self.assertEqual(connector.events, ["begin", "truncate", "copy", "commit"])

    def test_replace_from_staging_rolls_back_on_failure(self):
        connector = FakeStagingConnector(fail_copy=True)
        with self.assertRaises(RuntimeError):
            replace_from_staging(connector, "public", "orders", "stg", ["id"], _retry)
        self.assertIn("rollback", connector.events)

    def test_replace_from_staging_warns_on_mysql(self):
        connector = FakeStagingConnector(engine="mysql")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            replace_from_staging(connector, None, "orders", "stg", ["id"], _retry)
        self.assertTrue(any(issubclass(w.category, RuntimeWarning) for w in caught))

    def test_replace_from_staging_reuses_outer_transaction(self):
        connector = FakeStagingConnector()
        connector.begin()
        connector.events.clear()
        replace_from_staging(connector, "public", "orders", "stg", ["id"], _retry)
        # Already in a transaction: swap must not begin/commit its own.
        self.assertNotIn("commit", connector.events)
        self.assertEqual(connector.events, ["truncate", "copy"])


if __name__ == "__main__":
    unittest.main()
