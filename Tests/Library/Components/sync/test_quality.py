import unittest

from syncdb import Column, DatabaseConfig
from syncdb.connectors import SQLiteConnector
from syncdb.sync.models import TableSyncResult
from syncdb.sync.quality import validate_expectations


def _connector_with_rows(rows, columns):
    connector = SQLiteConnector(DatabaseConfig(engine="sqlite", database=":memory:"))
    connector.connect()
    connector.create_table(None, "t", columns)
    if rows:
        connector.insert_batch(None, "t", rows, list(rows[0].keys()))
    return connector


def _result():
    return TableSyncResult(name="t", source="s", destination="t", mode="append")


class QualityExpectationTests(unittest.TestCase):
    def setUp(self):
        self.columns = [
            Column("id", "integer", is_primary_key=True),
            Column("email", "text"),
            Column("score", "integer"),
        ]

    def test_no_expectations_is_noop(self):
        connector = _connector_with_rows([{"id": 1, "email": "a", "score": 5}], self.columns)
        result = _result()
        validate_expectations(connector, None, "t", None, result, 5000)
        self.assertEqual(result.expectations_failed, [])

    def test_min_rows_pass_and_fail(self):
        connector = _connector_with_rows(
            [{"id": 1, "email": "a", "score": 5}, {"id": 2, "email": "b", "score": 6}],
            self.columns,
        )
        validate_expectations(connector, None, "t", {"min_rows": 2}, _result(), 5000)
        with self.assertRaises(ValueError):
            validate_expectations(connector, None, "t", {"min_rows": 5}, _result(), 5000)

    def test_not_null_detects_nulls(self):
        connector = _connector_with_rows(
            [{"id": 1, "email": None, "score": 5}],
            self.columns,
        )
        with self.assertRaises(ValueError) as ctx:
            validate_expectations(connector, None, "t", {"not_null": ["email"]}, _result(), 5000)
        self.assertIn("email", str(ctx.exception))

    def test_unique_single_column(self):
        connector = _connector_with_rows(
            [{"id": 1, "email": "dup", "score": 1}, {"id": 2, "email": "dup", "score": 2}],
            self.columns,
        )
        with self.assertRaises(ValueError):
            validate_expectations(connector, None, "t", {"unique": ["email"]}, _result(), 5000)

    def test_unique_multi_column(self):
        connector = _connector_with_rows(
            [{"id": 1, "email": "a", "score": 1}, {"id": 2, "email": "a", "score": 1}],
            self.columns,
        )
        with self.assertRaises(ValueError):
            validate_expectations(connector, None, "t", {"unique": [["email", "score"]]}, _result(), 5000)

    def test_range_below_and_above(self):
        connector = _connector_with_rows(
            [{"id": 1, "email": "a", "score": -3}, {"id": 2, "email": "b", "score": 200}],
            self.columns,
        )
        with self.assertRaises(ValueError):
            validate_expectations(connector, None, "t", {"range": {"score": {"min": 0}}}, _result(), 5000)
        with self.assertRaises(ValueError):
            validate_expectations(connector, None, "t", {"range": {"score": {"max": 100}}}, _result(), 5000)

    def test_range_within_bounds_passes(self):
        connector = _connector_with_rows(
            [{"id": 1, "email": "a", "score": 50}],
            self.columns,
        )
        result = _result()
        validate_expectations(connector, None, "t", {"range": {"score": {"min": 0, "max": 100}}}, result, 5000)
        self.assertEqual(result.expectations_failed, [])

    def test_unsafe_column_name_rejected(self):
        connector = _connector_with_rows([{"id": 1, "email": "a", "score": 1}], self.columns)
        with self.assertRaises(ValueError):
            validate_expectations(connector, None, "t", {"not_null": ["email; DROP"]}, _result(), 5000)


if __name__ == "__main__":
    unittest.main()
