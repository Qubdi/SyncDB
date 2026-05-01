import unittest

from syncdb.sql import build_order_by, build_where_clause, parse_qualified_name, quote_qualified


class QueryBuilderTests(unittest.TestCase):
    def test_parse_qualified_name_uses_default_schema(self):
        parsed = parse_qualified_name("customers", default_schema="public")

        self.assertEqual(parsed.schema, "public")
        self.assertEqual(parsed.table, "customers")

    def test_build_where_clause_with_params(self):
        where, params = build_where_clause({"where": "created_at >= ?", "params": ["2026-01-01"]})

        self.assertEqual(where, " WHERE created_at >= ? ")
        self.assertEqual(params, ["2026-01-01"])

    def test_build_where_clause_rejects_unsafe_tokens(self):
        with self.assertRaises(ValueError):
            build_where_clause("1=1; DROP TABLE users")

    def test_build_order_by_quotes_identifiers(self):
        self.assertEqual(build_order_by(["id", "created_at"]), " ORDER BY [id], [created_at]")

    def test_quote_qualified(self):
        parsed = parse_qualified_name("dbo.Customers")

        self.assertEqual(quote_qualified(parsed, "["), "[dbo].[Customers]")


if __name__ == "__main__":
    unittest.main()
