import unittest

from syncdb.sql import (
    build_order_by,
    build_where_clause,
    parse_qualified_name,
    quote_qualified,
    validate_identifier,
    validate_type,
)


class QueryBuilderTests(unittest.TestCase):
    def test_parse_qualified_name_uses_default_schema(self):
        parsed = parse_qualified_name("customers", default_schema="public")

        self.assertEqual(parsed.schema, "public")
        self.assertEqual(parsed.table, "customers")

    def test_parse_qualified_name_two_part(self):
        parsed = parse_qualified_name("dbo.orders")

        self.assertEqual(parsed.schema, "dbo")
        self.assertEqual(parsed.table, "orders")

    def test_parse_qualified_name_rejects_three_parts(self):
        with self.assertRaises(ValueError):
            parse_qualified_name("db.dbo.orders")

    def test_build_where_clause_with_params(self):
        where, params = build_where_clause({"where": "created_at >= ?", "params": ["2026-01-01"]})

        self.assertEqual(where, " WHERE created_at >= ? ")
        self.assertEqual(params, ["2026-01-01"])

    def test_build_where_clause_empty_returns_no_clause(self):
        where, params = build_where_clause(None)

        self.assertEqual(where, "")
        self.assertEqual(params, [])

    def test_build_where_clause_rejects_unsafe_tokens(self):
        with self.assertRaises(ValueError):
            build_where_clause("1=1; DROP TABLE users")

    def test_build_order_by_quotes_identifiers(self):
        # Default quoting is the SQL-standard double-quote (portable across engines).
        self.assertEqual(build_order_by(["id", "created_at"]), ' ORDER BY "id", "created_at"')

    def test_build_order_by_single_string(self):
        self.assertEqual(build_order_by("id"), ' ORDER BY "id"')

    def test_build_order_by_honours_quote_char(self):
        self.assertEqual(build_order_by(["id"], quote_char="["), " ORDER BY [id]")
        self.assertEqual(build_order_by(["id"], quote_char="`"), " ORDER BY `id`")

    def test_build_order_by_none_returns_empty(self):
        self.assertEqual(build_order_by(None), "")

    def test_build_order_by_supports_direction_suffix(self):
        # Direction keywords are matched case-insensitively and normalised to uppercase.
        self.assertEqual(build_order_by("updated_at DESC"), ' ORDER BY "updated_at" DESC')
        self.assertEqual(build_order_by(["id asc", "created_at desc"]), ' ORDER BY "id" ASC, "created_at" DESC')
        self.assertEqual(build_order_by(["id DESC"], quote_char="["), " ORDER BY [id] DESC")

    def test_build_order_by_rejects_unsafe_direction_terms(self):
        # Only ASC/DESC may follow the column; anything else is rejected so no
        # other token can ride into the SQL string.
        with self.assertRaises(ValueError):
            build_order_by("id DESC; DROP TABLE users")
        with self.assertRaises(ValueError):
            build_order_by("id DESCENDING")
        with self.assertRaises(ValueError):
            build_order_by("id DESC extra")

    def test_validate_identifier_rejects_unsafe_names(self):
        with self.assertRaises(ValueError):
            validate_identifier("'; DROP TABLE users; --")
        with self.assertRaises(ValueError):
            validate_identifier("column name")
        with self.assertRaises(ValueError):
            validate_identifier("123bad")

    def test_validate_type_accepts_real_types(self):
        for good in ("int", "varchar(50)", "numeric(20,0)", "nvarchar(max)",
                     "double precision", "timestamp", "datetimeoffset", "text[]"):
            self.assertEqual(validate_type(good), good)

    def test_validate_type_rejects_injection(self):
        for bad in ("int); DROP TABLE users;--", "int; SELECT 1", "varchar(50) DEFAULT ''",
                    "'; DROP", "int)", "text/*c*/"):
            with self.assertRaises(ValueError):
                validate_type(bad)

    def test_build_where_clause_blocks_more_injection_tokens(self):
        for bad in ("id = 1 UNION SELECT * FROM secrets", "id = pg_sleep(5)",
                    "id = 1 /* comment */", "name = 0x41"):
            with self.assertRaises(ValueError):
                build_where_clause(bad)

    def test_quote_qualified(self):
        parsed = parse_qualified_name("dbo.Customers")

        self.assertEqual(quote_qualified(parsed, "["), "[dbo].[Customers]")


if __name__ == "__main__":
    unittest.main()
