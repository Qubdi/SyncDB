import unittest

from syncdb import Column, SchemaMapper


class TypeMappingTests(unittest.TestCase):
    def setUp(self):
        self.mapper = SchemaMapper()

    def test_maps_mssql_columns_to_postgresql(self):
        columns = [
            Column("id", "int", nullable=False, is_primary_key=True),
            Column("name", "nvarchar", char_length=100),
            Column("amount", "decimal", numeric_precision=18, numeric_scale=2),
            Column("payload", "varbinary"),
        ]

        mapped = self.mapper.map_columns(columns, "mssql", "postgresql")

        self.assertEqual([column.data_type for column in mapped], ["integer", "varchar(100)", "numeric(18,2)", "bytea"])
        self.assertFalse(mapped[0].nullable)
        self.assertTrue(mapped[0].is_primary_key)

    def test_maps_postgresql_to_mysql(self):
        self.assertEqual(self.mapper.map_type("postgresql", "mysql", "uuid"), "char(36)")
        self.assertEqual(self.mapper.map_type("postgresql", "mysql", "jsonb"), "json")

    def test_maps_unsigned_mysql_bigint_to_postgresql_numeric(self):
        mapped = self.mapper.map_type("mysql", "postgresql", "bigint", unsigned=True)

        self.assertEqual(mapped, "numeric(20,0)")


if __name__ == "__main__":
    unittest.main()
