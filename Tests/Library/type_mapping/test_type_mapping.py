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

    def test_maps_unsigned_mysql_tinyint_to_mssql_tinyint(self):
        # MySQL TINYINT UNSIGNED (0-255) should land on MSSQL TINYINT (0-255),
        # not SMALLINT, since the value ranges match exactly.
        mapped = self.mapper.map_type("mysql", "mssql", "tinyint", unsigned=True)

        self.assertEqual(mapped, "tinyint")

    def test_same_engine_preserves_varchar_with_length(self):
        self.assertEqual(self.mapper.map_type("postgresql", "postgresql", "varchar", char_length=50), "varchar(50)")
        self.assertEqual(self.mapper.map_type("mssql", "mssql", "nvarchar", char_length=120), "nvarchar(120)")

    def test_same_engine_preserves_decimal_precision(self):
        self.assertEqual(
            self.mapper.map_type("postgresql", "postgresql", "numeric", numeric_precision=10, numeric_scale=4),
            "numeric(10,4)",
        )

    def test_maps_mssql_datetime_types_to_postgresql(self):
        self.assertEqual(self.mapper.map_type("mssql", "postgresql", "datetime2"), "timestamp")
        self.assertEqual(self.mapper.map_type("mssql", "postgresql", "datetimeoffset"), "timestamptz")
        self.assertEqual(self.mapper.map_type("mssql", "postgresql", "uniqueidentifier"), "uuid")

    def test_unknown_type_falls_back_to_text(self):
        self.assertEqual(self.mapper.map_type("mssql", "postgresql", "geometry"), "text")
        self.assertEqual(self.mapper.map_type("mssql", "mysql", "geometry"), "longtext")


if __name__ == "__main__":
    unittest.main()
