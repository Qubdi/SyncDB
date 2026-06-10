import unittest

from syncdb import Column, SchemaMapper


class ToPostgresTests(unittest.TestCase):
    def setUp(self):
        self.m = SchemaMapper()

    def t(self, dtype, **kw):
        return self.m.map_type("mssql", "postgresql", dtype, **kw)

    def test_integer_family(self):
        self.assertEqual(self.t("bigint"), "bigint")
        self.assertEqual(self.m.map_type("mysql", "postgresql", "bigint", unsigned=True), "numeric(20,0)")
        self.assertEqual(self.t("int"), "integer")
        self.assertEqual(self.m.map_type("mysql", "postgresql", "int", unsigned=True), "bigint")
        self.assertEqual(self.t("smallint"), "smallint")
        self.assertEqual(self.m.map_type("mysql", "postgresql", "smallint", unsigned=True), "integer")
        self.assertEqual(self.t("tinyint"), "smallint")

    def test_misc_types(self):
        self.assertEqual(self.t("bit"), "boolean")
        self.assertEqual(self.t("uniqueidentifier"), "uuid")
        self.assertEqual(self.t("money"), "numeric")
        self.assertEqual(self.t("decimal", numeric_precision=10, numeric_scale=2), "numeric(10,2)")
        self.assertEqual(self.t("float"), "double precision")
        self.assertEqual(self.t("real"), "real")
        self.assertEqual(self.t("date"), "date")
        self.assertEqual(self.t("time"), "time")
        self.assertEqual(self.t("xml"), "xml")

    def test_string_and_binary(self):
        self.assertEqual(self.t("nvarchar", char_length=50), "varchar(50)")
        self.assertEqual(self.t("nvarchar"), "text")  # unbounded
        self.assertEqual(self.t("char", char_length=5), "char(5)")
        self.assertEqual(self.t("text"), "text")
        self.assertEqual(self.t("image"), "bytea")
        self.assertEqual(self.t("json"), "jsonb")


class ToMssqlTests(unittest.TestCase):
    def setUp(self):
        self.m = SchemaMapper()

    def t(self, dtype, source="postgresql", **kw):
        return self.m.map_type(source, "mssql", dtype, **kw)

    def test_integers_and_unsigned(self):
        self.assertEqual(self.t("bigint"), "bigint")
        self.assertEqual(self.m.map_type("mysql", "mssql", "bigint", unsigned=True), "decimal(20,0)")
        self.assertEqual(self.t("integer"), "int")
        self.assertEqual(self.m.map_type("mysql", "mssql", "int", unsigned=True), "bigint")
        self.assertEqual(self.m.map_type("mysql", "mssql", "smallint", unsigned=True), "int")
        self.assertEqual(self.t("smallint"), "smallint")

    def test_misc(self):
        self.assertEqual(self.t("boolean"), "bit")
        self.assertEqual(self.t("uuid"), "uniqueidentifier")
        self.assertEqual(self.t("money"), "decimal")
        self.assertEqual(self.t("double precision"), "float")
        self.assertEqual(self.t("real"), "real")
        self.assertEqual(self.t("timestamp"), "datetime2")
        self.assertEqual(self.t("timestamptz"), "datetimeoffset")
        self.assertEqual(self.t("date"), "date")
        self.assertEqual(self.t("time"), "time")

    def test_strings_and_binary(self):
        self.assertEqual(self.t("varchar", char_length=20), "nvarchar(20)")
        self.assertEqual(self.t("text"), "nvarchar(max)")
        self.assertEqual(self.t("bytea"), "varbinary(max)")
        self.assertEqual(self.t("geometry"), "nvarchar(max)")  # fallback


class ToMysqlTests(unittest.TestCase):
    def setUp(self):
        self.m = SchemaMapper()

    def t(self, dtype, **kw):
        return self.m.map_type("postgresql", "mysql", dtype, **kw)

    def test_all_branches(self):
        self.assertEqual(self.t("bigint"), "bigint")
        self.assertEqual(self.t("integer"), "int")
        self.assertEqual(self.t("smallint"), "smallint")
        self.assertEqual(self.t("boolean"), "tinyint(1)")
        self.assertEqual(self.t("uuid"), "char(36)")
        self.assertEqual(self.t("money"), "decimal")
        self.assertEqual(self.t("double precision"), "double")
        self.assertEqual(self.t("real"), "float")
        self.assertEqual(self.t("timestamptz"), "datetime")
        self.assertEqual(self.t("date"), "date")
        self.assertEqual(self.t("time"), "time")
        self.assertEqual(self.t("varchar", char_length=30), "varchar(30)")
        self.assertEqual(self.t("varchar"), "longtext")
        self.assertEqual(self.t("text"), "longtext")
        self.assertEqual(self.t("jsonb"), "json")
        self.assertEqual(self.t("bytea"), "longblob")
        self.assertEqual(self.t("geometry"), "longtext")


class ToSqliteTests(unittest.TestCase):
    def setUp(self):
        self.m = SchemaMapper()

    def t(self, dtype):
        return self.m.map_type("postgresql", "sqlite", dtype)

    def test_affinities(self):
        self.assertEqual(self.t("integer"), "integer")
        self.assertEqual(self.t("bigint"), "integer")
        self.assertEqual(self.t("boolean"), "integer")
        self.assertEqual(self.t("numeric"), "real")
        self.assertEqual(self.t("double precision"), "real")
        self.assertEqual(self.t("bytea"), "blob")
        self.assertEqual(self.t("varchar"), "text")
        self.assertEqual(self.t("geometry"), "text")


class EdgeCaseTests(unittest.TestCase):
    def setUp(self):
        self.m = SchemaMapper()

    def test_varchar_oversize_falls_back_to_unbounded_cross_engine(self):
        # The 65535 guard lives in _varchar (cross-engine); an oversized length
        # yields the unbounded form for the target.
        self.assertEqual(self.m.map_type("postgresql", "mysql", "varchar", char_length=70000), "longtext")
        self.assertEqual(self.m.map_type("mysql", "postgresql", "varchar", char_length=70000), "text")

    def test_same_engine_preserves_length_verbatim(self):
        # _preserve_type re-attaches the length as-is without the cross-engine guard.
        self.assertEqual(self.m.map_type("mysql", "mysql", "varchar", char_length=70000), "varchar(70000)")

    def test_numeric_without_modifiers_is_bare(self):
        self.assertEqual(self.m.map_type("postgresql", "mssql", "decimal"), "decimal")

    def test_empty_type_defaults_to_text(self):
        self.assertEqual(self.m.map_type("postgresql", "postgresql", ""), "text")

    def test_same_engine_character_varying_normalised(self):
        self.assertEqual(
            self.m.map_type("postgresql", "postgresql", "character varying", char_length=10),
            "varchar(10)",
        )

    def test_map_column_preserves_flags(self):
        col = Column("c", "int", nullable=False, is_primary_key=True, unsigned=True)
        out = self.m.map_column(col, "mysql", "postgresql")
        self.assertFalse(out.nullable)
        self.assertTrue(out.is_primary_key)
        self.assertTrue(out.unsigned)


if __name__ == "__main__":
    unittest.main()
