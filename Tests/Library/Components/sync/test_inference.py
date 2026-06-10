import unittest

from syncdb.sync.inference import infer_columns


class InferColumnsTests(unittest.TestCase):
    def test_infers_basic_types_to_postgres_then_target(self):
        rows = [{"i": 1, "f": 1.5, "b": True, "s": "x"}]
        cols = {c.name: c.data_type for c in infer_columns(rows, "postgresql")}
        self.assertEqual(cols["i"], "bigint")
        self.assertEqual(cols["f"], "double precision")
        self.assertEqual(cols["b"], "boolean")
        self.assertEqual(cols["s"], "text")

    def test_type_promotion_mixed_int_and_float(self):
        rows = [{"x": 1}, {"x": 2.5}]
        cols = {c.name: c.data_type for c in infer_columns(rows, "postgresql")}
        # int + float in the same column must widen to double precision.
        self.assertEqual(cols["x"], "double precision")

    def test_none_in_first_row_but_value_later(self):
        rows = [{"x": None}, {"x": 7}]
        cols = {c.name: c.data_type for c in infer_columns(rows, "postgresql")}
        self.assertEqual(cols["x"], "bigint")

    def test_all_none_falls_back_to_text(self):
        rows = [{"x": None}, {"x": None}]
        cols = {c.name: c.data_type for c in infer_columns(rows, "postgresql")}
        self.assertEqual(cols["x"], "text")

    def test_maps_to_target_engine(self):
        rows = [{"x": 1}]
        cols = {c.name: c.data_type for c in infer_columns(rows, "mysql")}
        self.assertEqual(cols["x"], "bigint")

    def test_empty_rows_raises(self):
        with self.assertRaises(ValueError):
            infer_columns([], "postgresql")


if __name__ == "__main__":
    unittest.main()
