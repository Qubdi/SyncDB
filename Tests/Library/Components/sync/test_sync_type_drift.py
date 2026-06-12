"""Schema-drift warnings for existing target columns.

Schema evolution only adds and drops columns — it never ALTERs an existing
column's type — so a drifted column (source type changed, or source varchar
widened past the target length) must be named up front in a RuntimeWarning
rather than surfacing later as an opaque insert error.
"""

import unittest
import warnings

from syncdb import Column

from .helpers import MemoryConnector, make_sync

_SPEC = {"users": {"source": "dbo.users", "destination": "public.users", "mode": "append"}}


def _source(v_type="nvarchar", v_length=50):
    rows = {("dbo", "users"): [{"id": 1, "v": "a"}]}
    cols = {
        ("dbo", "users"): [
            Column("id", "int", nullable=False, is_primary_key=True),
            Column("v", v_type, char_length=v_length),
        ]
    }
    return MemoryConnector("mssql", "dbo", rows_by_table=rows, columns_by_table=cols)


def _target_with_existing(v_column):
    cols = {
        ("public", "users"): [
            Column("id", "integer", nullable=False, is_primary_key=True),
            v_column,
        ]
    }
    rows = {("public", "users"): []}
    return MemoryConnector("postgresql", "public", rows_by_table=rows, columns_by_table=cols)


class TestTypeDriftWarnings(unittest.TestCase):
    def test_base_type_mismatch_warns_naming_the_column(self):
        # Source maps to varchar(50); the existing target column is integer.
        target = _target_with_existing(Column("v", "integer"))
        sync = make_sync(_source(), target)
        with self.assertWarnsRegex(RuntimeWarning, r"column 'v'.*does not match the mapped source type"):
            sync.sync_tables(_SPEC)

    def test_source_longer_than_target_warns_truncation_risk(self):
        target = _target_with_existing(Column("v", "varchar", char_length=20))
        sync = make_sync(_source(v_length=50), target)
        with self.assertWarnsRegex(RuntimeWarning, r"column 'v'.*source length 50 exceeds the existing target length 20"):
            sync.sync_tables(_SPEC)

    def test_matching_types_do_not_warn(self):
        # nvarchar(50) maps to varchar(50); information_schema-style metadata
        # reports the base name with char_length carried separately.
        target = _target_with_existing(Column("v", "varchar", char_length=50))
        sync = make_sync(_source(v_length=50), target)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            sync.sync_tables(_SPEC)
        drift = [w for w in caught if issubclass(w.category, RuntimeWarning) and "column" in str(w.message)]
        self.assertEqual(drift, [])

    def test_sqlite_target_is_exempt(self):
        # SQLite column types are affinity hints, not constraints — no warning
        # even when the declared types differ.
        cols = {
            (None, "users"): [
                Column("id", "integer", nullable=False, is_primary_key=True),
                Column("v", "integer"),
            ]
        }
        target = MemoryConnector("sqlite", None, rows_by_table={(None, "users"): []}, columns_by_table=cols)
        sync = make_sync(_source(), target)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            sync.sync_tables({"users": {"source": "dbo.users", "destination": "users", "mode": "append"}})
        drift = [w for w in caught if issubclass(w.category, RuntimeWarning) and "does not match" in str(w.message)]
        self.assertEqual(drift, [])


if __name__ == "__main__":
    unittest.main()
