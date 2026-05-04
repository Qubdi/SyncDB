"""Live tests: schema-transformation options — rename and type_overrides.

Covers single/multiple column renames, single/multiple type overrides, and
combinations of both.  Verifies MySQL column names and types after each run.

Run:
    pytest "Tests/Library/PGSQL to MySQL/Tables/test_schema_transform.py" -v
"""
from __future__ import annotations

import unittest

from .helpers import LiveBase, column_names, column_type, count, make_sync

_SRC = "public.customers"
_BASE = {
    "source": _SRC,
    "mode": "full_refresh",
    "primary_key": ["customer_id"],
    "order_by": ["customer_id"],
    "filter": "customer_id <= 500",
}


# ── rename ─────────────────────────────────────────────────────────────────────

class TestRenameSingleColumn(LiveBase):
    tables = ["t_ren_single"]

    def test_column_renamed_and_old_name_absent(self):
        spec = {"t": {**_BASE, "destination": "t_ren_single",
                      "rename": {"full_name": "name"}}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            cols = column_names("t_ren_single")
            self.assertIn("name", cols, f"run {run}: new name present")
            self.assertNotIn("full_name", cols, f"run {run}: old name absent")
            self.assertEqual(count("t_ren_single"), 500)


class TestRenameMultipleColumns(LiveBase):
    tables = ["t_ren_multi"]

    def test_all_renames_applied(self):
        spec = {"t": {**_BASE, "destination": "t_ren_multi",
                      "rename": {"full_name": "name", "email": "email_address"}}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            cols = column_names("t_ren_multi")
            self.assertIn("name", cols, f"run {run}")
            self.assertIn("email_address", cols, f"run {run}")
            self.assertNotIn("full_name", cols, f"run {run}")
            self.assertNotIn("email", cols, f"run {run}")
            self.assertEqual(count("t_ren_multi"), 500)


# ── type_overrides ─────────────────────────────────────────────────────────────

class TestTypeOverrideSingle(LiveBase):
    tables = ["t_to_char", "t_to_text"]

    def test_char_override_on_country(self):
        spec = {"t": {**_BASE, "destination": "t_to_char",
                      "type_overrides": {"country": "char(80)"}}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertIn("char", column_type("t_to_char", "country").lower(),
                          f"run {run}")
            self.assertEqual(count("t_to_char"), 500)

    def test_text_override_on_full_name(self):
        spec = {"t": {**_BASE, "destination": "t_to_text",
                      "type_overrides": {"full_name": "text"}}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertIn("text", column_type("t_to_text", "full_name").lower(),
                          f"run {run}")
            self.assertEqual(count("t_to_text"), 500)


class TestTypeOverrideMultiple(LiveBase):
    tables = ["t_to_multi"]

    def test_multiple_overrides_all_applied(self):
        spec = {"t": {**_BASE, "destination": "t_to_multi",
                      "type_overrides": {"country": "char(80)", "full_name": "varchar(500)"}}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            self.assertIn("char", column_type("t_to_multi", "country").lower())
            self.assertIn("varchar", column_type("t_to_multi", "full_name").lower())
            self.assertEqual(count("t_to_multi"), 500)


# ── rename + type_overrides combined ──────────────────────────────────────────

class TestRenameWithTypeOverride(LiveBase):
    tables = ["t_ren_type"]

    def test_rename_and_type_override_together(self):
        spec = {"t": {**_BASE, "destination": "t_ren_type",
                      "rename": {"full_name": "name"},
                      "type_overrides": {"country": "char(80)"}}}
        for run in range(1, 3):
            make_sync().sync_tables(spec)
            cols = column_names("t_ren_type")
            self.assertIn("name", cols, f"run {run}")
            self.assertNotIn("full_name", cols, f"run {run}")
            self.assertIn("char", column_type("t_ren_type", "country").lower(),
                          f"run {run}")
            self.assertEqual(count("t_ren_type"), 500)


if __name__ == "__main__":
    unittest.main()
