"""Common live tests for table schema transformation options."""

from __future__ import annotations

from ..helpers import LiveBase, column_names, column_type, count, materialize_scenario_classes


def _base(source: str) -> dict:
    return {
        "source": source,
        "mode": "full_refresh",
        "primary_key": ["customer_id"],
        "order_by": ["customer_id"],
        "filter": "customer_id <= 500",
    }


class _RenameSingleColumn(LiveBase):
    tables = ["t_ren_single"]

    def test_column_renamed_and_old_name_absent(self):
        spec = {"t": {**_base(self.source_customers), "destination": "t_ren_single", "rename": {"full_name": "name"}}}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            cols = column_names(self.scenario, "t_ren_single")
            self.assertIn("name", cols, f"run {run}: new name present")
            self.assertNotIn("full_name", cols, f"run {run}: old name absent")
            self.assertEqual(count(self.scenario, "t_ren_single"), 500)


class _RenameMultipleColumns(LiveBase):
    tables = ["t_ren_multi"]

    def test_all_renames_applied(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_ren_multi",
            "rename": {"full_name": "name", "email": "email_address"},
        }}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            cols = column_names(self.scenario, "t_ren_multi")
            self.assertIn("name", cols, f"run {run}")
            self.assertIn("email_address", cols, f"run {run}")
            self.assertNotIn("full_name", cols, f"run {run}")
            self.assertNotIn("email", cols, f"run {run}")
            self.assertEqual(count(self.scenario, "t_ren_multi"), 500)


class _TypeOverrideSingle(LiveBase):
    tables = ["t_to_char", "t_to_text"]

    def test_char_override_on_country(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_to_char",
            "type_overrides": {"country": "char(80)"},
        }}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            self.assertIn("char", column_type(self.scenario, "t_to_char", "country").lower(), f"run {run}")
            self.assertEqual(count(self.scenario, "t_to_char"), 500)

    def test_text_override_on_full_name(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_to_text",
            "type_overrides": {"full_name": "text"},
        }}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            self.assertIn("text", column_type(self.scenario, "t_to_text", "full_name").lower(), f"run {run}")
            self.assertEqual(count(self.scenario, "t_to_text"), 500)


class _TypeOverrideMultiple(LiveBase):
    tables = ["t_to_multi"]

    def test_multiple_overrides_all_applied(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_to_multi",
            "type_overrides": {"country": "char(80)", "full_name": "varchar(500)"},
        }}
        for _run in range(1, 3):
            self.make_sync().sync_tables(spec)
            self.assertIn("char", column_type(self.scenario, "t_to_multi", "country").lower())
            self.assertIn("varchar", column_type(self.scenario, "t_to_multi", "full_name").lower())
            self.assertEqual(count(self.scenario, "t_to_multi"), 500)


class _RenameWithTypeOverride(LiveBase):
    tables = ["t_ren_type"]

    def test_rename_and_type_override_together(self):
        spec = {"t": {
            **_base(self.source_customers),
            "destination": "t_ren_type",
            "rename": {"full_name": "name"},
            "type_overrides": {"country": "char(80)"},
        }}
        for run in range(1, 3):
            self.make_sync().sync_tables(spec)
            cols = column_names(self.scenario, "t_ren_type")
            self.assertIn("name", cols, f"run {run}")
            self.assertNotIn("full_name", cols, f"run {run}")
            self.assertIn("char", column_type(self.scenario, "t_ren_type", "country").lower(), f"run {run}")
            self.assertEqual(count(self.scenario, "t_ren_type"), 500)


materialize_scenario_classes(
    globals(),
    _RenameSingleColumn,
    _RenameMultipleColumns,
    _TypeOverrideSingle,
    _TypeOverrideMultiple,
    _RenameWithTypeOverride,
)

