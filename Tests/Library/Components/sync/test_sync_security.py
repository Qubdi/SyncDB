import unittest

from syncdb import Column

from .helpers import MemoryConnector, make_sync


def _source_with_id():
    return MemoryConnector(
        "mssql",
        "dbo",
        rows_by_table={("dbo", "t"): [{"id": 1}]},
        columns_by_table={("dbo", "t"): [Column("id", "int", is_primary_key=True)]},
    )


class TypeOverrideInjectionTests(unittest.TestCase):
    def test_malicious_type_override_value_is_rejected(self):
        source = _source_with_id()
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target)
        with self.assertRaises(ValueError):
            sync.sync_tables(
                {
                    "t": {
                        "source": "dbo.t",
                        "destination": "public.t",
                        "mode": "append",
                        "type_overrides": {"id": "int); DROP TABLE users;--"},
                    }
                }
            )

    def test_legitimate_type_override_is_applied(self):
        source = _source_with_id()
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target)
        sync.sync_tables(
            {
                "t": {
                    "source": "dbo.t",
                    "destination": "public.t",
                    "mode": "append",
                    "type_overrides": {"id": "numeric(20,0)"},
                }
            }
        )
        created = target.columns_by_table[("public", "t")]
        self.assertEqual(created[0].data_type, "numeric(20,0)")

    def test_malicious_rename_target_is_rejected(self):
        source = _source_with_id()
        target = MemoryConnector("postgresql", "public")
        sync = make_sync(source, target)
        with self.assertRaises(ValueError):
            sync.sync_tables(
                {
                    "t": {
                        "source": "dbo.t",
                        "destination": "public.t",
                        "mode": "append",
                        "rename": {"id": "id; DROP TABLE x"},
                    }
                }
            )


if __name__ == "__main__":
    unittest.main()
