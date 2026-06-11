import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from syncdb import TableSyncResult, cli


class CliParserTests(unittest.TestCase):
    def test_version_flag_exits_zero(self):
        with self.assertRaises(SystemExit) as ctx:
            cli.main(["--version"])
        self.assertEqual(ctx.exception.code, 0)

    def test_missing_command_errors(self):
        # argparse exits with code 2 on a usage error (no subcommand given).
        with self.assertRaises(SystemExit) as ctx:
            cli.main([])
        self.assertEqual(ctx.exception.code, 2)


class CliRunTests(unittest.TestCase):
    def test_run_invokes_run_config_file_and_returns_zero(self):
        fake_results = [
            TableSyncResult(name="t", source="a", destination="b", mode="append", rows_written=5),
        ]
        with mock.patch.object(cli.SyncDB, "run_config_file", return_value=fake_results) as run:
            code = cli.main(["run", "job.json"])
        self.assertEqual(code, 0)
        run.assert_called_once_with("job.json")

    def test_run_missing_file_returns_two(self):
        with mock.patch.object(cli.SyncDB, "run_config_file", side_effect=FileNotFoundError()):
            code = cli.main(["run", "nope.json"])
        self.assertEqual(code, 2)

    def test_run_generic_error_returns_one(self):
        with mock.patch.object(cli.SyncDB, "run_config_file", side_effect=ValueError("bad config")):
            code = cli.main(["run", "job.json"])
        self.assertEqual(code, 1)

    def test_run_end_to_end_sqlite_files(self):
        # A real sqlite→sqlite job exercises the full config → connector → sync path
        # without any optional driver.  Separate files so source and target persist.
        with TemporaryDirectory() as tmp:
            src_db = Path(tmp) / "src.sqlite"
            dst_db = Path(tmp) / "dst.sqlite"

            import sqlite3
            con = sqlite3.connect(src_db)
            con.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)")
            con.executemany("INSERT INTO people VALUES (?, ?)", [(1, "Ana"), (2, "Gio")])
            con.commit()
            con.close()

            config = {
                "source": {"engine": "sqlite", "database": str(src_db)},
                "target": {"engine": "sqlite", "database": str(dst_db)},
                "settings": {"progress_mode": "none", "verbose": None},
                "tables": {"people": {"source": "people", "destination": "people", "mode": "append"}},
            }
            config_path = Path(tmp) / "job.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")

            code = cli.main(["run", str(config_path)])
            self.assertEqual(code, 0)

            con = sqlite3.connect(dst_db)
            rows = con.execute("SELECT id, name FROM people ORDER BY id").fetchall()
            con.close()
            self.assertEqual(rows, [(1, "Ana"), (2, "Gio")])


if __name__ == "__main__":
    unittest.main()
