import io
import os
import sys
import unittest

from syncdb import ProgressMode, ProgressReporter
from syncdb.progress import format_elapsed


def _show_progress_output(stream: io.StringIO) -> None:
    if not os.getenv("SYNCDB_TEST_LIVE_OUTPUT"):
        return
    sys.__stdout__.write("PROGRESS OUTPUT\n")
    sys.__stdout__.write(stream.getvalue())
    if not stream.getvalue().endswith("\n"):
        sys.__stdout__.write("\n")
    sys.__stdout__.flush()


class ProgressReporterTests(unittest.TestCase):
    def test_multi_line_progress_writes_each_update(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.MULTI_LINE, width=10, stream=stream)

        reporter.update("public.customers", 5, 10)
        reporter.update("public.customers", 10, 10)
        _show_progress_output(stream)

        output = stream.getvalue().splitlines()
        self.assertEqual(len(output), 2)
        self.assertIn("50%", output[0])
        self.assertIn("100%", output[1])

    def test_lowercase_strings_resolve_to_correct_progress_mode(self):
        self.assertIs(ProgressMode("one_line"), ProgressMode.ONE_LINE)
        self.assertIs(ProgressMode("multi_line"), ProgressMode.MULTI_LINE)
        self.assertIs(ProgressMode("none"), ProgressMode.NONE)

    def test_uppercase_strings_resolve_to_correct_progress_mode(self):
        self.assertIs(ProgressMode("ONE_LINE"), ProgressMode.ONE_LINE)
        self.assertIs(ProgressMode("MULTI_LINE"), ProgressMode.MULTI_LINE)
        self.assertIs(ProgressMode("NONE"), ProgressMode.NONE)

    def test_one_line_progress_uses_carriage_return_and_finish_newline(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.ONE_LINE, width=10, stream=stream)

        reporter.update("orders", 1, 2)
        reporter.finish()
        _show_progress_output(stream)

        self.assertTrue(stream.getvalue().startswith("\rorders"))
        self.assertTrue(stream.getvalue().endswith("\n"))

    def test_none_mode_produces_no_output(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.NONE, stream=stream)

        reporter.update("customers", 1000, 250000)
        reporter.finish()
        _show_progress_output(stream)

        self.assertEqual(stream.getvalue(), "")

    def test_update_without_total_shows_row_count(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.MULTI_LINE, stream=stream)

        reporter.update("payments", 5000)
        _show_progress_output(stream)

        self.assertIn("5,000 rows", stream.getvalue())

    def test_finish_is_idempotent_in_one_line_mode(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.ONE_LINE, stream=stream)

        reporter.update("orders", 1, 1)
        reporter.finish()
        reporter.finish()  # second call must not add another newline
        _show_progress_output(stream)

        self.assertEqual(stream.getvalue().count("\n"), 1)

    def test_start_commits_previous_one_line_row(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.ONE_LINE, stream=stream)
        reporter.start()
        reporter.update("a", 1, 2)
        reporter.start()  # commits the prior open line with a newline
        self.assertIn("\n", stream.getvalue())

    def test_invalid_progress_mode_raises(self):
        with self.assertRaises(ValueError):
            ProgressMode("sideways")

    def test_format_elapsed_units(self):
        self.assertEqual(format_elapsed(5.0), "5.0s")
        self.assertEqual(format_elapsed(75.0), "1m 15.0s")
        self.assertEqual(format_elapsed(3725.0), "1h 2m 5.0s")


if __name__ == "__main__":
    unittest.main()
