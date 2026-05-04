import io
import unittest

from syncdb import ProgressMode, ProgressReporter


class ProgressReporterTests(unittest.TestCase):
    def test_multi_line_progress_writes_each_update(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.MULTI_LINE, width=10, stream=stream)

        reporter.update("public.customers", 5, 10)
        reporter.update("public.customers", 10, 10)

        output = stream.getvalue().splitlines()
        self.assertEqual(len(output), 2)
        self.assertIn("50%", output[0])
        self.assertIn("100%", output[1])

    def test_lowercase_progress_mode_aliases_match_string_values(self):
        self.assertIs(ProgressMode.one_line, ProgressMode.ONE_LINE)
        self.assertIs(ProgressMode.multi_line, ProgressMode.MULTI_LINE)
        self.assertIs(ProgressMode.none, ProgressMode.NONE)

    def test_one_line_progress_uses_carriage_return_and_finish_newline(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.ONE_LINE, width=10, stream=stream)

        reporter.update("orders", 1, 2)
        reporter.finish()

        self.assertTrue(stream.getvalue().startswith("\rorders"))
        self.assertTrue(stream.getvalue().endswith("\n"))

    def test_none_mode_produces_no_output(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.NONE, stream=stream)

        reporter.update("customers", 1000, 250000)
        reporter.finish()

        self.assertEqual(stream.getvalue(), "")

    def test_update_without_total_shows_row_count(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.MULTI_LINE, stream=stream)

        reporter.update("payments", 5000)

        self.assertIn("5000 rows", stream.getvalue())

    def test_finish_is_idempotent_in_one_line_mode(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.ONE_LINE, stream=stream)

        reporter.update("orders", 1, 1)
        reporter.finish()
        reporter.finish()  # second call must not add another newline

        self.assertEqual(stream.getvalue().count("\n"), 1)


if __name__ == "__main__":
    unittest.main()
