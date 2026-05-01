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

    def test_one_line_progress_uses_carriage_return_and_finish_newline(self):
        stream = io.StringIO()
        reporter = ProgressReporter(ProgressMode.ONE_LINE, width=10, stream=stream)

        reporter.update("orders", 1, 2)
        reporter.finish()

        self.assertTrue(stream.getvalue().startswith("\rorders"))
        self.assertTrue(stream.getvalue().endswith("\n"))


if __name__ == "__main__":
    unittest.main()
