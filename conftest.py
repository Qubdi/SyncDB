"""Pytest options for developer-facing SyncDB test output.

Normal test runs stay quiet so CI logs remain readable. When a developer wants
to inspect behavior interactively, --syncdb-live-output switches pytest into a
diagnostic mode that shows every test name, live stdout/stderr, SyncDB progress
updates, and final sync summaries from tests that use the shared make_sync helper.
"""

from __future__ import annotations

import os


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--syncdb-live-output",
        action="store_true",
        default=False,
        help="Show each test with live SyncDB progress and summary output.",
    )


def pytest_configure(config) -> None:
    if not config.getoption("--syncdb-live-output"):
        return

    # Equivalent to running with -vv -s, but tied to the SyncDB-specific flag so
    # developers do not have to remember several pytest switches.
    config.option.verbose = max(int(getattr(config.option, "verbose", 0) or 0), 2)
    config.option.capture = "no"

    os.environ["SYNCDB_TEST_LIVE_OUTPUT"] = "1"
    os.environ.setdefault("SYNCDB_TEST_PROGRESS_MODE", "multi_line")
    os.environ.setdefault("SYNCDB_TEST_VERBOSE", "standard")


def pytest_report_header(config) -> str | None:
    if not config.getoption("--syncdb-live-output"):
        return None
    return "SyncDB live test output: enabled"
