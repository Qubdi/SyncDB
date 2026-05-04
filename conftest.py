"""Pytest options for developer-facing SyncDB test output.

Normal test runs stay quiet so CI logs remain readable. When a developer wants
to inspect behavior interactively, --syncdb-live-output prints colored test
start/end sections, while --syncdb-live-output-detail also enables SyncDB
progress bars and per-sync summaries from tests that use shared make_sync helpers.
"""

from __future__ import annotations

from collections import Counter
import os
import time

import pytest


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--syncdb-live-output",
        action="store_true",
        default=False,
        help="Show readable colored test start/end blocks and final summary.",
    )
    parser.addoption(
        "--syncdb-live-output-detail",
        action="store_true",
        default=False,
        help="Show readable live output plus SyncDB progress bars and sync summaries.",
    )


def _live_enabled(config) -> bool:
    return bool(
        config.getoption("--syncdb-live-output")
        or config.getoption("--syncdb-live-output-detail")
    )


def _detail_enabled(config) -> bool:
    return bool(config.getoption("--syncdb-live-output-detail"))


def pytest_configure(config) -> None:
    if not _live_enabled(config):
        return

    # Keep pytest's own per-test node IDs quiet; this plugin prints a cleaner
    # workflow/scenario/test block instead.
    config.option.verbose = 0
    config.option.quiet = max(int(getattr(config.option, "quiet", 0) or 0), 2)
    config.option.capture = "no"
    config.option.tbstyle = "short"

    os.environ["SYNCDB_TEST_LIVE_OUTPUT"] = "1"
    if _detail_enabled(config):
        os.environ["SYNCDB_TEST_LIVE_OUTPUT_DETAIL"] = "1"
        os.environ.setdefault("SYNCDB_TEST_PROGRESS_MODE", "multi_line")
        os.environ.setdefault("SYNCDB_TEST_VERBOSE", "standard")
    else:
        os.environ.pop("SYNCDB_TEST_LIVE_OUTPUT_DETAIL", None)
        os.environ.setdefault("SYNCDB_TEST_PROGRESS_MODE", "none")
        os.environ.setdefault("SYNCDB_TEST_VERBOSE", "none")
    config._syncdb_live_summary = {
        "started_at": time.perf_counter(),
        "reports": [],
        "counts": Counter(),
    }


def pytest_report_header(config) -> str | None:
    if not _live_enabled(config):
        return None
    if _detail_enabled(config):
        return _color("SyncDB live output detail: start -> progress -> sync summary -> end -> summary", "cyan")
    return _color("SyncDB live output: start -> end -> summary", "cyan")


def pytest_runtest_logstart(nodeid: str, location) -> None:
    config = getattr(pytest_runtest_logstart, "_config", None)
    if config is None or not _live_enabled(config):
        return
    details = _describe_nodeid(nodeid)
    print("\n" + _color("=" * 78, "blue"), flush=True)
    print(f"{_color('WORKFLOW', 'cyan')} : {_color(details['workflow'], 'bold')}", flush=True)
    print(f"{_color('SCOPE', 'cyan')}    : {_color(details['scope'], 'bold')}", flush=True)
    if details["database"]:
        print(f"{_color('DATABASE', 'cyan')} : {_color(details['database'], 'yellow')}", flush=True)
    print(f"{_color('TEST', 'cyan')}     : {details['test']}", flush=True)
    print(_color("-" * 78, "blue"), flush=True)
    print(_color("START", "green"), flush=True)


def pytest_runtest_logreport(report) -> None:
    config = getattr(pytest_runtest_logreport, "_config", None)
    if config is None or not _live_enabled(config):
        return

    if report.when != "call":
        if report.when == "setup" and report.skipped:
            _record_report(config, report)
            details = _describe_nodeid(report.nodeid)
            print(
                f"{_color('END', 'yellow')}      : {_color('SKIPPED', 'yellow')} "
                f"({report.duration:.2f}s) - {details['test']}",
                flush=True,
            )
        return

    _record_report(config, report)
    status = "PASSED" if report.passed else "FAILED" if report.failed else "SKIPPED"
    status_color = "green" if report.passed else "red" if report.failed else "yellow"
    details = _describe_nodeid(report.nodeid)
    print(
        f"{_color('END', status_color)}      : {_color(status, status_color)} "
        f"({report.duration:.2f}s) - {details['test']}",
        flush=True,
    )


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:
    if not _live_enabled(config):
        return

    summary = getattr(config, "_syncdb_live_summary", None)
    if not summary:
        return

    elapsed = time.perf_counter() - summary["started_at"]
    counts = summary["counts"]
    reports = summary["reports"]
    slowest = sorted(reports, key=lambda item: item.duration, reverse=True)[:5]

    terminalreporter.write_sep("=", _color("SyncDB live test summary", "cyan"))
    terminalreporter.write_line(
        "Total: {total} | Passed: {passed} | Failed: {failed} | "
        "Skipped: {skipped} | Duration: {duration:.2f}s".format(
            total=len(reports),
            passed=_color(str(counts["passed"]), "green"),
            failed=_color(str(counts["failed"]), "red" if counts["failed"] else "green"),
            skipped=_color(str(counts["skipped"]), "yellow" if counts["skipped"] else "green"),
            duration=elapsed,
        )
    )
    if slowest:
        terminalreporter.write_line("Slowest tests:")
        for item in slowest:
            details = _describe_nodeid(item.nodeid)
            label = f"{details['workflow']} | {details['scope']} | {details['test']}"
            if details["database"]:
                label = f"{details['database']} | {label}"
            terminalreporter.write_line(f"  {item.duration:.2f}s  {label}")


def pytest_report_teststatus(report, config):
    if _live_enabled(config) and report.when == "call":
        return report.outcome, "", ""
    return None


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    config = item.config
    capture_manager = None
    if _live_enabled(config):
        capture_manager = config.pluginmanager.getplugin("capturemanager")
        if capture_manager is not None:
            capture_manager.stop_global_capturing()
            capture_manager._method = "no"
            capture_manager.start_global_capturing()
    outcome = yield
    if capture_manager is not None:
        capture_manager.suspend_global_capture(in_=True)
    return outcome


def pytest_sessionstart(session) -> None:
    config = session.config
    if _live_enabled(config):
        pytest_runtest_logstart._config = config
        pytest_runtest_logreport._config = config
        terminal_reporter = config.pluginmanager.getplugin("terminalreporter")
        if terminal_reporter is not None:
            terminal_reporter._showfspath = False
        capture_manager = config.pluginmanager.getplugin("capturemanager")
        if capture_manager is not None:
            capture_manager.suspend_global_capture(in_=True)


def _record_report(config, report) -> None:
    summary = getattr(config, "_syncdb_live_summary", None)
    if summary is None:
        return
    summary["reports"].append(report)
    if report.passed:
        summary["counts"]["passed"] += 1
    elif report.failed:
        summary["counts"]["failed"] += 1
    elif report.skipped:
        summary["counts"]["skipped"] += 1


def _describe_nodeid(nodeid: str) -> dict[str, str]:
    path, _, test_ref = nodeid.partition("::")
    parts = test_ref.split("::")
    class_name = parts[0] if len(parts) > 1 else ""
    test_name = parts[-1] if parts else nodeid

    workflow = "Component"
    scope = "General"
    database = ""

    if "DatabaseToDatabase" in path:
        workflow = "Database to Database"
        scope = "Whole Schema" if "/Schema/" in path.replace("\\", "/") else "Table Sync"
        database = _database_direction(class_name)
    elif "Components/progress" in path.replace("\\", "/"):
        workflow = "Component"
        scope = "Progress Bar"
    elif "Components/sync" in path.replace("\\", "/"):
        workflow = "Component"
        scope = "Sync Logic"
    elif "Components/files" in path.replace("\\", "/"):
        workflow = "Local File"
        scope = "Local to DB / DB to Local"
    elif "Components/connectors" in path.replace("\\", "/"):
        workflow = "Component"
        scope = "Connector"
    elif "Components/config" in path.replace("\\", "/"):
        workflow = "Component"
        scope = "Config"
    elif "Components/sql" in path.replace("\\", "/"):
        workflow = "Component"
        scope = "SQL Builder"
    elif "Components/type_mapping" in path.replace("\\", "/"):
        workflow = "Component"
        scope = "Type Mapping"

    return {
        "workflow": workflow,
        "scope": scope,
        "database": database,
        "test": _humanize_test_name(test_name),
    }


def _database_direction(class_name: str) -> str:
    value = class_name.removeprefix("Test")
    engines = ("Postgresql", "Mssql", "Mysql")
    for source in engines:
        prefix = f"{source}To"
        if not value.startswith(prefix):
            continue
        rest = value[len(prefix):]
        for target in engines:
            if rest.startswith(target):
                return f"{_display_engine(source)} -> {_display_engine(target)}"
    return ""


def _display_engine(value: str) -> str:
    normalized = value.lower()
    aliases = {
        "postgresql": "PGSQL",
        "postgres": "PGSQL",
        "pgsql": "PGSQL",
        "mssql": "MSSQL",
        "mysql": "MySQL",
        "sqlite": "SQLite",
    }
    return aliases.get(normalized, value)


def _humanize_test_name(name: str) -> str:
    if name.startswith("test_"):
        name = name[5:]
    return name.replace("_", " ").strip().capitalize()


def _color(text: str, color: str) -> str:
    if os.getenv("NO_COLOR"):
        return text
    codes = {
        "bold": "1",
        "red": "31",
        "green": "32",
        "yellow": "33",
        "blue": "34",
        "magenta": "35",
        "cyan": "36",
    }
    code = codes.get(color)
    if not code:
        return text
    return f"\033[{code}m{text}\033[0m"
