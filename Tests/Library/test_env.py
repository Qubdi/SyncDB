"""Shared test environment helpers.

Reads the SYNCDB_TEST_* environment variables that conftest.py sets when
--syncdb-live-output or --syncdb-live-output-detail flags are active.
Both the unit-test helpers (Components/sync/helpers.py) and integration-test
helpers (DatabaseToDatabase/helpers.py) import from here so the env-var names
stay consistent in one place.
"""

from __future__ import annotations

import os

from syncdb import ProgressMode


def live_output_enabled() -> bool:
    return os.getenv("SYNCDB_TEST_LIVE_OUTPUT_DETAIL", "").strip().lower() in {"1", "true", "yes", "on"}


def live_progress_mode() -> ProgressMode:
    value = os.getenv("SYNCDB_TEST_PROGRESS_MODE", ProgressMode.MULTI_LINE.value)
    return ProgressMode(value)


def live_verbose() -> str | None:
    detail = live_output_enabled()
    return os.getenv("SYNCDB_TEST_VERBOSE") if detail else None
