"""Retry helper for transient database write failures."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


def with_retries(
    operation: Callable[[], Any],
    retry_count: int,
    retry_delay_seconds: float,
    on_retry: Callable[[], None] | None = None,
) -> Any:
    """Run operation with exponential backoff and full jitter on failure.

    cap grows as 2**attempt * retry_delay_seconds; actual delay is uniform in
    [0, cap] to spread retries across time when many parallel jobs hit the same
    transient failure (thundering-herd prevention).

    on_retry is called after the sleep and before re-invoking operation; use it
    to reconnect a database connection that may have dropped during the failure.

    IMPORTANT — at-least-once semantics: operation is re-run in full.  If the
    failure occurred AFTER the database durably committed (e.g. a lost commit
    acknowledgement), a non-idempotent operation is applied twice.  Batch
    writes with a primary key (APPEND delete+insert, UPSERT) tolerate this;
    INSERT_ONLY and SNAPSHOT writes do not and can duplicate rows — see the
    TransferMode docstrings.
    """
    attempt = 0
    while True:
        try:
            return operation()
        except Exception as exc:
            if attempt >= retry_count:
                raise
            cap = retry_delay_seconds * (2**attempt)
            delay = random.uniform(0, cap)
            logger.warning(
                "Retry %d/%d after error: %s — sleeping %.2fs",
                attempt + 1,
                retry_count,
                exc,
                delay,
            )
            time.sleep(delay)
            if on_retry is not None:
                on_retry()
            attempt += 1
