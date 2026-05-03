"""Progress reporting for interactive terminals and log-like environments.

This module deliberately stays dependency-free and writes plain ASCII progress
bars. Progress output is part of operator experience, but it must also be safe
for CI logs, Windows terminals, and redirected files.

Three modes are supported:
  ONE_LINE   - overwrites the same terminal line using a carriage return (\r).
               Ideal for interactive terminals; ugly in CI logs.
  MULTI_LINE - emits a new line per batch update. Safe for log aggregators.
  NONE       - silent; no output at all.
"""

from __future__ import annotations

import sys
import time
from enum import Enum
from typing import TextIO


class ProgressMode(str, Enum):
    # Each mode has both an UPPER_CASE and lowercase alias so that both
    #   ProgressMode("ONE_LINE")  and  ProgressMode("one_line")
    # resolve to the same member.  This matters for YAML/JSON job configs where
    # the value may be typed in lowercase.  The canonical storage value is
    # always lowercase (e.g. "one_line") to match the Enum(str) contract.
    ONE_LINE = "one_line"
    one_line = "one_line"
    MULTI_LINE = "multi_line"
    multi_line = "multi_line"
    NONE = "none"
    none = "none"


class ProgressReporter:
    """Small progress writer used by SyncDB batch operations.

    Call start() when a new table begins, then update() once per batch.
    In ONE_LINE mode start() commits the previous table's line so each table
    gets its own permanent output row.
    """

    def __init__(
        self,
        mode: ProgressMode | str = ProgressMode.MULTI_LINE,
        width: int = 36,
        stream: TextIO | None = None,
    ) -> None:
        self.mode = ProgressMode(mode)
        self.width = width
        self.label_width: int = 0
        # Allow callers to inject a custom stream for testing or log capture.
        self.stream = stream or sys.stdout
        # Tracks whether a \r-terminated line is waiting for a terminal newline.
        # finish() must emit \n before any other output to avoid corrupted lines.
        self._last_line_open = False
        self._start_time: float | None = None

    def start(self) -> None:
        """Begin a new table.

        In ONE_LINE mode this commits the previous table's final line to its
        own row so that completed tables remain visible as new tables run.
        Resets the elapsed-time counter.
        """
        if self.mode == ProgressMode.ONE_LINE and self._last_line_open:
            self.stream.write("\n")
            self.stream.flush()
            self._last_line_open = False
        self._start_time = time.monotonic()

    def update(self, label: str, current: int, total: int | None = None) -> None:
        """Emit a progress line.  Call once per batch with the running row count."""
        if self.mode == ProgressMode.NONE:
            return
        elapsed = time.monotonic() - self._start_time if self._start_time is not None else None
        line = self._format_line(label, current, total, elapsed)
        if self.mode == ProgressMode.ONE_LINE:
            # \r moves the cursor to the start of the current line so the next
            # write overwrites it rather than appending a new line.
            self.stream.write("\r" + line)
            self.stream.flush()
            self._last_line_open = True
            return
        self.stream.write(line + "\n")
        self.stream.flush()

    def finish(self) -> None:
        """Seal the last ONE_LINE update with a proper newline.

        Call after all batches are done.  Without this, the shell prompt would
        appear on the same line as the final progress output.
        """
        if self.mode == ProgressMode.ONE_LINE and self._last_line_open:
            self.stream.write("\n")
            self.stream.flush()
            self._last_line_open = False

    def _format_line(
        self,
        label: str,
        current: int,
        total: int | None,
        elapsed: float | None,
    ) -> str:
        """Build the progress string.

        Falls back to a plain "N rows" message when total is unknown (e.g. when
        the connector lacks SELECT COUNT(*) permission; see SyncDB._safe_source_count).
        Format: label  [=======>...........]  45%  4,500 / 10,000  1.2s
        """
        padded_label = f"{label:<{self.label_width}}" if self.label_width else label
        elapsed_str = f"  {_format_elapsed(elapsed)}" if elapsed is not None else ""
        if not total or total <= 0:
            return f"{padded_label}  {current:,} rows{elapsed_str}"
        ratio = min(max(current / total, 0.0), 1.0)
        if ratio >= 1.0:
            bar = "=" * self.width
        else:
            filled = int(self.width * ratio)
            bar = "=" * filled + ">" + "." * (self.width - filled - 1)
        percent = int(ratio * 100)
        return f"{padded_label}  [{bar}]  {percent:3d}%  {current:>10,} / {total:,}{elapsed_str}"


def _format_elapsed(seconds: float) -> str:
    """Format a duration as '1.2s', '1m 2.3s', or '1h 3m 4.5s'."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{int(m)}m {s:.1f}s"
    h, m = divmod(m, 60)
    return f"{int(h)}h {int(m)}m {s:.1f}s"
