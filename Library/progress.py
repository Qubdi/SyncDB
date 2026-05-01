"""Progress reporting for interactive terminals and log-like environments."""

from __future__ import annotations

import sys
from enum import Enum
from typing import TextIO


class ProgressMode(str, Enum):
    ONE_LINE = "one_line"
    MULTI_LINE = "multi_line"
    NONE = "none"


class ProgressReporter:
    def __init__(
        self,
        mode: ProgressMode | str = ProgressMode.MULTI_LINE,
        width: int = 40,
        stream: TextIO | None = None,
    ) -> None:
        self.mode = ProgressMode(mode)
        self.width = width
        self.stream = stream or sys.stdout
        self._last_line_open = False

    def update(self, label: str, current: int, total: int | None = None) -> None:
        if self.mode == ProgressMode.NONE:
            return
        line = self._format_line(label, current, total)
        if self.mode == ProgressMode.ONE_LINE:
            self.stream.write("\r" + line)
            self.stream.flush()
            self._last_line_open = True
            return
        self.stream.write(line + "\n")
        self.stream.flush()

    def finish(self) -> None:
        if self.mode == ProgressMode.ONE_LINE and self._last_line_open:
            self.stream.write("\n")
            self.stream.flush()
            self._last_line_open = False

    def _format_line(self, label: str, current: int, total: int | None) -> str:
        if not total or total <= 0:
            return f"{label} {current} rows"
        ratio = min(max(current / total, 0.0), 1.0)
        filled = int(self.width * ratio)
        bar = "#" * filled + "." * (self.width - filled)
        percent = int(ratio * 100)
        return f"{label} [{bar}] {percent:3d}% ({current}/{total})"
