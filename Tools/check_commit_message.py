#!/usr/bin/env python3
"""Reject low-information commit messages (pre-commit, commit-msg stage).

The repository's history of bare "update" commits made `git bisect` and
release archaeology impossible; this hook enforces Conventional Commits
subjects going forward:

    <type>(<optional scope>): <summary>

Allowed types: feat fix docs style refactor perf test build chore ci revert

Examples that pass:
    feat(connectors): add COPY FROM bulk-load path for PostgreSQL
    fix: cap the Windows watermark lock retry loop
    chore: release v2.2.0

Git-generated subjects (Merge/Revert/fixup!/squash!) pass through untouched.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_TYPES = "feat|fix|docs|style|refactor|perf|test|build|chore|ci|revert"
_PATTERN = re.compile(rf"^({_TYPES})(\([\w./-]+\))?!?: \S.*")
_GENERATED_PREFIXES = ("Merge ", "Revert ", "fixup! ", "squash! ")


def main() -> int:
    # Windows consoles may use cp1252; never let the error report itself crash.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    message_file = Path(sys.argv[1])
    # utf-8-sig strips a BOM if an editor or shell redirect added one.
    lines = message_file.read_text(encoding="utf-8-sig").splitlines()
    subject = next((line.strip() for line in lines if line.strip() and not line.startswith("#")), "")

    if not subject:
        print("error: empty commit message")
        return 1
    if subject.startswith(_GENERATED_PREFIXES):
        return 0
    if _PATTERN.match(subject):
        return 0

    print(
        f"error: commit subject does not follow Conventional Commits:\n"
        f"    {subject}\n"
        f"Expected:  <type>(<optional scope>): <summary>\n"
        f"Types:     {_TYPES.replace('|', ' ')}\n"
        f"Example:   fix(watermark): bound the Windows lock retry loop"
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
