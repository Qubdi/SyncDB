"""Command-line interface for SyncDB.

Exposed as the ``syncdb`` console script (see pyproject.toml [project.scripts])
and runnable as ``python -m syncdb``.  Intentionally thin: it parses arguments,
configures logging, and delegates to the same public API that library callers
use, so the CLI never grows behavior the programmatic API lacks.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from . import __version__
from .sync import SyncDB


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser for the syncdb command."""
    parser = argparse.ArgumentParser(
        prog="syncdb",
        description="Move tabular data between MSSQL, PostgreSQL, MySQL, SQLite, and files.",
    )
    parser.add_argument("--version", action="version", version=f"SyncDB {__version__}")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable INFO-level logging to stderr.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run a table-sync job from a JSON or YAML config file.")
    run.add_argument("config", help="Path to a .json, .yaml, or .yml job config file.")
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan and report schema changes without writing any data or DDL.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the syncdb console script.  Returns a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.command == "run":
        try:
            # --dry-run forces a preview; without the flag the job file's own
            # settings.dry_run (if any) stays in effect.
            results = SyncDB.run_config_file(args.config, dry_run=True if args.dry_run else None)
        except FileNotFoundError:
            print(f"error: config file not found: {args.config}", file=sys.stderr)
            return 2
        except Exception as exc:  # surface a clean message, not a traceback, to the shell
            print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1
        total = sum(r.rows_written for r in results)
        if args.dry_run:
            print(f"Dry run: planned {len(results)} table(s); no data written.")
        else:
            print(f"Synced {len(results)} table(s); {total:,} rows written.")
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2  # unreachable; parser.error exits


if __name__ == "__main__":
    raise SystemExit(main())
