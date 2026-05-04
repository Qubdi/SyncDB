"""Summary reporting for SyncDB sync runs.

Renders a final ASCII table to a configurable stream when verbose mode is
enabled.  Keeping this out of core.py lets the SyncDB class stay focused on
orchestration logic while this module owns all formatting details.
"""

from __future__ import annotations

from typing import IO, Any

from ..progress import _format_elapsed
from .models import TableSyncResult


def emit_summary(results: list[TableSyncResult], verbose: str | None, stream: IO[str]) -> None:
    """Print a final sync summary when verbose mode is enabled."""
    if verbose is None:
        return
    if verbose == "standard":
        headers = ["table", "mode", "rows written", "batches", "created", "time"]
        rows = [
            [
                result.destination,
                result.mode,
                f"{result.rows_written:,}",
                str(result.batches),
                "yes" if result.table_created else "no",
                _format_elapsed(result.duration_seconds),
            ]
            for result in results
        ]
    else:
        headers = [
            "name", "source", "destination", "mode",
            "read", "written", "soft deleted", "batches",
            "schema", "table", "added", "dropped",
            "checks", "watermark", "dry run", "time",
        ]
        rows = [
            [
                result.name,
                result.source,
                result.destination,
                result.mode,
                f"{result.rows_read:,}",
                f"{result.rows_written:,}",
                f"{result.rows_soft_deleted:,}",
                str(result.batches),
                "yes" if result.schema_created else "no",
                "yes" if result.table_created else "no",
                ", ".join(result.columns_added) or "-",
                ", ".join(result.columns_dropped) or "-",
                "fail" if result.expectations_failed else "ok",
                str(result.watermark_value) if result.watermark_value is not None else "-",
                "yes" if result.dry_run else "no",
                _format_elapsed(result.duration_seconds),
            ]
            for result in results
        ]

    total_duration = sum(r.duration_seconds for r in results)
    stream.write(f"\nSyncDB summary ({verbose})\n")
    _write_table(headers, rows, stream)
    stream.write(
        f"total: {sum(result.rows_written for result in results):,} rows "
        f"in {sum(result.batches for result in results):,} batches "
        f"across {len(results):,} tables "
        f"in {_format_elapsed(total_duration)}\n"
    )
    stream.flush()


def _write_table(headers: list[str], rows: list[list[str]], stream: IO[str]) -> None:
    """Render a small ASCII table to stream."""
    widths = [
        max(len(header), *(len(row[index]) for row in rows)) if rows else len(header)
        for index, header in enumerate(headers)
    ]
    separator = "+" + "+".join("-" * (width + 2) for width in widths) + "+\n"
    header_line = "| " + " | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)) + " |\n"
    stream.write(separator)
    stream.write(header_line)
    stream.write(separator)
    for row in rows:
        stream.write(
            "| " + " | ".join(value.ljust(widths[index]) for index, value in enumerate(row)) + " |\n"
        )
    stream.write(separator)
