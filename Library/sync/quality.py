"""Data quality expectations for SyncDB.

After a table sync completes, the optional `expect` spec is evaluated against
the target table.  All checks are intentionally simple and suitable for tables
up to a few million rows.  For very large tables, prefer SQL-based monitoring
(dbt tests, Great Expectations) over this in-memory approach.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..sql import validate_identifier
from .models import TableSyncResult

if TYPE_CHECKING:
    from ..connectors.base import BaseConnector


def validate_expectations(
    target: "BaseConnector",
    schema: str | None,
    table: str,
    expect: dict[str, Any] | None,
    result: TableSyncResult,
    batch_size: int,
) -> None:
    """Run optional data-quality checks after a table sync."""
    if not expect:
        return
    rows = [row for batch in target.fetch_batches(schema, table, batch_size=batch_size) for row in batch]
    failures: list[str] = []

    min_rows = expect.get("min_rows")
    if min_rows is not None and len(rows) < int(min_rows):
        failures.append(f"expected at least {min_rows} rows, found {len(rows)}")

    for column in expect.get("not_null", []) or []:
        validate_identifier(column)
        null_count = sum(1 for row in rows if row.get(column) is None)
        if null_count:
            failures.append(f"{column} has {null_count} null values")

    for key in expect.get("unique", []) or []:
        columns = [key] if isinstance(key, str) else list(key)
        for column in columns:
            validate_identifier(column)
        seen: set[tuple[Any, ...]] = set()
        duplicates = 0
        for row in rows:
            value = tuple(row.get(column) for column in columns)
            if value in seen:
                duplicates += 1
            seen.add(value)
        if duplicates:
            failures.append(f"{', '.join(columns)} has {duplicates} duplicate rows")

    for column, bounds in (expect.get("range") or {}).items():
        validate_identifier(column)
        minimum = bounds.get("min")
        maximum = bounds.get("max")
        for row in rows:
            value = row.get(column)
            if value is None:
                continue
            if minimum is not None and value < minimum:
                failures.append(f"{column} has value below {minimum}: {value}")
                break
            if maximum is not None and value > maximum:
                failures.append(f"{column} has value above {maximum}: {value}")
                break

    result.expectations_failed = failures
    if failures:
        raise ValueError(f"Data quality checks failed for {result.destination}: " + "; ".join(failures))
