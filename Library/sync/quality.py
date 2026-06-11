"""Data quality expectations for SyncDB.

After a table sync completes, the optional `expect` spec is evaluated against
the target table using SQL aggregation queries.  No rows are loaded into Python
memory; all checks push work down to the database engine.

Supported checks:
  min_rows   - SELECT COUNT(*) must be >= threshold
  not_null   - SELECT COUNT(*) WHERE col IS NULL must be 0 for each column
  unique     - duplicate-row count must be 0 for each key set
  range      - SELECT MIN/MAX must be within [min, max] bounds for each column
"""

from __future__ import annotations

from typing import Any

from ..connectors.base import BaseConnector, result_scalar
from ..sql import quote_identifier, validate_identifier
from .models import TableSyncResult


def validate_expectations(
    target: BaseConnector,
    schema: str | None,
    table: str,
    expect: dict[str, Any] | None,
    result: TableSyncResult,
    batch_size: int,
) -> None:
    """Run optional data-quality checks after a table sync using SQL aggregates."""
    if not expect:
        return
    failures: list[str] = []
    tbl = target.quote_table(schema, table)
    q = target.quote_char

    min_rows = expect.get("min_rows")
    if min_rows is not None:
        rows = target.execute_query(f"SELECT COUNT(*) AS n FROM {tbl}")
        count = int(result_scalar(rows, "n"))
        if count < int(min_rows):
            failures.append(f"expected at least {min_rows} rows, found {count}")

    for column in expect.get("not_null", []) or []:
        validate_identifier(column)
        col_ref = quote_identifier(column, q)
        rows = target.execute_query(
            f"SELECT COUNT(*) AS n FROM {tbl} WHERE {col_ref} IS NULL"
        )
        null_count = int(result_scalar(rows, "n"))
        if null_count:
            failures.append(f"{column} has {null_count} null values")

    for key in expect.get("unique", []) or []:
        columns = [key] if isinstance(key, str) else list(key)
        for col in columns:
            validate_identifier(col)
        col_refs = ", ".join(quote_identifier(c, q) for c in columns)

        if len(columns) == 1:
            # Single-column: COUNT(*) - COUNT(DISTINCT col) is standard SQL on all engines.
            rows = target.execute_query(
                f"SELECT COUNT(*) - COUNT(DISTINCT {col_refs}) AS dups FROM {tbl}"
            )
        else:
            # Multi-column: COUNT(DISTINCT col1, col2) is not standard SQL — MySQL rejects
            # it and other engines vary.  Use a portable subquery instead.
            rows = target.execute_query(
                f"SELECT (SELECT COUNT(*) FROM {tbl}) - "
                f"(SELECT COUNT(*) FROM "
                f"(SELECT DISTINCT {col_refs} FROM {tbl}) AS __syncdb_distinct) "
                f"AS dups"
            )

        dups = int(result_scalar(rows, "dups"))
        if dups:
            failures.append(f"{', '.join(columns)} has {dups} duplicate rows")

    for column, bounds in (expect.get("range") or {}).items():
        validate_identifier(column)
        col_ref = quote_identifier(column, q)
        minimum = bounds.get("min")
        maximum = bounds.get("max")
        rows = target.execute_query(
            f"SELECT MIN({col_ref}) AS lo, MAX({col_ref}) AS hi FROM {tbl} WHERE {col_ref} IS NOT NULL"
        )
        if rows:
            lo = result_scalar(rows, "lo", default=None)
            hi = result_scalar(rows, "hi", default=None)
            if minimum is not None and lo is not None and lo < minimum:
                failures.append(f"{column} has value below {minimum}: {lo}")
            if maximum is not None and hi is not None and hi > maximum:
                failures.append(f"{column} has value above {maximum}: {hi}")

    result.expectations_failed = failures
    if failures:
        raise ValueError(f"Data quality checks failed for {result.destination}: " + "; ".join(failures))
