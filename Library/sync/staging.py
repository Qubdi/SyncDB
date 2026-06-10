"""Staging-table swap helpers for APPEND_STAGING transfer mode."""
from __future__ import annotations

import contextlib
import uuid
import warnings
from collections.abc import Callable, Sequence
from typing import Any

from ..type_mapping import Column


def create_staging_table(
    connector: Any,
    schema: str | None,
    table: str,
    columns: Sequence[Column],
    uid: str | None = None,
) -> str:
    """Drop and recreate a staging table, returning its name.

    The uid suffix (8-char hex token) ensures that concurrent syncs of the same
    table do not collide on the staging table name.  Always drops first so stale
    tables from failed previous runs never block re-runs.  The caller must drop
    the staging table in a finally block.
    """
    token = uid or uuid.uuid4().hex[:8]
    # Truncate the base table name to keep the full staging name within the
    # 128-character identifier limit imposed by MSSQL.
    staging = f"__syncdb_{table[:48]}_{token}_stg"
    connector.drop_table(schema, staging)
    connector.create_table(schema, staging, columns)
    return staging


def replace_from_staging(
    connector: Any,
    schema: str | None,
    table: str,
    staging_table: str,
    columns: Sequence[str],
    retry_fn: Callable[[Callable[[], None]], None],
) -> None:
    """Replace live table contents from a staging table.

    Truncates the live table and copies all rows from staging inside an explicit
    transaction so the live table is never left empty if the copy fails.
    Wrapped in retry_fn so transient failures on the swap step benefit from the
    same backoff policy as batch writes.

    NOTE: MySQL TRUNCATE TABLE is DDL and auto-commits regardless of transaction
    state, so the empty-table window cannot be eliminated on MySQL.  PostgreSQL
    and MSSQL roll back TRUNCATE correctly.
    """
    engine = getattr(connector, "engine", None)
    if engine == "mysql":
        warnings.warn(
            "APPEND_STAGING on MySQL: TRUNCATE TABLE is DDL and cannot be rolled back. "
            "A brief window exists where the live table is empty between TRUNCATE and the "
            "staging copy.  Use PostgreSQL or MSSQL for fully atomic staging swaps, or "
            "schedule this sync during a maintenance window.",
            RuntimeWarning,
            stacklevel=3,
        )

    def swap() -> None:
        already_in_tx = connector.is_in_transaction
        if not already_in_tx:
            connector.begin()
        try:
            connector.truncate_table(schema, table)
            connector.copy_table_rows(schema, staging_table, schema, table, columns)
            if not already_in_tx:
                connector.commit()
        except Exception:
            if not already_in_tx:
                with contextlib.suppress(Exception):
                    connector.rollback()
            raise

    retry_fn(swap)
