"""Incremental watermark tracking for SyncDB.

Watermarks persist the maximum processed cursor value between runs so the next
sync only fetches rows newer than the last run.  This is an at-least-once
guarantee: if a sync fails mid-stream the file is NOT updated, meaning the next
run re-reads from the last persisted value and may re-process some rows.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..sql import quote_identifier, validate_identifier


def load_watermark(spec: dict[str, Any]) -> dict[str, Any] | None:
    """Load incremental-sync state for a table spec, if configured."""
    column = spec.get("incremental_column")
    store = spec.get("watermark_store")
    if not column:
        return None
    validate_identifier(column)
    path = Path(store or ".syncdb_watermarks.json")
    key = spec.get("watermark_key") or f"{spec['source']}->{spec['destination']}:{column}"
    values = read_watermark_file(path)
    return {"path": path, "key": key, "column": column, "value": values.get(key)}


def read_watermark_file(path: Path) -> dict[str, Any]:
    """Read the JSON watermark store, returning an empty mapping when absent."""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def save_watermark(config: dict[str, Any], value: Any) -> None:
    """Persist the latest processed incremental value after a successful sync."""
    path: Path = config["path"]
    values = read_watermark_file(path)
    values[config["key"]] = value.isoformat() if hasattr(value, "isoformat") else value
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(values, handle, indent=2, sort_keys=True)


def apply_watermark_filter(
    where_sql: str,
    params: list[Any],
    column: str,
    value: Any,
    quote_char: str,
    placeholder: str,
) -> tuple[str, list[Any]]:
    """Append an incremental-column predicate to an existing WHERE clause."""
    if value in {None, ""}:
        return where_sql, params
    condition = f"{quote_identifier(column, quote_char)} > {placeholder}"
    if not where_sql:
        return f" WHERE {condition} ", [*params, value]
    existing = where_sql.strip()
    if existing.upper().startswith("WHERE "):
        existing = existing[6:].strip()
    return f" WHERE ({existing}) AND ({condition}) ", [*params, value]


def max_watermark_value(current: Any, rows: list[dict[str, Any]], column: str) -> Any:
    """Track the maximum non-null watermark value seen across fetched batches."""
    values = [row.get(column) for row in rows if row.get(column) is not None]
    if not values:
        return current
    batch_max = max(values)
    if current is None or batch_max > current:
        return batch_max
    return current
