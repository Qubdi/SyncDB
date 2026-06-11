"""Incremental watermark tracking for SyncDB.

Watermarks persist the maximum processed cursor value between runs so the next
sync only fetches rows newer than the last run.  This is an at-least-once
guarantee: if a sync fails mid-stream the file is NOT updated, meaning the next
run re-reads from the last persisted value and may re-process some rows.

Boundary-row caveat
-------------------
The default comparison is strict (``column > watermark``): a row committed AFTER
a sync finishes but carrying a timestamp EQUAL to the saved watermark is skipped
forever.  This happens with low-resolution timestamp columns or transactions that
commit out of timestamp order.  Set ``"watermark_comparison": ">="`` in the table
spec to re-read boundary rows each run — safe (idempotent) with the ``upsert``
mode, or ``append`` mode with a primary key, both of which replace re-processed
rows instead of duplicating them.  Do NOT combine ``>=`` with ``insert_only``,
``snapshot``, or PK-less specs: re-read rows would be inserted again.

Concurrency limitation
----------------------
The watermark store is a local JSON file.  save_watermark() writes atomically
(temp file + os.replace) so a single process never sees a half-written file, but
there is NO cross-process locking.  Two processes syncing the SAME table key
concurrently (e.g. overlapping cron runs, or multiple Kubernetes replicas) can
interleave their read-modify-write and lose one update.  For multi-writer
deployments, give each writer a distinct watermark_store path, serialise the runs,
or keep watermark state in a database keyed by the sync identity instead.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from ..sql import quote_identifier, validate_identifier


def _resolve_watermark_path(store: str | None) -> Path:
    """Return a safe Path for the watermark store.

    Relative paths must not contain '..' path components — that would allow a
    job config to write watermarks outside the working directory.  Absolute
    paths are accepted as-is (useful for production deployments that route
    state files to a dedicated directory).
    """
    if not store:
        return Path(".syncdb_watermarks.json")
    path = Path(store)
    if not path.is_absolute() and ".." in path.parts:
        raise ValueError(
            f"watermark_store '{store}' must not contain '..'. "
            "Use an absolute path to reference a directory outside the working directory."
        )
    return path


def load_watermark(spec: dict[str, Any]) -> dict[str, Any] | None:
    """Load incremental-sync state for a table spec, if configured.

    spec["watermark_comparison"] selects the filter operator: ">" (default,
    strict — see the boundary-row caveat in the module docstring) or ">="
    (inclusive — re-reads boundary rows; pair with an idempotent mode).
    """
    column = spec.get("incremental_column")
    store = spec.get("watermark_store")
    if not column:
        return None
    validate_identifier(column)
    comparison = str(spec.get("watermark_comparison", ">")).strip()
    if comparison not in {">", ">="}:
        raise ValueError(
            f"watermark_comparison must be '>' or '>=', got {comparison!r}"
        )
    path = _resolve_watermark_path(store)
    key = spec.get("watermark_key") or f"{spec['source']}->{spec['destination']}:{column}"
    values = read_watermark_file(path)
    return {
        "path": path,
        "key": key,
        "column": column,
        "value": values.get(key),
        "comparison": comparison,
    }


def read_watermark_file(path: Path) -> dict[str, Any]:
    """Read the JSON watermark store, returning an empty mapping when absent."""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def save_watermark(config: dict[str, Any], value: Any) -> None:
    """Persist the latest processed incremental value after a successful sync.

    Uses a write-to-temp-then-rename strategy so a crash mid-write never leaves
    the watermark file in a corrupt or empty state.
    """
    path: Path = config["path"]
    values = read_watermark_file(path)
    values[config["key"]] = value.isoformat() if hasattr(value, "isoformat") else value
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, prefix=".syncdb_watermarks_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(values, handle, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise


def apply_watermark_filter(
    where_sql: str,
    params: list[Any],
    column: str,
    value: Any,
    quote_char: str,
    placeholder: str,
    comparison: str = ">",
) -> tuple[str, list[Any]]:
    """Append an incremental-column predicate to an existing WHERE clause.

    comparison is ">" (strict, default) or ">=" (inclusive — re-reads rows at
    the boundary value; see the module docstring for when each is appropriate).
    """
    if value in {None, ""}:
        return where_sql, params
    if comparison not in {">", ">="}:
        raise ValueError(f"watermark comparison must be '>' or '>=', got {comparison!r}")
    condition = f"{quote_identifier(column, quote_char)} {comparison} {placeholder}"
    if not where_sql:
        return f" WHERE {condition} ", [*params, value]
    existing = where_sql.strip()
    if existing.upper().startswith("WHERE "):
        existing = existing[6:].strip()
    return f" WHERE ({existing}) AND ({condition}) ", [*params, value]


def max_watermark_value(current: Any, rows: list[dict[str, Any]], column: str) -> Any:
    """Track the maximum non-null watermark value seen across fetched batches."""
    values: list[Any] = [row.get(column) for row in rows if row.get(column) is not None]
    if not values:
        return current
    batch_max: Any = max(values)
    if current is None or batch_max > current:
        return batch_max
    return current
