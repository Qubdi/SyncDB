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

Storage backends
----------------
``"watermark_storage": "file"`` (default) keeps state in a local JSON file.
save_watermark() writes atomically (temp file + os.replace) so a reader never
sees a half-written file, and the read-modify-write is serialised by an
exclusive OS-level lock on a ``.lock`` sidecar (msvcrt on Windows, flock
elsewhere) plus an in-process mutex, so concurrent writers — overlapping cron
runs, replicas sharing a volume — cannot lose each other's updates.  The lock
only protects writers on the SAME filesystem.

``"watermark_storage": "database"`` keeps state in a ``__syncdb_watermarks``
table on the TARGET database (created on first save), keyed by the watermark
key and written with the connector's native atomic upsert.  Use this for
multi-replica deployments with independent local disks — every replica reads
and writes the same authoritative row, and the value travels with the target
data (restore the database, restore the cursor).  Values are JSON-encoded so
numeric watermarks round-trip as numbers, not strings.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
import tempfile
import threading
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..connectors.base import BaseConnector
from ..sql import quote_identifier, validate_identifier
from ..type_mapping import Column

# Name of the watermark table created on the target when
# watermark_storage="database" is used.
WATERMARK_TABLE = "__syncdb_watermarks"

# Columns for the database watermark store.  wm_value holds a JSON-encoded
# value so numeric/string watermark types survive the round-trip; varchar
# rather than text because MySQL cannot index/PK unbounded text.
_WATERMARK_COLUMNS = [
    Column(name="wm_key", data_type="varchar(255)", nullable=False, is_primary_key=True),
    Column(name="wm_value", data_type="varchar(512)", nullable=True),
    Column(name="updated_at", data_type="varchar(64)", nullable=True),
]

# In-process guard: OS file locks serialise processes, but on Windows
# msvcrt.locking is per-handle and two threads in one process could both
# acquire region locks on separate handles; this mutex closes that gap.
_PROCESS_LOCK = threading.Lock()


@contextlib.contextmanager
def _file_lock(path: Path) -> Iterator[None]:
    """Hold an exclusive OS-level lock on a sidecar ``<store>.lock`` file.

    Blocks until the lock is acquired.  The sidecar (rather than the store
    itself) is locked because save_watermark replaces the store file via
    os.replace — a lock on the replaced inode/handle would be meaningless.
    """
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    try:
        # sys.platform (not os.name) so mypy narrows the branch and does not
        # type-check fcntl attributes on Windows and vice versa.
        if sys.platform == "win32":
            import msvcrt

            handle.seek(0)
            # LK_LOCK retries for ~10s then raises; loop for indefinite blocking.
            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    continue
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


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


def load_watermark(spec: dict[str, Any], target: BaseConnector | None = None) -> dict[str, Any] | None:
    """Load incremental-sync state for a table spec, if configured.

    spec["watermark_comparison"] selects the filter operator: ">" (default,
    strict — see the boundary-row caveat in the module docstring) or ">="
    (inclusive — re-reads boundary rows; pair with an idempotent mode).

    spec["watermark_storage"] selects the backend: "file" (default) or
    "database" (state lives in a table on the target — see module docstring;
    requires the target connector to be passed in).
    """
    column = spec.get("incremental_column")
    if not column:
        return None
    validate_identifier(column)
    comparison = str(spec.get("watermark_comparison", ">")).strip()
    if comparison not in {">", ">="}:
        raise ValueError(f"watermark_comparison must be '>' or '>=', got {comparison!r}")
    storage = str(spec.get("watermark_storage", "file")).strip().lower()
    if storage not in {"file", "database"}:
        raise ValueError(f"watermark_storage must be 'file' or 'database', got {storage!r}")
    key = spec.get("watermark_key") or f"{spec['source']}->{spec['destination']}:{column}"

    if storage == "database":
        if target is None:
            raise ValueError("watermark_storage='database' requires a target connector")
        if len(key) > 255:
            raise ValueError(
                f"Watermark key exceeds 255 characters ({len(key)}); "
                "set an explicit shorter 'watermark_key' in the table spec"
            )
        return {
            "storage": "database",
            "key": key,
            "column": column,
            "value": _read_database_watermark(target, key),
            "comparison": comparison,
        }

    path = _resolve_watermark_path(spec.get("watermark_store"))
    values = read_watermark_file(path)
    return {
        "storage": "file",
        "path": path,
        "key": key,
        "column": column,
        "value": values.get(key),
        "comparison": comparison,
    }


def _read_database_watermark(target: BaseConnector, key: str) -> Any:
    """Fetch a watermark value from the target's __syncdb_watermarks table."""
    schema = target.config.default_schema
    if not target.table_exists(schema, WATERMARK_TABLE):
        return None
    rows = target.execute_query(
        f"SELECT {quote_identifier('wm_value', target.quote_char)} "
        f"FROM {target.quote_table(schema, WATERMARK_TABLE)} "
        f"WHERE {quote_identifier('wm_key', target.quote_char)} = {target.placeholder}",
        [key],
    )
    if not rows:
        return None
    raw = next(iter(rows[0].values()))
    if raw is None:
        return None
    # Values are JSON-encoded on save so numbers round-trip as numbers.
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


def _save_database_watermark(target: BaseConnector, key: str, value: Any) -> None:
    """Upsert a watermark row into the target's __syncdb_watermarks table.

    The connector's native upsert makes the write atomic, so concurrent
    replicas cannot interleave a lost update the way a file read-modify-write
    could.  The table is created lazily on first save.
    """
    schema = target.config.default_schema
    if not target.table_exists(schema, WATERMARK_TABLE):
        target.create_schema(schema)
        target.create_table(schema, WATERMARK_TABLE, _WATERMARK_COLUMNS)
    row = {
        "wm_key": key,
        "wm_value": json.dumps(value),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    target.upsert_batch(schema, WATERMARK_TABLE, [row], ["wm_key", "wm_value", "updated_at"], ["wm_key"])


def read_watermark_file(path: Path) -> dict[str, Any]:
    """Read the JSON watermark store, returning an empty mapping when absent."""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def save_watermark(config: dict[str, Any], value: Any, target: BaseConnector | None = None) -> None:
    """Persist the latest processed incremental value after a successful sync.

    Database storage upserts atomically into the target's watermark table.
    File storage uses write-to-temp-then-rename so a crash mid-write never
    leaves the file corrupt, with the whole read-modify-write under an
    exclusive cross-process lock (see _file_lock) so concurrent writers to the
    same store cannot drop each other's keys.
    """
    serialised = value.isoformat() if hasattr(value, "isoformat") else value
    if config.get("storage") == "database":
        if target is None:
            raise ValueError("watermark_storage='database' requires a target connector")
        _save_database_watermark(target, config["key"], serialised)
        return
    path: Path = config["path"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with _PROCESS_LOCK, _file_lock(path):
        values = read_watermark_file(path)
        values[config["key"]] = serialised
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
