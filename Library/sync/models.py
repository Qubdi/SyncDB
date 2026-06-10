from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TransferMode(str, Enum):
    # Deletes target rows matching incoming primary keys, then inserts all source rows.
    # Existing target rows with keys not present in the source batch are kept.
    # Use for incremental loads where source rows may have been updated.
    # Unlike UPSERT, no native ON CONFLICT / MERGE statement is used — the delete
    # and insert are two separate statements within the same batch.
    APPEND = "append"
    # Pure append: insert every source row as-is and never delete/update existing
    # target rows.  Useful for immutable event streams, audit logs, and history tables.
    INSERT_ONLY = "insert_only"
    # Explicit upsert mode.  Today it uses the same portable delete+insert strategy
    # as APPEND; connector-native MERGE/ON CONFLICT optimisations can replace this later.
    UPSERT = "upsert"
    # Append every source row with a _synced_at timestamp for historical snapshots.
    SNAPSHOT = "snapshot"
    # Upsert active source rows, then mark target rows missing from the source with deleted_at.
    SOFT_DELETE = "soft_delete"
    # Portable staging load: write all source rows to a staging table first, then
    # replace the live target contents from staging in a final step.
    APPEND_STAGING = "append_staging"
    # Truncate the target before loading.  Suitable for full daily refreshes.
    FULL_REFRESH = "full_refresh"


@dataclass
class TableSyncResult:
    """Runtime statistics and schema-change summary for one synced table.

    Returned by SyncDB.sync_tables so callers can audit what happened without
    parsing log output.  dry_run=True means no data or DDL was actually applied.
    """
    name: str
    source: str
    destination: str
    mode: str
    rows_read: int = 0
    rows_written: int = 0
    rows_soft_deleted: int = 0
    batches: int = 0
    schema_created: bool = False
    table_created: bool = False
    columns_added: list[str] = field(default_factory=list)
    columns_dropped: list[str] = field(default_factory=list)
    expectations_failed: list[str] = field(default_factory=list)
    watermark_value: Any = None
    dry_run: bool = False
    duration_seconds: float = 0.0
