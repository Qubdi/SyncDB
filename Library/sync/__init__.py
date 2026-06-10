"""SyncDB orchestration package.

This package replaces the single sync.py module to keep each concern in its
own file.  All public symbols are re-exported here so existing imports of the
form ``from .sync import SyncDB, TransferMode, TableSyncResult`` continue to
work without changes.

Internal layout
---------------
  core.py       — SyncDB class: orchestration, batching, retries, schema align
  models.py     — TransferMode enum and TableSyncResult dataclass
  watermark.py  — Incremental high-watermark tracking (load, save, filter)
  quality.py    — Data-quality expectation checks (min_rows, not_null, unique, range)
  reporting.py  — ASCII summary table rendering (emit_summary)
  retry.py      — with_retries(): exponential backoff with full jitter
  inference.py  — infer_columns(): Python-value to SQL-type mapping for file imports
  staging.py    — create_staging_table() / replace_from_staging() helpers
"""

from .core import SyncDB
from .models import TableSyncResult, TransferMode

__all__ = ["SyncDB", "TableSyncResult", "TransferMode"]
