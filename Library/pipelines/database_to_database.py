"""Database-to-database pipeline alias.

Exposes SyncDB, TableSyncResult, and TransferMode under the pipeline subpackage
so callers can use the more descriptive import path:
  from syncdb.pipelines.database_to_database import SyncDB, TransferMode
The implementation lives in sync.py; this module is a thin re-export.

Do not add behavior here unless this direction needs a public convenience API.
Keeping aliases thin prevents pipeline imports from drifting away from SyncDB.
"""

from ..sync import SyncDB, TableSyncResult, TransferMode

__all__ = ["SyncDB", "TableSyncResult", "TransferMode"]
