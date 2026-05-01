"""Database-to-local file pipeline helpers.

Exposes SyncDB under the pipeline subpackage for the database → local file direction.
Use SyncDB.export_query_to_file() to execute a source query and write the result
to CSV, Parquet, Excel, or Pickle.
"""

from ..sync import SyncDB

__all__ = ["SyncDB"]
