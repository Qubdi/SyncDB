"""Local file-to-database pipeline helpers.

Exposes SyncDB under the pipeline subpackage for the local file-to-database direction.
Use SyncDB.import_file_to_table() to read a CSV, Parquet, Excel, or Pickle file
and insert its rows into a target table, creating the table automatically if needed.

This stays as an import convenience layer. The real behavior belongs in SyncDB
so table creation, type inference, and connector cleanup follow one code path.
"""

from ..sync import SyncDB

__all__ = ["SyncDB"]
