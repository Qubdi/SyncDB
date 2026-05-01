"""Local file-to-database pipeline helpers.

Exposes SyncDB under the pipeline subpackage for the local file → database direction.
Use SyncDB.import_file_to_table() to read a CSV, Parquet, Excel, or Pickle file
and insert its rows into a target table, creating the table automatically if needed.
"""

from ..sync import SyncDB

__all__ = ["SyncDB"]
