"""SyncDB — cross-engine database and file synchronization library.

This is the single stable import surface for application code.  Internal module
names can change freely as the implementation evolves; only the symbols in
``__all__`` carry a backwards-compatibility commitment.

Three primary usage patterns
-----------------------------
1. Database → database  (most common)
       from syncdb import SyncDB, DatabaseConfig, TransferMode
       sync = SyncDB(
           source=DatabaseConfig(engine="mssql", host="srv", database="src", user="u", password="p"),
           target=DatabaseConfig(engine="postgresql", host="pg", database="dst", user="u", password="p"),
       )
       sync.sync_tables({"orders": {"source": "dbo.orders", "destination": "public.orders"}})

2. Database → local file
       from syncdb import SyncDB, DatabaseConfig
       sync = SyncDB(source=DatabaseConfig(engine="postgresql", ...))
       sync.export_query_to_file("SELECT * FROM big_table", "output.parquet")

3. Local file → database
       from syncdb import SyncDB, DatabaseConfig
       sync = SyncDB(target=DatabaseConfig(engine="mysql", ...))
       sync.import_file_to_table("data.csv", "staging.uploaded_data")

Extension points
----------------
- Add a new database engine: connectors/ (new subclass), connections.py (factory),
  config.py (alias map), type_mapping.py (mapping methods).
- Customize progress output: subclass ProgressReporter or pass a custom stream.
- Inject test doubles: pass a BaseConnector subclass directly instead of a DatabaseConfig.

Thread safety
-------------
DatabaseConfig is a frozen dataclass and is safe to share across threads.
SyncDB instances are NOT thread-safe; create one per thread/task.
"""

from .config import DatabaseConfig
from .files import FileFormat, FileTransfer
from .progress import ProgressMode, ProgressReporter
from .sync import SyncDB, TableSyncResult, TransferMode
from .type_mapping import Column, SchemaMapper

__all__ = [
    # Explicit exports make accidental internals invisible to wildcard imports
    # and give future maintainers one place to review the public API.
    "Column",
    "DatabaseConfig",
    "FileFormat",
    "FileTransfer",
    "ProgressMode",
    "ProgressReporter",
    "SchemaMapper",
    "SyncDB",
    "TableSyncResult",
    "TransferMode",
]
