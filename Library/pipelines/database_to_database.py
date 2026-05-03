"""Database-to-database pipeline alias.

Exposes SyncDB, TableSyncResult, and TransferMode under the pipeline subpackage
so callers can use the more descriptive import path:
  from syncdb.pipelines.database_to_database import SyncDB, TransferMode
The implementation lives in sync.py; this module is a thin re-export.

Typical usage
-------------
    from syncdb.pipelines.database_to_database import SyncDB, TransferMode
    from syncdb import DatabaseConfig

    sync = SyncDB(
        source=DatabaseConfig(engine="mssql", host="srv", database="src", user="u", password="p"),
        target=DatabaseConfig(engine="postgresql", host="pg", database="dst", user="u", password="p"),
        batch_size=10_000,
        retry_count=3,
    )

    # Sync a single table with upsert semantics:
    results = sync.sync_tables({
        "orders": {
            "source":      "dbo.orders",
            "destination": "public.orders",
            "mode":        TransferMode.APPEND,
            "primary_key": ["order_id"],
            "filter":      {"where": "status != 'draft'"},
        }
    })

    # Sync every table in a schema, skipping temp tables:
    results = sync.sync_schema("dbo", "public", exclude=["tmp_*", "staging_*"])

Key parameters (set on SyncDB constructor)
------------------------------------------
  batch_size          int or "N%" — rows per INSERT batch (default 5000)
  drop_extra_columns  bool        — drop target columns absent from source (default False)
  dry_run             bool        — schema-align only, no data written (default False)
  retry_count         int         — retry failed batch writes with backoff (default 0)
  verbose             str | None  — print summary after sync: "standard" | "detailed" | None

Transfer modes (TransferMode enum)
-----------------------------------
  APPEND         — upsert: delete-then-insert on primary key; append new rows
  INSERT_ONLY    — append only, never touch existing rows
  UPSERT         — explicit alias for APPEND; same delete-then-insert strategy
  SNAPSHOT       — append every run's rows with a _synced_at timestamp
  SOFT_DELETE    — upsert active rows; stamp deleted_at on rows missing from source
  APPEND_STAGING — load into a staging table first, then swap into the live table
  FULL_REFRESH   — truncate target before loading (daily full reload pattern)

Do not add behavior here unless this direction needs a public convenience API.
Keeping aliases thin prevents pipeline imports from drifting away from SyncDB.
"""

from ..sync import SyncDB, TableSyncResult, TransferMode

__all__ = ["SyncDB", "TableSyncResult", "TransferMode"]
