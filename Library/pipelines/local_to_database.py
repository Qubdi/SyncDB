"""Local file-to-database pipeline helpers.

Exposes SyncDB under the pipeline subpackage for the local file-to-database direction.
Use SyncDB.import_file_to_table() to read a CSV, Parquet, Excel, or Pickle file
and insert its rows into a target table, creating the table automatically if needed.

This stays as an import convenience layer. The real behavior belongs in SyncDB
so table creation, type inference, and connector cleanup follow one code path.

Typical usage
-------------
    from syncdb.pipelines.local_to_database import SyncDB
    from syncdb import DatabaseConfig

    sync = SyncDB(target=DatabaseConfig(engine="mssql", host="srv", database="db", user="u", password="p"))

    # First-time load — table is created automatically from CSV column names:
    rows_inserted = sync.import_file_to_table("data/customers.csv", "staging.customers")

    # Re-run with truncate (wipe existing rows, then re-insert):
    rows_inserted = sync.import_file_to_table("data/customers.csv", "staging.customers", fresh_insert=True)

    # Force format detection when the extension is ambiguous:
    rows_inserted = sync.import_file_to_table("data/export", "dbo.products", file_format="parquet")

Supported input formats  (auto-detected from file extension)
------------------------------------------------------------
  .csv           — stdlib csv, no extra dependency
  .parquet       — requires pandas + pyarrow or fastparquet
  .xlsx / .xls   — requires pandas + openpyxl
  .pickle        — stdlib pickle; see FileTransfer security note before using

Auto-create behavior
--------------------
When the target table does not exist, its schema is inferred from the first row
of the file (see SyncDB._infer_columns).  Type inference is intentionally broad:
booleans → boolean, integers → bigint, floats → double precision, everything else → text.
CSV files in particular yield only strings, so all columns will be text unless the
target table is pre-created manually with explicit types.

If the table already exists and fresh_insert=False (default), rows are appended
without any deduplication.  For upsert semantics use sync_tables() instead.
"""

from ..sync import SyncDB

__all__ = ["SyncDB"]
