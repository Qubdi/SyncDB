"""Database-to-local file pipeline helpers.

Exposes SyncDB under the pipeline subpackage for the database-to-local file direction.
Use SyncDB.export_query_to_file() to execute a source query and write the result
to CSV, Parquet, Excel, or Pickle.

This stays as an import convenience layer. The real behavior belongs in SyncDB
so configuration, progress, retry, and file-format handling remain centralized.

Typical usage
-------------
    from syncdb.pipelines.database_to_local import SyncDB
    from syncdb import DatabaseConfig

    sync = SyncDB(source=DatabaseConfig(engine="postgresql", host="pg", database="db", user="u", password="p"))

    # Export a query result to Parquet (pandas required):
    rows_written = sync.export_query_to_file(
        "SELECT * FROM analytics.events WHERE event_date >= '2024-01-01'",
        "output/events_2024.parquet",
    )

    # Export using a .sql file (useful for complex queries stored in version control):
    rows_written = sync.export_query_to_file(
        "queries/monthly_summary.sql",
        "output/monthly.csv",
    )

    # Pass bind parameters:
    rows_written = sync.export_query_to_file(
        "SELECT * FROM orders WHERE region = %s",
        "output/eu_orders.csv",
        params=["EU"],
    )

Supported output formats  (auto-detected from file extension)
-------------------------------------------------------------
  .csv     — stdlib csv, no extra dependency
  .parquet — requires pandas + pyarrow or fastparquet
  .xlsx    — requires pandas + openpyxl
  .pickle  — stdlib pickle; see FileTransfer security note before using

NOTE: export_query_to_file() loads the full query result into memory before writing.
For very large result sets, consider streaming to Parquet in chunks using
SyncDB.sync_tables() with a file-backed connector, or exporting directly via
the database's native bulk-export tooling (bcp, COPY, mysqldump).
"""

from ..sync import SyncDB

__all__ = ["SyncDB"]
