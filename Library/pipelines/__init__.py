"""Pipeline import namespace.

The direction-specific modules are thin aliases over SyncDB.  They exist purely
as a UX convenience so callers can use import paths that read like documentation:

    from syncdb.pipelines.database_to_database import SyncDB, TransferMode
    from syncdb.pipelines.database_to_local    import SyncDB
    from syncdb.pipelines.local_to_database    import SyncDB

All three paths resolve to the same SyncDB class in sync.py.  There is no
parallel implementation here — don't add logic to these modules.  If a
pipeline direction needs a public convenience wrapper, add a method to SyncDB
and re-export it from the relevant alias module.

Submodules
----------
  database_to_database  — SyncDB.sync_tables() / SyncDB.sync_schema()
  database_to_local     — SyncDB.export_query_to_file()
  local_to_database     — SyncDB.import_file_to_table()
"""
