# SyncDB

SyncDB is a class-based Python ETL helper for moving tabular data between
Microsoft SQL Server, PostgreSQL, MySQL, and local files.

## Current API

```python
from syncdb import DatabaseConfig, ProgressMode, SyncDB

source = DatabaseConfig(
    engine="mssql",
    connection_string="Driver={ODBC Driver 17 for SQL Server};Server=localhost,11433;Database=syncdb_test;UID=admin;PWD=admin;TrustServerCertificate=yes;",
)

target = DatabaseConfig(
    engine="postgresql",
    connection_string="postgresql://admin:admin@localhost:15432/syncdb_test",
)

sync = SyncDB(
    source=source,
    target=target,
    batch_size=10000,
    progress_mode=ProgressMode.ONE_LINE,
)

sync.sync_tables(
    {
        "customers": {
            "source": "dbo.customers",
            "destination": "public.customers",
            "mode": "append",
            "primary_key": ["customer_id"],
            "order_by": ["customer_id"],
        }
    }
)
```

## Implemented Package Structure

```text
syncdb/
  config.py
  connections.py
  files.py
  progress.py
  sql.py
  sync.py
  type_mapping.py
  connectors/
    base.py
    mssql.py
    postgres.py
    mysql.py
  pipelines/
    database_to_database.py
    database_to_local.py
    local_to_database.py
```

The concrete database drivers are optional dependencies and are imported only
when a connector opens a connection.

## Dependencies

The core package has no required third-party runtime dependencies. For full
database and local file support, install the pinned optional dependency set:

```bash
python -m pip install -r requirements.txt
```

## Tests

Run the unit test suite:

```bash
python -m unittest discover -v
```

Docker database infrastructure for MSSQL, PostgreSQL, and MySQL lives under
`Tests/DataBase`.
