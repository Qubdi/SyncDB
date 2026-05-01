# SyncDB

Python ETL helper for moving tabular data between **Microsoft SQL Server**, **PostgreSQL**, **MySQL**, and **local files** (CSV, Parquet, Excel, Pickle).

---

## Installation

```bash
pip install -e .
pip install -r requirements.txt
```

Install only the connectors you need:

```bash
pip install -e ".[mssql]"       # MSSQL only
pip install -e ".[postgres]"    # PostgreSQL only
pip install -e ".[mysql]"       # MySQL only
pip install -e ".[files]"       # Parquet + Excel support
pip install -e ".[all]"         # All connectors and file formats
```

---

## Quick Start

### Database → Database

```python
from syncdb import DatabaseConfig, ProgressMode, SyncDB

source = DatabaseConfig(
    engine="mssql",
    connection_string=(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost,1433;Database=mydb;"
        "UID=admin;PWD=admin;TrustServerCertificate=yes;"
    ),
)

target = DatabaseConfig(
    engine="postgresql",
    connection_string="postgresql://admin:admin@localhost:5432/mydb",
)

sync = SyncDB(
    source=source,
    target=target,
    batch_size=10_000,
    progress_mode=ProgressMode.ONE_LINE,
)

results = sync.sync_tables(
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

for r in results:
    print(f"{r.destination}: {r.rows_written} rows in {r.batches} batches")
```

### Database → Local File

```python
rows_written = sync.export_query_to_file(
    query="SELECT * FROM dbo.customers WHERE is_active = 1",
    output_path="customers.parquet",
)
```

### Local File → Database

```python
rows_inserted = sync.import_file_to_table(
    input_path="customers.parquet",
    destination="public.customers",
    fresh_insert=True,   # truncates before inserting
)
```

---

## Transfer Modes

| Mode | Behavior |
| --- | --- |
| `append` | Deletes matching PKs from target then inserts each batch |
| `append_staging` | Same as `append` currently; true staging-table implementation planned |
| `full_refresh` | Truncates target table, then inserts all rows |

---

## `DatabaseConfig` Parameters

| Parameter | Description | Default |
| --- | --- | --- |
| `engine` | `"mssql"`, `"postgresql"`, `"mysql"` (and aliases) | required |
| `connection_string` | Full DSN or URL string | `None` |
| `host` / `port` / `database` / `user` / `password` | Used when no `connection_string` is given | `None` |
| `default_schema` | Schema prefix for unqualified table names | engine-specific |
| `connect_timeout` | Seconds before a connection attempt fails | `30` |
| `pool_min` / `pool_max` | Connection pool size bounds | `1` / `5` |
| `options` | Extra driver-specific keyword arguments | `{}` |

Engine aliases: `"sqlserver"` / `"sql_server"` → `"mssql"`, `"postgres"` / `"pg"` → `"postgresql"`.

---

## `SyncDB` Parameters

| Parameter | Description | Default |
| --- | --- | --- |
| `source` / `target` | `DatabaseConfig` or `BaseConnector` | `None` |
| `batch_size` | Rows per read/write batch | `5000` |
| `progress_mode` | `ONE_LINE`, `MULTI_LINE`, or `NONE` | `MULTI_LINE` |
| `dry_run` | Report schema changes without writing data | `False` |
| `drop_extra_columns` | Drop target columns not present in source | `False` |

---

## `sync_tables` Table Spec

Each key in the `tables` dict is a logical name for the operation. The value is a dict with:

| Key | Required | Description |
| --- | --- | --- |
| `source` | yes | Source table (`"schema.table"` or `"table"`) |
| `destination` | yes | Target table |
| `mode` | no | Transfer mode (default: `"append"`) |
| `primary_key` | no | Override PK columns; auto-detected when omitted |
| `order_by` | no | Column(s) for deterministic source reads |
| `filter` | no | `{"where": "col > ?", "params": [value]}` or a plain `WHERE` string |

---

## `TableSyncResult` Fields

```python
result.rows_read       # rows read from source
result.rows_written    # rows written to target
result.batches         # number of batches processed
result.table_created   # True if target table was created
result.columns_added   # list of column names added to target
result.columns_dropped # list of column names dropped from target
result.dry_run         # True if this was a dry run
```

---

## Supported File Formats

| Format | Extension(s) | Notes |
| --- | --- | --- |
| CSV | `.csv` | All values read as strings |
| Parquet | `.parquet` | Requires `pandas` + `pyarrow` |
| Excel | `.xlsx`, `.xls` | Requires `pandas` + `openpyxl` |
| Pickle | `.pickle` | Python native; not cross-language |

---

## Running Tests

```bash
pytest
```

Integration tests against live databases require the Docker test environment:

```bash
cd Tests/DataBase
docker compose up -d --build
```

See [Tests/DataBase/README.md](Tests/DataBase/README.md) for details.
