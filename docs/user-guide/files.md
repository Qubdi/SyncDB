# File Operations

SyncDB can export query results to local files and import files into database tables. It also exposes `FileTransfer` for file-to-file conversions with no database involved.

## Supported formats

| Format | Extension | Extra required | Notes |
|--------|-----------|---------------|-------|
| CSV | `.csv` | none | Values are strings; no type inference |
| Parquet | `.parquet` | `pandas`, `pyarrow` | Preserves types; best for large data |
| Excel | `.xlsx`, `.xls` | `pandas`, `openpyxl` | Human-readable; slow for large files |
| Pickle | `.pickle` | none | Python-only; not portable |

Install file extras:

```bash
pip install "Qubdi-SyncDB[files]"   # pandas + pyarrow + openpyxl
```

## Export: Database → File

```python
# SQL string
sync.export_query_to_file(
    query="SELECT * FROM dbo.orders WHERE status = 'shipped'",
    output_path="exports/shipped_orders.parquet",
)

# Path to a .sql file — its contents are read and executed
sync.export_query_to_file(
    query="queries/shipped_orders.sql",
    output_path="exports/shipped_orders.parquet",
)

# Parameterized query (prevents SQL injection)
sync.export_query_to_file(
    query="SELECT * FROM dbo.orders WHERE region = ? AND year = ?",
    params=["US", 2024],
    output_path="us_orders_2024.parquet",
)

# Explicit format override (ignores extension)
sync.export_query_to_file(
    query="SELECT * FROM dbo.summary",
    output_path="summary.dat",
    file_format="excel",
)
```

Output parent directories are created automatically — no need to `mkdir` beforehand.

Returns the number of rows written.

## Import: File → Database

```python
# Append rows from a Parquet file into a PostgreSQL table
sync.import_file_to_table(
    input_path="exports/shipped_orders.parquet",
    destination="public.shipped_orders",
)

# Truncate first, then load
sync.import_file_to_table(
    input_path="customers.csv",
    destination="public.customers",
    fresh_insert=True,
)
```

If the target table does not exist, SyncDB creates it using column types inferred from the first row of data.

Returns the number of rows inserted.

## File-to-file conversion

Use `FileTransfer` directly for format conversion with no database:

```python
from syncdb import FileTransfer

ft = FileTransfer()

# Read any supported format — returns list[dict]
rows = ft.read("data.csv")
rows = ft.read("data.parquet")
rows = ft.read("data.xlsx")

# Write any supported format
ft.write(rows, "output.parquet")
ft.write(rows, "output.csv")
ft.write(rows, "output.xlsx")
```

### Common conversions

```python
ft = FileTransfer()

# CSV → Parquet (preserves column types from the first read)
rows = ft.read("data.csv")
ft.write(rows, "data.parquet")

# Parquet → Excel
rows = ft.read("report.parquet")
ft.write(rows, "report.xlsx")

# Filter rows in Python before writing
rows = ft.read("orders.parquet")
active = [r for r in rows if r["status"] == "active"]
ft.write(active, "active_orders.csv")
```

## Pipeline pattern: export → transform → import

```python
from syncdb import DatabaseConfig, FileTransfer, SyncDB

mssql_cfg = DatabaseConfig(engine="mssql", connection_string="...")
pg_cfg     = DatabaseConfig(engine="postgresql", connection_string="...")

# Step 1: export from MSSQL
src_sync = SyncDB(source=mssql_cfg)
src_sync.export_query_to_file(
    query="SELECT * FROM dbo.daily_report WHERE report_date = ?",
    params=["2024-12-31"],
    output_path="daily_report.parquet",
)

# Step 2: transform in Python
ft = FileTransfer()
rows = ft.read("daily_report.parquet")
rows = [r for r in rows if r["revenue"] > 0]   # drop zero-revenue rows
ft.write(rows, "daily_report_clean.parquet")

# Step 3: load into PostgreSQL
dst_sync = SyncDB(target=pg_cfg)
dst_sync.import_file_to_table(
    input_path="daily_report_clean.parquet",
    destination="public.daily_report",
    fresh_insert=False,
)
```
