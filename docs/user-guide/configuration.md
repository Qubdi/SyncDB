# Configuration

## DatabaseConfig

`DatabaseConfig` describes a single database connection. It is an immutable frozen dataclass — create one per database and pass it to `SyncDB`.

```python
from syncdb import DatabaseConfig
```

### Connection string

```python
# SQL Server / MSSQL
mssql = DatabaseConfig(
    engine="mssql",
    connection_string=(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=db.example.com,1433;Database=mydb;"
        "UID=sa;PWD=Password123;TrustServerCertificate=yes;"
    ),
)

# PostgreSQL
pg = DatabaseConfig(
    engine="postgresql",
    connection_string="postgresql://user:pass@db.example.com:5432/mydb",
)

# MySQL
mysql = DatabaseConfig(
    engine="mysql",
    connection_string="mysql://user:pass@db.example.com:3306/mydb",
)

# SQLite (for testing)
sqlite = DatabaseConfig(
    engine="sqlite",
    connection_string="mydb.sqlite3",
)
```

### Individual parameters

```python
pg = DatabaseConfig(
    engine="postgresql",
    host="db.example.com",
    port=5432,
    database="mydb",
    user="admin",
    password="secret",
    connect_timeout=60,
)
```

### Parameter reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `engine` | `str` | **required** | Database engine (see aliases below) |
| `connection_string` | `str \| None` | `None` | Full DSN or URL |
| `host` | `str \| None` | `None` | Server hostname |
| `port` | `int \| None` | engine default | Server port |
| `database` | `str \| None` | `None` | Database name |
| `user` | `str \| None` | `None` | Login username |
| `password` | `str \| None` | `None` | Login password |
| `default_schema` | `str \| None` | engine default | Schema prefix for unqualified table names |
| `connect_timeout` | `int` | `30` | Seconds before a connection attempt fails |
| `options` | `dict` | `{}` | Extra driver-specific keyword arguments |

### Engine aliases

| Alias | Resolved engine |
|-------|----------------|
| `"mssql"`, `"sqlserver"`, `"sql_server"` | `mssql` |
| `"postgresql"`, `"postgres"`, `"pg"` | `postgresql` |
| `"mysql"` | `mysql` |
| `"sqlite"`, `"sqlite3"` | `sqlite` |

### Default ports and schemas

| Engine | Default port | Default schema |
|--------|-------------|----------------|
| MSSQL | 1433 | `dbo` |
| PostgreSQL | 5432 | `public` |
| MySQL | 3306 | *(database name)* |
| SQLite | — | — |

## SyncDB constructor

```python
from syncdb import SyncDB, ProgressMode

sync = SyncDB(
    source=src,           # DatabaseConfig or None
    target=dst,           # DatabaseConfig or None
    batch_size=5_000,     # int or "10%" percentage string
    progress_mode=ProgressMode.multi_line,
    dry_run=False,
    drop_extra_columns=False,
    verbose="standard",   # "standard", "detailed", or None
    retry_count=0,
    retry_delay_seconds=1.0,
)
```

### Constructor parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `DatabaseConfig \| None` | `None` | Source database |
| `target` | `DatabaseConfig \| None` | `None` | Target database |
| `batch_size` | `int \| str` | `5000` | Rows per batch — integer or `"10%"` percentage |
| `progress_mode` | `ProgressMode \| str` | `multi_line` | Progress bar display style |
| `dry_run` | `bool` | `False` | Report changes without writing any data |
| `drop_extra_columns` | `bool` | `False` | Drop target columns absent from source |
| `verbose` | `str \| None` | `"standard"` | Auto-print summary after each sync |
| `verbose_stream` | `TextIO \| None` | `sys.stdout` | Output stream for the summary table |
| `retry_count` | `int` | `0` | Retry failed batch writes up to this many times |
| `retry_delay_seconds` | `float` | `1.0` | Initial retry delay; doubles after each attempt |

### Batch size levels

`batch_size` can be set at three levels. More specific settings always win:

| Level | How to set | Applies to |
|-------|-----------|------------|
| Global | `SyncDB(..., batch_size=5000)` | All tables in all calls |
| Per-call | `sync.sync_tables(tables, batch_size=10_000)` | All tables in that call |
| Per-table | `{"batch_size": 500}` in the table spec | That table only |

```python
results = sync.sync_tables({
    "wide_table": {
        "source": "dbo.wide_table",
        "destination": "public.wide_table",
        "batch_size": 500,    # overrides global and call-level
    },
    "small_table": {
        "source": "dbo.small_table",
        "destination": "public.small_table",
        # no batch_size — falls back to call-level or global
    },
})
```

## Reading credentials from environment variables

`DatabaseConfig` is a plain Python dataclass — use `os.environ` to avoid hard-coding credentials:

```python
import os
from syncdb import DatabaseConfig

pg = DatabaseConfig(
    engine="postgresql",
    host=os.environ["DB_HOST"],
    database=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASS"],
)
```

## Loading a job from a config file

For scheduled or CLI-driven jobs, store the full configuration in a YAML or JSON file and use `SyncDB.run_config_file`:

```yaml
# syncdb.yaml
source:
  engine: mssql
  connection_string: "Driver={ODBC Driver 17 for SQL Server};Server=..."

target:
  engine: postgresql
  connection_string: "postgresql://user:pass@localhost:5432/warehouse"

settings:
  batch_size: 10000
  verbose: standard

tables:
  orders:
    source: dbo.orders
    destination: public.orders
    mode: append
    primary_key: [order_id]
```

```python
results = SyncDB.run_config_file("syncdb.yaml")
```

If you need to load, inspect, or modify the config dict before running, use `SyncDB.from_job_config` to build the instance separately:

```python
import yaml
from syncdb import SyncDB

with open("syncdb.yaml") as f:
    config = yaml.safe_load(f)

# Optionally mutate config here before creating the instance
config["settings"]["dry_run"] = True

sync = SyncDB.from_job_config(config)
results = sync.sync_tables(config.get("tables") or {})
```

`from_job_config` reads `source`, `target`, and `settings` from the dict (same keys as the YAML schema) and returns a fully constructed `SyncDB` instance. Unrecognised keys inside `settings` are silently ignored.
