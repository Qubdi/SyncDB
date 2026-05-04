# Installation

## Requirements

- Python 3.10 or later
- Database-specific drivers (optional — install only what you need)

## From PyPI

```bash
pip install Qubdi-SyncDB
```

The base install has **no required dependencies**. Install extras for the engines and file formats you use:

```bash
pip install "Qubdi-SyncDB[mssql]"      # SQL Server / MSSQL  (pyodbc)
pip install "Qubdi-SyncDB[postgres]"   # PostgreSQL           (psycopg2-binary)
pip install "Qubdi-SyncDB[mysql]"      # MySQL / MariaDB      (mysql-connector-python)
pip install "Qubdi-SyncDB[files]"      # Parquet + Excel      (pandas, pyarrow, openpyxl)
pip install "Qubdi-SyncDB[all]"        # Everything above
```

> CSV and Pickle work without any extras — they use Python's standard library.

## From Source

```bash
git clone https://github.com/qubdi/syncdb.git
cd syncdb
pip install -e ".[all]"
```

## Database Drivers

### SQL Server (MSSQL)

`pyodbc` requires the Microsoft ODBC Driver for SQL Server on the host OS.

**Ubuntu / Debian**

```bash
curl -sSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

**macOS**

```bash
brew install microsoft/mssql-release/msodbcsql18
```

**Windows** — download and run the installer from the [Microsoft ODBC download page](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### PostgreSQL

`psycopg2-binary` is a self-contained wheel — no system library is required.

### MySQL / MariaDB

`mysql-connector-python` is a pure-Python driver — no system library is required.

## Verifying the Install

```python
import syncdb
print(syncdb.__version__)   # 1.0.0
```
