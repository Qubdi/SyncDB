# SyncDB Docker Test Databases

This folder contains local Docker infrastructure for integration testing against the three main SyncDB engines:

- Microsoft SQL Server
- PostgreSQL
- MySQL

The Compose file builds local Qubdi SyncDB test images and starts branded containers for the test databases.

The seed scripts create deterministic fake data directly inside each database. This gives enough data for batch transfer, schema sync, progress bar, filtering, and validation tests without committing large generated files.

## Table Of Contents

- [Start Databases](#start-databases)
- [Docker Images And Containers](#docker-images-and-containers)
- [Browser Database IDEs](#browser-database-ides)
  - [PostgreSQL IDE](#postgresql-ide)
  - [MySQL IDE](#mysql-ide)
  - [MSSQL IDE](#mssql-ide)
- [Connection Details](#connection-details)
  - [MSSQL](#mssql)
  - [PostgreSQL](#postgresql)
  - [MySQL](#mysql)
- [Data Validation Container](#data-validation-container)
- [Seeded Tables](#seeded-tables)
- [Engine-Specific Datatype Testing](#engine-specific-datatype-testing)
  - [MSSQL `datatype_samples`](#mssql-datatype_samples)
  - [PostgreSQL `datatype_samples`](#postgresql-datatype_samples)
  - [MySQL `datatype_samples`](#mysql-datatype_samples)
- [Reset Everything](#reset-everything)
- [Intended Test Coverage](#intended-test-coverage)

## Start Databases

Run from this folder:

```bash
docker compose up -d --build
docker compose run --rm mssql-init
docker compose run --rm data-check
```

PostgreSQL and MySQL seed automatically during first volume creation. MSSQL is seeded by the one-shot `mssql-init` job. The `--rm` flag deletes the init container after the seed script finishes.
The `data-check` job validates seeded row counts in all three databases and is also deleted automatically by `--rm`.

Run `mssql-init` before `data-check`. The MSSQL `admin/admin` login and MSSQL seed data are created by `mssql-init`, not by the base MSSQL container.

## Docker Images And Containers

Custom local images:

- `qubdi-syncdb-mssql:2022-CU23-ubuntu-22.04`
- `qubdi-syncdb-mssql-tools:2022-CU23-ubuntu-22.04`
- `qubdi-syncdb-postgres:18.3`
- `qubdi-syncdb-mysql:9.7.0`
- `qubdi-syncdb-pgadmin:9.14.0`
- `qubdi-syncdb-phpmyadmin:5.2.3`
- `qubdi-syncdb-cloudbeaver:26.0.3`
- `qubdi-syncdb-data-check:1.0.0`

Pinned upstream base images:

- `mcr.microsoft.com/mssql/server:2022-CU23-ubuntu-22.04`
- `postgres:18.3`
- `mysql:9.7.0`
- `dpage/pgadmin4:9.14.0`
- `phpmyadmin:5.2.3`
- `dbeaver/cloudbeaver:26.0.3`

Container names:

- `Qubdi-SyncDB-mssql`
- `Qubdi-SyncDB-mssql-init` only exists while `docker compose run --rm mssql-init` is running
- `Qubdi-SyncDB-postgres`
- `Qubdi-SyncDB-mysql`
- `Qubdi-SyncDB-postgres-ide`
- `Qubdi-SyncDB-mysql-ide`
- `Qubdi-SyncDB-mssql-ide`
- `Qubdi-SyncDB-data-check` only exists while `docker compose run --rm data-check` is running

## Browser Database IDEs

The Compose stack includes browser-based database IDEs so no local DB client installation is required.

### PostgreSQL IDE

Tool: pgAdmin

URL:

```text
http://localhost:18080
```

pgAdmin login:

```text
Email: admin@admin.com
Password: admin
```

PostgreSQL connection inside pgAdmin:

```text
Host: postgres
Port: 5432
Database: syncdb_test
User: admin
Password: admin
```

### MySQL IDE

Tool: phpMyAdmin

URL:

```text
http://localhost:18081
```

phpMyAdmin is preconfigured to use the MySQL container.

Login:

```text
User: admin
Password: admin
```

Root login is also available for admin testing:

```text
User: root
Password: admin
```

### MSSQL IDE

Tool: CloudBeaver

URL:

```text
http://localhost:18082
```

On first open, CloudBeaver asks for initial workspace/admin setup. After setup, create a SQL Server connection with:

```text
Host: mssql
Port: 1433
Database: syncdb_test
User: admin
Password: admin
```

CloudBeaver can also be used as a universal IDE for PostgreSQL and MySQL if needed. Use `postgres:5432` and `mysql:3306` as Docker-network hosts.

## Connection Details

### MSSQL

```text
Host: localhost
Port: 11433
User: admin
Password: admin
Database: syncdb_test
```

ODBC connection string example:

```text
Driver={ODBC Driver 17 for SQL Server};Server=localhost,11433;Database=syncdb_test;UID=admin;PWD=admin;TrustServerCertificate=yes;
```

### PostgreSQL

```text
Host: localhost
Port: 15432
User: admin
Password: admin
Database: syncdb_test
```

Connection string:

```text
postgresql://admin:admin@localhost:15432/syncdb_test
```

### MySQL

```text
Host: localhost
Port: 13306
User: admin
Password: admin
Database: syncdb_test
```

Connection string:

```text
mysql://admin:admin@localhost:13306/syncdb_test
```

## Data Validation Container

The stack includes a one-shot `data-check` container for validating that seed data exists in MSSQL, PostgreSQL, and MySQL.

Run it after the databases are started and MSSQL is seeded:

```bash
docker compose run --rm data-check
```

If it fails with an MSSQL authentication error, run:

```bash
docker compose run --rm mssql-init
docker compose run --rm data-check
```

Do not add `--build` to the `data-check` run command during normal use. Build happens in `docker compose up -d --build`; the check job should only validate the running stack.

Behavior:

- Exits `0` when all expected tables and row counts match.
- Exits non-zero when a database is unavailable, a table is missing, or a row count is wrong.
- Deletes itself after completion because it is run with `--rm`.

The checker validates:

- `customers`: 250,000 rows
- `products`: 2,500 rows
- `orders`: 1,000,000 rows
- `payments`: 1,000,000 rows
- `sync_audit`: 500 rows
- `datatype_samples`: 25 rows

## Seeded Tables

Each database receives the same logical dataset:

- `customers`: 250,000 rows
- `products`: 2,500 rows
- `orders`: 1,000,000 rows
- `payments`: 1,000,000 rows
- `sync_audit`: 500 rows
- `datatype_samples`: 25 rows

The dataset includes common ETL data types:

- integers and big integers
- strings
- decimals
- booleans
- dates and timestamps
- nullable values
- primary keys
- foreign-key-like relationships

## Engine-Specific Datatype Testing

Each database also contains a small `datatype_samples` table for testing how the API layer reads metadata and converts engine-specific column types.

### MSSQL `datatype_samples`

Includes SQL Server-specific and SQL Server-heavy types:

- `uniqueidentifier`
- `tinyint`
- `money`
- `smallmoney`
- `decimal(38, 10)`
- `date`
- `time(3)`
- `datetime`
- `datetime2(7)`
- `datetimeoffset(7)`
- `char`
- `nchar`
- `varchar`
- `nvarchar(max)`
- `binary`
- `varbinary(max)`
- `xml`
- JSON stored as `nvarchar(max)` with an `ISJSON` check
- `rowversion`

### PostgreSQL `datatype_samples`

Includes PostgreSQL-specific and PostgreSQL-heavy types:

- `uuid`
- `numeric(38, 10)`
- `money`
- `timestamp`
- `timestamptz`
- `interval`
- `bytea`
- `json`
- `jsonb`
- `inet`
- `cidr`
- `macaddr`
- `bit`
- `varbit`
- arrays
- `point`

### MySQL `datatype_samples`

Includes MySQL-specific and MySQL-heavy types:

- signed and unsigned integer types
- `mediumint`
- `decimal(38, 10)`
- `float`
- `double`
- `date`
- `time(3)`
- `datetime(6)`
- `timestamp(6)`
- `year`
- `binary`
- `varbinary`
- `blob`
- `json`
- `enum`
- `set`
- `bit`

## Reset Everything

```bash
docker compose down -v
docker compose up -d --build
docker compose run --rm mssql-init
docker compose run --rm data-check
```

Using `down -v` removes database volumes so the seed scripts run again.

## Intended Test Coverage

- MSSQL to PostgreSQL sync
- MSSQL to MySQL sync
- PostgreSQL to MSSQL sync
- PostgreSQL to MySQL sync
- MySQL to MSSQL sync
- MySQL to PostgreSQL sync
- database to local file export
- local file to database import
- append mode
- append staging mode
- full refresh mode
