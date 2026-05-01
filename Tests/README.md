# SyncDB Docker Test Databases

This folder contains local Docker infrastructure for integration testing against the three main SyncDB engines:

- Microsoft SQL Server
- PostgreSQL
- MySQL

The Compose file builds local Qubdi SyncDB test images and starts branded containers for the test databases.

The seed scripts create deterministic fake data directly inside each database. This gives enough data for batch transfer, schema sync, progress bar, filtering, and validation tests without committing large generated files.

## Start Databases

Run from this folder:

```bash
docker compose up -d --build
```

## Docker Images And Containers

Custom local images:

- `qubdi-syncdb-mssql:latest`
- `qubdi-syncdb-mssql-tools:latest`
- `qubdi-syncdb-postgres:latest`
- `qubdi-syncdb-mysql:latest`
- `qubdi-syncdb-pgadmin:latest`
- `qubdi-syncdb-phpmyadmin:latest`
- `qubdi-syncdb-cloudbeaver:latest`

Container names:

- `Qubdi-SyncDB-mssql`
- `Qubdi-SyncDB-mssql-init`
- `Qubdi-SyncDB-postgres`
- `Qubdi-SyncDB-mysql`
- `Qubdi-SyncDB-postgres-ide`
- `Qubdi-SyncDB-mysql-ide`
- `Qubdi-SyncDB-mssql-ide`

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
Email: admin@qubdi.local
Password: syncdb
```

PostgreSQL connection inside pgAdmin:

```text
Host: postgres
Port: 5432
Database: syncdb_test
User: syncdb
Password: syncdb
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
User: syncdb
Password: syncdb
```

Root login is also available for admin testing:

```text
User: root
Password: root
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
User: sa
Password: SyncDB_Strong_Passw0rd!
```

CloudBeaver can also be used as a universal IDE for PostgreSQL and MySQL if needed. Use `postgres:5432` and `mysql:3306` as Docker-network hosts.

## Connection Details

### MSSQL

```text
Host: localhost
Port: 11433
User: sa
Password: SyncDB_Strong_Passw0rd!
Database: syncdb_test
```

ODBC connection string example:

```text
Driver={ODBC Driver 17 for SQL Server};Server=localhost,11433;Database=syncdb_test;UID=sa;PWD=SyncDB_Strong_Passw0rd!;TrustServerCertificate=yes;
```

### PostgreSQL

```text
Host: localhost
Port: 15432
User: syncdb
Password: syncdb
Database: syncdb_test
```

Connection string:

```text
postgresql://syncdb:syncdb@localhost:15432/syncdb_test
```

### MySQL

```text
Host: localhost
Port: 13306
User: syncdb
Password: syncdb
Database: syncdb_test
```

Connection string:

```text
mysql://syncdb:syncdb@localhost:13306/syncdb_test
```

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
```

Using `down -v` removes database volumes so the init scripts run again.

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
- one-line progress mode
- multi-line progress mode
