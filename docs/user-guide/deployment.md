# Deployment Guide

## Docker

### Minimal Dockerfile

```dockerfile
FROM python:3.11-slim

# Install ODBC driver for MSSQL (skip if not needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc-dev curl gnupg && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "run_sync.py"]
```

### Environment variables for credentials

Never embed passwords in your image.  Pass them at runtime:

```bash
docker run --rm \
  -e SYNCDB_ENGINE=postgresql \
  -e SYNCDB_HOST=db.internal \
  -e SYNCDB_DATABASE=mydb \
  -e SYNCDB_USER=etl_user \
  -e SYNCDB_PASSWORD="$(vault kv get -field=password secret/db)" \
  myimage:latest
```

In the sync script:

```python
from syncdb import DatabaseConfig, SyncDB

src = DatabaseConfig.from_env("SOURCE")   # reads SOURCE_ENGINE, SOURCE_HOST, …
tgt = DatabaseConfig.from_env("TARGET")   # reads TARGET_ENGINE, TARGET_HOST, …
SyncDB(source=src, target=tgt).sync_tables(tables)
```

---

## Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: daily-sync
spec:
  schedule: "0 2 * * *"   # 02:00 UTC daily
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
          - name: syncdb
            image: myregistry/syncdb-job:latest
            envFrom:
            - secretRef:
                name: db-credentials     # Kubernetes Secret with SYNCDB_* vars
            env:
            - name: SYNCDB_ENGINE
              value: postgresql
            - name: SOURCE_ENGINE
              value: mssql
```

### Kubernetes Secret

```bash
kubectl create secret generic db-credentials \
  --from-literal=SOURCE_PASSWORD="$(vault kv get -field=pw secret/mssql)" \
  --from-literal=SYNCDB_PASSWORD="$(vault kv get -field=pw secret/pg)"
```

---

## Airflow

```python
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def customer_sync():
    @task()
    def sync_customers():
        from syncdb import DatabaseConfig, SyncDB
        src = DatabaseConfig(
            engine="mssql",
            host=Variable.get("mssql_host"),
            database="CRM",
            user=Variable.get("mssql_user"),
            password=Variable.get("mssql_password"),   # stored in Airflow Variables / Secrets Backend
        )
        tgt = DatabaseConfig(
            engine="postgresql",
            host=Variable.get("pg_host"),
            database="warehouse",
            user=Variable.get("pg_user"),
            password=Variable.get("pg_password"),
        )
        SyncDB(source=src, target=tgt, verbose="detailed").sync_tables({
            "customers": {"source": "dbo.Customers", "destination": "public.customers", "mode": "append"},
        })

    sync_customers()

customer_sync()
```

---

## Running a YAML job file

```bash
python -c "from syncdb import SyncDB; SyncDB.run_config_file('jobs/daily.yaml')"
```

Sample `jobs/daily.yaml`:

```yaml
source:
  engine: mssql
  host: ${SOURCE_HOST}
  database: CRM
  user: ${SOURCE_USER}
  password: ${SOURCE_PASSWORD}
  query_timeout: 120

target:
  engine: postgresql
  host: ${TARGET_HOST}
  database: warehouse
  user: ${TARGET_USER}
  password: ${TARGET_PASSWORD}

settings:
  batch_size: 10000
  progress_mode: multi_line
  verbose: standard
  retry_count: 3
  retry_delay_seconds: 2.0

tables:
  customers:
    source: dbo.Customers
    destination: public.customers
    mode: append
  orders:
    source: dbo.Orders
    destination: public.orders
    mode: soft_delete
    primary_key: [OrderID]
```

---

## Health checks

Use `connector.ping()` in Kubernetes readiness probes or Airflow sensors before starting a sync:

```python
from syncdb import DatabaseConfig
from syncdb.connections import create_connector

config = DatabaseConfig.from_env()
connector = create_connector(config)
if not connector.ping():
    raise RuntimeError("Database not reachable")
```

---

## Resource sizing

| Rows per table | RAM (typical) | Notes |
|----------------|---------------|-------|
| < 1 M | < 100 MB | Default batch_size=5000 |
| 1–10 M | 100–300 MB | Consider batch_size=1000 for wide tables |
| 10–100 M | 300 MB–1 GB | Use SOFT_DELETE v2 (SQL-based); avoid UPSERT |
| > 100 M | > 1 GB | Use FULL_REFRESH or APPEND_STAGING; watermark for incremental |

CPU usage is low (Python is the orchestrator; the DB does the heavy lifting).
Network bandwidth is the typical bottleneck for cross-datacenter syncs.
