# Secrets Management

## The rule: never embed passwords in code or config files

`DatabaseConfig.password` and connection strings contain credentials.  They must never appear in:
- Source code files checked into version control
- YAML/JSON job files committed to a repo
- Docker image layers
- Log output or error messages

SyncDB does not log passwords; `DatabaseConfig` is a frozen dataclass so accidental mutation is prevented.

---

## Pattern 1: Environment variables loaded at startup

The simplest and most portable pattern.  Load secrets from your secrets manager at process startup, set them as environment variables, then call `DatabaseConfig.from_env()`:

```python
import os
from syncdb import DatabaseConfig

# In production, a sidecar, init container, or entrypoint script populates these.
# Never hardcode them here.
config = DatabaseConfig.from_env()   # reads SYNCDB_ENGINE, SYNCDB_HOST, …
```

Supported `SYNCDB_*` variables:

| Variable | Description |
|----------|-------------|
| `SYNCDB_ENGINE` | Required: `postgresql`, `mssql`, `mysql`, `sqlite` |
| `SYNCDB_CONNECTION_STRING` | Full DSN (overrides individual fields) |
| `SYNCDB_HOST` | Server hostname |
| `SYNCDB_PORT` | TCP port |
| `SYNCDB_DATABASE` | Database name |
| `SYNCDB_USER` | Login user |
| `SYNCDB_PASSWORD` | Login password |
| `SYNCDB_DEFAULT_SCHEMA` | Default schema |
| `SYNCDB_CONNECT_TIMEOUT` | Connection timeout in seconds |
| `SYNCDB_QUERY_TIMEOUT` | Query execution timeout in seconds |

Use a custom prefix for multiple endpoints:

```python
src = DatabaseConfig.from_env("SOURCE")   # SOURCE_ENGINE, SOURCE_HOST, …
tgt = DatabaseConfig.from_env("TARGET")   # TARGET_ENGINE, TARGET_HOST, …
```

---

## Pattern 2: AWS Secrets Manager

```python
import json
import boto3
from syncdb import DatabaseConfig

def load_config_from_aws(secret_name: str, engine: str) -> DatabaseConfig:
    client = boto3.client("secretsmanager", region_name="us-east-1")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return DatabaseConfig(
        engine=engine,
        host=secret["host"],
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        port=secret.get("port"),
    )

src = load_config_from_aws("prod/mssql/crm", engine="mssql")
tgt = load_config_from_aws("prod/pg/warehouse", engine="postgresql")
```

---

## Pattern 3: HashiCorp Vault

```python
import hvac
from syncdb import DatabaseConfig

def load_config_from_vault(path: str, engine: str) -> DatabaseConfig:
    client = hvac.Client(url="https://vault.internal")
    client.auth.aws.iam_login(role="syncdb-role")   # or token auth
    secret = client.secrets.kv.v2.read_secret_version(path=path)["data"]["data"]
    return DatabaseConfig(
        engine=engine,
        host=secret["host"],
        database=secret["database"],
        user=secret["username"],
        password=secret["password"],
    )

src = load_config_from_vault("secret/mssql/crm", engine="mssql")
```

---

## Pattern 4: Azure Key Vault

```python
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from syncdb import DatabaseConfig

vault_url = "https://mykeyvault.vault.azure.net"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

config = DatabaseConfig(
    engine="mssql",
    host=client.get_secret("mssql-host").value,
    database=client.get_secret("mssql-database").value,
    user=client.get_secret("mssql-user").value,
    password=client.get_secret("mssql-password").value,
)
```

---

## Pickle file integrity

Pickle files execute arbitrary Python bytecode on load.  When you write pickle files with SyncDB and later read them from a different system or storage bucket, use HMAC to verify integrity:

```python
import os
from syncdb import FileTransfer

HMAC_KEY = os.environ["PICKLE_HMAC_KEY"]   # 32+ random bytes, stored in secrets manager
ft = FileTransfer()

# Write with HMAC — creates output.pkl + output.pkl.sig
ft.write(rows, "output.pkl", hmac_key=HMAC_KEY)

# Read with HMAC verification — raises ValueError if tampered
rows = ft.read("output.pkl", hmac_key=HMAC_KEY)
```

The `.sig` sidecar file contains a hex-encoded HMAC-SHA256 digest.  `hmac.compare_digest` is used for constant-time comparison to prevent timing attacks.

**Never** read pickle files from untrusted sources (user uploads, unverified S3 paths) without HMAC verification, regardless of HMAC support in SyncDB.

---

## Credential rotation

`DatabaseConfig` is a frozen dataclass — update it by creating a new instance:

```python
new_password = fetch_rotated_password()
config = DatabaseConfig(
    engine=config.engine,
    host=config.host,
    database=config.database,
    user=config.user,
    password=new_password,   # rotated
)
```

For long-running processes, close any open connectors and recreate them after rotation.
