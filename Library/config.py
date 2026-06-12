"""Configuration objects used by SyncDB.

This module owns normalization and validation for database endpoint settings.
Keep those rules here so connectors can assume they receive a canonical engine
name, engine defaults, and either a connection string or enough discrete fields
to open a connection. That keeps connector code focused on driver behavior
instead of duplicating user-input validation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Engine(str, Enum):
    MSSQL = "mssql"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"


# Accepts common informal spellings so callers don't need to know the canonical value.
# Keys are normalised to lowercase with hyphens replaced by underscores before lookup.
_ENGINE_ALIASES = {
    "mssql": Engine.MSSQL,
    "sqlserver": Engine.MSSQL,
    "sql_server": Engine.MSSQL,
    "postgres": Engine.POSTGRESQL,
    "postgresql": Engine.POSTGRESQL,
    "pg": Engine.POSTGRESQL,
    "mysql": Engine.MYSQL,
    "sqlite": Engine.SQLITE,
    "sqlite3": Engine.SQLITE,
}

# Well-known default TCP ports; applied when the caller omits port entirely.
_DEFAULT_PORTS = {
    Engine.MSSQL: 1433,
    Engine.POSTGRESQL: 5432,
    Engine.MYSQL: 3306,
    Engine.SQLITE: None,
}

# MySQL has no server-level schema namespace; databases ARE schemas there, so
# there is no meaningful "default schema" to pre-fill (the database itself serves
# that role and is already set in the database field).
_DEFAULT_SCHEMAS = {
    Engine.MSSQL: "dbo",
    Engine.POSTGRESQL: "public",
    Engine.MYSQL: None,
    Engine.SQLITE: None,
}


def normalize_engine(engine: str | Engine) -> Engine:
    """Return a supported engine enum from a user-facing engine string."""
    if isinstance(engine, Engine):
        return engine
    # strip/lower/replace so "SQL-Server", "SQL_SERVER", " Postgres " all resolve correctly.
    key = str(engine or "").strip().lower().replace("-", "_")
    try:
        return _ENGINE_ALIASES[key]
    except KeyError as exc:
        supported = ", ".join(sorted(_ENGINE_ALIASES))
        raise ValueError(f"Unsupported database engine '{engine}'. Supported: {supported}") from exc


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection and behavior settings for one database endpoint.

    Either supply a raw connection_string (passed through verbatim to the driver)
    or supply host + database + user; everything else is optional and defaults to
    sensible values for the chosen engine.

    The dataclass is frozen so instances can be used as dict keys or in sets.
    Mutating fields during __post_init__ therefore requires object.__setattr__.
    """

    engine: str | Engine
    # repr=False: a DSN routinely embeds the password ("postgresql://user:pw@host"),
    # so it must stay out of repr/tracebacks/logs just like the password field.
    connection_string: str | None = field(default=None, repr=False)
    host: str | None = None
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = field(default=None, repr=False)
    default_schema: str | None = None
    connect_timeout: int = 30
    # Maximum seconds a single query may run before the engine cancels it.
    # None means no limit beyond the driver default.
    # Applied per-engine: PostgreSQL uses SET statement_timeout (ms),
    # MSSQL uses the pyodbc query timeout, MySQL uses SET SESSION max_execution_time (ms).
    # SQLite has no query execution timeout and ignores this field.
    query_timeout: int | None = None
    # Engine-specific pass-through options forwarded verbatim to the driver.
    # Common uses per engine:
    #   MSSQL:      {"driver": "{ODBC Driver 18 for SQL Server}", "TrustServerCertificate": "no"}
    #   PostgreSQL: {"sslmode": "require", "application_name": "syncdb"}
    #   MySQL:      {"ssl_ca": "/path/to/ca.pem", "charset": "utf8mb4"}
    # These keys are merged last in as_connection_kwargs(), so they can override
    # any default field (including connect_timeout).
    options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized = normalize_engine(self.engine)
        # Store the canonical string value (e.g. "mssql") instead of the Engine
        # enum so that equality checks and serialisation work on plain strings.
        object.__setattr__(self, "engine", normalized.value)

        # Fill in engine-specific defaults that callers rarely want to override.
        if self.port is None:
            object.__setattr__(self, "port", _DEFAULT_PORTS[normalized])
        if self.default_schema is None:
            object.__setattr__(self, "default_schema", _DEFAULT_SCHEMAS[normalized])

        if self.connect_timeout <= 0:
            raise ValueError("connect_timeout must be greater than zero")
        if self.query_timeout is not None and self.query_timeout <= 0:
            raise ValueError("query_timeout must be greater than zero when set")

        # A raw connection_string is accepted as-is; individual credential fields
        # are only required when no connection_string was provided.
        if self.connection_string:
            return

        if normalized == Engine.SQLITE:
            if not self.database:
                raise ValueError("connection_string or database path is required for SQLite")
            return

        missing = [
            name
            for name, value in {
                "host": self.host,
                "database": self.database,
                "user": self.user,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(
                f"connection_string or host/database/user credentials are required; missing: {', '.join(missing)}"
            )

    @classmethod
    def from_env(cls, prefix: str = "SYNCDB") -> DatabaseConfig:
        """Build a DatabaseConfig from environment variables.

        Reads the following variables (substituting your prefix for SYNCDB):

          SYNCDB_ENGINE             — required; e.g. "postgresql", "mssql"
          SYNCDB_CONNECTION_STRING  — full DSN; when set, individual fields are ignored
          SYNCDB_HOST               — server hostname or IP
          SYNCDB_PORT               — TCP port (integer)
          SYNCDB_DATABASE           — database name
          SYNCDB_USER               — login user
          SYNCDB_PASSWORD           — login password
          SYNCDB_DEFAULT_SCHEMA     — default schema override
          SYNCDB_CONNECT_TIMEOUT    — connection timeout in seconds (integer)

        This method exists so that passwords and credentials are never embedded
        in source code or config files checked into version control.  Load them
        from a secrets manager (AWS Secrets Manager, HashiCorp Vault, Azure Key
        Vault) into environment variables at container/process startup time.

        Example::

            import os
            os.environ["SYNCDB_ENGINE"] = "postgresql"
            os.environ["SYNCDB_HOST"] = "db.example.com"
            os.environ["SYNCDB_DATABASE"] = "mydb"
            os.environ["SYNCDB_USER"] = "etl_user"
            os.environ["SYNCDB_PASSWORD"] = secret_from_vault()
            config = DatabaseConfig.from_env()
        """
        import os

        def _get(key: str) -> str | None:
            return os.environ.get(f"{prefix}_{key.upper()}") or None

        engine = _get("ENGINE")
        if not engine:
            raise ValueError(f"Environment variable {prefix}_ENGINE is required for DatabaseConfig.from_env()")
        kwargs: dict[str, Any] = {"engine": engine}
        if conn_str := _get("CONNECTION_STRING"):
            kwargs["connection_string"] = conn_str
        else:
            for field in ("host", "database", "user", "password"):
                if value := _get(field):
                    kwargs[field] = value
            if port_str := _get("PORT"):
                kwargs["port"] = int(port_str)
        if schema := _get("DEFAULT_SCHEMA"):
            kwargs["default_schema"] = schema
        if timeout_str := _get("CONNECT_TIMEOUT"):
            kwargs["connect_timeout"] = int(timeout_str)
        if query_timeout_str := _get("QUERY_TIMEOUT"):
            kwargs["query_timeout"] = int(query_timeout_str)
        return cls(**kwargs)

    @property
    def normalized_engine(self) -> Engine:
        """Return the enum form for code paths that need engine identity checks."""
        return normalize_engine(self.engine)

    def as_connection_kwargs(self) -> dict[str, Any]:
        """Return DB-API-style connection keyword arguments.

        None values are stripped so drivers that reject unexpected None kwargs
        (e.g. psycopg2 for an absent password) don't raise spurious errors.
        Caller-supplied options are merged last so they can override any default.

        NOTE: engine and default_schema are intentionally excluded — they are
        SyncDB concepts and not recognized by any DB driver.  PostgreSQL also
        requires a key rename: "database" → "dbname"; that rename is done in
        PostgresConnector.connect(), not here.
        """
        kwargs: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
        }
        kwargs.update(self.options)
        return {key: value for key, value in kwargs.items() if value is not None}
