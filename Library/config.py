"""Configuration objects used by SyncDB."""

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

# MySQL has no server-level schema namespace — databases ARE schemas there, so
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
    connection_string: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = None
    default_schema: str | None = None
    connect_timeout: int = 30
    pool_min: int = 1
    pool_max: int = 5
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
        if self.pool_min <= 0 or self.pool_max <= 0 or self.pool_min > self.pool_max:
            raise ValueError("pool_min and pool_max must be positive and pool_min <= pool_max")

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
                "connection_string or host/database/user credentials are required; "
                f"missing: {', '.join(missing)}"
            )

    @property
    def normalized_engine(self) -> Engine:
        return normalize_engine(self.engine)

    def as_connection_kwargs(self) -> dict[str, Any]:
        """Return DB-API-style connection keyword arguments.

        None values are stripped so drivers that reject unexpected None kwargs
        (e.g. psycopg2 for an absent password) don't raise spurious errors.
        Caller-supplied options are merged last so they can override any default.
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
