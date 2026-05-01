"""Configuration objects used by SyncDB."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Engine(str, Enum):
    MSSQL = "mssql"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"


_ENGINE_ALIASES = {
    "mssql": Engine.MSSQL,
    "sqlserver": Engine.MSSQL,
    "sql_server": Engine.MSSQL,
    "postgres": Engine.POSTGRESQL,
    "postgresql": Engine.POSTGRESQL,
    "pg": Engine.POSTGRESQL,
    "mysql": Engine.MYSQL,
}

_DEFAULT_PORTS = {
    Engine.MSSQL: 1433,
    Engine.POSTGRESQL: 5432,
    Engine.MYSQL: 3306,
}

_DEFAULT_SCHEMAS = {
    Engine.MSSQL: "dbo",
    Engine.POSTGRESQL: "public",
    Engine.MYSQL: None,
}


def normalize_engine(engine: str | Engine) -> Engine:
    """Return a supported engine enum from a user-facing engine string."""
    if isinstance(engine, Engine):
        return engine
    key = str(engine or "").strip().lower().replace("-", "_")
    try:
        return _ENGINE_ALIASES[key]
    except KeyError as exc:
        supported = ", ".join(sorted(_ENGINE_ALIASES))
        raise ValueError(f"Unsupported database engine '{engine}'. Supported: {supported}") from exc


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection and behavior settings for one database endpoint."""

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
        object.__setattr__(self, "engine", normalized.value)

        if self.port is None:
            object.__setattr__(self, "port", _DEFAULT_PORTS[normalized])
        if self.default_schema is None:
            object.__setattr__(self, "default_schema", _DEFAULT_SCHEMAS[normalized])

        if self.connect_timeout <= 0:
            raise ValueError("connect_timeout must be greater than zero")
        if self.pool_min <= 0 or self.pool_max <= 0 or self.pool_min > self.pool_max:
            raise ValueError("pool_min and pool_max must be positive and pool_min <= pool_max")

        if self.connection_string:
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
        """Return DB-API-style connection keyword arguments."""
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
