"""Connector factory.

Centralises the engine → connector class mapping so that SyncDB and any future
callers don't need to import every connector directly or replicate the switch logic.
"""

from __future__ import annotations

from .config import DatabaseConfig
from .connectors import MSSQLConnector, MySQLConnector, PostgresConnector
from .connectors.base import BaseConnector


def create_connector(config: DatabaseConfig) -> BaseConnector:
    """Instantiate the correct connector for the given DatabaseConfig.

    config.engine is always a lowercase canonical string ("mssql", "postgresql",
    "mysql") because DatabaseConfig.__post_init__ normalises it via normalize_engine.
    String comparisons are therefore safe here without another round-trip through
    the enum.
    """
    if config.engine == "mssql":
        return MSSQLConnector(config)
    if config.engine == "postgresql":
        return PostgresConnector(config)
    if config.engine == "mysql":
        return MySQLConnector(config)
    # Should only be reachable if a new Engine value is added without updating this factory.
    raise ValueError(f"Unsupported database engine: {config.engine}")
