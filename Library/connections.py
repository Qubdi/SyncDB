"""Connector factory."""

from __future__ import annotations

from .config import DatabaseConfig
from .connectors import MSSQLConnector, MySQLConnector, PostgresConnector
from .connectors.base import BaseConnector


def create_connector(config: DatabaseConfig) -> BaseConnector:
    # config.engine is already normalized to a canonical string by DatabaseConfig.__post_init__.
    if config.engine == "mssql":
        return MSSQLConnector(config)
    if config.engine == "postgresql":
        return PostgresConnector(config)
    if config.engine == "mysql":
        return MySQLConnector(config)
    raise ValueError(f"Unsupported database engine: {config.engine}")
