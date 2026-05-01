"""Connector factory."""

from __future__ import annotations

from .config import DatabaseConfig, normalize_engine
from .connectors import MSSQLConnector, MySQLConnector, PostgresConnector
from .connectors.base import BaseConnector


def create_connector(config: DatabaseConfig) -> BaseConnector:
    engine = normalize_engine(config.engine).value
    if engine == "mssql":
        return MSSQLConnector(config)
    if engine == "postgresql":
        return PostgresConnector(config)
    if engine == "mysql":
        return MySQLConnector(config)
    raise ValueError(f"Unsupported database engine: {config.engine}")
