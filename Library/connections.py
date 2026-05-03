"""Connector factory.

Centralises the engine-to-connector class mapping so that SyncDB and any future
callers don't need to import every connector directly or replicate the switch logic.

Adding a new engine — update these four files in the same commit/PR:
  1. connectors/<engine>.py   — implement all BaseConnector abstract methods
  2. connectors/__init__.py   — re-export the new class
  3. connections.py (here)    — add a branch in create_connector()
  4. config.py                — add alias entries in _ENGINE_ALIASES,
                                 a port in _DEFAULT_PORTS, and a schema in _DEFAULT_SCHEMAS
  5. type_mapping.py          — add _to_<engine>() mapping method in SchemaMapper

Missing any step will cause an engine name that DatabaseConfig accepts to raise
an unhandled ValueError or AttributeError at runtime.
"""

from __future__ import annotations

from .config import DatabaseConfig
from .connectors import MSSQLConnector, MySQLConnector, PostgresConnector, SQLiteConnector
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
    if config.engine == "sqlite":
        return SQLiteConnector(config)
    # Should only be reachable if a new Engine value is added without updating this factory.
    raise ValueError(f"Unsupported database engine: {config.engine}")
