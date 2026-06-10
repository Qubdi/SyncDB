"""Connector factory.

Centralises the engine-to-connector class mapping so that SyncDB and any future
callers don't need to import every connector directly or replicate the switch logic.

Adding a new engine — update these five files in the same commit/PR:
  1. connectors/<engine>.py   — implement all BaseConnector abstract methods
  2. connectors/__init__.py   — re-export the new class
  3. connections.py (here)    — add an entry to _CONNECTOR_REGISTRY
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

# Dict-based registry replaces a manual if/elif chain so that adding a new engine
# only requires a single entry here — no logic to edit.
_CONNECTOR_REGISTRY: dict[str, type[BaseConnector]] = {
    "mssql": MSSQLConnector,
    "postgresql": PostgresConnector,
    "mysql": MySQLConnector,
    "sqlite": SQLiteConnector,
}


def create_connector(config: DatabaseConfig) -> BaseConnector:
    """Instantiate the correct connector for the given DatabaseConfig.

    config.engine is always a lowercase canonical string ("mssql", "postgresql",
    "mysql") because DatabaseConfig.__post_init__ normalises it via normalize_engine.
    """
    connector_class = _CONNECTOR_REGISTRY.get(config.engine)
    if connector_class is None:
        # Reached only if a new Engine value was added to config.py without a
        # corresponding entry in _CONNECTOR_REGISTRY above.
        registered = ", ".join(sorted(_CONNECTOR_REGISTRY))
        raise ValueError(
            f"No connector registered for engine '{config.engine}'. "
            f"Registered engines: {registered}. "
            "Add an entry to _CONNECTOR_REGISTRY in connections.py."
        )
    return connector_class(config)
