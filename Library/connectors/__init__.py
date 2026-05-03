"""Database connector implementations.

Re-exports all connector classes so callers can import from the package root:
  from syncdb.connectors import MSSQLConnector
rather than reaching into individual submodules.

When adding a connector, update this file, the factory in connections.py, and
the engine normalization rules in config.py in the same change.
"""

from .base import BaseConnector
from .mssql import MSSQLConnector
from .mysql import MySQLConnector
from .postgres import PostgresConnector
from .sqlite import SQLiteConnector

__all__ = [
    "BaseConnector",
    "MSSQLConnector",
    "MySQLConnector",
    "PostgresConnector",
    "SQLiteConnector",
]
