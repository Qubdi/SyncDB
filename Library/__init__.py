"""Public API for the SyncDB package.

Keep this module as the stable import surface for application code. Internal
module names can evolve, but symbols exported here should be treated as part of
the package contract and changed with backwards compatibility in mind.
"""

from .config import DatabaseConfig
from .files import FileFormat, FileTransfer
from .progress import ProgressMode, ProgressReporter
from .sync import SyncDB, TableSyncResult, TransferMode
from .type_mapping import Column, SchemaMapper

__all__ = [
    # Explicit exports make accidental internals invisible to wildcard imports
    # and give future maintainers one place to review the public API.
    "Column",
    "DatabaseConfig",
    "FileFormat",
    "FileTransfer",
    "ProgressMode",
    "ProgressReporter",
    "SchemaMapper",
    "SyncDB",
    "TableSyncResult",
    "TransferMode",
]
