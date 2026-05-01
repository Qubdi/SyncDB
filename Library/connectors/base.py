"""Connector base classes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from ..config import DatabaseConfig
from ..sql import QualifiedName, quote_identifier, quote_qualified
from ..type_mapping import Column


class BaseConnector(ABC):
    """Contract implemented by supported database connectors."""

    engine: str
    quote_char = '"'
    placeholder = "?"

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self.connection = None

    @abstractmethod
    def connect(self) -> None:
        """Open an underlying DB connection."""

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def quote_table(self, schema: str | None, table: str) -> str:
        return quote_qualified(QualifiedName(schema, table), self.quote_char)

    @abstractmethod
    def execute_query(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Execute a query and return rows as dictionaries."""

    @abstractmethod
    def fetch_batches(
        self,
        schema: str | None,
        table: str,
        columns: Sequence[str] | None = None,
        where: str = "",
        params: Sequence[Any] | None = None,
        order_by: str = "",
        batch_size: int = 5000,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield table rows in batches."""

    @abstractmethod
    def insert_batch(
        self,
        schema: str | None,
        table: str,
        rows: Iterable[dict[str, Any]],
        columns: Sequence[str],
    ) -> int:
        """Insert rows and return the number inserted."""

    @abstractmethod
    def get_columns(self, schema: str | None, table: str) -> list[Column]:
        """Return source/target column metadata."""

    @abstractmethod
    def get_primary_keys(self, schema: str | None, table: str) -> list[str]:
        """Return primary-key column names."""

    @abstractmethod
    def table_exists(self, schema: str | None, table: str) -> bool:
        """Return whether a table exists."""

    @abstractmethod
    def create_schema(self, schema: str | None) -> None:
        """Create a schema/database namespace when the engine supports it."""

    @abstractmethod
    def create_table(self, schema: str | None, table: str, columns: Sequence[Column]) -> None:
        """Create a table from mapped columns."""

    @abstractmethod
    def add_column(self, schema: str | None, table: str, column: Column) -> None:
        """Add a missing target column."""

    @abstractmethod
    def drop_column(self, schema: str | None, table: str, column_name: str) -> None:
        """Drop an extra target column."""

    @abstractmethod
    def truncate_table(self, schema: str | None, table: str) -> None:
        """Remove all rows from a table."""

    def get_row_count(self, schema: str | None, table: str, where: str = "", params: Sequence[Any] | None = None) -> int:
        name = self.quote_table(schema, table)
        row = self.execute_query(f"SELECT COUNT(*) AS row_count FROM {name}{where}", params or [])[0]
        return int(row["row_count"])

    def delete_matching_rows(
        self,
        schema: str | None,
        table: str,
        rows: Sequence[dict[str, Any]],
        primary_key: Sequence[str],
    ) -> int:
        """Delete target rows matching incoming primary-key values."""
        if not rows or not primary_key:
            return 0
        predicates = []
        params: list[Any] = []
        for row in rows:
            predicates.append(
                "("
                + " AND ".join(
                    f"{quote_identifier(column, self.quote_char)} = {self.placeholder}"
                    for column in primary_key
                )
                + ")"
            )
            params.extend(row[column] for column in primary_key)
        query = f"DELETE FROM {self.quote_table(schema, table)} WHERE " + " OR ".join(predicates)
        self.execute_query(query, params)
        return len(rows)
