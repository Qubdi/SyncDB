"""SQLite connector.

Uses Python's stdlib sqlite3 module, so it adds local-file database support
without any optional dependency.  SQLite has no schema namespace; schema arguments
are accepted for API compatibility and ignored by table metadata queries.

SQLite support is useful for tests, demos, and lightweight local sync jobs. Keep
behavior compatible with the shared connector contract even when SQLite accepts
more flexible SQL than the server databases.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .base import BaseConnector
from ..sql import quote_identifier
from ..type_mapping import Column


class SQLiteConnector(BaseConnector):
    engine = "sqlite"
    quote_char = '"'
    placeholder = "?"

    def connect(self) -> None:
        """Open an idempotent sqlite3 connection and configure row dictionaries."""
        if self.connection is not None:
            return
        database = self._database_path()
        if database != ":memory:":
            Path(database).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database, timeout=self.config.connect_timeout)
        # row_factory = sqlite3.Row makes cursor rows support both index-based and
        # column-name-based access (e.g. row["name"]).  dict(row) then produces a
        # plain dict matching the connector contract, without needing to zip column
        # names manually as the other connectors do.
        self.connection.row_factory = sqlite3.Row

    def execute_query(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, tuple(params or []))
        if not cursor.description:
            self.connection.commit()
            return []
        return [dict(row) for row in cursor.fetchall()]

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
        self.connect()
        names = ", ".join(quote_identifier(col, self.quote_char) for col in columns) if columns else "*"
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT {names} FROM {self.quote_table(schema, table)}{where}{order_by}", tuple(params or []))
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [dict(row) for row in rows]

    def insert_batch(
        self,
        schema: str | None,
        table: str,
        rows: Iterable[dict[str, Any]],
        columns: Sequence[str],
    ) -> int:
        records = list(rows)
        if not records:
            return 0
        self.connect()
        column_sql = ", ".join(quote_identifier(col, self.quote_char) for col in columns)
        placeholders = ", ".join(["?"] * len(columns))
        query = f"INSERT INTO {self.quote_table(schema, table)} ({column_sql}) VALUES ({placeholders})"
        values = [[row.get(column) for column in columns] for row in records]
        self.connection.executemany(query, values)
        self.connection.commit()
        return len(records)

    def get_columns(self, schema: str | None, table: str) -> list[Column]:
        rows = self.execute_query(f"PRAGMA table_info({quote_identifier(table, self.quote_char)})")
        return [
            Column(
                name=row["name"],
                data_type=row["type"] or "text",
                nullable=not bool(row["notnull"]),
                is_primary_key=bool(row["pk"]),
            )
            for row in rows
        ]

    def get_primary_keys(self, schema: str | None, table: str) -> list[str]:
        return [column.name for column in self.get_columns(schema, table) if column.is_primary_key]

    def table_exists(self, schema: str | None, table: str) -> bool:
        rows = self.execute_query(
            "SELECT 1 AS exists_flag FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table],
        )
        return bool(rows)

    def create_schema(self, schema: str | None) -> None:
        """No-op: SQLite does not support server-side schemas."""
        return

    def create_table(self, schema: str | None, table: str, columns: Sequence[Column]) -> None:
        definitions = [self._column_definition(column) for column in columns]
        primary_keys = [quote_identifier(column.name, self.quote_char) for column in columns if column.is_primary_key]
        if primary_keys:
            definitions.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
        self.execute_query(f"CREATE TABLE {self.quote_table(schema, table)} ({', '.join(definitions)})")

    def add_column(self, schema: str | None, table: str, column: Column) -> None:
        self.execute_query(f"ALTER TABLE {self.quote_table(schema, table)} ADD COLUMN {self._column_definition(column)}")

    def drop_column(self, schema: str | None, table: str, column_name: str) -> None:
        self.execute_query(f"ALTER TABLE {self.quote_table(schema, table)} DROP COLUMN {quote_identifier(column_name, self.quote_char)}")

    def truncate_table(self, schema: str | None, table: str) -> None:
        # SQLite has no TRUNCATE statement; DELETE FROM achieves the same logical
        # effect.  Unlike server databases, SQLite's DELETE logs individual row
        # deletions (affecting WAL/journal size), but for the row counts SyncDB
        # works with this is not a meaningful performance difference.
        self.execute_query(f"DELETE FROM {self.quote_table(schema, table)}")

    def list_tables(self, schema: str | None = None) -> list[str]:
        rows = self.execute_query(
            """
            SELECT name AS table_name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        return [row["table_name"] for row in rows]

    def quote_table(self, schema: str | None, table: str) -> str:
        """Ignore schema for SQLite while preserving the shared connector API."""
        return quote_identifier(table, self.quote_char)


    def _database_path(self) -> str:
        """Resolve sqlite:// URLs, filesystem paths, and in-memory databases."""
        if self.config.connection_string:
            parsed = urlparse(self.config.connection_string)
            if parsed.scheme not in {"sqlite", "sqlite3"}:
                raise ValueError(f"Unsupported SQLite connection string scheme: {parsed.scheme}")
            if parsed.path in {"", "/"}:
                return ":memory:"
            return parsed.path.lstrip("/") if parsed.netloc else parsed.path
        return self.config.database or ":memory:"
