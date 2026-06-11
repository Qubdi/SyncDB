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
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..sql import quote_identifier
from ..type_mapping import Column
from .base import BaseConnector


class SQLiteConnector(BaseConnector):
    engine = "sqlite"
    quote_char = '"'
    placeholder = "?"
    # SQLite has no native timestamp type; ISO-8601 strings in TEXT columns.
    timestamp_type = "text"

    def connect(self) -> None:
        """Open an idempotent sqlite3 connection and configure row dictionaries."""
        if self.connection is not None:
            return
        database = self._database_path()
        if database != ":memory:":
            Path(database).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database, timeout=self.config.connect_timeout)
        # row_factory = sqlite3.Row makes cursor rows support column-name-based
        # access so dict(row) produces a plain dict matching the connector contract.
        self.connection.row_factory = sqlite3.Row
        # SQLite has no server-side query timeout; connect_timeout controls how
        # long to wait when the database file is locked by another process.

    def execute_query(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, tuple(params or []))
            if not cursor.description:
                if not self._in_transaction:
                    self.connection.commit()
                return []
            return [dict(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

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
        if not self._in_transaction:
            self.connection.commit()
        return len(records)

    def upsert_batch(
        self,
        schema: str | None,
        table: str,
        rows: Iterable[dict[str, Any]],
        columns: Sequence[str],
        primary_key: Sequence[str],
    ) -> int:
        """Native upsert using INSERT ... ON CONFLICT DO UPDATE (SQLite 3.24+).

        Falls back to INSERT OR REPLACE when there are no non-PK columns, which
        deletes the existing row and inserts a new one (preserves uniqueness but
        resets any columns not in the sync column list).
        """
        records = list(rows)
        if not records:
            return 0
        if not primary_key:
            return self.insert_batch(schema, table, records, columns)
        self.connect()
        column_sql = ", ".join(quote_identifier(col, self.quote_char) for col in columns)
        placeholders = ", ".join(["?"] * len(columns))
        pk_set = set(primary_key)
        non_pk = [col for col in columns if col not in pk_set]
        if non_pk:
            pk_sql = ", ".join(quote_identifier(pk, self.quote_char) for pk in primary_key)
            updates = ", ".join(
                f"{quote_identifier(col, self.quote_char)} = excluded.{quote_identifier(col, self.quote_char)}"
                for col in non_pk
            )
            query = (
                f"INSERT INTO {self.quote_table(schema, table)} ({column_sql}) VALUES ({placeholders}) "
                f"ON CONFLICT ({pk_sql}) DO UPDATE SET {updates}"
            )
        else:
            # No non-PK columns — INSERT OR REPLACE is the simplest idempotent option.
            query = f"INSERT OR REPLACE INTO {self.quote_table(schema, table)} ({column_sql}) VALUES ({placeholders})"
        values = [[row.get(col) for col in columns] for row in records]
        self.connection.executemany(query, values)
        if not self._in_transaction:
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
        # SQLite has no TRUNCATE statement; DELETE FROM achieves the same effect.
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
