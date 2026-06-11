"""MySQL connector.

Supports two drivers transparently:
  1. mysql-connector-python (official Oracle driver, preferred)
  2. pymysql (pure-Python fallback, no C extension required)

Driver selection is automatic: mysql.connector is tried first; if not installed,
pymysql is tried next.  Both use the same %s placeholder style.

The two drivers disagree on one keyword argument name:
  mysql-connector-python  -> connection_timeout
  pymysql                 -> connect_timeout
The connect() method normalises this difference after driver selection.

MySQL treats databases as schemas. Whenever this connector receives a schema
argument from the shared API, interpret it as the database name for metadata and
DDL queries.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any
from urllib.parse import unquote, urlparse

from ..sql import quote_identifier
from ..type_mapping import Column
from .base import BaseConnector


class MySQLConnector(BaseConnector):
    engine = "mysql"
    # MySQL uses backtick quoting for identifiers.
    quote_char = "`"
    # Both mysql-connector-python and pymysql use %s placeholders.
    placeholder = "%s"
    # MySQL DDL (CREATE/DROP/TRUNCATE TABLE) auto-commits and cannot be rolled
    # back, so temp-table strategies must not run inside explicit transactions.
    ddl_transactional = False

    def connect(self) -> None:
        """Open an idempotent MySQL connection using the first available driver."""
        if self.connection is not None:
            return
        try:
            import mysql.connector as mysql_connector
        except ImportError:
            try:
                import pymysql
            except ImportError as exc:
                raise ImportError("mysql-connector-python or pymysql is required for MySQL connections") from exc
            self.connection = pymysql.connect(**self._connection_kwargs())
            self._apply_query_timeout()
            return
        kwargs = self._connection_kwargs()
        if "connect_timeout" in kwargs:
            kwargs["connection_timeout"] = kwargs.pop("connect_timeout")
        self.connection = mysql_connector.connect(**kwargs)
        self._apply_query_timeout()

    def _apply_query_timeout(self) -> None:
        """Set session-level max_execution_time when query_timeout is configured.

        max_execution_time is in milliseconds (MySQL 5.7.8+).  On older MySQL
        and MariaDB versions the variable does not exist; only that specific
        database-level error is suppressed — all other errors propagate.
        """
        if not self.config.query_timeout:
            return
        try:
            cursor = self.connection.cursor()
            try:
                cursor.execute(f"SET SESSION max_execution_time = {int(self.config.query_timeout * 1000)}")
                self.connection.commit()
            finally:
                cursor.close()
        except Exception as exc:
            # Tolerate only "Unknown system variable" from engines that predate
            # max_execution_time support.  Any other error (auth, network, syntax)
            # must propagate so callers are aware of a broken session.
            if "unknown system variable" not in str(exc).lower():
                raise

    def execute_query(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Execute SQL and return rows as dictionaries using driver-neutral cursors."""
        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, tuple(params or []))
            if not cursor.description:
                if not self._in_transaction:
                    self.connection.commit()
                return []
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _batch_cursor(self, batch_size: int) -> Any:
        """Return a streaming cursor for batch reads.

        pymysql's default Cursor buffers the entire result set client-side at
        execute(); SSCursor streams rows from the server as they are fetched.
        mysql-connector-python cursors are unbuffered by default and already
        stream, so they use the plain cursor.
        """
        if type(self.connection).__module__.startswith("pymysql"):
            import pymysql.cursors
            return self.connection.cursor(pymysql.cursors.SSCursor)
        return self.connection.cursor()

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
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {self.quote_table(schema, table)} ({column_sql}) VALUES ({placeholders})"
        values = [[row.get(column) for column in columns] for row in records]
        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, values)
            if not self._in_transaction:
                self.connection.commit()
        finally:
            cursor.close()
        return len(records)

    def upsert_batch(
        self,
        schema: str | None,
        table: str,
        rows: Iterable[dict[str, Any]],
        columns: Sequence[str],
        primary_key: Sequence[str],
    ) -> int:
        """Native upsert using INSERT ... ON DUPLICATE KEY UPDATE.

        Non-PK columns are updated to the incoming VALUES on conflict.
        When there are no non-PK columns, updates the first PK column to itself
        (a no-op) so the statement remains valid SQL.
        """
        records = list(rows)
        if not records:
            return 0
        if not primary_key:
            return self.insert_batch(schema, table, records, columns)
        self.connect()
        column_sql = ", ".join(quote_identifier(col, self.quote_char) for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        pk_set = set(primary_key)
        non_pk = [col for col in columns if col not in pk_set]
        update_targets = non_pk if non_pk else list(primary_key[:1])
        updates = ", ".join(
            f"{quote_identifier(col, self.quote_char)} = VALUES({quote_identifier(col, self.quote_char)})"
            for col in update_targets
        )
        query = (
            f"INSERT INTO {self.quote_table(schema, table)} ({column_sql}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )
        values = [[row.get(col) for col in columns] for row in records]
        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, values)
            if not self._in_transaction:
                self.connection.commit()
        finally:
            cursor.close()
        return len(records)

    def get_columns(self, schema: str | None, table: str) -> list[Column]:
        rows = self.execute_query(
            """
            SELECT column_name, data_type, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable, column_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            [schema or self.config.database, table],
        )
        primary_keys = set(self.get_primary_keys(schema, table))
        return [
            Column(
                name=row["column_name"],
                data_type=row["data_type"],
                char_length=row["character_maximum_length"],
                numeric_precision=row["numeric_precision"],
                numeric_scale=row["numeric_scale"],
                nullable=str(row["is_nullable"]).upper() == "YES",
                is_primary_key=row["column_name"] in primary_keys,
                unsigned="unsigned" in str(row.get("column_type", "")).lower(),
            )
            for row in rows
        ]

    def get_primary_keys(self, schema: str | None, table: str) -> list[str]:
        rows = self.execute_query(
            """
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY'
            ORDER BY ordinal_position
            """,
            [schema or self.config.database, table],
        )
        return [row["column_name"] for row in rows]

    def table_exists(self, schema: str | None, table: str) -> bool:
        rows = self.execute_query(
            "SELECT 1 AS exists_flag FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            [schema or self.config.database, table],
        )
        return bool(rows)

    def create_schema(self, schema: str | None) -> None:
        if schema:
            self.execute_query(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(schema, self.quote_char)}")

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
        self.execute_query(f"TRUNCATE TABLE {self.quote_table(schema, table)}")

    def _connection_kwargs(self) -> dict[str, Any]:
        """Build a kwargs dict from the config, parsing a URL connection string if needed."""
        if not self.config.connection_string:
            return self.config.as_connection_kwargs()
        parsed = urlparse(self.config.connection_string)
        if parsed.scheme not in {"mysql", "mysql+pymysql", "mysql+mysqlconnector"}:
            raise ValueError(f"Unsupported MySQL connection string scheme: {parsed.scheme}")
        kwargs: dict[str, Any] = {
            "host": parsed.hostname,
            "port": parsed.port or self.config.port,
            "database": parsed.path.lstrip("/") or None,
            "user": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "connect_timeout": self.config.connect_timeout,
        }
        kwargs.update(self.config.options)
        return {key: value for key, value in kwargs.items() if value is not None and value != ""}
