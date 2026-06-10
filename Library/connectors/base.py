"""Connector base classes.

BaseConnector defines the interface that all engine-specific connectors must implement.
The concrete subclasses (MSSQLConnector, PostgresConnector, MySQLConnector) override
every @abstractmethod; several non-abstract helpers are provided here because their
implementation is identical across all engines.

This contract is intentionally small and row-dictionary based. Keep new shared
features here only when they are portable across all supported engines; otherwise
add the minimum engine-specific implementation in the concrete connector.
"""

from __future__ import annotations

import contextlib
import warnings
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from ..config import DatabaseConfig
from ..sql import QualifiedName, quote_identifier, quote_qualified
from ..type_mapping import Column


class BaseConnector(ABC):
    """Contract implemented by supported database connectors.

    Each engine subclass sets three class-level attributes that drive SQL generation:
      engine      - canonical engine string ("mssql", "postgresql", "mysql")
      quote_char  - identifier quote character for that engine
                    '"' (PostgreSQL/MySQL double-quote), '`' (MySQL backtick), '[' (MSSQL)
      placeholder - parameterised query placeholder: '?' (pyodbc) or '%s' (psycopg2/pymysql)

    Implementing a new connector — checklist
    -----------------------------------------
    1. Subclass BaseConnector and set `engine`, `quote_char`, `placeholder` as class attrs.
    2. Implement all @abstractmethod methods.  The shared helpers (list_tables,
       get_row_count, delete_matching_rows, update_matching_rows, copy_table_rows,
       drop_table, upsert_batch, apply_soft_deletes_sql, execute_query_batches)
       use only `execute_query` and `quote_char`, so they are free for the subclass
       to inherit without override unless the engine needs different SQL.
    3. Make connect() idempotent: guard with `if self.connection is not None: return`.
    4. Lazy-import the driver inside connect() so users who don't need this engine
       don't pay the import cost or get ImportError at package load time.
    5. execute_query() must auto-commit DML/DDL when not self._in_transaction, and
       return [] for them.
    6. insert_batch() must auto-commit when not self._in_transaction.
    7. fetch_batches() must use cursor.fetchmany(batch_size), not fetchall().
    8. create_schema() must be idempotent (IF NOT EXISTS equivalent).
    9. Override upsert_batch() with a native implementation (ON CONFLICT / MERGE /
       ON DUPLICATE KEY UPDATE) to avoid the delete+insert round-trip.
    10. Override execute_query_batches() with cursor.fetchmany() for true streaming.
    11. Register the connector in connections.py, connectors/__init__.py, config.py,
        and type_mapping.py — see connections.py module docstring for the full checklist.
    """

    engine: str
    # PostgreSQL and MySQL (double-quote mode) default; MSSQL overrides to "[".
    quote_char = '"'
    # pyodbc uses "?"; psycopg2 and pymysql use "%s".  Subclasses override accordingly.
    placeholder = "?"

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        # Lazily set by connect(); None signals that no live connection exists yet.
        self.connection = None
        # When True, execute_query/insert_batch defer commits; caller must call
        # commit() or rollback() explicitly.
        self._in_transaction = False

    @abstractmethod
    def connect(self) -> None:
        """Open an underlying DB connection.

        Implementations must be idempotent: calling connect() when self.connection
        is already set should be a no-op, not raise or open a second connection.
        """

    def close(self) -> None:
        """Close the underlying connection and reset self.connection to None."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def reconnect(self) -> None:
        """Close the current connection and reopen it.

        Called between retry attempts so that a dropped TCP connection does not
        permanently block subsequent retries.  After close(), self.connection is
        None and the next connect() call opens a fresh session.
        """
        self.close()
        self.connect()

    def ping(self) -> bool:
        """Return True if the database is reachable and a trivial query succeeds."""
        try:
            self.connect()
            self.execute_query("SELECT 1")
            return True
        except Exception:
            return False

    def begin(self) -> None:
        """Begin an explicit transaction; auto-commit is suspended until commit() or rollback()."""
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the current transaction and resume auto-commit mode."""
        if self.connection is not None:
            self.connection.commit()
        self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the current transaction and resume auto-commit mode."""
        if self.connection is not None:
            self.connection.rollback()
        self._in_transaction = False

    def __enter__(self):
        """Support the 'with connector:' context manager pattern."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def quote_table(self, schema: str | None, table: str) -> str:
        """Return a fully-quoted, engine-appropriate table reference."""
        return quote_qualified(QualifiedName(schema, table), self.quote_char)

    @abstractmethod
    def execute_query(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Execute a query and return rows as dictionaries.

        DML statements (INSERT, DELETE, TRUNCATE) return an empty list and
        auto-commit when not self._in_transaction; SELECT statements return a
        list of column-name-to-value dicts.
        """

    def execute_query_batches(
        self,
        query: str,
        params: Sequence[Any] | None = None,
        batch_size: int = 5000,
    ) -> Iterator[list[dict[str, Any]]]:
        """Execute a SQL query and yield rows in batches of up to batch_size each.

        Default implementation materialises all rows first and then slices into
        batches.  Connectors MUST override this with cursor.fetchmany() for true
        streaming — especially important for large result sets.  This fallback
        issues a RuntimeWarning to surface the oversight during development.
        """
        warnings.warn(
            f"{type(self).__name__} does not override execute_query_batches(); "
            "falling back to full materialisation via execute_query(). "
            "Override this method with cursor.fetchmany() for true streaming.",
            RuntimeWarning,
            stacklevel=2,
        )
        rows = self.execute_query(query, params)
        for i in range(0, max(1, len(rows)), batch_size):
            chunk = rows[i:i + batch_size]
            if chunk:
                yield chunk

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
        """Yield table rows in batches of up to batch_size rows each.

        Uses cursor.fetchmany() under the hood so the full result set is never
        loaded into memory at once.
        """

    @abstractmethod
    def insert_batch(
        self,
        schema: str | None,
        table: str,
        rows: Iterable[dict[str, Any]],
        columns: Sequence[str],
    ) -> int:
        """Insert rows and return the number of rows inserted."""

    def upsert_batch(
        self,
        schema: str | None,
        table: str,
        rows: Iterable[dict[str, Any]],
        columns: Sequence[str],
        primary_key: Sequence[str],
    ) -> int:
        """Upsert rows using a native engine statement; fall back to delete+insert.

        Connectors should override this with an engine-native statement:
          PostgreSQL: INSERT ... ON CONFLICT DO UPDATE
          MSSQL:      MERGE INTO ... USING ... ON ...
          MySQL:      INSERT ... ON DUPLICATE KEY UPDATE
          SQLite:     INSERT OR REPLACE / INSERT ... ON CONFLICT DO UPDATE

        The default implementation here uses delete+insert, which is correct but
        not atomic.  Override to get true atomic upsert semantics.
        """
        records = list(rows)
        if not records or not primary_key:
            return self.insert_batch(schema, table, records, columns)
        self.delete_matching_rows(schema, table, records, primary_key)
        return self.insert_batch(schema, table, records, columns)

    @abstractmethod
    def get_columns(self, schema: str | None, table: str) -> list[Column]:
        """Return ordered column metadata from INFORMATION_SCHEMA."""

    @abstractmethod
    def get_primary_keys(self, schema: str | None, table: str) -> list[str]:
        """Return primary-key column names in key ordinal order."""

    @abstractmethod
    def table_exists(self, schema: str | None, table: str) -> bool:
        """Return True if the table exists in the given schema."""

    @abstractmethod
    def create_schema(self, schema: str | None) -> None:
        """Create a schema/database namespace if it does not already exist.

        Implementations must be idempotent (IF NOT EXISTS / IF SCHEMA_ID IS NULL).
        A None schema is silently ignored.
        """

    @abstractmethod
    def create_table(self, schema: str | None, table: str, columns: Sequence[Column]) -> None:
        """Create a table from a list of mapped Columns, including a PRIMARY KEY if any."""

    @abstractmethod
    def add_column(self, schema: str | None, table: str, column: Column) -> None:
        """ALTER TABLE ADD COLUMN for a missing target column."""

    @abstractmethod
    def drop_column(self, schema: str | None, table: str, column_name: str) -> None:
        """ALTER TABLE DROP COLUMN for an extra target column."""

    @abstractmethod
    def truncate_table(self, schema: str | None, table: str) -> None:
        """Remove all rows from a table without logging individual deletes."""

    def list_tables(self, schema: str | None = None) -> list[str]:
        """Return base-table names in a schema for schema-level sync."""
        schema_name = schema or self.config.default_schema or self.config.database
        rows = self.execute_query(
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = {self.placeholder} AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            [schema_name],
        )
        # INFORMATION_SCHEMA column names are lowercase on PostgreSQL/MySQL but
        # uppercase on MSSQL; handle both.  Filter out any None values that would
        # result from a driver returning an unexpected column name casing.
        return [
            name for name in (
                row.get("table_name") or row.get("TABLE_NAME")
                for row in rows
            )
            if name is not None
        ]

    def get_row_count(self, schema: str | None, table: str, where: str = "", params: Sequence[Any] | None = None) -> int:
        """Return SELECT COUNT(*) for the table, optionally filtered by a WHERE clause."""
        name = self.quote_table(schema, table)
        row = self.execute_query(f"SELECT COUNT(*) AS row_count FROM {name}{where}", params or [])[0]
        # Use next(iter(...)) to avoid case-sensitivity issues across engines
        # (the alias is lowercase but some drivers normalise column names).
        return int(next(iter(row.values())))

    def delete_matching_rows(
        self,
        schema: str | None,
        table: str,
        rows: Sequence[dict[str, Any]],
        primary_key: Sequence[str],
    ) -> int:
        """Delete target rows matching incoming primary-key values.

        Sub-batches the predicate to stay well under driver parameter limits
        (pyodbc caps at ~2100; this uses a conservative 500-param ceiling per
        statement so a 5,000-row batch with a 2-column PK stays safe).
        """
        if not rows or not primary_key:
            return 0
        # Maximum parameters per DELETE statement; keeps each statement well
        # under pyodbc's ~2100 limit and produces manageable query plans.
        max_params = 500
        sub_size = max(1, max_params // len(primary_key))
        total = 0
        for start in range(0, len(rows), sub_size):
            chunk = rows[start:start + sub_size]
            predicates = []
            params: list[Any] = []
            for row in chunk:
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
            total += len(chunk)
        return total

    def update_matching_rows(
        self,
        schema: str | None,
        table: str,
        rows: Sequence[dict[str, Any]],
        primary_key: Sequence[str],
        values: dict[str, Any],
    ) -> int:
        """Update rows matching primary-key values with fixed column values."""
        if not rows or not primary_key or not values:
            return 0
        assignments = ", ".join(
            f"{quote_identifier(column, self.quote_char)} = {self.placeholder}"
            for column in values
        )
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
        query = f"UPDATE {self.quote_table(schema, table)} SET {assignments} WHERE " + " OR ".join(predicates)
        self.execute_query(query, list(values.values()) + params)
        return len(rows)

    # ------------------------------------------------------------------
    # Streaming SOFT_DELETE helpers
    # ------------------------------------------------------------------

    def init_seen_keys_table(
        self,
        schema: str | None,
        table: str,
        pk_columns: list[Column],
        uid: str,
    ) -> str:
        """Create a fresh temp table for streaming SOFT_DELETE PK accumulation.

        Returns the temp table name.  The uid suffix prevents concurrent syncs of
        the same table from colliding on the same temp table name.  The caller is
        responsible for dropping the table in a finally block via drop_table().
        """
        keys_table = f"__syncdb_{table[:40]}_{uid}_keys"
        key_col_defs = [Column(name=col.name, data_type=col.data_type, nullable=True) for col in pk_columns]
        self.drop_table(schema, keys_table)
        self.create_table(schema, keys_table, key_col_defs)
        return keys_table

    def apply_soft_deletes_from_keys_table(
        self,
        schema: str | None,
        table: str,
        keys_table: str,
        pk_columns: list[Column],
        deleted_at_value: str,
    ) -> int:
        """Mark rows absent from keys_table as soft-deleted.

        Runs a SQL NOT EXISTS query so no target rows are loaded into Python memory.
        Returns the count of rows newly marked as deleted_at = deleted_at_value.
        """
        primary_key = [col.name for col in pk_columns]
        target_ref = self.quote_table(schema, table)
        keys_ref = self.quote_table(schema, keys_table)
        join_conds = " AND ".join(
            f"{target_ref}.{quote_identifier(pk, self.quote_char)} = "
            f"{keys_ref}.{quote_identifier(pk, self.quote_char)}"
            for pk in primary_key
        )
        deleted_col = quote_identifier("deleted_at", self.quote_char)
        self.execute_query(
            f"UPDATE {target_ref} SET {deleted_col} = {self.placeholder} "
            f"WHERE {deleted_col} IS NULL AND NOT EXISTS ("
            f"SELECT 1 FROM {keys_ref} WHERE {join_conds}"
            f")",
            [deleted_at_value],
        )
        cnt = self.execute_query(
            f"SELECT COUNT(*) AS cnt FROM {target_ref} "
            f"WHERE {deleted_col} = {self.placeholder}",
            [deleted_at_value],
        )
        return int(cnt[0].get("cnt") or cnt[0].get("CNT") or 0)

    def apply_soft_deletes_sql(
        self,
        schema: str | None,
        table: str,
        pk_columns: list[Column],
        seen_keys: set[tuple[Any, ...]],
        deleted_at_value: str,
        batch_size: int = 5000,
    ) -> int:
        """Mark rows missing from seen_keys as soft-deleted.

        Creates a temporary key table, bulk-inserts source PKs in batches, then
        updates the target with one SQL NOT EXISTS statement.

        NOTE: This method materialises all source PKs in Python memory before
        inserting them into the database.  For tables with > 1M rows consider
        using init_seen_keys_table() + apply_soft_deletes_from_keys_table() to
        stream PKs directly without the Python-side accumulation.
        """
        import uuid as _uuid
        uid = _uuid.uuid4().hex[:8]
        primary_key = [col.name for col in pk_columns]
        key_rows = [dict(zip(primary_key, key, strict=False)) for key in seen_keys]
        keys_table = f"__syncdb_{table[:40]}_{uid}_keys"
        key_col_defs = [Column(name=col.name, data_type=col.data_type, nullable=True) for col in pk_columns]
        try:
            self.drop_table(schema, keys_table)
            self.create_table(schema, keys_table, key_col_defs)
            for i in range(0, max(1, len(key_rows)), batch_size):
                chunk = key_rows[i:i + batch_size]
                if chunk:
                    self.insert_batch(schema, keys_table, chunk, primary_key)
            return self.apply_soft_deletes_from_keys_table(
                schema, table, keys_table, pk_columns, deleted_at_value
            )
        finally:
            with contextlib.suppress(Exception):
                self.drop_table(schema, keys_table)

    def copy_table_rows(
        self,
        source_schema: str | None,
        source_table: str,
        target_schema: str | None,
        target_table: str,
        columns: Sequence[str],
    ) -> int:
        """Copy all rows from one table to another table in the same database."""
        column_sql = ", ".join(quote_identifier(column, self.quote_char) for column in columns)
        self.execute_query(
            f"INSERT INTO {self.quote_table(target_schema, target_table)} ({column_sql}) "
            f"SELECT {column_sql} FROM {self.quote_table(source_schema, source_table)}"
        )
        return self.get_row_count(source_schema, source_table)

    def drop_table(self, schema: str | None, table: str) -> None:
        """Drop a table if it exists."""
        self.execute_query(f"DROP TABLE IF EXISTS {self.quote_table(schema, table)}")

    def _column_definition(self, column: Column) -> str:
        """Build a single column definition fragment for CREATE TABLE / ALTER TABLE."""
        null_sql = " NULL" if column.nullable else " NOT NULL"
        return f"{quote_identifier(column.name, self.quote_char)} {column.data_type}{null_sql}"
