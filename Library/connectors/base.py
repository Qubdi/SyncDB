"""Connector base classes.

BaseConnector defines the interface that all engine-specific connectors must implement.
The concrete subclasses (MSSQLConnector, PostgresConnector, MySQLConnector) override
every @abstractmethod; two non-abstract helpers (get_row_count, delete_matching_rows)
are provided here because their implementation is identical across all engines.

This contract is intentionally small and row-dictionary based. Keep new shared
features here only when they are portable across all supported engines; otherwise
add the minimum engine-specific implementation in the concrete connector.
"""

from __future__ import annotations

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
       drop_table) use only `execute_query` and `quote_char`, so they are free for the
       subclass to inherit without override unless the engine needs different SQL.
    3. Make connect() idempotent: guard with `if self.connection is not None: return`.
    4. Lazy-import the driver inside connect() so users who don't need this engine
       don't pay the import cost or get ImportError at package load time.
    5. execute_query() must auto-commit DML/DDL (no result set) and return [] for them.
    6. fetch_batches() must use cursor.fetchmany(batch_size), not fetchall(), to avoid
       loading entire result sets into memory.
    7. create_schema() must be idempotent (IF NOT EXISTS equivalent).
    8. Register the connector in connections.py, connectors/__init__.py, config.py, and
       type_mapping.py — see connections.py module docstring for the full checklist.
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

    def ping(self) -> bool:
        """Return True if the database is reachable and a trivial query succeeds.

        Useful for health checks in orchestrators (Airflow, Prefect, Kubernetes
        readiness probes) before starting a long-running sync job.  Opens a new
        connection if none is currently held; does NOT keep the connection open.
        """
        try:
            self.connect()
            self.execute_query("SELECT 1")
            return True
        except Exception:
            return False

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
        auto-commit; SELECT statements return a list of column-name-to-value dicts.
        """

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
        loaded into memory at once.  The iterator is exhausted when the cursor
        returns an empty batch.
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
        """Return base-table names in a schema for schema-level sync.

        The information_schema query works for MSSQL, PostgreSQL, and MySQL.
        SQLite overrides this because it stores table metadata in sqlite_master.
        """
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
        return [row.get("table_name") or row.get("TABLE_NAME") for row in rows]

    def get_row_count(self, schema: str | None, table: str, where: str = "", params: Sequence[Any] | None = None) -> int:
        """Return SELECT COUNT(*) for the table, optionally filtered by a WHERE clause."""
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
        """Delete target rows matching incoming primary-key values.

        Builds a single DELETE WHERE (pk1=? AND pk2=?) OR (...) statement.
        One parameterised predicate is emitted per source row, so the parameter
        list and OR-clause length both scale linearly with batch_size.  For very
        large batches (> ~10 000 rows) this can hit driver parameter limits on
        some engines; use a smaller batch_size if that becomes an issue.
        """
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
        # SET placeholders are filled first (values.values()), then WHERE predicates (params).
        # The order must match the left-to-right appearance of placeholders in the query.
        self.execute_query(query, list(values.values()) + params)
        return len(rows)

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
        """Build a single column definition fragment for CREATE TABLE / ALTER TABLE.

        Shared by all connectors; the engine-appropriate quote_char is taken from
        self.quote_char so subclasses get correct quoting without overriding this method.
        """
        null_sql = " NULL" if column.nullable else " NOT NULL"
        return f"{quote_identifier(column.name, self.quote_char)} {column.data_type}{null_sql}"
