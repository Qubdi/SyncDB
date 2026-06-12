"""Microsoft SQL Server connector.

Uses pyodbc as the DB-API driver.  pyodbc is not a hard install-time dependency;
the ImportError is raised lazily on the first connect() call so that users who only
work with PostgreSQL or MySQL don't need the ODBC stack installed.

Keep MSSQL-specific syntax in this module. Shared behavior belongs in
BaseConnector only when PostgreSQL, MySQL, and SQLite can execute the same shape
of SQL with their own quote characters and placeholders.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from ..sql import quote_identifier, validate_identifier
from ..type_mapping import Column
from .base import BaseConnector


class MSSQLConnector(BaseConnector):
    engine = "mssql"
    # MSSQL uses [square brackets] for identifier quoting.
    quote_char = "["
    # pyodbc uses "?" positional placeholders (ODBC standard).
    placeholder = "?"
    # System timestamp columns (_synced_at, deleted_at) use datetime2.
    timestamp_type = "datetime2"
    # T-SQL rejects the optional COLUMN keyword in ALTER TABLE ... ADD.
    _add_column_keyword = "ADD"

    @staticmethod
    def _odbc_escape(value: str) -> str:
        """Wrap an ODBC connection-string value in braces when it contains special chars.

        Per the ODBC spec, values containing '{', '}', ';', or '=' must be enclosed
        in curly braces.  A literal '}' inside the value is escaped as '}}' so the
        driver does not misparse the closing brace as the end of the quoted section.

        Prevents passwords or hostnames that contain ';' from injecting extra
        ODBC attributes into the connection string.
        """
        s = str(value)
        if any(c in s for c in ("{", "}", ";", "=")):
            return "{" + s.replace("}", "}}") + "}"
        return s

    def connect(self) -> None:
        """Open an idempotent pyodbc connection for SQL Server."""
        if self.connection is not None:
            return
        try:
            import pyodbc
        except ImportError as exc:
            raise ImportError("pyodbc is required for MSSQL connections") from exc
        if self.config.connection_string:
            # query_timeout is passed as pyodbc's `timeout` which applies to
            # query execution (not just connection).  Falls back to connect_timeout
            # when query_timeout is not set.
            timeout = self.config.query_timeout or self.config.connect_timeout
            self.connection = pyodbc.connect(self.config.connection_string, timeout=timeout)
        else:
            # Each individual value is ODBC-escaped so passwords or database names
            # that contain ';' or '=' cannot inject additional connection attributes.
            # Default to ODBC Driver 18 (current GA; TLS 1.3, strict-encrypt aware).
            # Override via options={"driver": "{ODBC Driver 17 for SQL Server}"} for
            # older installs.  Note Driver 18 defaults Encrypt=yes, so a server with
            # a self-signed cert needs options={"TrustServerCertificate": "yes"}.
            connection_string = (
                f"Driver={self.config.options.get('driver', '{ODBC Driver 18 for SQL Server}')};"
                f"Server={self._odbc_escape(f'{self.config.host},{self.config.port}')};"
                f"Database={self._odbc_escape(self.config.database or '')};"
                f"UID={self._odbc_escape(self.config.user or '')};"
                f"PWD={self._odbc_escape(self.config.password or '')};"
                f"TrustServerCertificate={self.config.options.get('TrustServerCertificate', 'no')};"
                f"LoginTimeout={self.config.connect_timeout};"
            )
            timeout = self.config.query_timeout or 0
            self.connection = pyodbc.connect(connection_string, timeout=timeout)

    def execute_query(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
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
        cursor = self.connection.cursor()
        try:
            # pyodbc fast_executemany can mis-size string buffers for mixed-length
            # varchar/nvarchar batches, raising HY000 truncation errors.  Off by default.
            cursor.fast_executemany = bool(self.config.options.get("fast_executemany", False))
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
        """Native upsert using MERGE INTO ... USING VALUES ON ... WHEN MATCHED / NOT MATCHED.

        The VALUES clause is split into sub-batches to stay under pyodbc's ~2100
        parameter limit.  Each sub-batch is one MERGE statement.
        """
        records = list(rows)
        if not records:
            return 0
        if not primary_key:
            return self.insert_batch(schema, table, records, columns)
        self.connect()
        pk_set = set(primary_key)
        non_pk = [col for col in columns if col not in pk_set]
        target_ref = self.quote_table(schema, table)
        col_sql = ", ".join(quote_identifier(col, self.quote_char) for col in columns)
        source_col_sql = ", ".join(quote_identifier(col, self.quote_char) for col in columns)
        on_clause = " AND ".join(
            f"target.{quote_identifier(pk, self.quote_char)} = source.{quote_identifier(pk, self.quote_char)}"
            for pk in primary_key
        )
        if non_pk:
            update_clause = ", ".join(
                f"target.{quote_identifier(col, self.quote_char)} = source.{quote_identifier(col, self.quote_char)}"
                for col in non_pk
            )
            when_matched = f"WHEN MATCHED THEN UPDATE SET {update_clause}"
        else:
            when_matched = ""
        insert_cols = col_sql
        insert_src = ", ".join(f"source.{quote_identifier(col, self.quote_char)}" for col in columns)
        when_not_matched = f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_src})"

        # Sub-batch to stay under pyodbc's ~2100 parameter limit.
        sub_size = max(1, 2000 // len(columns))
        total = 0
        for i in range(0, len(records), sub_size):
            chunk = records[i : i + sub_size]
            row_placeholders = ", ".join(["?"] * len(columns))
            values_rows = ", ".join(f"({row_placeholders})" for _ in chunk)
            merge_sql = (
                f"MERGE INTO {target_ref} AS target "
                f"USING (VALUES {values_rows}) AS source({source_col_sql}) "
                f"ON ({on_clause}) "
                f"{when_matched} "
                f"{when_not_matched};"
            )
            params = [row.get(col) for row in chunk for col in columns]
            cursor = self.connection.cursor()
            try:
                cursor.execute(merge_sql, params)
                if not self._in_transaction:
                    self.connection.commit()
            finally:
                cursor.close()
            total += len(chunk)
        return total

    def get_columns(self, schema: str | None, table: str) -> list[Column]:
        rows = self.execute_query(
            """
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            [schema or self.config.default_schema, table],
        )
        primary_keys = set(self.get_primary_keys(schema, table))
        return [
            Column(
                name=row["COLUMN_NAME"],
                data_type=row["DATA_TYPE"],
                char_length=row["CHARACTER_MAXIMUM_LENGTH"],
                numeric_precision=row["NUMERIC_PRECISION"],
                numeric_scale=row["NUMERIC_SCALE"],
                nullable=str(row["IS_NULLABLE"]).upper() == "YES",
                is_primary_key=row["COLUMN_NAME"] in primary_keys,
            )
            for row in rows
        ]

    def get_primary_keys(self, schema: str | None, table: str) -> list[str]:
        rows = self.execute_query(
            """
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
             AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY kcu.ORDINAL_POSITION
            """,
            [schema or self.config.default_schema, table],
        )
        return [row["COLUMN_NAME"] for row in rows]

    def table_exists(self, schema: str | None, table: str) -> bool:
        rows = self.execute_query(
            "SELECT 1 AS exists_flag FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            [schema or self.config.default_schema, table],
        )
        return bool(rows)

    def create_schema(self, schema: str | None) -> None:
        if not schema:
            return
        validate_identifier(schema)
        # CREATE SCHEMA must be the first statement in a T-SQL batch, so it is
        # wrapped in EXEC sp_executesql.  The schema name is bracket-quoted after
        # validate_identifier ensures it contains only safe identifier characters,
        # giving two independent layers of injection prevention.
        quoted = f"[{schema}]"
        self.execute_query(f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC sp_executesql N'CREATE SCHEMA {quoted}'")

    # create_table, add_column, drop_column, and truncate_table are inherited
    # from BaseConnector; only _add_column_keyword differs for MSSQL.
