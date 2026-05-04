"""Microsoft SQL Server connector.

Uses pyodbc as the DB-API driver.  pyodbc is not a hard install-time dependency;
the ImportError is raised lazily on the first connect() call so that users who only
work with PostgreSQL or MySQL don't need the ODBC stack installed.

Keep MSSQL-specific syntax in this module. Shared behavior belongs in
BaseConnector only when PostgreSQL, MySQL, and SQLite can execute the same shape
of SQL with their own quote characters and placeholders.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from .base import BaseConnector
from ..sql import quote_identifier, validate_identifier
from ..type_mapping import Column


class MSSQLConnector(BaseConnector):
    engine = "mssql"
    # MSSQL uses [square brackets] for identifier quoting.
    quote_char = "["
    # pyodbc uses "?" positional placeholders (ODBC standard).
    placeholder = "?"

    def connect(self) -> None:
        """Open an idempotent pyodbc connection for SQL Server."""
        if self.connection is not None:
            return
        try:
            import pyodbc
        except ImportError as exc:
            raise ImportError("pyodbc is required for MSSQL connections") from exc
        if self.config.connection_string:
            self.connection = pyodbc.connect(self.config.connection_string, timeout=self.config.connect_timeout)
        else:
            # Build an ODBC connection string from individual config fields.
            # TrustServerCertificate defaults to "yes" for ease of use in dev
            # environments where the server uses a self-signed cert; override via
            # config.options["TrustServerCertificate"] = "no" for production.
            connection_string = (
                f"Driver={self.config.options.get('driver', '{ODBC Driver 17 for SQL Server}')};"
                f"Server={self.config.host},{self.config.port};"
                f"Database={self.config.database};"
                f"UID={self.config.user};"
                f"PWD={self.config.password or ''};"
                f"TrustServerCertificate={self.config.options.get('TrustServerCertificate', 'yes')};"
            )
            self.connection = pyodbc.connect(connection_string, timeout=self.config.connect_timeout)

    def execute_query(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, tuple(params or []))
        if not cursor.description:
            # DML statements (INSERT, DELETE, TRUNCATE, DDL) return no description;
            # commit immediately so the change is visible to subsequent queries.
            self.connection.commit()
            return []
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

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
        headers = [col[0] for col in cursor.description]
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [dict(zip(headers, row)) for row in rows]

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
        # pyodbc fast_executemany can mis-size string buffers for mixed-length
        # varchar/nvarchar batches and nvarchar(max), raising HY000 truncation or
        # MemoryError before SQL Server sees the rows. Keep the reliable DB-API
        # path as the default; callers can opt into the faster path once their
        # driver/table shape is known to be safe.
        cursor.fast_executemany = bool(self.config.options.get("fast_executemany", False))
        cursor.executemany(query, values)
        self.connection.commit()
        return len(records)

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
        # validate_identifier is also called upstream by parse_qualified_name,
        # but we MUST call it here too because schema is embedded directly in a
        # raw string literal inside the EXEC call — parameterisation is impossible
        # for schema names in MSSQL DDL.  Removing this check would open a DDL
        # injection vector even if upstream validation is present.
        validate_identifier(schema)
        # CREATE SCHEMA must run in its own batch (no other statements on the same
        # batch); EXEC() isolates it so it can follow other DDL in the same connection.
        self.execute_query(f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA {schema}')")

    def create_table(self, schema: str | None, table: str, columns: Sequence[Column]) -> None:
        definitions = [self._column_definition(column) for column in columns]
        primary_keys = [quote_identifier(column.name, self.quote_char) for column in columns if column.is_primary_key]
        if primary_keys:
            definitions.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
        self.execute_query(f"CREATE TABLE {self.quote_table(schema, table)} ({', '.join(definitions)})")

    def add_column(self, schema: str | None, table: str, column: Column) -> None:
        # MSSQL uses "ALTER TABLE ADD col type" (no "COLUMN" keyword).
        self.execute_query(f"ALTER TABLE {self.quote_table(schema, table)} ADD {self._column_definition(column)}")

    def drop_column(self, schema: str | None, table: str, column_name: str) -> None:
        self.execute_query(f"ALTER TABLE {self.quote_table(schema, table)} DROP COLUMN {quote_identifier(column_name, self.quote_char)}")

    def truncate_table(self, schema: str | None, table: str) -> None:
        self.execute_query(f"TRUNCATE TABLE {self.quote_table(schema, table)}")

