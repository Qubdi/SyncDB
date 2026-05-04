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

from collections.abc import Iterable, Iterator, Sequence
from typing import Any
from urllib.parse import unquote, urlparse

from .base import BaseConnector
from ..sql import quote_identifier
from ..type_mapping import Column


class MySQLConnector(BaseConnector):
    engine = "mysql"
    # MySQL uses backtick quoting for identifiers.
    quote_char = "`"
    # Both mysql-connector-python and pymysql use %s placeholders.
    placeholder = "%s"

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
            # pymysql uses connect_timeout (matches our config key directly).
            self.connection = pymysql.connect(**self._connection_kwargs())
            return
        # mysql-connector-python uses connection_timeout, not connect_timeout;
        # rename the key so the driver doesn't raise an unexpected keyword error.
        kwargs = self._connection_kwargs()
        if "connect_timeout" in kwargs:
            kwargs["connection_timeout"] = kwargs.pop("connect_timeout")
        self.connection = mysql_connector.connect(**kwargs)

    def execute_query(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Execute SQL and return rows as dictionaries using driver-neutral cursors."""
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, tuple(params or []))
        if not cursor.description:
            self.connection.commit()
            return []
        columns = [col[0].lower() for col in cursor.description]
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
        headers = [col[0].lower() for col in cursor.description]
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
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {self.quote_table(schema, table)} ({column_sql}) VALUES ({placeholders})"
        values = [[row.get(column) for column in columns] for row in records]
        cursor = self.connection.cursor()
        cursor.executemany(query, values)
        self.connection.commit()
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
            # MySQL has no server-level schema namespace; database IS the schema.
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
                # data_type only returns the base type (e.g. "int"); column_type
                # returns the full definition (e.g. "int unsigned"), so we extract
                # the UNSIGNED flag from column_type for correct range-widening in
                # SchemaMapper._to_postgresql and _to_mssql.
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
            # In MySQL a "schema" is a database; CREATE DATABASE is the equivalent.
            self.execute_query(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(schema, self.quote_char)}")

    def create_table(self, schema: str | None, table: str, columns: Sequence[Column]) -> None:
        definitions = [self._column_definition(column) for column in columns]
        primary_keys = [quote_identifier(column.name, self.quote_char) for column in columns if column.is_primary_key]
        if primary_keys:
            definitions.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
        self.execute_query(f"CREATE TABLE {self.quote_table(schema, table)} ({', '.join(definitions)})")

    def add_column(self, schema: str | None, table: str, column: Column) -> None:
        # MySQL requires the "COLUMN" keyword in ALTER TABLE ADD COLUMN.
        self.execute_query(f"ALTER TABLE {self.quote_table(schema, table)} ADD COLUMN {self._column_definition(column)}")

    def drop_column(self, schema: str | None, table: str, column_name: str) -> None:
        self.execute_query(f"ALTER TABLE {self.quote_table(schema, table)} DROP COLUMN {quote_identifier(column_name, self.quote_char)}")

    def truncate_table(self, schema: str | None, table: str) -> None:
        self.execute_query(f"TRUNCATE TABLE {self.quote_table(schema, table)}")


    def _connection_kwargs(self) -> dict[str, Any]:
        """Build a kwargs dict from the config, parsing a URL connection string if needed.

        Accepted URL schemes: mysql://, mysql+pymysql://, mysql+mysqlconnector://
        URL percent-encoding is decoded from username and password fields.

        NOTE: The "mysql+driver" prefixes follow SQLAlchemy URL convention so that
        connection strings written for SQLAlchemy-based tools (Alembic, dbt, etc.)
        can be reused here without modification.  Only the host/port/user/password/db
        components are extracted; SQLAlchemy query parameters (e.g. ?charset=utf8mb4)
        are NOT parsed.  Pass those via DatabaseConfig.options instead.
        """
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
        # Strip None and empty-string values; drivers raise on unexpected None kwargs.
        return {key: value for key, value in kwargs.items() if value is not None and value != ""}
