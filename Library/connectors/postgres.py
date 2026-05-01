"""PostgreSQL connector.

Uses psycopg2 as the DB-API driver.  psycopg2 is not a hard install-time dependency;
the ImportError is raised lazily on the first connect() call so that users who only
work with MSSQL or MySQL don't need libpq or the psycopg2 binary installed.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from .base import BaseConnector
from ..sql import quote_identifier
from ..type_mapping import Column


class PostgresConnector(BaseConnector):
    engine = "postgresql"
    # PostgreSQL uses standard double-quote identifier quoting.
    quote_char = '"'
    # psycopg2 uses %s placeholders (same as pymysql / mysql-connector).
    placeholder = "%s"

    def connect(self) -> None:
        if self.connection is not None:
            return
        try:
            import psycopg2
        except ImportError as exc:
            raise ImportError("psycopg2 is required for PostgreSQL connections") from exc
        if self.config.connection_string:
            # psycopg2 accepts libpq connection strings or DSNs directly.
            # connect_timeout is passed as a separate kwarg because some DSN
            # forms don't include it and psycopg2 accepts it outside the DSN.
            self.connection = psycopg2.connect(self.config.connection_string, connect_timeout=self.config.connect_timeout)
        else:
            kwargs = self.config.as_connection_kwargs()
            # psycopg2 uses "dbname", not "database" — rename before passing.
            kwargs["dbname"] = kwargs.pop("database")
            self.connection = psycopg2.connect(**kwargs)

    def execute_query(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, tuple(params or []))
        if not cursor.description:
            # DML/DDL without a result set; commit to make the change visible.
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
                   numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            [schema or self.config.default_schema, table],
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
            )
            for row in rows
        ]

    def get_primary_keys(self, schema: str | None, table: str) -> list[str]:
        rows = self.execute_query(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """,
            [schema or self.config.default_schema, table],
        )
        return [row["column_name"] for row in rows]

    def table_exists(self, schema: str | None, table: str) -> bool:
        rows = self.execute_query(
            "SELECT 1 AS exists_flag FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            [schema or self.config.default_schema, table],
        )
        return bool(rows)

    def create_schema(self, schema: str | None) -> None:
        if schema:
            # IF NOT EXISTS avoids an error when two parallel processes both try
            # to create the same schema on the first run.
            self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(schema, self.quote_char)}")

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

    def _column_definition(self, column: Column) -> str:
        null_sql = " NULL" if column.nullable else " NOT NULL"
        return f"{quote_identifier(column.name, self.quote_char)} {column.data_type}{null_sql}"
