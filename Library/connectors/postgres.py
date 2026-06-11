"""PostgreSQL connector.

Uses psycopg2 as the DB-API driver.  psycopg2 is not a hard install-time dependency;
the ImportError is raised lazily on the first connect() call so that users who only
work with MSSQL or MySQL don't need libpq or the psycopg2 binary installed.

PostgreSQL is the most standards-compliant connector in this package. Use it as
the reference point for portable behavior, but keep PostgreSQL-only SQL here so
other engines are not forced to emulate it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from ..sql import quote_identifier
from ..type_mapping import Column
from .base import BaseConnector


class PostgresConnector(BaseConnector):
    engine = "postgresql"
    # PostgreSQL uses standard double-quote identifier quoting.
    quote_char = '"'
    # psycopg2 uses %s placeholders (same as pymysql / mysql-connector).
    placeholder = "%s"

    # Rows per execute_values page.  Caps the multi-row VALUES statement size so a
    # very large batch (e.g. 50k rows x 100 cols) is not assembled into one giant
    # statement that stresses the client string builder and the server parser.
    # The batch is still one cursor call; psycopg2 internally splits into pages.
    _EXECUTE_VALUES_PAGE_SIZE = 1000

    def connect(self) -> None:
        """Open an idempotent psycopg2 connection."""
        if self.connection is not None:
            return
        try:
            import psycopg2
            import psycopg2.extensions
            import psycopg2.extras
        except ImportError as exc:
            raise ImportError("psycopg2 is required for PostgreSQL connections") from exc
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
        if self.config.connection_string:
            self.connection = psycopg2.connect(
                self.config.connection_string,
                connect_timeout=self.config.connect_timeout,
            )
        else:
            kwargs = self.config.as_connection_kwargs()
            # psycopg2 uses "dbname" where every other driver uses "database".
            kwargs["dbname"] = kwargs.pop("database")
            self.connection = psycopg2.connect(**kwargs)
        # Enforce query execution timeout via session-level statement_timeout.
        # connect_timeout only covers the connection handshake; statement_timeout
        # cancels any query that runs longer than the limit (value in milliseconds).
        if self.config.query_timeout:
            cursor = self.connection.cursor()
            try:
                cursor.execute(f"SET statement_timeout = {int(self.config.query_timeout * 1000)}")
                self.connection.commit()
            finally:
                cursor.close()

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

    def _batch_cursor(self, batch_size: int) -> Any:
        """Return a named (server-side) cursor so batch reads truly stream.

        An unnamed psycopg2 cursor makes libpq materialise the ENTIRE result set
        in client memory at execute() — fetchmany() would only chunk the dict
        conversion.  A named cursor declares a server-side cursor and pulls
        itersize rows per network round-trip, so memory stays bounded by
        batch_size regardless of table size.
        """
        import uuid
        cursor = self.connection.cursor(name=f"syncdb_{uuid.uuid4().hex[:12]}")
        cursor.itersize = batch_size
        return cursor

    def insert_batch(
        self,
        schema: str | None,
        table: str,
        rows: Iterable[dict[str, Any]],
        columns: Sequence[str],
    ) -> int:
        """Bulk-insert rows using psycopg2.extras.execute_values.

        execute_values sends the entire batch in a single multi-row INSERT statement
        (one network round-trip) rather than one statement per row, which is
        dramatically faster for large batches than executemany().
        """
        records = list(rows)
        if not records:
            return 0
        self.connect()
        from psycopg2.extras import execute_values
        column_sql = ", ".join(quote_identifier(col, self.quote_char) for col in columns)
        table_ref = self.quote_table(schema, table)
        values = [tuple(row.get(col) for col in columns) for row in records]
        cursor = self.connection.cursor()
        try:
            execute_values(
                cursor,
                f"INSERT INTO {table_ref} ({column_sql}) VALUES %s",
                values,
                page_size=self._EXECUTE_VALUES_PAGE_SIZE,
            )
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
        """Native upsert using INSERT ... ON CONFLICT DO UPDATE via execute_values.

        Non-PK columns are updated to the EXCLUDED (incoming) values on conflict.
        PK-only tables (no non-PK columns) use DO NOTHING to avoid a no-op update.
        """
        records = list(rows)
        if not records:
            return 0
        if not primary_key:
            return self.insert_batch(schema, table, records, columns)
        self.connect()
        from psycopg2.extras import execute_values
        column_sql = ", ".join(quote_identifier(col, self.quote_char) for col in columns)
        pk_sql = ", ".join(quote_identifier(pk, self.quote_char) for pk in primary_key)
        pk_set = set(primary_key)
        non_pk = [col for col in columns if col not in pk_set]
        if non_pk:
            updates = ", ".join(
                f"{quote_identifier(col, self.quote_char)} = EXCLUDED.{quote_identifier(col, self.quote_char)}"
                for col in non_pk
            )
            conflict_action = f"DO UPDATE SET {updates}"
        else:
            conflict_action = "DO NOTHING"
        table_ref = self.quote_table(schema, table)
        query = (
            f"INSERT INTO {table_ref} ({column_sql}) VALUES %s "
            f"ON CONFLICT ({pk_sql}) {conflict_action}"
        )
        values = [tuple(row.get(col) for col in columns) for row in records]
        cursor = self.connection.cursor()
        try:
            execute_values(cursor, query, values, page_size=self._EXECUTE_VALUES_PAGE_SIZE)
            if not self._in_transaction:
                self.connection.commit()
        finally:
            cursor.close()
        return len(records)

    def get_columns(self, schema: str | None, table: str) -> list[Column]:
        rows = self.execute_query(
            """
            SELECT column_name, data_type, udt_name, character_maximum_length,
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
                data_type=self._resolve_data_type(row["data_type"], row.get("udt_name", "")),
                char_length=row["character_maximum_length"],
                numeric_precision=row["numeric_precision"],
                numeric_scale=row["numeric_scale"],
                nullable=str(row["is_nullable"]).upper() == "YES",
                is_primary_key=row["column_name"] in primary_keys,
            )
            for row in rows
        ]

    @staticmethod
    def _resolve_data_type(data_type: str, udt_name: str) -> str:
        # information_schema reports 'ARRAY' for array columns; the actual element
        # type is in udt_name with a leading underscore (e.g. '_text' → 'text[]').
        if (data_type or "").upper() == "ARRAY":
            element = (udt_name or "").lstrip("_")
            return f"{element}[]" if element else "text[]"
        if (data_type or "").lower() == "character varying":
            return "varchar"
        return data_type

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
