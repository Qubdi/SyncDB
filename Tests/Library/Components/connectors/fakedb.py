"""A minimal fake DB-API 2.0 connection/cursor for connector unit tests.

Pre-setting connector.connection to a FakeConnection makes BaseConnector.connect()
a no-op (its idempotency guard returns early when connection is not None), so the
real engine connectors can be exercised without a live database or driver socket.

The fake records every executed statement so tests can assert the exact SQL and
parameters a connector generates, and it returns canned result sets matched by a
substring of the query (so metadata queries like get_columns can be stubbed).
"""

from __future__ import annotations

from typing import Any


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self._conn = connection
        self.description: list[tuple[Any, ...]] | None = None
        self._rows: list[tuple[Any, ...]] = []
        # MSSQLConnector.insert_batch assigns this; accept it like a real pyodbc cursor.
        self.fast_executemany = False
        # PostgresConnector._batch_cursor assigns this like a psycopg2 named cursor.
        self.itersize = 0
        # execute_update reads this; DB-API default for "unknown" is -1.
        self.rowcount = -1

    def execute(self, query: str, params: Any = None) -> None:
        self._conn.executed.append((query, tuple(params or ())))
        self._apply_result(query)

    def executemany(self, query: str, seq_of_params: Any) -> None:
        self._conn.executed.append((query, [tuple(p) for p in seq_of_params]))
        # executemany is only used for writes here — no result set.
        self.description = None
        self._rows = []

    def _apply_result(self, query: str) -> None:
        lowered = query.lower()
        for substring, headers, rows in self._conn.results:
            if substring.lower() in lowered:
                self.description = [(h,) for h in headers]
                self._rows = list(rows)
                return
        # No canned match → treat as DML/DDL (no rows).
        self.description = None
        self._rows = []

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows, self._rows = self._rows, []
        return rows

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        batch = self._rows[:size]
        self._rows = self._rows[size:]
        return batch

    def close(self) -> None:
        pass


class FakeConnection:
    """Records executed SQL and serves canned result sets.

    results: list of (query_substring, headers, rows) tuples.  The first whose
    substring appears (case-insensitively) in an executed query supplies the
    description/rows for that statement.
    """

    def __init__(self, results: list[tuple[str, list[str], list[tuple[Any, ...]]]] | None = None) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.results = results or []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        # When set, close() raises once to simulate a driver teardown failure.
        self.fail_close = False

    def cursor(self, *args: Any, **kwargs: Any) -> FakeCursor:
        # Real drivers take cursor-class/name arguments (pymysql SSCursor,
        # psycopg2 named cursors); the fake accepts and ignores them.
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        if self.fail_close:
            self.fail_close = False
            raise RuntimeError("simulated close failure")
        self.closed = True

    # Convenience for assertions ------------------------------------------------
    def queries(self) -> list[str]:
        return [q for q, _ in self.executed]

    def last_query(self) -> str:
        return self.executed[-1][0]
