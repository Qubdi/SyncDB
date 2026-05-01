import os
import sys
import time
from dataclasses import dataclass
from typing import Callable

import mysql.connector
import psycopg
import pymssql


EXPECTED_COUNTS = {
    "customers": 250_000,
    "products": 2_500,
    "orders": 1_000_000,
    "payments": 1_000_000,
    "sync_audit": 500,
    "datatype_samples": 25,
}


@dataclass(frozen=True)
class DatabaseTarget:
    name: str
    connect: Callable[[], object]
    table_name: Callable[[str], str]
    auth_failure_hint: str = ""


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def is_auth_failure(exc: Exception) -> bool:
    text = str(exc).lower()
    auth_markers = (
        "login failed",
        "access denied",
        "authentication failed",
        "password authentication failed",
    )
    return any(marker in text for marker in auth_markers)


def wait_for_connection(target: DatabaseTarget, attempts: int = 60, delay_seconds: int = 2):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            conn = target.connect()
            print(f"{target.name}: connection ready")
            return conn
        except Exception as exc:
            last_error = exc
            if target.auth_failure_hint and is_auth_failure(exc):
                raise RuntimeError(f"{target.name}: authentication failed. {target.auth_failure_hint}") from exc
            print(f"{target.name}: waiting for connection ({attempt}/{attempts}) - {exc}")
            time.sleep(delay_seconds)

    raise RuntimeError(f"{target.name}: connection failed after {attempts} attempts: {last_error}")


def fetch_count(conn, target: DatabaseTarget, table: str) -> int:
    query = f"SELECT COUNT(*) FROM {target.table_name(table)}"
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return int(row[0])


def check_database(target: DatabaseTarget) -> list[str]:
    errors = []
    conn = wait_for_connection(target)
    try:
        print(f"\n{target.name}: validating seeded row counts")
        for table, expected in EXPECTED_COUNTS.items():
            actual = fetch_count(conn, target, table)
            status = "OK" if actual == expected else "FAIL"
            print(f"{target.name}: {table:<18} expected={expected:<10} actual={actual:<10} {status}")

            if actual != expected:
                errors.append(
                    f"{target.name}.{table}: expected {expected}, got {actual}"
                )
    finally:
        conn.close()

    return errors


def main() -> int:
    targets = [
        DatabaseTarget(
            name="mssql",
            connect=lambda: pymssql.connect(
                server=env("MSSQL_HOST", "mssql"),
                port=int(env("MSSQL_PORT", "1433")),
                user=env("MSSQL_USER", "admin"),
                password=env("MSSQL_PASSWORD", "admin"),
                database=env("MSSQL_DATABASE", "syncdb_test"),
                login_timeout=5,
                timeout=30,
            ),
            table_name=lambda table: f"dbo.{table}",
            auth_failure_hint=(
                "The admin/admin login is created by the MSSQL seed job. "
                "Run: docker compose run --rm mssql-init"
            ),
        ),
        DatabaseTarget(
            name="postgres",
            connect=lambda: psycopg.connect(
                host=env("POSTGRES_HOST", "postgres"),
                port=int(env("POSTGRES_PORT", "5432")),
                user=env("POSTGRES_USER", "admin"),
                password=env("POSTGRES_PASSWORD", "admin"),
                dbname=env("POSTGRES_DATABASE", "syncdb_test"),
                connect_timeout=5,
            ),
            table_name=lambda table: table,
        ),
        DatabaseTarget(
            name="mysql",
            connect=lambda: mysql.connector.connect(
                host=env("MYSQL_HOST", "mysql"),
                port=int(env("MYSQL_PORT", "3306")),
                user=env("MYSQL_USER", "admin"),
                password=env("MYSQL_PASSWORD", "admin"),
                database=env("MYSQL_DATABASE", "syncdb_test"),
                connection_timeout=5,
            ),
            table_name=lambda table: table,
        ),
    ]

    all_errors = []
    for target in targets:
        try:
            all_errors.extend(check_database(target))
        except Exception as exc:
            all_errors.append(f"{target.name}: validation failed with error: {exc}")

    if all_errors:
        print("\nDATA CHECK FAILED")
        for error in all_errors:
            print(f"- {error}")
        return 1

    print("\nDATA CHECK PASSED")
    print("All expected tables and row counts are present in MSSQL, PostgreSQL, and MySQL.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
