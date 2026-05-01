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

LINE = "-" * 76
USE_COLOR = os.environ.get("NO_COLOR", "").lower() not in {"1", "true", "yes"}


class Color:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"


def color(text: str, code: str) -> str:
    if not USE_COLOR:
        return text
    return f"{code}{text}{Color.RESET}"


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


def print_header(title: str) -> None:
    print()
    print(color(LINE, Color.CYAN))
    print(color(title, Color.BOLD + Color.CYAN))
    print(color(LINE, Color.CYAN))


def print_count_header() -> None:
    print(color(f"{'table':<20} {'expected':>12} {'actual':>12} {'status':>10}", Color.BOLD))
    print(color(f"{'-' * 20} {'-' * 12} {'-' * 12} {'-' * 10}", Color.DIM))


def wait_for_connection(target: DatabaseTarget, attempts: int = 60, delay_seconds: int = 2):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            conn = target.connect()
            print(f"{color(target.name, Color.BOLD)}: {color('connection ready', Color.GREEN)}")
            print(color(LINE, Color.DIM))
            return conn
        except Exception as exc:
            last_error = exc
            if target.auth_failure_hint and is_auth_failure(exc):
                raise RuntimeError(f"{target.name}: authentication failed. {target.auth_failure_hint}") from exc
            print(
                f"{color(target.name, Color.BOLD)}: "
                f"{color(f'waiting for connection ({attempt}/{attempts})', Color.YELLOW)} - {exc}"
            )
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
    print_header(f"{target.name.upper()} DATA VALIDATION")
    conn = wait_for_connection(target)
    try:
        print_count_header()
        for table, expected in EXPECTED_COUNTS.items():
            actual = fetch_count(conn, target, table)
            passed = actual == expected
            status_text = "OK" if passed else "FAIL"
            status = color(f"{status_text:>10}", Color.GREEN if passed else Color.RED)
            print(f"{table:<20} {expected:>12,} {actual:>12,} {status}")

            if not passed:
                errors.append(
                    f"{target.name}.{table}: expected {expected}, got {actual}"
                )
    finally:
        conn.close()

    return errors


def main() -> int:
    print_header("QUBDI SYNCDB SEED DATA CHECK")
    print("Expected row counts will be validated in MSSQL, PostgreSQL, and MySQL.")

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
                "Check logs: docker compose logs mssql-init"
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
        print_header("DATA CHECK FAILED")
        for error in all_errors:
            print(color(f"- {error}", Color.RED))
        return 1

    print_header("DATA CHECK PASSED")
    print(color("All expected tables and row counts are present in MSSQL, PostgreSQL, and MySQL.", Color.GREEN))
    return 0


if __name__ == "__main__":
    sys.exit(main())


