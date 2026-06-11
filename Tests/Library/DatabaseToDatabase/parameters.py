"""Database-pair parameters for live database-to-database tests.

Common tests import these scenarios instead of living under one folder per
database combination. To add another live pair, add a DatabaseScenario here and
include its id in SYNCDB_LIVE_SCENARIOS.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from syncdb import DatabaseConfig


@dataclass(frozen=True)
class DatabaseEndpoint:
    """One seeded database used by the Docker live-test stack."""

    id: str
    config: DatabaseConfig
    schema: str | None

    def table_name(self, table: str) -> str:
        if self.schema:
            return f"{self.schema}.{table}"
        return table


@dataclass(frozen=True)
class DatabaseScenario:
    """A source/target database pair for the shared live tests."""

    id: str
    label: str
    source: DatabaseEndpoint
    target: DatabaseEndpoint

    def source_table(self, table: str) -> str:
        return self.source.table_name(table)

    @property
    def source_schema(self) -> str | None:
        return self.source.schema

    @property
    def target_schema(self) -> str | None:
        return self.target.schema


POSTGRES = DatabaseEndpoint(
    id="postgresql",
    config=DatabaseConfig(
        engine="postgresql",
        connection_string="postgresql://admin:admin@localhost:15432/syncdb_test",
    ),
    schema="public",
)

MYSQL = DatabaseEndpoint(
    id="mysql",
    config=DatabaseConfig(
        engine="mysql",
        host="localhost",
        port=13306,
        database="syncdb_test",
        user="admin",
        password="admin",
    ),
    schema=None,
)

MSSQL = DatabaseEndpoint(
    id="mssql",
    config=DatabaseConfig(
        engine="mssql",
        host="localhost",
        port=11433,
        database="syncdb_test",
        user="admin",
        password="admin",
        options={"driver": "{ODBC Driver 17 for SQL Server}"},
    ),
    schema="dbo",
)


SCENARIOS: dict[str, DatabaseScenario] = {
    "postgresql_to_mysql": DatabaseScenario(
        id="postgresql_to_mysql",
        label="PostgreSQL to MySQL",
        source=POSTGRES,
        target=MYSQL,
    ),
    "postgresql_to_mssql": DatabaseScenario(
        id="postgresql_to_mssql",
        label="PostgreSQL to MSSQL",
        source=POSTGRES,
        target=MSSQL,
    ),
    "mysql_to_postgresql": DatabaseScenario(
        id="mysql_to_postgresql",
        label="MySQL to PostgreSQL",
        source=MYSQL,
        target=POSTGRES,
    ),
    "mysql_to_mssql": DatabaseScenario(
        id="mysql_to_mssql",
        label="MySQL to MSSQL",
        source=MYSQL,
        target=MSSQL,
    ),
    "mssql_to_postgresql": DatabaseScenario(
        id="mssql_to_postgresql",
        label="MSSQL to PostgreSQL",
        source=MSSQL,
        target=POSTGRES,
    ),
    "mssql_to_mysql": DatabaseScenario(
        id="mssql_to_mysql",
        label="MSSQL to MySQL",
        source=MSSQL,
        target=MYSQL,
    ),
}

DEFAULT_SCENARIOS = tuple(SCENARIOS)


def enabled_scenarios() -> tuple[DatabaseScenario, ...]:
    """Return selected scenarios from SYNCDB_LIVE_SCENARIOS.

    The env var accepts comma-separated scenario ids or "all". Unknown ids fail
    fast so a misspelled CI matrix value does not silently run the wrong pair.
    """

    raw = os.getenv("SYNCDB_LIVE_SCENARIOS", ",".join(DEFAULT_SCENARIOS)).strip()
    if not raw:
        names = DEFAULT_SCENARIOS
    elif raw.lower() == "all":
        names = tuple(SCENARIOS)
    else:
        names = tuple(part.strip() for part in raw.split(",") if part.strip())

    unknown = [name for name in names if name not in SCENARIOS]
    if unknown:
        known = ", ".join(sorted(SCENARIOS))
        raise ValueError(
            f"Unknown SYNCDB_LIVE_SCENARIOS value(s): {', '.join(unknown)}. "
            f"Known: {known}"
        )
    return tuple(SCENARIOS[name] for name in names)
