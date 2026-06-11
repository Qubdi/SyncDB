"""Shared helpers for live database-to-database tests."""

from __future__ import annotations

import dataclasses
import sys
import unittest

from syncdb import ProgressMode, SyncDB
from syncdb.connections import create_connector
from syncdb.sql import quote_identifier

from Tests.Library.test_env import live_output_enabled, live_progress_mode, live_verbose

from .parameters import DatabaseScenario, enabled_scenarios

SKIP_MSG = "Docker DB stack not running - start with: docker compose up -d --build"
TEST_TABLE_PREFIXES = ("t_", "syncdb_test_")

def scenario_class_name(scenario: DatabaseScenario, case_name: str) -> str:
    prefix = "".join(part.capitalize() for part in scenario.id.split("_"))
    return f"Test{prefix}{case_name.removeprefix('_')}"


def materialize_scenario_classes(module_globals: dict[str, object], *case_classes: type) -> None:
    """Create concrete unittest classes for every enabled database scenario."""

    for case_class in case_classes:
        case_class.__test__ = False

    for scenario in enabled_scenarios():
        for case_class in case_classes:
            name = scenario_class_name(scenario, case_class.__name__)
            module_globals[name] = type(name, (case_class,), {"scenario": scenario, "__test__": True})


# Reachability is probed once per endpoint per pytest session.  Without this
# cache, every scenario class's setUpClass opened fresh connections and — with
# the Docker stack down — the default suite spent minutes waiting on connection
# timeouts before skipping.
_REACHABILITY_CACHE: dict[tuple[object, ...], bool] = {}


def _endpoint_reachable(cfg) -> bool:
    key = (cfg.engine, cfg.host, cfg.port, cfg.database, cfg.connection_string)
    if key not in _REACHABILITY_CACHE:
        # Short probe timeout: deciding whether to skip should take seconds,
        # not the production 30s connect_timeout per endpoint per class.
        probe = dataclasses.replace(cfg, connect_timeout=3)
        try:
            connector = create_connector(probe)
            connector.connect()
            connector.close()
            _REACHABILITY_CACHE[key] = True
        except Exception:
            _REACHABILITY_CACHE[key] = False
    return _REACHABILITY_CACHE[key]


def databases_reachable(scenario: DatabaseScenario) -> bool:
    return _endpoint_reachable(scenario.source.config) and _endpoint_reachable(scenario.target.config)


def source_placeholder(scenario: DatabaseScenario) -> str:
    return create_connector(scenario.source.config).placeholder


def parameterized_filter(scenario: DatabaseScenario, where: str, params: list[object]) -> dict:
    return {"where": where.format(p=source_placeholder(scenario)), "params": params}


def make_sync(scenario: DatabaseScenario, **kwargs) -> SyncDB:
    detail = live_output_enabled()
    sync = SyncDB(
        source=scenario.source.config,
        target=scenario.target.config,
        progress_mode=live_progress_mode(),
        verbose=live_verbose(),
        verbose_stream=sys.__stdout__ if detail else None,
        **kwargs,
    )
    if detail:
        sync.progress.stream = sys.__stdout__
    return sync


def target_connector(scenario: DatabaseScenario):
    connector = create_connector(scenario.target.config)
    connector.connect()
    return connector


def drop(scenario: DatabaseScenario, *tables: str) -> None:
    connector = target_connector(scenario)
    try:
        for table in tables:
            connector.drop_table(scenario.target_schema, table)
    finally:
        connector.close()


def drop_all_target_tables(scenario: DatabaseScenario, *extra: str) -> None:
    connector = target_connector(scenario)
    try:
        names = [
            table
            for table in connector.list_tables(scenario.target_schema)
            if table.startswith(TEST_TABLE_PREFIXES)
        ] + list(extra)
        for table in names:
            connector.drop_table(scenario.target_schema, table)
    finally:
        connector.close()


def count(scenario: DatabaseScenario, table: str) -> int:
    connector = target_connector(scenario)
    try:
        return connector.get_row_count(scenario.target_schema, table)
    finally:
        connector.close()


def count_where_not_null(scenario: DatabaseScenario, table: str, column: str) -> int:
    connector = target_connector(scenario)
    try:
        quoted = quote_identifier(column, connector.quote_char)
        return connector.get_row_count(scenario.target_schema, table, f" WHERE {quoted} IS NOT NULL")
    finally:
        connector.close()


def fetch_rows(
    scenario: DatabaseScenario,
    table: str,
    order_by: str = "customer_id",
    limit: int = 5,
) -> list[dict]:
    connector = target_connector(scenario)
    try:
        table_sql = connector.quote_table(scenario.target_schema, table)
        parts = order_by.split()
        order_sql = quote_identifier(parts[0], connector.quote_char)
        if len(parts) > 1:
            order_sql = f"{order_sql} {' '.join(parts[1:])}"
        if scenario.target.config.engine == "mssql":
            query = f"SELECT TOP ({int(limit)}) * FROM {table_sql} ORDER BY {order_sql}"
        else:
            query = f"SELECT * FROM {table_sql} ORDER BY {order_sql} LIMIT {int(limit)}"
        return connector.execute_query(query)
    finally:
        connector.close()


def column_names(scenario: DatabaseScenario, table: str) -> list[str]:
    connector = target_connector(scenario)
    try:
        return [column.name for column in connector.get_columns(scenario.target_schema, table)]
    finally:
        connector.close()


def column_type(scenario: DatabaseScenario, table: str, column: str) -> str:
    connector = target_connector(scenario)
    try:
        for item in connector.get_columns(scenario.target_schema, table):
            if item.name.lower() == column.lower():
                return item.data_type
        return ""
    finally:
        connector.close()


class LiveBase(unittest.TestCase):
    """Base class for concrete scenario tests."""

    scenario: DatabaseScenario
    tables: list[str] = []

    @classmethod
    def setUpClass(cls) -> None:
        if not databases_reachable(cls.scenario):
            raise unittest.SkipTest(f"{SKIP_MSG}; scenario={cls.scenario.id}")

    @property
    def source_customers(self) -> str:
        return self.scenario.source_table("customers")

    def make_sync(self, **kwargs) -> SyncDB:
        return make_sync(self.scenario, **kwargs)

    def setUp(self) -> None:
        drop(self.scenario, *self.tables)

    def tearDown(self) -> None:
        drop(self.scenario, *self.tables)


class SchemaLiveBase(unittest.TestCase):
    """Base class for scenario-level schema sync tests."""

    scenario: DatabaseScenario

    @classmethod
    def setUpClass(cls) -> None:
        if not databases_reachable(cls.scenario):
            raise unittest.SkipTest(f"{SKIP_MSG}; scenario={cls.scenario.id}")

    def make_sync(self, **kwargs) -> SyncDB:
        return make_sync(self.scenario, **kwargs)

    def setUp(self) -> None:
        drop_all_target_tables(self.scenario)

    def tearDown(self) -> None:
        drop_all_target_tables(self.scenario)
