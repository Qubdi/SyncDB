"""Small SQL-building helpers shared by connectors and pipelines."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UNSAFE_WHERE_TOKENS = (";", "--", "/*", "*/", " xp_", " sp_")


@dataclass(frozen=True)
class QualifiedName:
    schema: str | None
    table: str


def validate_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(str(name or "")):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def parse_qualified_name(name: str, default_schema: str | None = None) -> QualifiedName:
    parts = [part.strip() for part in str(name or "").split(".") if part.strip()]
    if len(parts) == 1:
        return QualifiedName(default_schema, validate_identifier(parts[0]))
    if len(parts) == 2:
        return QualifiedName(validate_identifier(parts[0]), validate_identifier(parts[1]))
    raise ValueError(f"Table name must be 'table' or 'schema.table', got: {name}")


def quote_identifier(name: str, quote_char: str = '"') -> str:
    validate_identifier(name)
    if quote_char == "[":
        return f"[{name}]"
    return f"{quote_char}{name}{quote_char}"


def quote_qualified(name: QualifiedName, quote_char: str = '"') -> str:
    table = quote_identifier(name.table, quote_char)
    if not name.schema:
        return table
    return f"{quote_identifier(name.schema, quote_char)}.{table}"


def validate_where_clause(where_clause: str) -> str:
    lowered = f" {where_clause.lower()} "
    for token in _UNSAFE_WHERE_TOKENS:
        if token in lowered:
            raise ValueError(f"Unsafe token '{token.strip()}' detected in WHERE clause")
    return where_clause


def build_where_clause(filter_cfg: dict[str, Any] | str | None) -> tuple[str, list[Any]]:
    if not filter_cfg:
        return "", []
    if isinstance(filter_cfg, str):
        where = filter_cfg.strip()
        return (f" WHERE {validate_where_clause(where)} ", []) if where else ("", [])

    where = str(filter_cfg.get("where", "")).strip()
    params = list(filter_cfg.get("params", []) or [])
    return (f" WHERE {validate_where_clause(where)} ", params) if where else ("", [])


def build_order_by(order_by: list[str] | tuple[str, ...] | str | None, quote_char: str = "[") -> str:
    if not order_by:
        return ""
    columns = [order_by] if isinstance(order_by, str) else list(order_by)
    quoted = [quote_identifier(column, quote_char) for column in columns]
    return " ORDER BY " + ", ".join(quoted)
