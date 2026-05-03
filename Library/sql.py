"""Small SQL-building helpers shared by connectors and pipelines.

This is the defensive boundary for SQL text generated from user input. Keep all
identifier validation and raw-filter checks here so connector implementations
can build SQL consistently and reviewers have one place to audit injection risk.

Security model
--------------
Identifiers (table names, column names, schema names) are validated against a
strict allowlist regex before being embedded in SQL strings.  User-supplied WHERE
clauses are accepted as raw SQL but scanned for a deny-list of dangerous tokens.
All runtime data values go through parameterised queries and are never interpolated.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


# Restricts identifiers to safe ASCII letters, digits, and underscores.
# Quoted identifiers that could contain spaces or special chars are not supported
# by design; the added complexity isn't worth the risk.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Token-based deny-list for raw WHERE clause strings.
# Blocks statement terminators (;), inline comments (--), block comments (/* */),
# and MSSQL extended stored procedure prefixes (xp_, sp_) that could be used for
# command execution or stacked queries.
_UNSAFE_WHERE_TOKENS = (";", "--", "/*", "*/", " xp_", " sp_")


@dataclass(frozen=True)
class QualifiedName:
    """A schema-qualified table reference.  schema may be None for unqualified names."""
    schema: str | None
    table: str


def validate_identifier(name: str) -> str:
    """Raise ValueError if name contains characters outside the safe identifier set."""
    if not _IDENTIFIER_RE.match(str(name or "")):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def parse_qualified_name(name: str, default_schema: str | None = None) -> QualifiedName:
    """Parse 'table' or 'schema.table' into a QualifiedName.

    default_schema is applied when only a bare table name is provided and the
    caller wants schema to be populated automatically (e.g. "dbo" for MSSQL).
    """
    parts = [part.strip() for part in str(name or "").split(".") if part.strip()]
    if len(parts) == 1:
        return QualifiedName(default_schema, validate_identifier(parts[0]))
    if len(parts) == 2:
        return QualifiedName(validate_identifier(parts[0]), validate_identifier(parts[1]))
    raise ValueError(f"Table name must be 'table' or 'schema.table', got: {name}")


def quote_identifier(name: str, quote_char: str = '"') -> str:
    """Wrap a validated identifier in the engine-appropriate quote characters.

    MSSQL uses [brackets]; PostgreSQL and MySQL use double-quotes and backticks
    respectively, but those connectors pass their own quote_char.  The "[" form
    produces "[name]" because the closing bracket is implicit in MSSQL syntax.
    """
    validate_identifier(name)
    if quote_char == "[":
        return f"[{name}]"
    return f"{quote_char}{name}{quote_char}"


def quote_qualified(name: QualifiedName, quote_char: str = '"') -> str:
    """Return a fully-qualified, quoted table reference (e.g. [dbo].[Orders])."""
    table = quote_identifier(name.table, quote_char)
    if not name.schema:
        return table
    return f"{quote_identifier(name.schema, quote_char)}.{table}"


def validate_where_clause(where_clause: str) -> str:
    """Reject WHERE strings that contain known SQL-injection patterns.

    The clause is padded with spaces before matching so token patterns that rely
    on word boundaries (e.g. " xp_") aren't bypassed by placing them at position 0.
    """
    lowered = f" {where_clause.lower()} "
    for token in _UNSAFE_WHERE_TOKENS:
        if token in lowered:
            raise ValueError(f"Unsafe token '{token.strip()}' detected in WHERE clause")
    return where_clause


def build_where_clause(filter_cfg: dict[str, Any] | str | None) -> tuple[str, list[Any]]:
    """Build a parameterised WHERE clause from a filter specification.

    Accepts two forms for caller convenience:
      - str:  a raw WHERE expression (no "WHERE" keyword) with no bind params.
              Example: "status = 'active' AND created_at > '2024-01-01'"
      - dict: {"where": "<expression>", "params": [<values>]}
              Bind params are passed back to the caller and forwarded to the driver.

    Returns ("", []) when filter_cfg is falsy so callers can concatenate
    the result directly into a query string without an extra branch.
    """
    if not filter_cfg:
        return "", []
    if isinstance(filter_cfg, str):
        where = filter_cfg.strip()
        return (f" WHERE {validate_where_clause(where)} ", []) if where else ("", [])

    where = str(filter_cfg.get("where", "")).strip()
    params = list(filter_cfg.get("params", []) or [])
    return (f" WHERE {validate_where_clause(where)} ", params) if where else ("", [])


def build_order_by(order_by: list[str] | tuple[str, ...] | str | None, quote_char: str = "[") -> str:
    """Build a quoted ORDER BY clause from a column name or list of column names.

    Returns "" when order_by is falsy so callers can concatenate the result
    directly into a query string.  Defaults to MSSQL-style [brackets] because
    ORDER BY is most commonly needed for MSSQL's offset-based pagination.
    """
    if not order_by:
        return ""
    columns = [order_by] if isinstance(order_by, str) else list(order_by)
    quoted = [quote_identifier(column, quote_char) for column in columns]
    return " ORDER BY " + ", ".join(quoted)
