"""Small SQL-building helpers shared by connectors and pipelines.

This is the defensive boundary for SQL text generated from user input. Keep all
identifier validation and raw-filter checks here so connector implementations
can build SQL consistently and reviewers have one place to audit injection risk.

Security model
--------------
Identifiers (table names, column names, schema names) are validated against a
strict allowlist regex before being embedded in SQL strings.  All runtime data
values go through parameterised queries and are never interpolated.

User-supplied WHERE clauses are accepted as raw SQL and screened by
validate_where_clause(): string literals are stripped, then the remainder is
checked for statement terminators, comments, and a word-boundary keyword
deny-list.  This is a hardened safety net for developer-authored filter
expressions, NOT a SQL parser — raw WHERE strings from untrusted sources
(user HTTP parameters, un-vetted config values) must still be treated as
injection risks and should use the {"where": ..., "params": [...]} form.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# Restricts identifiers to safe ASCII letters, digits, and underscores.
# Quoted identifiers that could contain spaces or special chars are not supported
# by design; the added complexity isn't worth the risk.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Restricts SQL type strings (used for type_overrides and generated DDL) to a safe
# shape: a base name of letters/digits/spaces/underscores (e.g. "double precision",
# "datetime2"), an optional single parenthetical modifier holding only
# letters/digits/commas/spaces (covers "varchar(50)", "numeric(20,0)",
# "nvarchar(max)"), and an optional trailing "[]" array suffix (PostgreSQL).
# This blocks injection via type strings (";", ")", quotes, comments) while still
# accepting every type this library emits or a caller would reasonably override to.
_TYPE_RE = re.compile(
    r"^[A-Za-z][A-Za-z0-9_ ]*"  # base type name
    r"(\([A-Za-z0-9_, ]+\))?"  # optional (n), (p,s), or (max)
    r"(\s*\[\s*\])?$"  # optional array suffix
)

# Deny-list machinery for raw WHERE clause strings.
#
# Matching strategy (see validate_where_clause):
#   1. SQL-standard single-quoted string literals ('it''s') are stripped first,
#      so values like "hex_val = '0x1f'" are not false-positives — to the SQL
#      engine those characters are inert literal data.
#   2. The remainder is checked for statement terminators / comment tokens.
#   3. Keywords are matched on \b word boundaries, so "id IN(SELECT ...)" and
#      "1 UNION(SELECT 1)" are caught regardless of surrounding punctuation,
#      while identifiers like "updated_at" or "deleted_flag" are not.
#
# IMPORTANT — this is a hardened safety net, NOT a full SQL parser.  Raw WHERE
# clauses from untrusted sources (user HTTP parameters, un-validated config
# values) must still be treated as injection risks.  The intended use case is
# developer-authored filter expressions in job configs.
# Never add a flag to skip this check; extend the deny-list instead if a
# legitimate token is being incorrectly blocked.
_WHERE_COMMENT_TOKENS = (
    ";",  # statement terminator — stacked queries
    "--",  # inline comment
    "/*",  # block comment open
    "*/",  # block comment close
)
_WHERE_KEYWORD_RE = re.compile(
    r"\b(?:"
    r"select|insert|update|delete|merge"  # DML / subquery injection
    r"|drop|alter|create|truncate"  # DDL injection
    r"|grant|revoke"  # permission changes
    r"|union"  # UNION-based exfiltration
    r"|exec|execute|declare"  # procedure calls / T-SQL variables
    r"|waitfor|shutdown"  # MSSQL time-based / DoS
    r"|sleep|benchmark|pg_sleep"  # time-based blind injection
    r"|load_file|outfile|dumpfile"  # MySQL file read/write
    r"|xp_\w+|sp_\w+"  # MSSQL extended/system procedures
    r")\b"
)
# Hex literals (0x41...) are an evasion vector outside of quoted strings.
_WHERE_HEX_LITERAL_RE = re.compile(r"\b0x[0-9a-f]")
# SQL-standard single-quoted string literal with '' escaping.
_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")


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


def validate_type(data_type: str) -> str:
    """Raise ValueError if a SQL type string contains unsafe characters.

    Type strings are embedded directly in CREATE/ALTER TABLE DDL and cannot be
    parameterised, so they must be validated like identifiers.  This guards the
    `type_overrides` job-config option, whose values would otherwise reach the
    DDL builder verbatim and allow injection (e.g. "int); DROP TABLE x;--").
    """
    if not _TYPE_RE.match(str(data_type or "").strip()):
        raise ValueError(f"Unsafe SQL type: {data_type!r}")
    return data_type


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

    String literals are stripped before matching so legitimate values (e.g.
    "hex_val = '0x1f'") are not blocked, then the remaining SQL text is checked
    for comment/terminator tokens, deny-listed keywords on word boundaries, and
    bare hex literals.  Clauses with unbalanced quotes or backslash-escaped
    quotes are rejected outright because their literal boundaries are ambiguous.

    This is a safety net for developer-authored filters, not a SQL parser; see
    the module docstring for the threat model.
    """
    clause = str(where_clause)
    if "\x00" in clause:
        raise ValueError("Null byte detected in WHERE clause")
    # MySQL-style backslash escaping makes literal boundaries ambiguous to a
    # regex-based stripper; standard SQL escapes a quote by doubling it ('').
    if "\\'" in clause:
        raise ValueError(
            "Backslash-escaped quote in WHERE clause; use SQL-standard '' escaping "
            "or the parameterised {'where': ..., 'params': [...]} filter form"
        )
    stripped = _STRING_LITERAL_RE.sub("''", clause)
    if "'" in stripped.replace("''", ""):
        raise ValueError("Unbalanced string literal in WHERE clause")
    lowered = stripped.lower()
    for token in _WHERE_COMMENT_TOKENS:
        if token in lowered:
            raise ValueError(f"Unsafe token '{token}' detected in WHERE clause")
    match = _WHERE_KEYWORD_RE.search(lowered)
    if match:
        raise ValueError(f"Unsafe token '{match.group()}' detected in WHERE clause")
    if _WHERE_HEX_LITERAL_RE.search(lowered):
        raise ValueError("Hex literal (0x...) detected in WHERE clause")
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


def _order_by_term(term: str, quote_char: str) -> str:
    """Quote one ORDER BY term of the form "column" or "column ASC|DESC".

    The column part goes through quote_identifier (and therefore
    validate_identifier); the direction keyword is matched against a two-word
    allowlist, so no other token can ride along into the SQL string.
    """
    parts = str(term).strip().split()
    if len(parts) == 1:
        return quote_identifier(parts[0], quote_char)
    if len(parts) == 2 and parts[1].upper() in {"ASC", "DESC"}:
        return f"{quote_identifier(parts[0], quote_char)} {parts[1].upper()}"
    raise ValueError(f"order_by term must be 'column' or 'column ASC|DESC', got: {term!r}")


def build_order_by(order_by: list[str] | tuple[str, ...] | str | None, quote_char: str = '"') -> str:
    """Build a quoted ORDER BY clause from column names, each optionally
    suffixed with a direction ("updated_at DESC").

    Returns "" when order_by is falsy so callers can concatenate the result
    directly into a query string.  Defaults to the SQL-standard double-quote so a
    caller that forgets to pass quote_char still produces portable SQL; engines
    that need a different quote (MSSQL brackets, MySQL backticks) pass their own.
    """
    if not order_by:
        return ""
    columns = [order_by] if isinstance(order_by, str) else list(order_by)
    quoted = [_order_by_term(column, quote_char) for column in columns]
    return " ORDER BY " + ", ".join(quoted)
