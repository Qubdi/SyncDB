"""Schema metadata and cross-engine type mapping.

The SchemaMapper translates SQL types between MSSQL, PostgreSQL, and MySQL so that
column definitions remain semantically equivalent after crossing engine boundaries.
Type mapping is intentionally lossy in some directions — the goal is a working
target column, not bit-perfect round-trip fidelity.
"""

from __future__ import annotations

from dataclasses import dataclass

from .config import normalize_engine


@dataclass(frozen=True)
class Column:
    """Immutable column descriptor used throughout the connector and sync layers.

    char_length, numeric_precision, and numeric_scale are None when the type does
    not carry those modifiers (e.g. integer).  unsigned is MySQL-specific and is
    preserved through the mapping to allow correct range-widening decisions.
    """
    name: str
    data_type: str
    nullable: bool = True
    char_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    is_primary_key: bool = False
    unsigned: bool = False


class SchemaMapper:
    """Map column types between MSSQL, PostgreSQL, and MySQL.

    All _to_* methods accept the normalised lowercase source engine string so the
    unsigned flag can adjust the output for MySQL-specific range semantics (e.g.
    BIGINT UNSIGNED exceeds PostgreSQL's bigint range and must map to numeric(20,0)).
    """

    def map_column(self, column: Column, source_engine: str, target_engine: str) -> Column:
        """Return a new Column with data_type translated to the target engine."""
        return Column(
            name=column.name,
            data_type=self.map_type(
                source_engine=source_engine,
                target_engine=target_engine,
                data_type=column.data_type,
                char_length=column.char_length,
                numeric_precision=column.numeric_precision,
                numeric_scale=column.numeric_scale,
                unsigned=column.unsigned,
            ),
            nullable=column.nullable,
            char_length=column.char_length,
            numeric_precision=column.numeric_precision,
            numeric_scale=column.numeric_scale,
            is_primary_key=column.is_primary_key,
            unsigned=column.unsigned,
        )

    def map_columns(
        self,
        columns: list[Column],
        source_engine: str,
        target_engine: str,
    ) -> list[Column]:
        return [self.map_column(column, source_engine, target_engine) for column in columns]

    def map_type(
        self,
        source_engine: str,
        target_engine: str,
        data_type: str,
        char_length: int | None = None,
        numeric_precision: int | None = None,
        numeric_scale: int | None = None,
        unsigned: bool = False,
    ) -> str:
        """Translate a single SQL type string from source_engine to target_engine.

        When source == target, _preserve_type re-attaches precision/length modifiers
        that may have been stripped by the connector's metadata query.
        """
        source = normalize_engine(source_engine).value
        target = normalize_engine(target_engine).value
        base = (data_type or "").lower().strip()
        if source == target:
            return self._preserve_type(base, char_length, numeric_precision, numeric_scale)
        if target == "postgresql":
            return self._to_postgresql(source, base, char_length, numeric_precision, numeric_scale, unsigned)
        if target == "mssql":
            return self._to_mssql(source, base, char_length, numeric_precision, numeric_scale, unsigned)
        if target == "mysql":
            return self._to_mysql(source, base, char_length, numeric_precision, numeric_scale, unsigned)
        # Safety fallback; should not be reachable given normalize_engine validation.
        return "text"

    def _preserve_type(
        self,
        data_type: str,
        char_length: int | None,
        precision: int | None,
        scale: int | None,
    ) -> str:
        """Re-attach length/precision modifiers when copying within the same engine.

        INFORMATION_SCHEMA returns "varchar" without the length; we must re-append
        it so the generated CREATE/ALTER TABLE DDL is complete.
        "character varying" is normalised to "varchar" for consistency in output DDL.
        """
        if data_type in {"varchar", "nvarchar", "char", "nchar", "character varying"} and char_length and char_length > 0:
            normalized = "varchar" if data_type == "character varying" else data_type
            return f"{normalized}({char_length})"
        if data_type in {"decimal", "numeric"} and precision is not None and scale is not None:
            return f"{data_type}({precision},{scale})"
        return data_type or "text"

    def _to_postgresql(
        self,
        source: str,
        data_type: str,
        char_length: int | None,
        precision: int | None,
        scale: int | None,
        unsigned: bool,
    ) -> str:
        if data_type in {"bigint", "bigserial"}:
            # MySQL BIGINT UNSIGNED max (18446744073709551615) exceeds PostgreSQL
            # bigint max (9223372036854775807), so numeric(20,0) is the safe choice.
            return "numeric(20,0)" if source == "mysql" and unsigned else "bigint"
        if data_type in {"int", "integer", "mediumint", "serial"}:
            # MySQL INT UNSIGNED max (4294967295) exceeds integer but fits in bigint.
            return "bigint" if source == "mysql" and unsigned else "integer"
        if data_type in {"smallint", "tinyint"}:
            # SMALLINT UNSIGNED / TINYINT UNSIGNED both fit in PostgreSQL integer.
            return "integer" if unsigned else "smallint"
        if data_type in {"bit", "boolean", "bool"}:
            return "boolean"
        if data_type in {"uniqueidentifier", "uuid"}:
            return "uuid"
        if data_type in {"decimal", "numeric", "money", "smallmoney"}:
            return self._numeric("numeric", precision, scale)
        if data_type in {"float", "double", "double precision"}:
            return "double precision"
        if data_type == "real":
            return "real"
        if data_type in {"datetime", "smalldatetime", "datetime2", "timestamp"}:
            # PostgreSQL "timestamp" is without time zone; timezone-aware types
            # map to "timestamptz" below.
            return "timestamp"
        if data_type in {"datetimeoffset", "timestamptz"}:
            return "timestamptz"
        if data_type == "date":
            return "date"
        if data_type == "time":
            return "time"
        if data_type in {"nvarchar", "varchar", "character varying", "enum", "set"}:
            # Unbounded varchar maps to "text" (no length limit in PostgreSQL).
            return self._varchar(char_length, "varchar", unbounded="text")
        if data_type in {"nchar", "char"}:
            return self._varchar(char_length, "char", unbounded="char")
        if data_type in {"text", "ntext", "longtext", "mediumtext", "tinytext"}:
            return "text"
        if data_type in {"binary", "varbinary", "image", "bytea", "blob", "longblob", "mediumblob", "tinyblob", "rowversion"}:
            return "bytea"
        if data_type in {"json", "jsonb"}:
            # Always use jsonb for PostgreSQL; it is indexed and queried more efficiently.
            return "jsonb"
        if data_type == "xml":
            return "xml"
        return "text"

    def _to_mssql(
        self,
        source: str,
        data_type: str,
        char_length: int | None,
        precision: int | None,
        scale: int | None,
        unsigned: bool,
    ) -> str:
        if data_type in {"bigint", "bigserial"}:
            # BIGINT UNSIGNED doesn't fit in MSSQL bigint; use decimal(20,0).
            return "decimal(20,0)" if unsigned else "bigint"
        if data_type in {"int", "integer", "serial", "mediumint"}:
            # INT UNSIGNED max (4294967295) doesn't fit in MSSQL int; use bigint.
            return "bigint" if unsigned else "int"
        if data_type in {"smallint", "tinyint"}:
            if unsigned and data_type == "smallint":
                return "int"
            # MySQL TINYINT UNSIGNED (0-255) maps exactly to MSSQL TINYINT (0-255).
            if unsigned and data_type == "tinyint":
                return "tinyint"
            return "smallint"
        if data_type in {"boolean", "bool", "bit"}:
            # MSSQL has no boolean; bit is the conventional substitute (0/1).
            return "bit"
        if data_type in {"uuid", "uniqueidentifier"}:
            return "uniqueidentifier"
        if data_type in {"decimal", "numeric", "money", "smallmoney"}:
            return self._numeric("decimal", precision, scale)
        if data_type in {"double", "double precision", "float"}:
            return "float"
        if data_type == "real":
            return "real"
        if data_type in {"timestamp", "datetime", "datetime2", "smalldatetime"}:
            # datetime2 has sub-millisecond precision and a wider date range than
            # datetime; it's the recommended replacement in MSSQL 2008+.
            return "datetime2"
        if data_type in {"timestamptz", "datetimeoffset"}:
            return "datetimeoffset"
        if data_type == "date":
            return "date"
        if data_type == "time":
            return "time"
        if data_type in {"varchar", "nvarchar", "character varying", "char", "nchar", "text", "json", "jsonb", "xml", "enum", "set"}:
            # Always use nvarchar (Unicode) for MSSQL to avoid data loss when the
            # source contains multibyte characters.  Unbounded → nvarchar(max).
            return self._varchar(char_length, "nvarchar", unbounded="nvarchar(max)")
        if data_type in {"bytea", "binary", "varbinary", "blob", "longblob", "mediumblob", "tinyblob"}:
            return "varbinary(max)"
        return "nvarchar(max)"

    def _to_mysql(
        self,
        source: str,
        data_type: str,
        char_length: int | None,
        precision: int | None,
        scale: int | None,
        unsigned: bool,
    ) -> str:
        if data_type in {"bigint", "bigserial"}:
            return "bigint"
        if data_type in {"int", "integer", "serial", "mediumint"}:
            return "int"
        if data_type in {"smallint", "tinyint"}:
            return "smallint"
        if data_type in {"boolean", "bool", "bit"}:
            # MySQL's canonical boolean is TINYINT(1) — BIT(1) exists but is
            # treated inconsistently by different drivers and ORMs.
            return "tinyint(1)"
        if data_type in {"uuid", "uniqueidentifier"}:
            # MySQL has no native UUID type; CHAR(36) stores the standard
            # hyphenated string form without lossy conversion.
            return "char(36)"
        if data_type in {"decimal", "numeric", "money", "smallmoney"}:
            return self._numeric("decimal", precision, scale)
        if data_type in {"double", "double precision", "float"}:
            return "double"
        if data_type == "real":
            return "float"
        if data_type in {"timestamp", "timestamptz", "datetime", "datetime2", "datetimeoffset", "smalldatetime"}:
            # MySQL DATETIME has no timezone; timezone info from timestamptz /
            # datetimeoffset is silently discarded on this path.
            return "datetime"
        if data_type == "date":
            return "date"
        if data_type == "time":
            return "time"
        if data_type in {"varchar", "nvarchar", "character varying", "char", "nchar", "enum", "set"}:
            # Unbounded or very long strings map to longtext (up to 4 GiB).
            return self._varchar(char_length, "varchar", unbounded="longtext")
        if data_type in {"text", "ntext", "xml"}:
            return "longtext"
        if data_type in {"json", "jsonb"}:
            return "json"
        if data_type in {"bytea", "binary", "varbinary", "image", "blob", "rowversion"}:
            return "longblob"
        return "longtext"

    def _numeric(self, name: str, precision: int | None, scale: int | None) -> str:
        """Return "name(p,s)" when both modifiers are known, or bare "name" otherwise."""
        if precision is not None and scale is not None:
            return f"{name}({precision},{scale})"
        return name

    def _varchar(self, char_length: int | None, name: str, unbounded: str) -> str:
        """Return a sized varchar when char_length is usable, else the unbounded form.

        The upper bound of 65535 guards against emitting an oversized VARCHAR that
        would exceed the target engine's row-size limit (MySQL's row limit is 65535
        bytes; PostgreSQL allows up to 1 GiB but a VARCHAR(>10485760) is unusual).
        """
        if char_length and 0 < char_length <= 65535:
            return f"{name}({char_length})"
        return unbounded
