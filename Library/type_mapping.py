"""Schema metadata and cross-engine type mapping."""

from __future__ import annotations

from dataclasses import dataclass

from .config import normalize_engine


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    nullable: bool = True
    char_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    is_primary_key: bool = False
    unsigned: bool = False


class SchemaMapper:
    """Map column types between MSSQL, PostgreSQL, and MySQL."""

    def map_column(self, column: Column, source_engine: str, target_engine: str) -> Column:
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
        return "text"

    def _preserve_type(
        self,
        data_type: str,
        char_length: int | None,
        precision: int | None,
        scale: int | None,
    ) -> str:
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
            return "numeric(20,0)" if source == "mysql" and unsigned else "bigint"
        if data_type in {"int", "integer", "mediumint", "serial"}:
            return "bigint" if source == "mysql" and unsigned else "integer"
        if data_type in {"smallint", "tinyint"}:
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
            return "timestamp"
        if data_type in {"datetimeoffset", "timestamptz"}:
            return "timestamptz"
        if data_type == "date":
            return "date"
        if data_type == "time":
            return "time"
        if data_type in {"nvarchar", "varchar", "character varying", "enum", "set"}:
            return self._varchar(char_length, "varchar", unbounded="text")
        if data_type in {"nchar", "char"}:
            return self._varchar(char_length, "char", unbounded="char")
        if data_type in {"text", "ntext", "longtext", "mediumtext", "tinytext"}:
            return "text"
        if data_type in {"binary", "varbinary", "image", "bytea", "blob", "longblob", "mediumblob", "tinyblob", "rowversion"}:
            return "bytea"
        if data_type in {"json", "jsonb"}:
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
            return "decimal(20,0)" if unsigned else "bigint"
        if data_type in {"int", "integer", "serial", "mediumint"}:
            return "bigint" if unsigned else "int"
        if data_type in {"smallint", "tinyint"}:
            return "int" if unsigned and data_type == "smallint" else "smallint"
        if data_type in {"boolean", "bool", "bit"}:
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
            return "datetime2"
        if data_type in {"timestamptz", "datetimeoffset"}:
            return "datetimeoffset"
        if data_type == "date":
            return "date"
        if data_type == "time":
            return "time"
        if data_type in {"varchar", "nvarchar", "character varying", "char", "nchar", "text", "json", "jsonb", "xml", "enum", "set"}:
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
            return "tinyint(1)"
        if data_type in {"uuid", "uniqueidentifier"}:
            return "char(36)"
        if data_type in {"decimal", "numeric", "money", "smallmoney"}:
            return self._numeric("decimal", precision, scale)
        if data_type in {"double", "double precision", "float"}:
            return "double"
        if data_type == "real":
            return "float"
        if data_type in {"timestamp", "timestamptz", "datetime", "datetime2", "datetimeoffset", "smalldatetime"}:
            return "datetime"
        if data_type == "date":
            return "date"
        if data_type == "time":
            return "time"
        if data_type in {"varchar", "nvarchar", "character varying", "char", "nchar", "enum", "set"}:
            return self._varchar(char_length, "varchar", unbounded="longtext")
        if data_type in {"text", "ntext", "xml"}:
            return "longtext"
        if data_type in {"json", "jsonb"}:
            return "json"
        if data_type in {"bytea", "binary", "varbinary", "image", "blob", "rowversion"}:
            return "longblob"
        return "longtext"

    def _numeric(self, name: str, precision: int | None, scale: int | None) -> str:
        if precision is not None and scale is not None:
            return f"{name}({precision},{scale})"
        return name

    def _varchar(self, char_length: int | None, name: str, unbounded: str) -> str:
        if char_length and 0 < char_length <= 65535:
            return f"{name}({char_length})"
        return unbounded
