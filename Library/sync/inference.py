"""Column type inference from Python values for file-to-database imports."""

from __future__ import annotations

from typing import Any

from ..type_mapping import Column, SchemaMapper

# Type hierarchy from most general to most specific.  When mixed types appear
# in a column's sample (e.g. bool and int), the more general type wins so no
# value is silently truncated or misrepresented.
_TYPE_RANK: dict[str, int] = {
    "boolean": 0,
    "bigint": 1,
    "double precision": 2,
    "text": 3,
}


def infer_columns(
    rows: list[dict[str, Any]],
    target_engine: str,
    schema_mapper: SchemaMapper | None = None,
) -> list[Column]:
    """Infer column types from Python values across a sample of file rows.

    Uses PostgreSQL type names as the intermediate representation, then maps
    to the target engine via SchemaMapper.  Only four broad types are produced
    (boolean, bigint, double precision, text) — pre-create the target table
    with explicit types for finer control, especially for CSV files where
    every value arrives as a string anyway.

    Samples up to 100 rows and scans ALL non-None values so a column that is
    None in the first row but numeric in later rows is inferred correctly.  If
    ALL sampled values are None the column falls back to text.
    """
    if not rows:
        raise ValueError("Cannot infer a target table from an empty file")
    mapper = schema_mapper or SchemaMapper()
    sample = rows[:100]
    column_names = list(rows[0].keys())
    columns: list[Column] = []
    for col_name in column_names:
        # Collect the inferred type for every non-None value in the sample.
        # When types conflict (e.g. bool and float in the same column), the
        # higher-ranked (more general) type wins via the _TYPE_RANK table.
        inferred: str = "boolean"  # start at most specific
        found_any = False
        for row in sample:
            value = row.get(col_name)
            if value is None:
                continue
            found_any = True
            # bool must be checked before int: bool is a subclass of int in Python.
            if isinstance(value, bool):
                candidate = "boolean"
            elif isinstance(value, int):
                candidate = "bigint"
            elif isinstance(value, float):
                candidate = "double precision"
            else:
                candidate = "text"
            # Promote to the more general type if needed.
            if _TYPE_RANK.get(candidate, 3) > _TYPE_RANK.get(inferred, 0):
                inferred = candidate
            # text is the ceiling — no need to keep scanning.
            if inferred == "text":
                break
        data_type = inferred if found_any else "text"
        columns.append(Column(name=col_name, data_type=data_type, nullable=True))
    return mapper.map_columns(columns, "postgresql", target_engine)
