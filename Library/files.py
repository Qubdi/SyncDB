"""Local file read/write helpers.

Supports CSV, Parquet, Excel, and Pickle without making pandas a hard dependency:
CSV and Pickle use only the stdlib; pandas is imported lazily and only when a
Parquet or Excel path is encountered.

All public methods convert file content to list[dict[str, Any]]. That shape is
the handoff contract with SyncDB and connectors; preserve it when adding new
formats so pipelines do not need format-specific branches.
"""

from __future__ import annotations

import csv
import pickle
from enum import Enum
from pathlib import Path
from typing import Any, Iterable


class FileFormat(str, Enum):
    # Canonical lowercase values intentionally match common file extensions so
    # _resolve_format can use FileFormat(suffix) as a direct lookup.
    CSV = "csv"
    PARQUET = "parquet"
    EXCEL = "excel"
    PICKLE = "pickle"


class FileTransfer:
    """Read and write tabular records in supported local formats.

    All formats are normalised to a list[dict] so downstream connectors and
    callers work with a single in-memory representation regardless of source format.
    """

    def read(self, path: str | Path, file_format: FileFormat | str | None = None) -> list[dict[str, Any]]:
        """Read a file and return its rows as a list of dicts.

        file_format overrides extension-based detection when provided.
        """
        file_path = Path(path)
        fmt = self._resolve_format(file_path, file_format)
        if fmt == FileFormat.CSV:
            # newline="" is required by the csv spec to prevent the universal
            # newline translator from mangling \r\n inside quoted fields.
            with file_path.open("r", encoding="utf-8", newline="") as handle:
                return list(csv.DictReader(handle))
        if fmt == FileFormat.PICKLE:
            with file_path.open("rb") as handle:
                data = pickle.load(handle)
            return self._records_from_object(data)
        if fmt in {FileFormat.PARQUET, FileFormat.EXCEL}:
            return self._read_with_pandas(file_path, fmt)
        raise ValueError(f"Unsupported input format: {fmt}")

    def write(
        self,
        rows: Iterable[dict[str, Any]],
        path: str | Path,
        file_format: FileFormat | str | None = None,
    ) -> int:
        """Write rows to a file and return the count of rows written.

        Parent directories are created automatically if they don't exist.
        file_format overrides extension-based detection when provided.
        """
        file_path = Path(path)
        # Ensure parent dirs exist so callers don't have to pre-create them.
        file_path.parent.mkdir(parents=True, exist_ok=True)
        fmt = self._resolve_format(file_path, file_format)
        # Materialise the iterable once; needed for both len() and multi-pass writers.
        records = list(rows)
        if fmt == FileFormat.CSV:
            self._write_csv(records, file_path)
            return len(records)
        if fmt == FileFormat.PICKLE:
            with file_path.open("wb") as handle:
                pickle.dump(records, handle)
            return len(records)
        if fmt in {FileFormat.PARQUET, FileFormat.EXCEL}:
            return self._write_with_pandas(records, file_path, fmt)
        raise ValueError(f"Unsupported output format: {fmt}")

    def _resolve_format(self, path: Path, file_format: FileFormat | str | None) -> FileFormat:
        """Determine FileFormat from explicit override or file extension."""
        if file_format:
            return FileFormat(str(file_format).lower())
        suffix = path.suffix.lower().lstrip(".")
        # .xlsx/.xls don't match any FileFormat value directly, so they need an
        # explicit mapping before the generic FileFormat(suffix) lookup.
        if suffix in {"xlsx", "xls"}:
            return FileFormat.EXCEL
        try:
            return FileFormat(suffix)
        except ValueError:
            supported = ", ".join(f.value for f in FileFormat)
            raise ValueError(
                f"Cannot infer file format from extension '.{suffix}'. "
                f"Supported: {supported}. Pass file_format explicitly to override."
            ) from None

    def _write_csv(self, records: list[dict[str, Any]], path: Path) -> None:
        # Column order is taken from the first row; empty files produce a header-only CSV.
        fieldnames = list(records[0].keys()) if records else []
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

    def _records_from_object(self, data: Any) -> list[dict[str, Any]]:
        """Normalise a pickle payload to list[dict].

        Accepts a list of mappings (the natural output of connector.execute_query)
        or a pandas DataFrame (common when the pickle was produced by external tools).
        """
        if isinstance(data, list):
            return [dict(row) for row in data]
        if hasattr(data, "to_dict"):
            return data.to_dict(orient="records")
        raise TypeError("Pickle file must contain a list of mappings or a pandas DataFrame")

    def _read_with_pandas(self, path: Path, fmt: FileFormat) -> list[dict[str, Any]]:
        pd = self._import_pandas()
        if fmt == FileFormat.PARQUET:
            frame = pd.read_parquet(path)
        else:
            frame = pd.read_excel(path)
        # orient="records" produces [{col: val, ...}, ...] matching our internal format.
        return frame.to_dict(orient="records")

    def _write_with_pandas(self, records: list[dict[str, Any]], path: Path, fmt: FileFormat) -> int:
        pd = self._import_pandas()
        frame = pd.DataFrame.from_records(records)
        if fmt == FileFormat.PARQUET:
            # index=False avoids writing an unnamed integer index column that
            # would appear as an extra column when the file is read back.
            frame.to_parquet(path, index=False)
        else:
            frame.to_excel(path, index=False)
        return len(frame)

    def _import_pandas(self):
        # Deferred import: pandas is an optional dependency (~30 MB). Raising here
        # rather than at module load means CSV/Pickle users are never penalised.
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("pandas is required for Excel and Parquet support") from exc
        return pd
