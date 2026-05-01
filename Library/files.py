"""Local file read/write helpers."""

from __future__ import annotations

import csv
import pickle
from enum import Enum
from pathlib import Path
from typing import Any, Iterable


class FileFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"
    EXCEL = "excel"
    PICKLE = "pickle"


class FileTransfer:
    """Read and write tabular records in supported local formats."""

    def read(self, path: str | Path, file_format: FileFormat | str | None = None) -> list[dict[str, Any]]:
        file_path = Path(path)
        fmt = self._resolve_format(file_path, file_format)
        if fmt == FileFormat.CSV:
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
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        fmt = self._resolve_format(file_path, file_format)
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
        if file_format:
            return FileFormat(str(file_format).lower())
        suffix = path.suffix.lower().lstrip(".")
        if suffix in {"xlsx", "xls"}:
            return FileFormat.EXCEL
        return FileFormat(suffix)

    def _write_csv(self, records: list[dict[str, Any]], path: Path) -> None:
        fieldnames = list(records[0].keys()) if records else []
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

    def _records_from_object(self, data: Any) -> list[dict[str, Any]]:
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
        return frame.to_dict(orient="records")

    def _write_with_pandas(self, records: list[dict[str, Any]], path: Path, fmt: FileFormat) -> int:
        pd = self._import_pandas()
        frame = pd.DataFrame.from_records(records)
        if fmt == FileFormat.PARQUET:
            frame.to_parquet(path, index=False)
        else:
            frame.to_excel(path, index=False)
        return len(frame)

    def _import_pandas(self):
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("pandas is required for Excel and Parquet support") from exc
        return pd
