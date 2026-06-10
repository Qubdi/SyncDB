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
import warnings
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Iterator


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

    def read(
        self,
        path: str | Path,
        file_format: FileFormat | str | None = None,
        hmac_key: bytes | str | None = None,
        hmac_alg: str = "sha256",
    ) -> list[dict[str, Any]]:
        """Read a file and return its rows as a list of dicts.

        file_format overrides extension-based detection when provided.

        SECURITY: Pickle files execute arbitrary Python bytecode on load.
        Only read pickle files from sources you control.  Pass hmac_key to
        enforce HMAC-SHA256 integrity verification before loading — the key
        must match the one used when the file was written via write().
        A companion .sig file is expected alongside the pickle; loading
        fails with ValueError if the signature is absent or invalid.
        Never expose pickle loading to user-uploaded files without HMAC.
        """
        file_path = Path(path)
        fmt = self._resolve_format(file_path, file_format)
        if fmt == FileFormat.CSV:
            # newline="" is required by the csv spec to prevent the universal
            # newline translator from mangling \r\n inside quoted fields.
            with file_path.open("r", encoding="utf-8", newline="") as handle:
                return list(csv.DictReader(handle))
        if fmt == FileFormat.PICKLE:
            if hmac_key is None:
                warnings.warn(
                    "Loading a pickle file without HMAC verification. "
                    "Pickle files execute arbitrary Python code on load — only "
                    "read files from sources you fully control. "
                    "Pass hmac_key= to enforce integrity verification.",
                    UserWarning,
                    stacklevel=2,
                )
            else:
                self._verify_hmac(file_path, hmac_key, hmac_alg)
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
        hmac_key: bytes | str | None = None,
        hmac_alg: str = "sha256",
    ) -> int:
        """Write rows to a file and return the count of rows written.

        Parent directories are created automatically if they don't exist.
        file_format overrides extension-based detection when provided.

        For Pickle files, passing hmac_key writes a companion .sig file with
        an HMAC digest so readers can verify integrity via read(hmac_key=...).
        """
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
            if hmac_key is not None:
                self._write_hmac(file_path, hmac_key, hmac_alg)
            return len(records)
        if fmt in {FileFormat.PARQUET, FileFormat.EXCEL}:
            return self._write_with_pandas(records, file_path, fmt)
        raise ValueError(f"Unsupported output format: {fmt}")

    def write_streaming(
        self,
        batches: Iterator[list[dict[str, Any]]],
        path: str | Path,
        file_format: FileFormat | str | None = None,
    ) -> int:
        """Write rows from a batch iterator to a file without loading all rows into memory.

        CSV and Parquet support true streaming (rows are written as batches arrive).
        Excel and Pickle materialise all rows first — they have no incremental writer API.
        Returns the total number of rows written.
        """
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        fmt = self._resolve_format(file_path, file_format)
        if fmt == FileFormat.CSV:
            return self._write_csv_streaming(batches, file_path)
        if fmt == FileFormat.PARQUET:
            return self._write_parquet_streaming(batches, file_path)
        # Excel and Pickle have no incremental writer; materialise first.
        records = [row for batch in batches for row in batch]
        return self.write(records, path, file_format)

    def _resolve_format(self, path: Path, file_format: FileFormat | str | None) -> FileFormat:
        """Determine FileFormat from explicit override or file extension."""
        if file_format:
            return FileFormat(str(file_format).lower())
        suffix = path.suffix.lower().lstrip(".")
        # .xlsx/.xls don't match any FileFormat value directly, so they need an
        # explicit mapping before the generic FileFormat(suffix) lookup.
        if suffix in {"xlsx", "xls"}:
            return FileFormat.EXCEL
        if suffix == "pkl":
            return FileFormat.PICKLE
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

    def _write_csv_streaming(self, batches: Iterator[list[dict[str, Any]]], path: Path) -> int:
        count = 0
        writer: csv.DictWriter | None = None
        with path.open("w", encoding="utf-8", newline="") as handle:
            for batch in batches:
                if not batch:
                    continue
                if writer is None:
                    writer = csv.DictWriter(handle, fieldnames=list(batch[0].keys()))
                    writer.writeheader()
                writer.writerows(batch)
                count += len(batch)
        return count

    def _write_parquet_streaming(self, batches: Iterator[list[dict[str, Any]]], path: Path) -> int:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError("pyarrow is required for streaming Parquet writes") from exc
        count = 0
        writer: pq.ParquetWriter | None = None
        try:
            for batch in batches:
                if not batch:
                    continue
                table = pa.Table.from_pylist(batch)
                if writer is None:
                    writer = pq.ParquetWriter(str(path), table.schema)
                writer.write_table(table)
                count += len(batch)
        finally:
            if writer is not None:
                writer.close()
        return count

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

    def _verify_hmac(self, file_path: Path, key: bytes | str, alg: str) -> None:
        """Verify the HMAC signature of a pickle file before loading.

        Raises ValueError if the .sig sidecar file is missing or the digest
        does not match.  Uses hmac.compare_digest to prevent timing attacks.
        """
        import hmac as hmac_mod
        sig_path = file_path.with_suffix(file_path.suffix + ".sig")
        if not sig_path.exists():
            raise ValueError(
                f"HMAC signature file not found: {sig_path}. "
                "Write the pickle with hmac_key= to generate a .sig file, "
                "or omit hmac_key= to skip verification (trusted sources only)."
            )
        key_bytes = key if isinstance(key, bytes) else key.encode()
        data = file_path.read_bytes()
        expected = hmac_mod.new(key_bytes, data, alg).hexdigest()
        actual = sig_path.read_text().strip()
        if not hmac_mod.compare_digest(expected, actual):
            raise ValueError(
                "Pickle file HMAC verification failed — "
                "the file may have been tampered with or the wrong key was supplied."
            )

    def _write_hmac(self, file_path: Path, key: bytes | str, alg: str) -> None:
        """Write an HMAC digest of a pickle file to a companion .sig sidecar."""
        import hmac as hmac_mod
        key_bytes = key if isinstance(key, bytes) else key.encode()
        data = file_path.read_bytes()
        sig = hmac_mod.new(key_bytes, data, alg).hexdigest()
        sig_path = file_path.with_suffix(file_path.suffix + ".sig")
        sig_path.write_text(sig, encoding="utf-8")
