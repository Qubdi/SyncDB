"""
MSSQL to Local File Data Export Pipeline
==========================================
Production-grade data extraction pipeline with:
- SQL script execution from directory
- Multiple output formats (CSV, Parquet, Excel, Pickle)
- Configurable download modes (full/batch)
- Streaming support for large datasets
- Progress tracking and reporting
- Automatic file management

Author: Senior Data Engineering Team
"""

import os
import re
import pandas as pd
import logging
import time
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def print_progress(current: int, total: int, prefix: str = "") -> None:
    """
    Log progress bar for long-running operations.
    
    Args:
        current: Current progress value
        total: Total expected value (0 for unknown total)
        prefix: Optional prefix for log message
    """
    if total <= 0:
        logging.info("%s%s rows", prefix, current)
        return

    width = 40
    ratio = min(max(current / total, 0.0), 1.0)
    filled = int(width * ratio)
    bar = "█" * filled + "░" * (width - filled)
    percent = int(ratio * 100)

    logging.info("%s[%s] %3d%% (%s/%s)", prefix, bar, percent, current, total)


def build_count_queries(query: str) -> list[str]:
    """
    Generate COUNT query candidates from a SELECT statement for progress tracking.
    
    Attempts multiple strategies to create a valid COUNT query:
    1. Wrap original query in subquery: SELECT COUNT(*) FROM (query) AS src
    2. Simple rewrite: Replace SELECT columns with COUNT(*) (for simple queries)
    
    Args:
        query: Original SQL SELECT statement
    
    Returns:
        List of candidate COUNT queries to try (in order of preference)
    """
    cleaned = (query or "").strip().rstrip(";")

    # Remove trailing ORDER BY to make the subquery count-safe
    cleaned = re.sub(r"\border\s+by\b[\s\S]*$", "", cleaned, flags=re.IGNORECASE)

    candidates = [f"SELECT COUNT(*) FROM ({cleaned}) AS src"]

    # Simple rewrite fallback: SELECT COUNT(*) + FROM ... (no GROUP BY/UNION/DISTINCT/HAVING)
    has_complex = re.search(r"\b(union|group\s+by|distinct|having)\b", cleaned, re.IGNORECASE)
    from_match = re.search(r"\bfrom\b", cleaned, re.IGNORECASE)
    if from_match and not has_complex:
        from_sql = cleaned[from_match.start():]
        candidates.append(f"SELECT COUNT(*) {from_sql}")

    return candidates


def parse_top_limit(query: str) -> int:
    """
    Extract TOP N limit from SQL Server SELECT statement.
    
    Args:
        query: SQL query string
    
    Returns:
        Limit number if found, 0 otherwise
    
    Examples:
        >>> parse_top_limit("SELECT TOP 100 * FROM table")
        100
        >>> parse_top_limit("SELECT TOP(50) * FROM table")
        50
    """
    match = re.search(r"\bselect\s+top\s*\(?\s*(\d+)\s*\)?", query, re.IGNORECASE)
    return int(match.group(1)) if match else 0


# ============================================================================
# DATA EXPORT PIPELINE
# ============================================================================

def sync_data(
    conn_str: str,
    sql_dir: str,
    save_path: str,
    output_format: str = 'csv',
    download_mode: str = "full",
    batch_size: int = 5000,
    show_progress: bool = True,
) -> None:
    """
    Execute all SQL scripts in a directory and export results to local files.
    
    This function orchestrates the complete data extraction pipeline:
    1. Scans directory for .sql files
    2. Executes each query against MSSQL database
    3. Exports results in specified format
    4. Tracks progress and generates reports
    
    Args:
        conn_str: ODBC connection string for MSSQL
        sql_dir: Directory containing `.sql` files to execute
        save_path: Output directory for query results
        output_format: Output format ('csv', 'parquet', 'pickle', 'excel')
        download_mode: Data transfer mode:
            - 'full': Load entire dataset into memory, then save
            - 'batch': Stream data in chunks (more memory efficient)
        batch_size: Number of rows per batch (for batch mode)
        show_progress: Whether to display progress bars during batch processing
    
    Raises:
        ValueError: If unsupported output_format or download_mode specified
    
    Notes:
        - Batch mode streaming only supports CSV and Parquet formats
        - Parquet streaming requires pyarrow package
        - Existing output files are automatically overwritten
    
    Examples:
        >>> sync_data(
        ...     conn_str="Driver={...};Server=...;Database=...",
        ...     sql_dir="./queries",
        ...     save_path="./output",
        ...     output_format="parquet",
        ...     download_mode="batch",
        ...     batch_size=10000
        ... )
    """
    logging.info("\n\n")

    # ===== INITIALIZATION =====
    # Normalize paths
    sql_dir = os.path.join(sql_dir)
    save_path = os.path.join(save_path)

    # Create directories if they do not exist
    os.makedirs(sql_dir, exist_ok=True)
    os.makedirs(save_path, exist_ok=True)

    # Collect all .sql files from the provided directory
    sql_files = [f for f in os.listdir(sql_dir) if f.endswith('.sql')]

    # Return a warning if no .sql files are found
    if not sql_files:
        logging.warning(f"⚠️ No .sql files found in {sql_dir}")
        return

    # Validate download mode
    download_mode = (download_mode or "full").lower()
    if download_mode not in {"full", "batch"}:
        raise ValueError("⚠️ download_mode must be 'full' or 'batch'")

    # ===== PROCESS EACH SQL FILE =====
    for sql_file in sql_files:
        # Build file paths
        sql_path = os.path.join(sql_dir, sql_file)
        output_filename = os.path.splitext(sql_file)[0] + '.' + output_format
        output_path = os.path.join(save_path, output_filename)

        try:
            
            # ===== VALIDATION AND SETUP =====
            logging.info("=" * 70)
            logging.info("Processing: %s", sql_file)
            logging.info("=" * 70)
            logging.info("Mode: %s | Format: %s | Batch size: %s", download_mode, output_format, batch_size)
            if download_mode == "batch" and output_format not in {"csv", "parquet"}:
                logging.warning(
                    "⚠️ Batch streaming is only supported for CSV and Parquet. Skipping: %s",
                    output_filename,
                )
                continue
            if download_mode == "batch" and output_format == "parquet" and (pa is None or pq is None):
                logging.warning(
                    "⚠️ Parquet streaming requires pyarrow. Skipping: %s",
                    output_filename,
                )
                continue

            # Clean up existing output file
            if os.path.exists(output_path):
                os.remove(output_path)
                logging.info("Removed existing output: %s", output_path)
            
            start_time = time.monotonic()
            
            # Read SQL query from file
            with open(sql_path, 'r') as f:
                query = f.read()

            # Initialize tracking variables
            total_rows_read = None
            total_cols = None

            # ===== EXECUTE QUERY AND TRANSFER DATA =====
            conn = pyodbc.connect(conn_str)
            logging.info("Opened MSSQL connection for %s", sql_file)
            try:
                if download_mode == "full":
                    # Full mode: Load entire dataset into memory
                    df = pd.read_sql(query, conn)
                else:
                    # Batch mode: Stream data in chunks
                    cursor = conn.cursor()
                    cursor.execute(query)
                    total_cols = len(cursor.description or [])

                    # Attempt to determine total row count for progress tracking
                    total_rows = parse_top_limit(query)
                    if show_progress:
                        try:
                            if total_rows > 0:
                                raise StopIteration
                            count_cursor = conn.cursor()
                            for count_query in build_count_queries(query):
                                try:
                                    count_cursor.execute(count_query)
                                    total_rows = int(count_cursor.fetchone()[0])
                                    break
                                except Exception:
                                    continue
                        except StopIteration:
                            pass
                        except Exception as e:
                            total_rows = 0
                            logging.warning("⚠️ Unable to compute total rows for progress bar: %s", e)

                    # Initialize batch processing
                    rows = cursor.fetchmany(batch_size)
                    total_read = 0
                    parquet_writer = None

                    # ===== STREAM DATA TO FILE =====
                    # Write CSV or Parquet incrementally
                    if output_format == "csv":
                        write_header = True
                        while rows:
                            df_chunk = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description])
                            df_chunk.to_csv(output_path, index=False, mode="a", header=write_header)
                            write_header = False

                            total_read += len(df_chunk)
                            if show_progress:
                                print_progress(total_read, total_rows, prefix=f"{sql_file} ")

                            rows = cursor.fetchmany(batch_size)

                        df = None
                        total_rows_read = total_read
                    elif output_format == "parquet" and pa is not None and pq is not None:
                        while rows:
                            df_chunk = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description])
                            table = pa.Table.from_pandas(df_chunk, preserve_index=False)

                            if parquet_writer is None:
                                # Widen all decimal columns to precision 38 so that any batch's
                                # values fit regardless of PyArrow's per-batch precision inference
                                safe_fields = [
                                    pa.field(f.name, pa.decimal128(38, f.type.scale))
                                    if pa.types.is_decimal(f.type) else f
                                    for f in table.schema
                                ]
                                safe_schema = pa.schema(safe_fields)
                                parquet_writer = pq.ParquetWriter(output_path, safe_schema)

                            # Cast each batch to the fixed schema
                            table = table.cast(parquet_writer.schema)
                            parquet_writer.write_table(table)
                            total_read += len(df_chunk)

                            if show_progress:
                                print_progress(total_read, total_rows, prefix=f"{sql_file} ")

                            rows = cursor.fetchmany(batch_size)

                        if parquet_writer is not None:
                            parquet_writer.close()

                        df = None
                        total_rows_read = total_read
            finally:
                conn.close()
                logging.info("Closed MSSQL connection for %s", sql_file)

            # ===== SAVE OUTPUT (FULL MODE ONLY) =====
            # Batch mode already wrote file during streaming
            if output_format == 'csv':
                if download_mode == "full":
                    df.to_csv(output_path, index=False)
            elif output_format == 'parquet':
                if download_mode == "full":
                    df.to_parquet(output_path, index=False)
            elif output_format == 'excel':
                if download_mode == "full":
                    df.to_excel(output_path, index=False)
            elif output_format == 'pickle':
                if download_mode == "full":
                    df.to_pickle(output_path)
            else:
                # Raise an error if an unsupported format is requested
                raise ValueError(f"⚠️ Unsupported output format: {output_format}")

            # ===== GENERATE REPORT =====
            logging.info("Saved output: %s", output_path)

            # Calculate and log transfer statistics
            if download_mode == "full":
                rows, cols = df.shape
            else:
                rows = total_rows_read or 0
                cols = total_cols or 0

            file_size_bytes = os.path.getsize(output_path)
            file_size_gb = file_size_bytes / (1024 ** 3)
            elapsed_minutes = (time.monotonic() - start_time) / 60
            
            # Log transfer summary
            logging.info("=" * 70)
            logging.info("Transfer Summary")
            logging.info("=" * 70)
            logging.info("File:                         %s", output_filename)
            logging.info("Rows:                         %s", rows)
            logging.info("Columns:                      %s", cols)
            logging.info("File size:                    %.6f GB", file_size_gb)
            logging.info("Transfer time:                %.2f minutes", elapsed_minutes)
            logging.info("=" * 70 + "\n\n\n")

        except Exception as e:
            # Log failure and re-raise
            logging.error("⚠️ Failed to process %s: %s", sql_file, e)
            raise

