"""
Local File to MSSQL Data Import Pipeline
==========================================
Production-grade data import pipeline with:
- Multiple input formats (CSV, Parquet, Excel, Pickle)
- Configurable upload modes (bulk/batch)
- Progress tracking and reporting
- Automatic file management

Author: Senior Data Engineering Team
"""

import os
import pandas as pd
import logging
import time
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq


def print_progress(current: int, total: int, prefix: str = "") -> None:
    if total <= 0:
        logging.info("%s%s rows", prefix, current)
        return
    width = 40
    ratio = min(max(current / total, 0.0), 1.0)
    filled = int(width * ratio)
    bar = "█" * filled + "░" * (width - filled)
    percent = int(ratio * 100)
    logging.info("%s[%s] %3d%% (%s/%s)", prefix, bar, percent, current, total)


def insert_data(
    conn_str: str,
    data_dir: str,
    table_name: str,
    schema: str = None,
    input_format: str = 'csv',
    insert_mode: str = "bulk",
    batch_size: int = 5000,
    show_progress: bool = True,
    columns: list = None,
    fresh_insert: bool = False,
    **kwargs
) -> None:
    """
    Import data from local files to MSSQL table.
    Args:
        conn_str: ODBC connection string for MSSQL
        data_dir: Directory containing data files to import
        table_name: Target MSSQL table name
        input_format: Input format ('csv', 'parquet', 'pickle', 'excel')
        insert_mode: Data transfer mode:
            - 'bulk': Use BULK INSERT (CSV only)
            - 'batch': Insert in batches using pyodbc
        batch_size: Number of rows per batch (for batch mode)
        show_progress: Whether to display progress bars during batch processing
        columns: List of columns to insert (optional)
    """
    logging.info("\n\n")
    data_dir = os.path.join(data_dir)
    if not os.path.isfile(data_dir):
        os.makedirs(data_dir, exist_ok=True)
    # If data_dir is a file, use it directly
    if os.path.isfile(data_dir):
        data_files = [os.path.basename(data_dir)]
        data_dir_path = os.path.dirname(data_dir)
    else:
        data_dir_path = data_dir
        data_files = [f for f in os.listdir(data_dir_path) if f.endswith(input_format)]
    if not data_files:
        logging.warning(f"⚠️ No .{input_format} files found in {data_dir}")
        return
    for data_file in data_files:
        file_path = os.path.join(data_dir_path, data_file)
        # Compose full table name with schema if provided
        full_table_name = f"[{schema}].[{table_name}]" if schema else table_name
        logging.info("=" * 70)
        logging.info("Processing: %s", data_file)
        logging.info("=" * 70)
        logging.info("Mode: %s | Format: %s | Batch size: %s", insert_mode, input_format, batch_size)
        start_time = time.monotonic()
        try:
            if insert_mode == "bulk" and input_format == "csv":
                conn = pyodbc.connect(conn_str, autocommit=True)
                cursor = conn.cursor()
                if fresh_insert:
                    drop_sql = f"IF OBJECT_ID(N'{full_table_name}', N'U') IS NOT NULL DROP TABLE {full_table_name}"
                    cursor.execute(drop_sql)
                    conn.commit()
                    logging.info(f"Table {full_table_name} dropped and recreated for fresh insert.")
                sql = f"""
                BULK INSERT {full_table_name}
                FROM '{file_path}'
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '\n',
                    TABLOCK
                )
                """
                cursor.execute(sql)
                conn.close()
                logging.info(f"BULK INSERT completed for {data_file}")
                rows = 'unknown'
                cols = 'unknown'
            else:
                # Batch mode or non-CSV: use pandas
                if input_format == 'csv':
                    df = pd.read_csv(file_path)
                elif input_format == 'parquet':
                    df = pd.read_parquet(file_path)
                elif input_format == 'excel':
                    df = pd.read_excel(file_path)
                elif input_format == 'pickle':
                    df = pd.read_pickle(file_path)
                else:
                    raise ValueError(f"Unsupported input format: {input_format}")
                if columns:
                    df = df[columns]
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                total_rows = len(df)
                total_cols = len(df.columns)

                def create_table():
                    col_defs = []
                    for col, dtype in zip(df.columns, df.dtypes):
                        if dtype.name.startswith("int"):
                            sql_type = "BIGINT"
                        elif dtype.name.startswith("float"):
                            sql_type = "FLOAT"
                        elif dtype.name.startswith("datetime"):
                            sql_type = "DATETIME2"
                        else:
                            sql_type = "NVARCHAR(MAX)"
                        col_defs.append(f"[{col}] {sql_type}")
                    col_defs_sql = ", ".join(col_defs)
                    create_sql = f"CREATE TABLE {full_table_name} ({col_defs_sql})"
                    cursor.execute(create_sql)
                    conn.commit()

                def create_table_if_not_exists():
                    col_defs = []
                    for col, dtype in zip(df.columns, df.dtypes):
                        if dtype.name.startswith("int"):
                            sql_type = "BIGINT"
                        elif dtype.name.startswith("float"):
                            sql_type = "FLOAT"
                        elif dtype.name.startswith("datetime"):
                            sql_type = "DATETIME2"
                        else:
                            sql_type = "NVARCHAR(MAX)"
                        col_defs.append(f"[{col}] {sql_type}")
                    col_defs_sql = ", ".join(col_defs)
                    create_sql = f"IF OBJECT_ID(N'{full_table_name}', N'U') IS NULL CREATE TABLE {full_table_name} ({col_defs_sql})"
                    cursor.execute(create_sql)
                    conn.commit()

                if fresh_insert:
                    drop_sql = f"IF OBJECT_ID(N'{full_table_name}', N'U') IS NOT NULL DROP TABLE {full_table_name}"
                    cursor.execute(drop_sql)
                    conn.commit()
                    create_table()
                    logging.info(f"Table {full_table_name} dropped and recreated for fresh insert.")
                else:
                    create_table_if_not_exists()

                for start in range(0, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    chunk = df.iloc[start:end]
                    placeholders = ','.join(['?'] * len(chunk.columns))
                    insert_sql = f"INSERT INTO {full_table_name} ({','.join(f'[{col}]' for col in chunk.columns)}) VALUES ({placeholders})"
                    cursor.fast_executemany = True
                    rows = chunk.astype(object).where(chunk.notnull(), None).values.tolist()
                    cursor.executemany(insert_sql, rows)
                    if show_progress:
                        print_progress(end, total_rows, prefix=f"{data_file} ")
                conn.commit()
                conn.close()
                rows = total_rows
                cols = total_cols
            file_size_bytes = os.path.getsize(file_path)
            file_size_gb = file_size_bytes / (1024 ** 3)
            elapsed_minutes = (time.monotonic() - start_time) / 60
            logging.info("=" * 70)
            logging.info("Import Summary")
            logging.info("=" * 70)
            logging.info("File:                         %s", data_file)
            logging.info("Rows:                         %s", rows)
            logging.info("Columns:                      %s", cols)
            logging.info("File size:                    %.6f GB", file_size_gb)
            logging.info("Transfer time:                %.2f minutes", elapsed_minutes)
            logging.info("=" * 70 + "\n\n\n")
        except Exception as e:
            logging.error("⚠️ Failed to import %s: %s", data_file, e)
            raise
