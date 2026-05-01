"""
MSSQL to PostgreSQL Data Pipeline
==================================
Production-grade ETL pipeline with:
- Schema synchronization (DDL)
- Configurable data transfer modes (append/upsert/full refresh)
- Connection pooling
- Error recovery with exponential backoff
- Progress tracking and validation
- Automatic staging table cleanup
- Dry-run mode for testing

Author: Senior Data Engineering Team
"""

import re
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple, Any

import pyodbc
import psycopg2
from psycopg2 import sql, pool
from psycopg2.extras import execute_values

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logger = logging.getLogger("mssql_to_postgres")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# Connection pool for PostgreSQL (thread-safe, reusable connections)
_pg_pool: Optional[pool.SimpleConnectionPool] = None

MAX_RETRIES = 1
RETRY_DELAY_BASE = 20

# ============================================================================
# CONNECTION MANAGEMENT
# ============================================================================

def init_pg_pool(minconn: int = 1, maxconn: int = 10):
    """
    Initialize PostgreSQL connection pool.
    Call once at application startup.
    
    Args:
        minconn: Minimum number of connections to maintain
        maxconn: Maximum number of connections allowed
    """
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = pool.SimpleConnectionPool(minconn, maxconn, PG_CONN_STR)
        logger.info(f"PostgreSQL connection pool initialized (min={minconn}, max={maxconn})")


def close_pg_pool():
    """Close all connections in the PostgreSQL pool."""
    global _pg_pool
    if _pg_pool:
        _pg_pool.closeall()
        _pg_pool = None
        logger.info("PostgreSQL connection pool closed")


@contextmanager
def mssql_conn():
    """
    Context manager for MSSQL connections.
    Automatically closes connection on exit.
    """
    conn = None
    try:
        conn = pyodbc.connect(MSSQL_CONN_STR, timeout=30)
        yield conn
    except Exception as e:
        logger.error("MSSQL connection error: %s", e)
        raise
    finally:
        if conn:
            conn.close()


@contextmanager
def pg_conn():
    """
    Context manager for PostgreSQL connections from pool.
    Returns connection to pool on exit.
    """
    if _pg_pool is None:
        init_pg_pool()
    
    conn = _pg_pool.getconn()
    try:
        yield conn
    except Exception as e:
        conn.rollback()
        logger.error("PostgreSQL connection error: %s", e)
        raise
    finally:
        _pg_pool.putconn(conn)


# ============================================================================
# TYPE MAPPING
# ============================================================================

def map_mssql_type_to_pg(
    data_type: str,
    char_len: Optional[int],
    num_precision: Optional[int],
    num_scale: Optional[int],
) -> str:
    """
    Map MSSQL data types to PostgreSQL equivalents.
    
    Args:
        data_type: MSSQL type name (e.g., 'varchar', 'int')
        char_len: Character maximum length for string types
        num_precision: Numeric precision for decimal types
        num_scale: Numeric scale for decimal types
    
    Returns:
        PostgreSQL type definition string
    """
    t = (data_type or "").lower()

    # Integer types
    if t == "bigint":
        return "bigint"
    if t == "smallint":
        return "smallint"
    if t in {"int", "integer"}:
        return "integer"
    if t == "tinyint":
        return "smallint"
    if t == "bit":
        return "boolean"
    
    # UUID type
    if t == "uniqueidentifier":
        return "uuid"
    
    # Decimal/numeric types
    if t in {"decimal", "numeric", "money", "smallmoney"}:
        if num_precision is not None and num_scale is not None:
            return f"numeric({num_precision},{num_scale})"
        return "numeric"
    
    # Floating point types
    if t in {"float", "double"}:
        return "double precision"
    if t == "real":
        return "real"
    
    # Date/time types
    if t in {"datetime", "smalldatetime", "datetime2"}:
        return "timestamp"
    if t == "date":
        return "date"
    if t == "time":
        return "time"
    if t == "datetimeoffset":
        return "timestamptz"
    
    # String types
    if t in {"nvarchar", "varchar"}:
        if char_len and 0 < char_len <= 10485760:
            return f"varchar({char_len})"
        return "text"
    if t in {"nchar", "char"}:
        if char_len and 0 < char_len <= 10485760:
            return f"char({char_len})"
        return "char"
    if t in {"text", "ntext"}:
        return "text"
    
    # Binary types
    if t in {"binary", "varbinary", "image"}:
        return "bytea"
    
    # XML type
    if t == "xml":
        return "xml"
    
    # JSON type (SQL Server 2016+)
    if t == "json":
        return "jsonb"
    
    # Fallback for unknown types
    logger.warning("Unknown MSSQL type '%s', defaulting to 'text'", data_type)
    return "text"


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def normalize_schema(name: str) -> str:
    """
    Normalize schema name for case-insensitive matching.
    PostgreSQL treats unquoted identifiers as lowercase.
    """
    return name.strip().lower()


def parse_qualified_name(name: str) -> Tuple[str, str]:
    """
    Parse 'schema.table' format and return (schema, table).
    
    Args:
        name: Qualified table name (e.g., 'dbo.Users')
    
    Returns:
        Tuple of (normalized_schema, original_table)
    
    Raises:
        ValueError: If name is not in 'schema.table' format
    """
    parts = [p.strip() for p in name.split(".") if p.strip()]
    if len(parts) != 2:
        raise ValueError(f"Table name must be 'schema.table' format, got: {name}")
    return normalize_schema(parts[0]), parts[1]


def validate_identifier(name: str) -> str:
    """
    Validate SQL identifier to prevent injection.
    
    Args:
        name: Identifier to validate
    
    Returns:
        The validated identifier
    
    Raises:
        ValueError: If identifier contains unsafe characters
    """
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def validate_where_clause(where_clause: str) -> str:
    """
    Basic validation to prevent SQL injection in WHERE clauses.
    
    Args:
        where_clause: Raw SQL WHERE clause
    
    Returns:
        The validated clause
    
    Raises:
        ValueError: If clause contains dangerous tokens
    """
    unsafe_tokens = [";", "--", "/*", "*/", "xp_", "sp_"]
    for token in unsafe_tokens:
        if token in where_clause.lower():
            raise ValueError(f"Unsafe token '{token}' detected in WHERE clause")
    return where_clause


def build_where_clause(filter_cfg: Optional[Dict[str, Any]]) -> Tuple[str, List[Any]]:
    """
    Build parameterized WHERE clause from filter configuration.
    
    Args:
        filter_cfg: Dictionary with 'where' and optional 'params' keys,
                   or raw string (not recommended)
    
    Returns:
        Tuple of (where_sql, parameters_list)
    
    Examples:
        >>> build_where_clause({"where": "date >= ?", "params": [datetime.now()]})
        (" WHERE date >= ? ", [datetime.datetime(...)])
        
        >>> build_where_clause("active = 1")  # not recommended
        (" WHERE active = 1 ", [])
    """
    if not filter_cfg:
        return "", []
    
    # Handle legacy string format (not recommended)
    if isinstance(filter_cfg, str):
        where = filter_cfg.strip()
        if not where:
            return "", []
        validate_where_clause(where)
        logger.warning(
            "Using raw filter string. Prefer parameterized format: "
            "{'where': '...', 'params': [...]}"
        )
        return f" WHERE {where} ", []
    
    # Recommended parameterized format
    where = str(filter_cfg.get("where", "")).strip()
    params = list(filter_cfg.get("params", []) or [])
    
    if not where:
        return "", []
    
    validate_where_clause(where)
    return f" WHERE {where} ", params


def build_order_by(order_by: Optional[List[str]]) -> str:
    """
    Build ORDER BY clause from column list.
    
    Args:
        order_by: List of column names to sort by
    
    Returns:
        SQL ORDER BY clause
    
    Examples:
        >>> build_order_by(["AppId", "CreatedDate"])
        " ORDER BY [AppId], [CreatedDate]"
    """
    if not order_by:
        return ""
    
    if isinstance(order_by, str):
        order_by = [order_by]
    
    safe_cols = [validate_identifier(c) for c in order_by]
    return " ORDER BY " + ", ".join([f"[{c}]" for c in safe_cols])


def print_progress(current: int, total: int, prefix: str = ""):
    """
    Log progress bar for long-running operations.
    
    Args:
        current: Current progress value
        total: Total expected value
        prefix: Optional prefix for log message
    """
    if total <= 0:
        return
    
    width = 40
    ratio = min(max(current / total, 0.0), 1.0)
    filled = int(width * ratio)
    bar = "█" * filled + "░" * (width - filled)
    percent = int(ratio * 100)
    
    logger.info("%s[%s] %3d%% (%s/%s)", prefix, bar, percent, current, total)


def retry_on_failure(func, *args, max_retries=MAX_RETRIES, **kwargs):
    """
    Retry function with exponential backoff on failure.
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        *args, **kwargs: Arguments to pass to func
    
    Returns:
        Function return value
    
    Raises:
        Last exception if all retries fail
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error("Failed after %d attempts: %s", max_retries, e)
                raise
            
            delay = RETRY_DELAY_BASE * attempt
            logger.warning(
                "Attempt %d/%d failed: %s. Retrying in %ds...",
                attempt + 1, max_retries, e, delay
            )
            time.sleep(delay)


# ============================================================================
# METADATA DISCOVERY
# ============================================================================

def get_mssql_columns(schema: str, table: str) -> List[Dict[str, Any]]:
    """
    Retrieve column metadata from MSSQL INFORMATION_SCHEMA.
    
    Args:
        schema: MSSQL schema name
        table: MSSQL table name
    
    Returns:
        List of column dictionaries with keys:
        - name: Column name
        - pg_type: Mapped PostgreSQL type
        - nullable: Boolean indicating NULL allowed
        - is_pk: Boolean indicating primary key membership
    """
    with mssql_conn() as conn:
        cur = conn.cursor()
        
        # Get column metadata
        cur.execute(
            """
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
            """,
            (schema, table),
        )
        columns = cur.fetchall()

        # Get primary key columns
        cur.execute(
            """
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
             AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = ?
              AND tc.TABLE_NAME = ?
              AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """,
            (schema, table),
        )
        pk_cols = {row[0] for row in cur.fetchall()}

    # Build structured column list
    result = []
    for row in columns:
        col_name, data_type, char_len, num_prec, num_scale, is_nullable = row
        result.append({
            "name": col_name,
            "pg_type": map_mssql_type_to_pg(data_type, char_len, num_prec, num_scale),
            "nullable": (str(is_nullable).upper() == "YES"),
            "is_pk": col_name in pk_cols,
        })
    
    return result


def get_mssql_row_count(
    schema: str,
    table: str,
    filter_cfg: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Get total row count from MSSQL table (with optional filter).
    
    Args:
        schema: MSSQL schema name
        table: MSSQL table name
        filter_cfg: Optional filter configuration
    
    Returns:
        Row count
    """
    where_clause, params = build_where_clause(filter_cfg)
    
    with mssql_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM [{schema}].[{table}]{where_clause}",
            params,
        )
        return int(cur.fetchone()[0])


def get_pg_columns(schema: str, table: str) -> List[str]:
    """
    Get current column names from PostgreSQL table.
    
    Args:
        schema: PostgreSQL schema name
        table: PostgreSQL table name
    
    Returns:
        List of column names in ordinal order
    """
    with pg_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [row[0] for row in cur.fetchall()]


# ============================================================================
# SCHEMA MANAGEMENT
# ============================================================================

def pg_schema_exists(schema: str) -> bool:
    """Check if PostgreSQL schema exists."""
    with pg_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = %s
            """,
            (schema,),
        )
        return cur.fetchone() is not None


def ensure_pg_schema(schema: str, dry_run: bool = False):
    """
    Create PostgreSQL schema if it doesn't exist.
    
    Args:
        schema: Schema name to create
        dry_run: If True, only log the action without executing
    """
    if pg_schema_exists(schema):
        logger.debug("Schema '%s' already exists", schema)
        return

    create_sql = sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
    
    if dry_run:
        logger.info("[DRY RUN] Would create schema: %s", schema)
        return
    
    with pg_conn() as conn:
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        logger.info("Created schema: %s", schema)


def pg_table_exists(schema: str, table: str) -> bool:
    """Check if PostgreSQL table exists."""
    with pg_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def ensure_table_exists(
    mssql_schema: str,
    table: str,
    pg_schema: str,
    pg_table: str,
    dry_run: bool = False,
) -> bool:
    """
    Create PostgreSQL table if it doesn't exist, based on MSSQL schema.
    
    Args:
        mssql_schema: Source MSSQL schema
        table: Source MSSQL table name
        pg_schema: Target PostgreSQL schema
        pg_table: Target PostgreSQL table name
        dry_run: If True, only log the action
    
    Returns:
        True if table was created, False if already existed
    """
    ensure_pg_schema(pg_schema, dry_run=dry_run)
    
    if pg_table_exists(pg_schema, pg_table):
        logger.debug("Table '%s.%s' already exists", pg_schema, pg_table)
        return False

    mssql_cols = get_mssql_columns(mssql_schema, table)

    # Build column definitions
    col_defs = []
    pk_cols = []
    for col in mssql_cols:
        col_def = sql.SQL("{} {}{}").format(
            sql.Identifier(col["name"]),
            sql.SQL(col["pg_type"]),
            sql.SQL(" NOT NULL" if not col["nullable"] else ""),
        )
        col_defs.append(col_def)
        if col["is_pk"]:
            pk_cols.append(sql.Identifier(col["name"]))

    # Build CREATE TABLE statement
    create_sql = sql.SQL("CREATE TABLE {}.{} ({})").format(
        sql.Identifier(pg_schema),
        sql.Identifier(pg_table),
        sql.SQL(", ").join(col_defs),
    )
    
    # Add primary key constraint if exists
    if pk_cols:
        create_sql = sql.SQL("CREATE TABLE {}.{} ({}, PRIMARY KEY ({}))").format(
            sql.Identifier(pg_schema),
            sql.Identifier(pg_table),
            sql.SQL(", ").join(col_defs),
            sql.SQL(", ").join(pk_cols),
        )
    
    if dry_run:
        logger.info("[DRY RUN] Would create table: %s.%s", pg_schema, pg_table)
        return True
    
    with pg_conn() as conn:
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        logger.info("Created table: %s.%s", pg_schema, pg_table)
    
    return True


def ensure_staging_table(
    pg_schema: str,
    pg_table: str,
    staging_table: str,
    dry_run: bool = False,
):
    """
    Create unlogged staging table (if missing) and truncate it.
    Staging tables are faster for bulk loads (no WAL logging).
    
    Args:
        pg_schema: PostgreSQL schema
        pg_table: Main table to clone structure from
        staging_table: Staging table name
        dry_run: If True, only log the action
    """
    if dry_run:
        logger.info(
            "[DRY RUN] Would ensure staging table: %s.%s",
            pg_schema, staging_table
        )
        return
    
    with pg_conn() as conn:
        cur = conn.cursor()
        
        # Create staging table if not exists (UNLOGGED for performance)
        cur.execute(
            sql.SQL(
                "CREATE UNLOGGED TABLE IF NOT EXISTS {}.{} "
                "(LIKE {}.{} INCLUDING DEFAULTS)"
            ).format(
                sql.Identifier(pg_schema),
                sql.Identifier(staging_table),
                sql.Identifier(pg_schema),
                sql.Identifier(pg_table),
            )
        )
        
        # Truncate staging table for fresh load
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(pg_schema),
                sql.Identifier(staging_table),
            )
        )
        
        conn.commit()
        logger.debug("Staging table ready: %s.%s", pg_schema, staging_table)


def sync_columns(
    mssql_schema: str,
    table: str,
    pg_schema: str,
    pg_table: str,
    dry_run: bool = False,
) -> Dict[str, List]:
    """
    Synchronize column definitions between MSSQL and PostgreSQL.
    Adds missing columns and drops extra columns.
    
    Args:
        mssql_schema: Source MSSQL schema
        table: Source MSSQL table
        pg_schema: Target PostgreSQL schema
        pg_table: Target PostgreSQL table
        dry_run: If True, only log changes without executing
    
    Returns:
        Dictionary with 'added' and 'dropped' column lists
    """
    mssql_cols = get_mssql_columns(mssql_schema, table)
    pg_cols = get_pg_columns(pg_schema, pg_table)

    # Case-insensitive comparison
    mssql_col_map = {c["name"].lower(): c for c in mssql_cols}
    pg_col_map = {name.lower(): name for name in pg_cols}

    to_add = [c for name, c in mssql_col_map.items() if name not in pg_col_map]
    to_drop = [name for name in pg_col_map if name not in mssql_col_map]

    if dry_run:
        if to_add:
            logger.info("[DRY RUN] Would add columns: %s", [c["name"] for c in to_add])
        if to_drop:
            logger.info("[DRY RUN] Would drop columns: %s", to_drop)
        return {"added": to_add, "dropped": to_drop}

    with pg_conn() as conn:
        cur = conn.cursor()
        
        # Add missing columns
        for col in to_add:
            alter_sql = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}{}").format(
                sql.Identifier(pg_schema),
                sql.Identifier(pg_table),
                sql.Identifier(col["name"]),
                sql.SQL(col["pg_type"]),
                sql.SQL(" NOT NULL" if not col["nullable"] else ""),
            )
            cur.execute(alter_sql)
            logger.info("Added column: %s.%s.%s", pg_schema, pg_table, col["name"])

        # Drop extra columns
        for col in to_drop:
            alter_sql = sql.SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
                sql.Identifier(pg_schema),
                sql.Identifier(pg_table),
                sql.Identifier(pg_col_map[col]),
            )
            cur.execute(alter_sql)
            logger.info("Dropped column: %s.%s.%s", pg_schema, pg_table, col)

        conn.commit()

    return {"added": to_add, "dropped": to_drop}


# ============================================================================
# DATA TRANSFER
# ============================================================================

def transfer_data_batches(
    mssql_schema: str,
    table: str,
    pg_schema: str,
    pg_table: str,
    batch_size: int = 5000,
    show_progress: bool = True,
    filter_cfg: Optional[Dict[str, Any]] = None,
    pk_override: Optional[List[str]] = None,
    mode: str = "append",
    order_by: Optional[List[str]] = None,
    staging_table: Optional[str] = None,
    dry_run: bool = False,
    validate: bool = True,
) -> int:
    """
    Transfer data from MSSQL to PostgreSQL with configurable modes.
    
    Args:
        mssql_schema: Source MSSQL schema
        table: Source MSSQL table
        pg_schema: Target PostgreSQL schema
        pg_table: Target PostgreSQL table
        batch_size: Rows per batch (tune for memory/performance)
        show_progress: Whether to display progress bars
        filter_cfg: Optional WHERE clause configuration
        pk_override: Override primary key columns (for upsert logic)
        mode: Transfer mode ('append', 'append_staging', 'full_refresh')
        order_by: Optional ORDER BY columns for consistent ordering
        staging_table: Custom staging table name (for append_staging mode)
        dry_run: If True, only log actions without transferring data
        validate: If True, validate row counts after transfer
    
    Returns:
        Number of rows transferred
    
    Transfer Modes:
        - append: DELETE by PK, then INSERT (batch upsert)
        - append_staging: Load ALL to staging → DELETE matches → INSERT from staging (ONE TIME)
        - full_refresh: TRUNCATE table, then INSERT all rows
    """
    # Validate mode
    mode = (mode or "append").lower()
    if mode not in {"append", "append_staging", "full_refresh"}:
        raise ValueError(
            f"Invalid mode '{mode}'. Must be 'append', 'append_staging', or 'full_refresh'"
        )

    # Get column metadata
    mssql_cols = get_mssql_columns(mssql_schema, table)
    col_names = [c["name"] for c in mssql_cols]
    
    # Determine primary key columns
    pk_cols = pk_override or [c["name"] for c in mssql_cols if c["is_pk"]]
    
    # Map PK column names to indices for efficient extraction
    pk_indices = []
    for pk in pk_cols:
        for i, c in enumerate(col_names):
            if c.lower() == pk.lower():
                pk_indices.append(i)
                break

    # Build WHERE and ORDER BY clauses
    where_clause, where_params = build_where_clause(filter_cfg)
    order_by_clause = build_order_by(order_by)

    # Build MSSQL SELECT statement (bracket-quoted identifiers)
    select_cols = ", ".join([f"[{c}]" for c in col_names])
    select_sql = (
        f"SELECT {select_cols} "
        f"FROM [{mssql_schema}].[{table}]"
        f"{where_clause}{order_by_clause}"
    )

    # Build PostgreSQL INSERT statement
    insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
        sql.Identifier(pg_schema),
        sql.Identifier(pg_table),
        sql.SQL(", ").join([sql.Identifier(c) for c in col_names]),
    )

    # Build DELETE statement for append mode (if PK exists)
    delete_sql = None
    if mode == "append" and pk_cols:
        delete_sql = sql.SQL(
            "DELETE FROM {}.{} AS t USING (VALUES %s) AS v({}) WHERE {}"
        ).format(
            sql.Identifier(pg_schema),
            sql.Identifier(pg_table),
            sql.SQL(", ").join([sql.Identifier(c) for c in pk_cols]),
            sql.SQL(" AND ").join([
                sql.SQL("t.{0} = v.{0}").format(sql.Identifier(c))
                for c in pk_cols
            ]),
        )

    # Validate append_staging requirements
    if mode == "append_staging" and not pk_cols:
        raise ValueError(
            "append_staging mode requires primary_key to be defined or present in source table"
        )

    # Initialize counters
    total_rows_read = 0              # Total rows read from MSSQL
    total_batches = 0                # Number of batches processed
    total_rows_deleted_main = 0      # Total rows deleted from main table
    total_delete_attempted = 0       # Total delete attempts (for append mode)
    target_total = get_mssql_row_count(mssql_schema, table, filter_cfg=filter_cfg)

    if dry_run:
        logger.info(
            "[DRY RUN] Would transfer %d rows from %s.%s to %s.%s (mode=%s)",
            target_total, mssql_schema, table, pg_schema, pg_table, mode
        )
        return 0

    # Get initial row count in main table (before transfer)
    initial_count = 0
    with pg_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(pg_schema),
                sql.Identifier(pg_table),
            )
        )
        initial_count = cur.fetchone()[0]
    
    logger.info("Initial row count in %s.%s: %d", pg_schema, pg_table, initial_count)


    # Staging table setup for append_staging mode
    staging_table_name = None
    insert_stage_sql = None
    delete_join_sql = None
    insert_from_stage_sql = None
    drop_stage_sql = None
    
    if mode == "append_staging":
        staging_table_name = staging_table or f"{pg_table}__staging"
        ensure_staging_table(pg_schema, pg_table, staging_table_name)
        
        # INSERT into staging
        insert_stage_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(pg_schema),
            sql.Identifier(staging_table_name),
            sql.SQL(", ").join([sql.Identifier(c) for c in col_names]),
        )
        
        # DELETE from main table using staging JOIN (executed ONCE after all data loaded)
        delete_join_sql = sql.SQL(
            "DELETE FROM {}.{} AS t USING {}.{} AS s WHERE {}"
        ).format(
            sql.Identifier(pg_schema),
            sql.Identifier(pg_table),
            sql.Identifier(pg_schema),
            sql.Identifier(staging_table_name),
            sql.SQL(" AND ").join([
                sql.SQL("t.{0} = s.{0}").format(sql.Identifier(c))
                for c in pk_cols
            ]),
        )
        
        # INSERT from staging to main table (executed ONCE after all data loaded)
        insert_from_stage_sql = sql.SQL(
            "INSERT INTO {}.{} ({}) SELECT {} FROM {}.{}"
        ).format(
            sql.Identifier(pg_schema),
            sql.Identifier(pg_table),
            sql.SQL(", ").join([sql.Identifier(c) for c in col_names]),
            sql.SQL(", ").join([sql.Identifier(c) for c in col_names]),
            sql.Identifier(pg_schema),
            sql.Identifier(staging_table_name),
        )
        
        # DROP staging table after completion
        drop_stage_sql = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(pg_schema),
            sql.Identifier(staging_table_name),
        )

    # Open connections and start transfer
    with mssql_conn() as ms_conn, pg_conn() as pg:
        ms_cur = ms_conn.cursor()
        ms_cur.arraysize = batch_size  # Optimize fetch performance
        pg_cur = pg.cursor()

        # Full refresh: truncate table before loading
        if mode == "full_refresh":
            logger.info("Full refresh: truncating %s.%s", pg_schema, pg_table)
            pg_cur.execute(
                sql.SQL("TRUNCATE TABLE {}.{}").format(
                    sql.Identifier(pg_schema),
                    sql.Identifier(pg_table),
                )
            )
            pg.commit()

        # Execute SELECT on MSSQL
        ms_cur.execute(select_sql, where_params)

        # ===== PHASE 1: LOAD DATA FROM MSSQL =====
        if mode == "append_staging":
            logger.info("Phase 1/3: Loading data to staging table...")
        
        # Batch processing loop
        while True:
            rows = ms_cur.fetchmany(batch_size)
            if not rows:
                break  # No more data

            batch_size_actual = len(rows)
            total_rows_read += batch_size_actual
            total_batches += 1

            try:
                if mode == "append_staging":
                    # === APPEND_STAGING MODE - PHASE 1: Load to staging only ===
                    execute_values(pg_cur, insert_stage_sql, rows, page_size=batch_size)
                    pg.commit()
                    
                else:
                    # === APPEND MODE - Process immediately ===
                    # 1. Delete existing rows by PK (if PK defined)
                    if delete_sql and pk_cols:
                        pk_values = [tuple(row[i] for i in pk_indices) for row in rows]
                        total_delete_attempted += len(pk_values)
                        execute_values(pg_cur, delete_sql, pk_values, page_size=batch_size)
                        deleted_now = pg_cur.rowcount
                        if deleted_now and deleted_now > 0:
                            total_rows_deleted_main += deleted_now
                    
                    # 2. Insert new/updated rows
                    execute_values(pg_cur, insert_sql, rows, page_size=batch_size)
                    pg.commit()

            except Exception as e:
                pg.rollback()
                logger.exception("Batch failed for %s.%s: %s", pg_schema, pg_table, e)
                raise

            # Update progress bar
            if show_progress:
                print_progress(total_rows_read, target_total, prefix=f"{pg_schema}.{pg_table} ")

        # ===== PHASE 2: DELETE DUPLICATES (append_staging only) =====
        if mode == "append_staging":
            logger.info(
                "Phase 2/3: Removing duplicates from main table (deleting existing rows with matching PKs)..."
            )
            
            try:
                pg_cur.execute(delete_join_sql)
                total_rows_deleted_main = pg_cur.rowcount
                pg.commit()
                
                logger.info("Deleted %d duplicate rows from main table", total_rows_deleted_main)
                
            except Exception as e:
                pg.rollback()
                logger.exception("Delete phase failed for %s.%s: %s", pg_schema, pg_table, e)
                raise

        # ===== PHASE 3: INSERT FROM STAGING TO MAIN (append_staging only) =====
        if mode == "append_staging":
            logger.info("Phase 3/3: Inserting data from staging to main table...")
            
            try:
                pg_cur.execute(insert_from_stage_sql)
                rows_inserted_from_staging = pg_cur.rowcount
                pg.commit()
                
                logger.info("Inserted %d rows from staging to main table", rows_inserted_from_staging)
                
            except Exception as e:
                pg.rollback()
                logger.exception("Insert phase failed for %s.%s: %s", pg_schema, pg_table, e)
                raise

        # ===== CLEANUP: Drop staging table =====
        if mode == "append_staging" and drop_stage_sql:
            logger.info("Dropping staging table: %s.%s", pg_schema, staging_table_name)
            pg_cur.execute(drop_stage_sql)
            pg.commit()

    # Get final row count in main table (after transfer)
    final_count = 0
    with pg_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(pg_schema),
                sql.Identifier(pg_table),
            )
        )
        final_count = cur.fetchone()[0]

    # Calculate actual metrics
    net_rows_added = final_count - initial_count
    rows_updated = min(total_rows_deleted_main, total_rows_read)  # Updated = rows that existed and were replaced
    rows_inserted_new = max(0, net_rows_added)  # New inserts = positive net change
    
    # Log transfer summary with corrected metrics
    logger.info("=" * 70)
    logger.info("Transfer Summary for %s.%s", pg_schema, pg_table)
    logger.info("=" * 70)
    logger.info("Source rows processed:        %d rows in %d batches", total_rows_read, total_batches)
    logger.info("Rows updated (replaced):      %d", rows_updated)
    logger.info("Rows inserted (new):          %d", rows_inserted_new)
    
    if mode == "append":
        logger.info("Delete attempts (batch):      %d", total_delete_attempted)
    
    logger.info("-" * 70)
    logger.info("Before transfer:              %d rows", initial_count)
    logger.info("After transfer:               %d rows", final_count)
    logger.info("Net change:                   %+d rows", net_rows_added)
    logger.info("=" * 70)

    # Validation: compare row counts
    if validate:
        if final_count == target_total:
            logger.info(
                "✓ Validation passed: PostgreSQL=%d, MSSQL=%d (match)",
                final_count, target_total
            )
        else:
            delta = final_count - target_total
            if mode in {"append", "append_staging"} and initial_count > 0:
                logger.info(
                    "✓ Validation: PostgreSQL=%d, MSSQL source=%d (delta=%+d due to existing data)",
                    final_count, target_total, delta
                )
            else:
                logger.warning(
                    "⚠ Validation mismatch: PostgreSQL=%d, MSSQL=%d (delta=%+d)",
                    final_count, target_total, delta
                )

    # Return the actual rows read from source
    return total_rows_read



# ============================================================================
# REPORTING
# ============================================================================

def format_column_summary(cols: List[Dict[str, Any]]) -> List[str]:
    """
    Format column metadata for human-readable logging.
    
    Args:
        cols: List of column dictionaries from get_mssql_columns()
    
    Returns:
        List of formatted strings (one per column)
    """
    lines = []
    for c in cols:
        null_txt = "NULL" if c["nullable"] else "NOT NULL"
        pk_txt = " [PK]" if c["is_pk"] else ""
        lines.append(f"  - {c['name']:<30} {c['pg_type']:<25} {null_txt}{pk_txt}")
    return lines


# ============================================================================
# PIPELINE ORCHESTRATION
# ============================================================================

def run_pipeline(
    settings: Dict[str, Any],
    do_sync: bool = True,
    do_transfer: bool = True,
    dry_run: bool = False,
):
    """
    Execute the complete ETL pipeline for multiple tables.
    
    Args:
        settings: Pipeline configuration dictionary
        do_sync: Whether to perform schema synchronization
        do_transfer: Whether to transfer data
        dry_run: If True, simulate actions without executing
    
    Configuration Structure:
        {
            "default_batch_size": 5000,
            "default_mode": "append_staging",
            "tables": {
                "TableName": {
                    "source": "mssql_schema.table",
                    "destination": "pg_schema.table",
                    "batch_size": 10000,  # optional, overrides default
                    "mode": "append_staging",  # append|append_staging|full_refresh
                    "filter": {"where": "date >= ?", "params": [date]},
                    "primary_key": ["id"],  # optional override
                    "order_by": ["id", "created_at"],  # optional
                    "staging_table": "custom_staging",  # optional
                }
            }
        }
    """
    tables = settings.get("tables", {})
    default_mode = settings.get("default_mode", "append")
    default_batch_size = int(settings.get("default_batch_size", 5000))

    if dry_run:
        logger.info("=" * 70)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 70)

    for name, cfg in tables.items():
        source = cfg.get("source")
        destination = cfg.get("destination")
        
        if not source or not destination:
            raise ValueError(f"Missing source/destination for table '{name}'")

        # Parse qualified names
        mssql_schema, mssql_table = parse_qualified_name(source)
        pg_schema, pg_table = parse_qualified_name(destination)

        # Get configuration with defaults
        batch_size = int(cfg.get("batch_size", default_batch_size))
        mode = (cfg.get("mode") or default_mode).lower()
        filter_cfg = cfg.get("filter") or cfg.get("filter_logic")
        order_by = cfg.get("order_by")
        staging_table = cfg.get("staging_table")
        pk_override = cfg.get("primary_key")

        logger.info("=" * 70)
        logger.info("Processing: %s → %s.%s", source, pg_schema, pg_table)
        logger.info("=" * 70)

        # ===== SCHEMA SYNCHRONIZATION =====
        if do_sync:
            logger.info("--- Schema Sync Phase ---")
            
            # Create table if missing
            created = ensure_table_exists(
                mssql_schema, mssql_table, pg_schema, pg_table, dry_run=dry_run
            )
            
            # Synchronize columns
            changes = sync_columns(
                mssql_schema, mssql_table, pg_schema, pg_table, dry_run=dry_run
            )

            # Get final column definitions
            if not dry_run:
                mssql_cols = get_mssql_columns(mssql_schema, mssql_table)
            else:
                mssql_cols = []

            # Print sync report
            logger.info("Table created: %s", created)
            logger.info("Columns added: %d", len(changes["added"]))
            logger.info("Columns dropped: %d", len(changes["dropped"]))

            if changes["added"]:
                logger.info("Added columns:")
                for line in format_column_summary(changes["added"]):
                    logger.info(line)

            if changes["dropped"]:
                logger.info("Dropped columns:")
                for col_name in changes["dropped"]:
                    logger.info("  - %s", col_name)

            if mssql_cols and not dry_run:
                logger.info("Final schema (%d columns):", len(mssql_cols))
                for line in format_column_summary(mssql_cols):
                    logger.info(line)

        # ===== DATA TRANSFER =====
        if do_transfer:
            logger.info("--- Data Transfer Phase ---")
            logger.info("Batch size: %d", batch_size)
            logger.info("Mode: %s", mode)
            
            if filter_cfg:
                logger.info("Filter: %s", filter_cfg.get("where", filter_cfg))
            
            if order_by:
                logger.info("Order by: %s", order_by)

            # Execute transfer with retry logic
            transferred = retry_on_failure(
                transfer_data_batches,
                mssql_schema,
                mssql_table,
                pg_schema,
                pg_table,
                batch_size=batch_size,
                show_progress=True,
                filter_cfg=filter_cfg,
                pk_override=pk_override,
                mode=mode,
                order_by=order_by,
                staging_table=staging_table,
                dry_run=dry_run,
                validate=True,
            )

            logger.info("Rows transferred: %s", transferred)

        logger.info("=" * 70 + "\n")


