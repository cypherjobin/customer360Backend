"""
Oracle Database Connection Utility
===================================
Read-only connection to Cerillion CRM PROD.

Requirements:
    pip install oracledb

Environment Variables:
    ORACLE_DB_PASSWORD     - Database password (required)
    ORACLE_DB_USERNAME    - Database username (default: tester)
    ORACLE_DB_HOST        - Database host (default: ora01-primary-vip.prod.vmie.local)
    ORACLE_DB_PORT        - Database port (default: 1556)
    ORACLE_DB_SERVICE     - Service name (default: crmprod)

Usage:
    # Set password in environment first
    export ORACLE_DB_PASSWORD='your_password'

    # Run the utility
    python oracle_db_util.py

    # Or import as a module
    from oracle_db_util import OracleConnection
    with OracleConnection() as conn:
        df = conn.query("SELECT * FROM customers FETCH FIRST 5 ROWS ONLY")
"""

import os
import sys
import logging
from contextlib import contextmanager
from typing import Optional, List, Dict, Any
import oracledb

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

class OracleConfig:
    """Database connection configuration from environment variables."""

    @staticmethod
    def get_config() -> Dict[str, str]:
        """Get connection config from environment variables with defaults."""
        config = {
            "user": os.getenv("ORACLE_DB_USERNAME", "tester"),
            "password": os.getenv("ORACLE_DB_PASSWORD", ""),
            "host": os.getenv("ORACLE_DB_HOST", "ora01-primary-vip.prod.vmie.local"),
            "port": int(os.getenv("ORACLE_DB_PORT", "1556")),
            "service_name": os.getenv("ORACLE_DB_SERVICE", "crmprod"),
        }

        # Validate password is set
        if not config["password"]:
            raise ValueError(
                "ORACLE_DB_PASSWORD environment variable not set. "
                "Please set it before connecting:\n"
                "  export ORACLE_DB_PASSWORD='your_password'"
            )

        return config


# ============================================================
# CONNECTION MANAGER
# ============================================================

class OracleConnection:
    """
    Oracle database connection manager with context manager support.

    Uses oracledb in thin mode (no Oracle Client required).

    Example:
        with OracleConnection() as conn:
            tables = conn.list_tables(limit=10)
            columns = conn.describe_table("CUSTOMER")
            rows = conn.query("SELECT * FROM customers FETCH FIRST 5 ROWS ONLY")
    """

    def __init__(self, config: Optional[Dict[str, str]] = None):
        """
        Initialize connection with configuration.

        Args:
            config: Optional config dict. If not provided, loads from environment.
        """
        self.config = config or OracleConfig.get_config()
        self._connection: Optional[oracledb.Connection] = None

    def connect(self) -> oracledb.Connection:
        """
        Establish database connection.

        Returns:
            oracledb.Connection object

        Raises:
            ValueError: If password not configured
            oracledb.Error: If connection fails
        """
        if self._connection:
            return self._connection

        try:
            logger.info(
                f"Connecting to Oracle DB: {self.config['user']}@"
                f"{self.config['host']}:{self.config['port']}/{self.config['service_name']}"
            )

            # Build DSN for thin client
            dsn = (
                f"(DESCRIPTION="
                f"(ADDRESS=(PROTOCOL=TCP)(HOST={self.config['host']})(PORT={self.config['port']}))"
                f"(CONNECT_DATA=(SERVICE_NAME={self.config['service_name']})))"
            )

            # Create connection (thin mode - no Oracle Client library needed)
            self._connection = oracledb.connect(
                user=self.config["user"],
                password=self.config["password"],
                dsn=dsn
            )

            # Set session to read-only mode for safety
            cursor = self._connection.cursor()
            try:
                cursor.execute("ALTER SESSION SET READ_ONLY = TRUE")
                logger.info("Session set to READ-ONLY mode")
            except Exception as e:
                logger.warning(f"Could not set read-only mode: {e}")
            finally:
                cursor.close()

            logger.info("Connected successfully")

            # Log database version
            version = self._connection.version
            logger.info(f"Oracle Database Version: {version}")

            return self._connection

        except oracledb.Error as e:
            logger.error(f"Oracle connection failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            raise

    def close(self):
        """Close the database connection if open."""
        if self._connection:
            try:
                self._connection.close()
                logger.info("Connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None

    def is_connected(self) -> bool:
        """Check if connection is active."""
        return self._connection is not None

    # ============================================================
    # QUERY METHODS
    # ============================================================

    def list_tables(self, limit: int = 10, owner: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List tables in the database.

        Args:
            limit: Maximum number of tables to return (default: 10)
            owner: Filter by owner/schema (default: user's schema)

        Returns:
            List of dicts with table information

        Safety: Uses ALL_TABLES with FETCH FIRST to avoid full scans.
        """
        with self._connection.cursor() as cursor:
            if owner:
                sql = """
                    SELECT table_name, owner, tablespace_name, status, num_rows
                    FROM all_tables
                    WHERE owner = UPPER(:owner)
                    ORDER BY table_name
                    FETCH FIRST :limit ROWS ONLY
                """
                cursor.execute(sql, {"owner": owner, "limit": limit})
            else:
                sql = """
                    SELECT table_name, owner, tablespace_name, status, num_rows
                    FROM all_tables
                    WHERE owner = USER
                    ORDER BY table_name
                    FETCH FIRST :limit ROWS ONLY
                """
                cursor.execute(sql, {"limit": limit})

            columns = [col[0] for col in cursor.description]
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))

            return results

    def describe_table(self, table_name: str, owner: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get column information for a specific table.

        Args:
            table_name: Name of the table (case-insensitive)
            owner: Table owner (default: user's schema)

        Returns:
            List of dicts with column information

        Safety: Uses ALL_TAB_COLUMNS with exact table match.
        """
        with self._connection.cursor() as cursor:
            sql = """
                SELECT
                    column_name,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    column_id,
                    data_default
                FROM all_tab_columns
                WHERE table_name = UPPER(:table_name)
            """

            params = {"table_name": table_name}

            if owner:
                sql += " AND owner = UPPER(:owner)"
                params["owner"] = owner
            else:
                sql += " AND owner = USER"

            sql += " ORDER BY column_id"

            cursor.execute(sql, params)

            columns = [col[0] for col in cursor.description]
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))

            return results

    def query(self, sql: str, params: Optional[Dict] = None, fetch_all: bool = True) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as list of dicts.

        Args:
            sql: SQL query string (should include FETCH FIRST for production safety)
            params: Optional bind parameters
            fetch_all: If True, fetch all rows; if False, return cursor for chunked fetching

        Returns:
            List of dicts (column_name: value) or cursor if fetch_all=False

        Safety Warning:
            Always include FETCH FIRST N ROWS ONLY in queries for production use.
            This method is read-only but doesn't prevent expensive queries.
        """
        if not self._connection:
            self.connect()

        with self._connection.cursor() as cursor:
            try:
                logger.info(f"Executing query: {sql[:100]}...")
                cursor.execute(sql, params or {})

                if not fetch_all:
                    return cursor

                columns = [col[0] for col in cursor.description]
                results = []
                for row in cursor:
                    results.append(dict(zip(columns, row)))

                logger.info(f"Query returned {len(results)} rows")
                return results

            except oracledb.Error as e:
                logger.error(f"Query failed: {e}")
                raise

    def fetch_rows(self, table_name: str, limit: int = 5,
                   columns: Optional[str] = "*",
                   where_clause: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch rows from a table safely (always includes FETCH FIRST).

        Args:
            table_name: Name of the table
            limit: Maximum rows to fetch (default: 5)
            columns: Columns to select (default: "*")
            where_clause: Optional WHERE clause (without "WHERE")

        Returns:
            List of dicts with row data

        Safety: Always uses FETCH FIRST to limit results.
        """
        # Build safe query with FETCH FIRST
        sql = f"SELECT {columns} FROM {table_name}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        sql += f" FETCH FIRST {limit} ROWS ONLY"

        return self.query(sql)

    # ============================================================
    # CONTEXT MANAGER
    # ============================================================

    def __enter__(self):
        """Context manager entry - auto connect."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto close."""
        self.close()
        if exc_type:
            logger.error(f"Exception in context: {exc_type}: {exc_val}")
        return False  # Don't suppress exceptions


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def print_table_info(table_info: List[Dict[str, Any]]):
    """Print table information in a readable format."""
    if not table_info:
        print("No tables found.")
        return

    print(f"\n{'TABLE NAME':<30} {'OWNER':<20} {'STATUS':<10} {'ROWS':>10}")
    print("-" * 75)

    for table in table_info:
        row_count = table.get("NUM_ROWS") or "N/A"
        print(
            f"{table['TABLE_NAME']:<30} "
            f"{table['OWNER']:<20} "
            f"{table['STATUS']:<10} "
            f"{str(row_count):>10}"
        )


def print_column_info(columns: List[Dict[str, Any]]):
    """Print column information in a readable format."""
    if not columns:
        print("No columns found.")
        return

    print(f"\n{'COLUMN':<30} {'TYPE':<15} {'LENGTH':>8} {'PRECISION':>10} {'NULLABLE':<8}")
    print("-" * 80)

    for col in columns:
        data_length = col.get("DATA_LENGTH") or ""
        data_precision = col.get("DATA_PRECISION") or ""
        if col.get("DATA_SCALE"):
            data_precision = f"{data_precision},{col['DATA_SCALE']}"

        print(
            f"{col['COLUMN_NAME']:<30} "
            f"{col['DATA_TYPE']:<15} "
            f"{str(data_length):>8} "
            f"{str(data_precision):>10} "
            f"{col['NULLABLE']:<8}"
        )


def print_rows(rows: List[Dict[str, Any]], max_width: int = 100):
    """Print query results in a readable format."""
    if not rows:
        print("No rows returned.")
        return

    # Get column names and widths
    columns = list(rows[0].keys())
    col_widths = {col: min(max(len(str(row.get(col, ""))) for row in rows), max_width) for col in columns}

    # Print header
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    print("\n" + header)
    print("-" * len(header))

    # Print rows
    for row in rows:
        row_str = " | ".join(str(row.get(col, ""))[:max_width].ljust(col_widths[col]) for col in columns)
        print(row_str)

    print(f"\nTotal: {len(rows)} rows")


# ============================================================
# MAIN - DEMONSTRATION
# ============================================================

def main():
    """Demonstration of Oracle connection utility."""

    print("=" * 60)
    print("Oracle Database Connection Utility")
    print("=" * 60)

    # Check for password
    if not os.getenv("ORACLE_DB_PASSWORD"):
        print("\nERROR: ORACLE_DB_PASSWORD environment variable not set!")
        print("\nBefore running, set your password:")
        print("  export ORACLE_DB_PASSWORD='your_password'")
        print("\nOr on Windows:")
        print("  set ORACLE_DB_PASSWORD=your_password")
        print("  $env:ORACLE_DB_PASSWORD='your_password'  (PowerShell)")
        return 1

    try:
        with OracleConnection() as conn:
            # 1. List first 10 tables
            print("\n" + "=" * 60)
            print("STEP 1: LIST FIRST 10 TABLES")
            print("=" * 60)

            tables = conn.list_tables(limit=10)
            print_table_info(tables)

            # 2. Describe a specific table (if tables exist)
            if tables:
                table_name = tables[0]["TABLE_NAME"]
                print("\n" + "=" * 60)
                print(f"STEP 2: DESCRIBE TABLE '{table_name}'")
                print("=" * 60)

                columns = conn.describe_table(table_name)
                print_column_info(columns)

                # 3. Fetch first 5 rows
                print("\n" + "=" * 60)
                print(f"STEP 3: FETCH FIRST 5 ROWS FROM '{table_name}'")
                print("=" * 60)

                rows = conn.fetch_rows(table_name, limit=5)
                print_rows(rows)

            # 4. Example: Custom query with parameters
            print("\n" + "=" * 60)
            print("STEP 4: CUSTOM QUERY EXAMPLE")
            print("=" * 60)
            print("\nExample: Query with bind parameters")
            print("  sql = \"SELECT * FROM customers WHERE status = :status FETCH FIRST 5 ROWS ONLY\"")
            print("  results = conn.query(sql, {'status': 'ACTIVE'})")

        print("\n" + "=" * 60)
        print("All operations completed successfully!")
        print("=" * 60)

        return 0

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except oracledb.Error as e:
        logger.error(f"Database error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
