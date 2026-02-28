"""
VIBO Database Module
====================
Connection management for SQL Server (DBUATL01).
Uses pyodbc with connection pooling for concurrent access.
"""

import pyodbc
import logging
from contextlib import contextmanager
from vibo_config import DB_CONN_STRING, DB_CONFIG

logger = logging.getLogger("vibo.database")

# Enable pyodbc connection pooling
pyodbc.pooling = True


def get_connection() -> pyodbc.Connection:
    """Get a database connection from the pool."""
    try:
        conn = pyodbc.connect(DB_CONN_STRING, timeout=30)
        # Use UTF-16 which handles all characters and avoids encoding errors
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-16le")
        conn.setencoding(encoding="utf-16le")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise ConnectionError(
            f"Cannot connect to {DB_CONFIG['server']}/{DB_CONFIG['database']}: {e}"
        )


@contextmanager
def db_cursor(commit=False):
    """
    Context manager yielding a database cursor.
    Automatically closes connection on exit.
    
    Usage:
        with db_cursor() as cursor:
            cursor.execute("SELECT ...")
            rows = cursor.fetchall()
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def test_connection() -> dict:
    """Test database connectivity and return server info."""
    try:
        with db_cursor() as cursor:
            cursor.execute("SELECT @@SERVERNAME AS server, DB_NAME() AS db_name, @@VERSION AS version")
            row = cursor.fetchone()
            
            # Check VIBO tables exist
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME LIKE 'VIBO_%'
                ORDER BY TABLE_NAME
            """)
            vibo_tables = [r.TABLE_NAME for r in cursor.fetchall()]
            
            # Check source tables exist
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME IN (
                    'Customer360_Events', 'CallTranscript', 
                    'Revenue_Cache', 'Customer_Device_Assets',
                    'LLM_Customer_Summary'
                )
                ORDER BY TABLE_NAME
            """)
            source_tables = [r.TABLE_NAME for r in cursor.fetchall()]
            
            return {
                "status": "connected",
                "server": row.server,
                "database": row.db_name,
                "version": row.version.split("\n")[0],
                "vibo_tables": vibo_tables,
                "source_tables": source_tables,
            }
    except Exception as e:
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    info = test_connection()
    print(f"Status: {info['status']}")
    if info["status"] == "connected":
        print(f"Server: {info['server']}")
        print(f"Database: {info['database']}")
        print(f"VIBO tables: {info['vibo_tables']}")
        print(f"Source tables: {info['source_tables']}")
    else:
        print(f"Error: {info['error']}")
