"""
Customer 360 - Test Oracle Revenue Cache
========================================
Quick test script to validate Oracle connection and data retrieval.

Usage:
    python test_oracle_revenue.py
"""

import os
import sys
import io

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from oracle_db_util import OracleConnection
import pyodbc

# Set Oracle password from environment
# Make sure ORACLE_DB_PASSWORD is set before running
if not os.getenv("ORACLE_DB_PASSWORD"):
    print("WARNING: ORACLE_DB_PASSWORD not set!")
    print("Please set it first:")
    print("  set ORACLE_DB_PASSWORD=mNf39cBTAPEb42wa")
    sys.exit(1)

print("="*70)
print("  ORACLE REVENUE CACHE - CONNECTION TEST")
print("="*70)

# Test 1: Oracle Connection
print("\n[1/4] Testing Oracle Connection...")
try:
    oracle = OracleConnection()
    print("  ✓ Connected to Oracle DSN")

    # Test query - use context manager
    with oracle as conn:
        result = conn.query("SELECT COUNT(*) as count FROM SUPER.CUSTOMERS WHERE ROWNUM <= 1")
        print(f"  ✓ Query successful: {result}")
except Exception as e:
    print(f"  ✗ Oracle connection failed: {e}")
    sys.exit(1)

# Test 2: Fetch Sample Customer Data
print("\n[2/4] Testing Customer Data Retrieval...")
try:
    oracle = OracleConnection()
    with oracle as conn:
        query = """
            SELECT
                CUSTOMER_ID,
                BILLING_ACCOUNT_NO,
                ACCOUNT_STATUS,
                CREDIT_CLASS
            FROM SUPER.CUSTOMERS
            WHERE CUSTOMER_TYPE = 'MOBILE'
            AND ROWNUM <= 5
        """
        results = conn.query(query)
    print(f"  ✓ Fetched {len(results)} sample customers:")

    for row in results[:3]:
        print(f"    - {row.get('CUSTOMER_ID')}: {row.get('BILLING_ACCOUNT_NO')} - {row.get('ACCOUNT_STATUS')}")
except Exception as e:
    print(f"  ✗ Query failed: {e}")
    print(f"     This might mean column names are different.")
    print(f"     Please verify column names in Oracle CUSTOMERS table")

# Test 3: SQL Server Connection
print("\n[3/4] Testing SQL Server Connection...")
try:
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=DBUATL01;"
        "DATABASE=Customer_FeedBack_JIT;"
        "Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Check if Revenue_Cache_Oracle table exists
    cursor.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Revenue_Cache_Oracle'"
    )
    count = cursor.fetchone()[0]

    if count > 0:
        print(f"  ✓ Revenue_Cache_Oracle table exists")
    else:
        print(f"  ! Revenue_Cache_Oracle table NOT found")
        print(f"    Run: sqlcmd -S DBUATL01 -d Customer_FeedBack_JIT -i create_revenue_cache_oracle_table.sql")

    conn.close()
except Exception as e:
    print(f"  ✗ SQL Server connection failed: {e}")

# Test 4: Compare Revenue Tables
print("\n[4/4] Revenue Data Comparison...")
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Count records in each table
    cursor.execute("SELECT COUNT(*) FROM Revenue_Cache")
    iero_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM Revenue_Cache_Oracle")
    oracle_count = cursor.fetchone()[0]

    print(f"  IEROXAPP2 Revenue_Cache:     {iero_count:,} customers")
    print(f"  Oracle Revenue_Cache_Oracle: {oracle_count:,} customers")

    if oracle_count > 0:
        print(f"\n  ✓ Oracle data available for comparison!")
    else:
        print(f"  ! Run: python refresh_revenue_cache_oracle.py")

    conn.close()
except Exception as e:
    print(f"  ✗ Comparison failed: {e}")

print("\n" + "="*70)
print("  TEST COMPLETE")
print("="*70)
print("\nNext Steps:")
print("  1. Verify Oracle CUSTOMERS table columns match expected names")
print("  2. Run: python refresh_revenue_cache_oracle.py --dry-run")
print("  3. If dry-run looks good: python refresh_revenue_cache_oracle.py")
print("  4. Compare: python refresh_revenue_cache_oracle.py --compare")
print("="*70)
