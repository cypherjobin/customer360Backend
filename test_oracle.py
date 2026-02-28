import os
import sys

# Set Oracle credentials
os.environ["ORACLE_DB_PASSWORD"] = "mNf39cBTAPEb42wa"
os.environ["ORACLE_DB_USERNAME"] = "tester"

sys.path.insert(0, "E:\\Customer360\\app")
from oracle_db_util import OracleConnection

try:
    print("Testing Oracle connection...")
    print(f"Host: ora01-primary-vip.prod.vmie.local")
    print(f"Port: 1556")
    print(f"Service: crmprod")
    print(f"Username: tester")
    print()

    conn = OracleConnection()
    conn.connect()
    print("SUCCESS: Connected to Oracle!")
    print(f"Database Version: {conn._connection.version}")

    # List some tables
    tables = conn.list_tables(limit=5)
    print(f"\nFound {len(tables)} tables:")
    for table in tables:
        print(f"  - {table['TABLE_NAME']} (Owner: {table['OWNER']})")

    conn.close()
    print("\nOracle connection test PASSED!")

except Exception as e:
    print(f"FAILED: {e}")
    sys.exit(1)
