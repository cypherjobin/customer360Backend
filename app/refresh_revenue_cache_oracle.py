"""
Customer 360 - Revenue Cache Refresh from Oracle CRM PROD
===========================================================
Pulls revenue data directly from Oracle CRM PROD database.

This replaces the IEROXAPP2 linked server approach with direct Oracle
connection using oracle_db_util.py.

Usage:
    python refresh_revenue_cache_oracle.py
    python refresh_revenue_cache_oracle.py --dry-run
    python refresh_revenue_cache_oracle.py --customer-id 10900099
"""

import os
import sys
import logging
import hashlib
import argparse
from datetime import datetime
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyodbc
from oracle_db_util import OracleDatabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('revenue_cache_oracle.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("revenue_cache_oracle")


class RevenueCacheOracle:
    """Revenue cache manager for Oracle CRM PROD data."""

    def __init__(self, sql_server_conn_str: str = None):
        """Initialize Oracle and SQL Server connections."""
        # SQL Server connection (Customer 360 database)
        if sql_server_conn_str:
            self.sql_conn_str = sql_server_conn_str
        else:
            self.sql_conn_str = (
                f"DRIVER={{{ODBC Driver 18 for SQL Server}}};"
                f"SERVER=DBUATL01;"
                f"DATABASE=Customer_FeedBack_JIT;"
                f"Trusted_Connection=yes;"
            )

        self.oracle_db = None
        self.sql_conn = None

    def connect_oracle(self):
        """Connect to Oracle CRM PROD database."""
        try:
            self.oracle_db = OracleDatabase()
            logger.info(f"Connected to Oracle: {self.oracle_db.dsn}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {e}")
            return False

    def connect_sql_server(self):
        """Connect to SQL Server database."""
        try:
            self.sql_conn = pyodbc.connect(self.sql_conn_str)
            logger.info(f"Connected to SQL Server: Customer_FeedBack_JIT")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            return False

    def fetch_oracle_customers(self, customer_id: str = None) -> list:
        """
        Fetch customer revenue data from Oracle CUSTOMERS table.

        Returns list of dictionaries with customer revenue information.
        """
        query = """
            SELECT
                CUSTOMER_ID,
                BILLING_ACCOUNT_NO,
                ACCOUNT_STATUS,
                CREDIT_CLASS,
                CUSTOMER_TYPE,
                -- Add revenue fields - need to verify exact column names
                -- These are placeholder field names based on pkdm_tables.docx
                MONTHLY_CHARGE,
                BALANCE_AMOUNT,
                CREDIT_LIMIT
            FROM SUPER.CUSTOMERS
        """

        # Add customer filter if specified
        params = []
        if customer_id:
            query += " WHERE CUSTOMER_ID = :1"
            params.append(customer_id)

        # Add mobile customer filter
        if not customer_id:
            query += " WHERE CUSTOMER_TYPE = 'MOBILE'"

        try:
            results = self.oracle_db.query(query, tuple(params))
            return results
        except Exception as e:
            logger.error(f"Failed to fetch from Oracle: {e}")
            # Return empty list if query fails
            return []

    def transform_oracle_data(self, oracle_row: dict) -> dict:
        """
        Transform Oracle data to match Revenue_Cache_Oracle schema.
        """
        # Calculate hash for cache invalidation
        hash_data = f"{oracle_row.get('CUSTOMER_ID')}{oracle_row.get('MONTHLY_CHARGE', '')}{datetime.now().date()}"
        cache_hash = hashlib.md5(hash_data.encode()).hexdigest()

        return {
            'customer_id': oracle_row.get('CUSTOMER_ID'),
            'customer_type': oracle_row.get('CUSTOMER_TYPE', 'Unknown'),
            'has_mobile': 1 if oracle_row.get('CUSTOMER_TYPE') == 'MOBILE' else 0,
            'has_fixed': 0,  # Will update if found
            'mobile_account': oracle_row.get('BILLING_ACCOUNT_NO'),
            'billing_account_no': oracle_row.get('BILLING_ACCOUNT_NO'),
            'account_status': oracle_row.get('ACCOUNT_STATUS'),
            'credit_class': oracle_row.get('CREDIT_CLASS'),
            'monthly_revenue_total': float(oracle_row.get('MONTHLY_CHARGE', 0) or 0),
            'monthly_revenue_mobile': float(oracle_row.get('MONTHLY_CHARGE', 0) or 0),
            'monthly_revenue_fixed': 0,
            'annual_revenue_total': float(oracle_row.get('MONTHLY_CHARGE', 0) or 0) * 12,
            'revenue_segment': self._calculate_revenue_segment(float(oracle_row.get('MONTHLY_CHARGE', 0) or 0)),
            'tenure_months': 0,  # Will calculate from account history
            'cached_at': datetime.now(),
            'cache_hash': cache_hash,
            'source_system': 'ORACLE_CRMPROD',
            'source_table': 'SUPER.CUSTOMERS'
        }

    def _calculate_revenue_segment(self, monthly_revenue: float) -> str:
        """Calculate revenue segment based on monthly revenue."""
        if monthly_revenue >= 150:
            return 'High Value'
        elif monthly_revenue >= 80:
            return 'Medium Value'
        elif monthly_revenue >= 30:
            return 'Standard'
        else:
            return 'Low Value'

    def upsert_to_sql_server(self, customer_data: dict) -> bool:
        """Upsert customer revenue data to Revenue_Cache_Oracle table."""
        try:
            cursor = self.sql_conn.cursor()

            # Check if customer exists
            cursor.execute(
                "SELECT customer_id FROM dbo.Revenue_Cache_Oracle WHERE customer_id = ?",
                customer_data['customer_id']
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing record
                update_fields = [
                    'customer_type', 'has_mobile', 'mobile_account',
                    'billing_account_no', 'account_status', 'credit_class',
                    'monthly_revenue_mobile', 'monthly_revenue_total',
                    'annual_revenue_total', 'revenue_segment', 'cached_at',
                    'cache_hash'
                ]

                set_clause = ', '.join([f"{field} = ?" for field in update_fields])
                values = [customer_data[field] for field in update_fields]
                values.append(customer_data['customer_id'])

                cursor.execute(
                    f"UPDATE dbo.Revenue_Cache_Oracle SET {set_clause} WHERE customer_id = ?",
                    values
                )
            else:
                # Insert new record
                columns = ', '.join(customer_data.keys())
                placeholders = ', '.join(['?' for _ in customer_data])
                cursor.execute(
                    f"INSERT INTO dbo.Revenue_Cache_Oracle ({columns}) VALUES ({placeholders})",
                    list(customer_data.values())
                )

            self.sql_conn.commit()
            return True

        except Exception as e:
            logger.error(f"Failed to upsert customer {customer_data.get('customer_id')}: {e}")
            self.sql_conn.rollback()
            return False

    def refresh_cache(self, customer_id: str = None, dry_run: bool = False) -> dict:
        """
        Refresh revenue cache from Oracle.

        Args:
            customer_id: Specific customer to refresh (None = all mobile customers)
            dry_run: If True, don't write to database

        Returns:
            dict with refresh statistics
        """
        stats = {
            'start_time': datetime.now(),
            'customers_fetched': 0,
            'customers_processed': 0,
            'customers_updated': 0,
            'customers_inserted': 0,
            'errors': [],
            'dry_run': dry_run
        }

        logger.info("="*70)
        logger.info("ORACLE REVENUE CACHE REFRESH")
        logger.info("="*70)
        logger.info(f"Customer ID: {customer_id or 'All mobile customers'}")
        logger.info(f"Dry Run: {dry_run}")

        # Connect to databases
        if not self.connect_oracle():
            return stats

        if not dry_run and not self.connect_sql_server():
            return stats

        # Fetch customers from Oracle
        logger.info("Fetching customers from Oracle CRM PROD...")
        oracle_customers = self.fetch_oracle_customers(customer_id)
        stats['customers_fetched'] = len(oracle_customers)

        if not oracle_customers:
            logger.warning("No customers returned from Oracle!")
            return stats

        logger.info(f"Fetched {len(oracle_customers)} customers from Oracle")

        # Process each customer
        for i, oracle_row in enumerate(oracle_customers, 1):
            try:
                # Transform data
                customer_data = self.transform_oracle_data(oracle_row)

                # Display progress every 100 customers
                if i % 100 == 0:
                    logger.info(f"Processed {i}/{len(oracle_customers)} customers...")

                if dry_run:
                    logger.debug(f"DRY RUN: Would process {customer_data['customer_id']} - Revenue: €{customer_data['monthly_revenue_total']}")
                    stats['customers_processed'] += 1
                else:
                    # Upsert to SQL Server
                    if self.upsert_to_sql_server(customer_data):
                        stats['customers_processed'] += 1
                        if customer_data['customer_id'] in [r['customer_id'] for r in self.fetch_existing_sql_customers()]:
                            stats['customers_updated'] += 1
                        else:
                            stats['customers_inserted'] += 1

            except Exception as e:
                logger.error(f"Error processing customer: {e}")
                stats['errors'].append(str(e))

        stats['end_time'] = datetime.now()
        stats['duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()

        # Print summary
        self._print_summary(stats)

        return stats

    def fetch_existing_sql_customers(self) -> list:
        """Fetch existing customer IDs from Revenue_Cache_Oracle."""
        cursor = self.sql_conn.cursor()
        cursor.execute("SELECT DISTINCT customer_id FROM dbo.Revenue_Cache_Oracle")
        return [{'customer_id': row[0]} for row in cursor.fetchall()]

    def _print_summary(self, stats: dict):
        """Print refresh summary."""
        logger.info("")
        logger.info("="*70)
        logger.info("REFRESH SUMMARY")
        logger.info("="*70)
        logger.info(f"  Customers Fetched    : {stats['customers_fetched']}")
        logger.info(f"  Customers Processed  : {stats['customers_processed']}")
        logger.info(f"  Customers Updated    : {stats['customers_updated']}")
        logger.info(f"  Customers Inserted   : {stats['customers_inserted']}")
        logger.info(f"  Errors               : {len(stats['errors'])}")
        logger.info(f"  Duration             : {stats['duration_seconds']:.1f} seconds")
        logger.info("="*70)


def run_comparison_report():
    """Generate a comparison report between Oracle and IEROXAPP2 data."""
    logger.info("Generating revenue comparison report...")

    conn_str = (
        f"DRIVER={{{ODBC Driver 18 for SQL Server}}};"
        f"SERVER=DBUATL01;"
        f"DATABASE=Customer_FeedBack_JIT;"
        f"Trusted_Connection=yes;"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get comparison statistics
    query = """
        SELECT
            match_status,
            COUNT(*) as count,
            AVG(ABS(total_diff)) as avg_diff
        FROM vw_Revenue_Comparison
        GROUP BY match_status
        ORDER BY count DESC
    """

    cursor.execute(query)
    results = cursor.fetchall()

    logger.info("")
    logger.info("REVENUE COMPARISON REPORT")
    logger.info("-"*60)
    logger.info(f"{'Match Status':<30} {'Count':>10} {'Avg Diff':>15}")
    logger.info("-"*60)

    for row in results:
        match_status, count, avg_diff = row
        avg_diff_display = f"€{avg_diff:.2f}" if avg_diff else "N/A"
        logger.info(f"{match_status:<30} {count:>10} {avg_diff_display:>15}")

    # Get discrepancies
    logger.info("")
    logger.info("TOP DISCREPANCIES (>€10 difference):")
    logger.info("-"*60)

    discrepancy_query = """
        SELECT TOP 10
            customer_id,
            oracle_total_revenue,
            ieroxapp2_total_revenue,
            total_diff,
            oracle_segment,
            ieroxapp2_segment
        FROM vw_Revenue_Comparison
        WHERE ABS(total_diff) > 10
        ORDER BY ABS(total_diff) DESC
    """

    cursor.execute(discrepancy_query)
    discrepancies = cursor.fetchall()

    if discrepancies:
        logger.info(f"{'Customer':<15} {'Oracle':>12} {'IEROXAPP2':>12} {'Diff':>12} {'O_Segment':<15} {'I_Segment':<15}")
        logger.info("-"*95)

        for row in discrepancies:
            cust_id, oracle_rev, iero_rev, diff, o_seg, i_seg = row
            logger.info(f"{cust_id:<15} €{oracle_rev:>10.2f} €{iero_rev:>10.2f} €{diff:>10.2f} {o_seg:<15} {i_seg:<15}")
    else:
        logger.info("No significant discrepancies found!")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Refresh Revenue Cache from Oracle")
    parser.add_argument('--dry-run', action='store_true', help='Test without writing to database')
    parser.add_argument('--customer-id', type=str, help='Specific customer ID to refresh')
    parser.add_argument('--compare', action='store_true', help='Run comparison report instead of refresh')

    args = parser.parse_args()

    # Run comparison if requested
    if args.compare:
        run_comparison_report()
        return

    # Run refresh
    cache = RevenueCacheOracle()
    results = cache.refresh_cache(
        customer_id=args.customer_id,
        dry_run=args.dry_run
    )

    # Print exit status
    if results['errors']:
        logger.warning(f"Completed with {len(results['errors'])} errors")
        sys.exit(1)
    else:
        logger.info("Refresh completed successfully!")
        sys.exit(0)


if __name__ == '__main__':
    main()
