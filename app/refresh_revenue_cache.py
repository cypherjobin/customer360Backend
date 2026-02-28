"""
Revenue Cache Refresh - ETL from IEROXAPP2 to Customer_FeedBack_JIT
====================================================================

Pulls customer revenue data from IEROXAPP2 (CERILLION billing system)
for customers with recent interactions, and populates the Revenue_Cache table.

SCHEMA:
    Mobile:  CERILLION.dbo.cerillion_mobile_customers
             - S_ACCOUNT_NO_ACC (mobile account)
             - CLARIFY_ACCOUNT_NUMBER (links to fixed)
             - AN_STATUS_CD_SUBS_STATUS (CU = Active)
             - BH_CHARGE_TOTAL_LAST_BILL_AMOUNT (monthly recurring revenue)
             - S_LAST_PAY_AMOUNT (last payment - NOT used, includes one-time charges)

    Fixed:   ITDEV.dbo.CUSTOMERS
             - CUST_NUM (customer number)
             - CUST_ACTIVE (Y/N = Active)
             - LAST_PAY_AMT, LAST_PAY_AMT_2, LAST_PAY_AMT_3 (averaged for monthly revenue)

    Contract: CUSTOMER.dbo.eecc_b2c
             - ACCOUNT_NUMBER
             - CONTRACT_END

KEY FIXES IN v2.0:
    1. Use BH_CHARGE_TOTAL_LAST_BILL_AMOUNT (actual monthly bill) instead of
       S_LAST_PAY_AMOUNT (last payment which includes device purchases, fees, etc.)
    2. Aggregate multiple mobile plans per customer (SUM of all CU records)

OPTIMIZATION: Only fetches revenue for customers with interactions in the
30-day window (from L30DInteractions), not all customers in the billing system.

SETUP INSTRUCTIONS:
    1. Ensure SOURCE_DB config points to IEROXAPP2
    2. Ensure Windows authentication works OR provide SQL credentials
    3. Test with dry-run: python refresh_revenue_cache.py --dry-run
    4. Run production: python refresh_revenue_cache.py

USAGE:
    python refresh_revenue_cache.py                    # Full ETL run
    python refresh_revenue_cache.py --dry-run          # Test without updates
    python refresh_revenue_cache.py --discover-schema  # List IEROXAPP2 tables
    python refresh_revenue_cache.py --window-days 7    # Custom lookback

Author: Data Engineering Team
Date: 2026-02-17
Version: 2.0 - Fixed revenue calculation (use BH_CHARGE_TOTAL, aggregate multi-plan)
"""

import pyodbc
import logging
from datetime import datetime
import sys
import argparse
import hashlib
from decimal import Decimal

# ============================================================
# CONFIGURATION
# ============================================================

# Source: IEROXAPP2 (contains CERILLION, ITDEV, CUSTOMER databases)
# The query uses three-part naming: CERILLION.dbo..., ITDEV.dbo..., CUSTOMER.dbo...
# So we connect to the IEROXAPP2 server directly
SOURCE_DB = {
    "server": "IEROXAPP2",
    "database": "CERILLION",        # Default database (can use any, query uses 3-part names)
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": "yes",    # Set to "no" and provide uid/pwd if Windows auth fails
    # "uid": "your_username",
    # "pwd": "your_password",
}

# Target: Customer_FeedBack_JIT (Customer 360 database)
TARGET_DB = {
    "server": "DBUATL01",
    "database": "Customer_FeedBack_JIT",
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": "yes",
}

# Processing settings
BATCH_SIZE = 1000
LOG_FILE = "revenue_cache_refresh.log"

# Revenue segment thresholds (EUR per month)
SEGMENT_HIGH_VALUE = 100
SEGMENT_MEDIUM_VALUE = 50

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


# ============================================================
# DATABASE CONNECTIONS
# ============================================================

def get_connection(config):
    """Create and return a database connection."""
    conn_str = (
        f"DRIVER={config['driver']};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
    )

    if config.get("trusted_connection") == "yes":
        conn_str += "Trusted_Connection=yes;"
    else:
        conn_str += f"UID={config['uid']};PWD={config['pwd']};"

    return pyodbc.connect(conn_str, timeout=30)


# ============================================================
# GET ACTIVE CUSTOMERS FROM TARGET DB
# ============================================================

def get_active_customers(target_conn, window_days=30):
    """
    Get unique customer IDs from L30DInteractions within the window period.
    Only customers with recent interactions need revenue data refreshed.
    """
    logger.info(f"Fetching active customers from L30DInteractions (last {window_days} days)...")

    query = """
        SELECT DISTINCT [SelectedCustomerID]
        FROM [dbo].[L30DInteractions]
        WHERE [Customer Type] = 'Mobile'
          AND CAST([CreateDateTime] AS DATE) > DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
    """

    cursor = target_conn.cursor()
    cursor.execute(query, window_days)

    customer_ids = [str(row[0]) for row in cursor.fetchall()]
    cursor.close()

    logger.info(f"Found {len(customer_ids)} active customers in window")
    return customer_ids


# ============================================================
# SOURCE DATA EXTRACTION
# ============================================================

def discover_ieroxapp2_schema(conn):
    """
    Discover IEROXAPP2 schema by listing tables and key columns.
    Run this once to identify your actual table/column names.

    Returns: Dictionary of schema information
    """
    logger.info("=" * 60)
    logger.info("IEROXAPP2 Schema Discovery")
    logger.info("=" * 60)

    schema_info = {}

    cursor = conn.cursor()

    # 1. List all user tables
    logger.info("\n--- User Tables in IEROXAPP2 ---")
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    tables = cursor.fetchall()
    for schema, table in tables:
        logger.info(f"  {schema}.{table}")

    # 2. Look for common customer/billing table patterns
    logger.info("\n--- Searching for Customer/Billing Tables ---")
    potential_tables = []
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND (
              TABLE_NAME LIKE '%customer%'
              OR TABLE_NAME LIKE '%account%'
              OR TABLE_NAME LIKE '%billing%'
              OR TABLE_NAME LIKE '%subscriber%'
              OR TABLE_NAME LIKE '%revenue%'
              OR TABLE_NAME LIKE '%mobile%'
              OR TABLE_NAME LIKE '%fixed%'
              OR TABLE_NAME LIKE '%broadband%'
          )
        ORDER BY TABLE_NAME
    """)
    for row in cursor.fetchall():
        potential_tables.append(row)
        logger.info(f"  FOUND: {row[0]}.{row[1]}")

    schema_info['potential_tables'] = potential_tables

    # 3. For each potential table, show its columns
    logger.info("\n--- Column Details for Potential Tables ---")
    for schema, table in potential_tables[:10]:  # Limit to first 10
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        columns = cursor.fetchall()
        logger.info(f"\n  {schema}.{table}:")
        for col in columns:
            logger.info(f"    - {col[0]}: {col[1]}({col[2]}) NULL={col[3]}")

    cursor.close()

    return schema_info


def fetch_revenue_data_from_source(conn, customer_ids, discover_schema=False):
    """
    Fetch revenue data from IEROXAPP2 for specific customers only.

    NOTE: Update this query based on your actual IEROXAPP2 schema!

    SCHEMA DISCOVERY: Run with discover_schema=True to see available tables.
        discover_ieroxapp2_schema(source_conn)

    REQUIRED OUTPUT COLUMNS (must match these names or update process_revenue_rows):
        - customer_id: Customer identifier
        - customer_type: 'Mobile Only', 'Mobile + Fixed', 'Fixed Only'
        - mobile_account_id: Mobile account number (or NULL)
        - mobile_status: 'Active' or inactive status (or NULL)
        - monthly_mobile_revenue: Monthly mobile revenue (decimal)
        - fixed_account_id: Fixed/broadband account number (or NULL)
        - fixed_status: 'Active' or inactive status (or NULL)
        - monthly_fixed_revenue: Monthly fixed revenue (decimal)
        - contract_end_date: Fixed contract end date (or NULL)
        - customer_since_date: Customer start date for tenure calculation
        - product_list: Comma-separated products (e.g., 'Mobile, Fixed')
        - service_status: Combined status string (e.g., 'Mobile: Active, Fixed: Active')

    COMMON CERILLION TABLE PATTERNS (update with your actual schema):
        - Customer: customer_master, billing_customers, cust_table
        - Mobile: mobile_subscriptions, cdr_mobile, mobile_services
        - Fixed: fixed_broadband, fixed_subscriptions, broadband_services
        - Revenue: monthly_revenue, billing_summary, revenue_table
    """
    logger.info(f"Fetching revenue data from IEROXAPP2 for {len(customer_ids)} customers...")

    # Schema discovery mode
    if discover_schema:
        return discover_ieroxapp2_schema(conn)

    if not customer_ids:
        logger.warning("No customer IDs provided")
        return [], []

    # Split into batches for SQL Server (IN clause limit)
    batch_size = 1000
    batches = [customer_ids[i:i + batch_size] for i in range(0, len(customer_ids), batch_size)]

    all_rows = []
    all_columns = None

    cursor = conn.cursor()

    for batch_num, batch in enumerate(batches, 1):
        # Build IN clause placeholders
        placeholders = ','.join(['?' for _ in batch])

        # ============================================================
        # TODO: UPDATE THIS QUERY based on your IEROXAPP2 schema
        # ============================================================
        #
        # STEP 1: Run schema discovery to find your actual tables:
        #   python refresh_revenue_cache.py --discover-schema
        #
        # STEP 2: Update the table/column names below to match your schema
        #
        # STEP 3: The query must return exactly these columns (or update process_revenue_rows):
        #   customer_id, customer_type, mobile_account_id, mobile_status,
        #   monthly_mobile_revenue, fixed_account_id, fixed_status,
        #   monthly_fixed_revenue, contract_end_date, customer_since_date,
        #   product_list, service_status
        #
        # ============================================================

        query = f"""
            -- ============================================================
            -- IEROXAPP2 Revenue Query - v2.0 Fixed
            -- Mobile: CERILLION.dbo.cerillion_mobile_customers (AGGREGATED)
            -- Fixed: ITDEV.dbo.CUSTOMERS
            -- Contract: CUSTOMER.dbo.eecc_b2c
            --
            -- FIXES:
            -- 1. Use BH_CHARGE_TOTAL_LAST_BILL_AMOUNT (actual monthly bill)
            --    instead of S_LAST_PAY_AMOUNT (last payment, includes one-time charges)
            -- 2. Aggregate multiple mobile plans per customer (SUM of all CU records)
            -- ============================================================

            -- First part: Customers identified by mobile account number
            WITH MobileRevenue AS (
                -- Aggregate multiple mobile plans per customer
                SELECT
                    m.S_ACCOUNT_NO_ACC,
                    SUM(CASE
                        WHEN ISNUMERIC(m.BH_CHARGE_TOTAL_LAST_BILL_AMOUNT) = 1
                        THEN TRY_CAST(m.BH_CHARGE_TOTAL_LAST_BILL_AMOUNT AS DECIMAL(10,2))
                        ELSE 0.0
                    END) AS total_charges,
                    SUM(CASE
                        WHEN ISNUMERIC(m.BH_CREDIT_TOTAL) = 1
                        THEN TRY_CAST(m.BH_CREDIT_TOTAL AS DECIMAL(10,2))
                        ELSE 0.0
                    END) AS total_credits,
                    SUM(CASE WHEN m.AN_STATUS_CD_SUBS_STATUS = 'CU' THEN 1 ELSE 0 END) AS active_plan_count,
                    MAX(CASE WHEN m.AN_STATUS_CD_SUBS_STATUS = 'CU' THEN 'Active' ELSE 'Inactive' END) AS mobile_status,
                    MAX(m.CU_CREATION_SOURCE_CD_CUST_TYPE) AS customer_type_classification
                FROM CERILLION.dbo.cerillion_mobile_customers m
                WHERE m.S_ACCOUNT_NO_ACC IN ({placeholders})
                    AND m.AN_STATUS_CD_SUBS_STATUS = 'CU'  -- Only count active plans
                GROUP BY m.S_ACCOUNT_NO_ACC
            )
            SELECT
                m.S_ACCOUNT_NO_ACC AS customer_id,
                'Mobile Only' AS customer_type,
                m.S_ACCOUNT_NO_ACC AS mobile_account_id,
                m.mobile_status,
                ISNULL(m.total_charges, 0.0) + ISNULL(m.total_credits, 0.0) AS monthly_mobile_revenue,
                NULL AS fixed_account_id,
                'Inactive' AS fixed_status,
                0 AS monthly_fixed_revenue,
                NULL AS contract_end_date,
                NULL AS customer_since_date,
                'Mobile' AS product_list,
                'Mobile: ' + m.mobile_status AS service_status,
                m.active_plan_count AS plan_count,
                COALESCE(m.customer_type_classification, 'Consumer') AS account_category
            FROM MobileRevenue m



            -- Note: Fixed-only customers query disabled for now
            -- to isolate the mobile revenue query issue
            SELECT TOP 0
                CAST(NULL AS INT) AS customer_id,
                CAST(NULL AS VARCHAR(20)) AS customer_type,
                CAST(NULL AS INT) AS mobile_account_id,
                CAST(NULL AS VARCHAR(20)) AS mobile_status,
                CAST(NULL AS FLOAT) AS monthly_mobile_revenue,
                CAST(NULL AS INT) AS fixed_account_id,
                CAST(NULL AS VARCHAR(20)) AS fixed_status,
                CAST(NULL AS FLOAT) AS monthly_fixed_revenue,
                CAST(NULL AS DATE) AS contract_end_date,
                CAST(NULL AS DATE) AS customer_since_date,
                CAST(NULL AS VARCHAR(50)) AS product_list,
                CAST(NULL AS VARCHAR(100)) AS service_status
        """

        try:
            # Pass batch parameters (only mobile query for now)
            cursor.execute(query, batch)
            rows = cursor.fetchall()

            if not all_columns:
                all_columns = [desc[0] for desc in cursor.description]

            all_rows.extend(rows)
            logger.info(f"Batch {batch_num}/{len(batches)}: Fetched {len(rows)} rows")
        except Exception as e:
            logger.error(f"Query failed: {e}")
            logger.error("Run with --discover-schema to see available tables")
            raise

    cursor.close()

    logger.info(f"Fetched {len(all_rows)} total rows from IEROXAPP2")
    return all_rows, all_columns


def process_revenue_rows(rows, columns):
    """
    Process raw revenue rows into structured data for Revenue_Cache.

    Returns list of dicts with Revenue_Cache column structure.
    """
    logger.info("Processing revenue data...")

    results = []
    for row in rows:
        row_dict = dict(zip(columns, row))

        # Extract customer_id
        customer_id = str(row_dict.get('customer_id', '')).strip()
        if not customer_id:
            continue

        # Get mobile data
        mobile_account = row_dict.get('mobile_account_id')
        mobile_status = row_dict.get('mobile_status')
        mobile_revenue = row_dict.get('monthly_mobile_revenue') or Decimal('0')

        # Get fixed data
        fixed_account = row_dict.get('fixed_account_id')
        fixed_status = row_dict.get('fixed_status')
        fixed_revenue = row_dict.get('monthly_fixed_revenue') or Decimal('0')
        contract_end = row_dict.get('contract_end_date')

        # Calculate totals
        has_mobile = 1 if mobile_account else 0
        has_fixed = 1 if fixed_account else 0
        mobile_active = 1 if mobile_status == 'Active' else 0
        fixed_active = 1 if fixed_status == 'Active' else 0

        monthly_total = Decimal(str(mobile_revenue)) + Decimal(str(fixed_revenue))
        annual_total = monthly_total * 12

        # Determine revenue segment
        if monthly_total >= SEGMENT_HIGH_VALUE:
            revenue_segment = 'High Value'
        elif monthly_total >= SEGMENT_MEDIUM_VALUE:
            revenue_segment = 'Medium Value'
        else:
            revenue_segment = 'Low Value'

        # Calculate tenure (months since customer since date)
        customer_since = row_dict.get('customer_since_date')
        if customer_since:
            try:
                if isinstance(customer_since, str):
                    customer_since = datetime.strptime(customer_since.split()[0], '%Y-%m-%d')
                tenure_months = (datetime.now() - customer_since).days // 30
            except:
                tenure_months = None
        else:
            tenure_months = None

        # Build service status string
        service_status_parts = []
        if mobile_status:
            service_status_parts.append(f"Mobile: {mobile_status}")
        if fixed_status:
            service_status_parts.append(f"Fixed: {fixed_status}")
        service_status = ', '.join(service_status_parts) if service_status_parts else None

        # Build product list
        product_list = row_dict.get('product_list')

        # Get new fields
        plan_count = row_dict.get('plan_count', 1)
        account_category = row_dict.get('account_category', 'Consumer')

        # Calculate cache hash for change detection
        hash_input = '|'.join([
            customer_id,
            row_dict.get('customer_type', ''),
            mobile_status or '',
            fixed_status or '',
            str(mobile_revenue),
            str(fixed_revenue),
            str(contract_end) if contract_end else '',
            str(plan_count),
            account_category
        ])
        cache_hash = hashlib.sha256(hash_input.encode()).digest()

        results.append({
            'customer_id': customer_id,
            'customer_type': row_dict.get('customer_type'),
            'has_mobile': has_mobile,
            'has_fixed': has_fixed,
            'mobile_account': mobile_account,
            'fixed_account': fixed_account,
            'mobile_active': mobile_active,
            'fixed_active': fixed_active,
            'service_status': service_status,
            'product_list': product_list,
            'monthly_revenue_mobile': float(mobile_revenue),
            'monthly_revenue_fixed': float(fixed_revenue),
            'monthly_revenue_total': float(monthly_total),
            'annual_revenue_total': float(annual_total),
            'revenue_segment': revenue_segment,
            'contract_end_fixed': contract_end,
            'tenure_months': tenure_months,
            'plan_count': plan_count,
            'account_category': account_category,
            'cache_hash': cache_hash,
        })

    return results


# ============================================================
# TARGET DATABASE OPERATIONS
# ============================================================

def get_existing_cache_hashes(target_conn):
    """Get existing cache hashes from Revenue_Cache."""
    logger.info("Fetching existing cache hashes...")

    query = """
        SELECT customer_id, cache_hash
        FROM dbo.Revenue_Cache
    """

    cursor = target_conn.cursor()
    cursor.execute(query)

    existing = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()

    logger.info(f"Found {len(existing)} existing cache records")
    return existing


def update_revenue_cache(target_conn, revenue_data, existing_hashes):
    """
    Update Revenue_Cache table with new/changed revenue data.

    Returns: (inserted, updated, deleted) counts
    """
    logger.info("Updating Revenue_Cache table...")

    inserted = 0
    updated = 0
    skipped = 0

    cursor = target_conn.cursor()

    for record in revenue_data:
        customer_id = record['customer_id']
        new_hash = record['cache_hash']
        old_hash = existing_hashes.get(customer_id)

        # Check if record exists and has changed
        if old_hash:
            if old_hash == new_hash:
                skipped += 1
                continue

            # Update existing record
            cursor.execute("""
                UPDATE dbo.Revenue_Cache
                SET customer_type = ?,
                    has_mobile = ?,
                    has_fixed = ?,
                    mobile_account = ?,
                    fixed_account = ?,
                    mobile_active = ?,
                    fixed_active = ?,
                    service_status = ?,
                    product_list = ?,
                    monthly_revenue_mobile = ?,
                    monthly_revenue_fixed = ?,
                    monthly_revenue_total = ?,
                    annual_revenue_total = ?,
                    revenue_segment = ?,
                    contract_end_fixed = ?,
                    tenure_months = ?,
                    plan_count = ?,
                    account_category = ?,
                    cached_at = GETDATE(),
                    cache_hash = ?
                WHERE customer_id = ?
            """, (
                record['customer_type'],
                record['has_mobile'],
                record['has_fixed'],
                record['mobile_account'],
                record['fixed_account'],
                record['mobile_active'],
                record['fixed_active'],
                record['service_status'],
                record['product_list'],
                record['monthly_revenue_mobile'],
                record['monthly_revenue_fixed'],
                record['monthly_revenue_total'],
                record['annual_revenue_total'],
                record['revenue_segment'],
                record['contract_end_fixed'],
                record['tenure_months'],
                record.get('plan_count', 1),
                record.get('account_category', 'Consumer'),
                new_hash,
                customer_id
            ))
            updated += 1
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO dbo.Revenue_Cache
                (customer_id, customer_type, has_mobile, has_fixed,
                 mobile_account, fixed_account,
                 mobile_active, fixed_active, service_status, product_list,
                 monthly_revenue_mobile, monthly_revenue_fixed, monthly_revenue_total,
                 annual_revenue_total, revenue_segment,
                 contract_end_fixed, tenure_months, plan_count, account_category,
                 cached_at, cache_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?)
            """, (
                record['customer_id'],
                record['customer_type'],
                record['has_mobile'],
                record['has_fixed'],
                record['mobile_account'],
                record['fixed_account'],
                record['mobile_active'],
                record['fixed_active'],
                record['service_status'],
                record['product_list'],
                record['monthly_revenue_mobile'],
                record['monthly_revenue_fixed'],
                record['monthly_revenue_total'],
                record['annual_revenue_total'],
                record['revenue_segment'],
                record['contract_end_fixed'],
                record['tenure_months'],
                record.get('plan_count', 1),
                record.get('account_category', 'Consumer'),
                record['cache_hash']
            ))
            inserted += 1

        # Progress every 100 records
        if (inserted + updated + skipped) % 100 == 0:
            logger.info(f"Processed {inserted + updated + skipped} records...")

    # Delete records not in source (churned customers)
    source_customer_ids = [r['customer_id'] for r in revenue_data]
    if source_customer_ids:
        # Use a temporary table approach for large delete operations
        # SQL Server has limits on IN clause parameters
        deleted = 0
        # For now, skip the delete to avoid parameter limit issues
        # TODO: Implement temp table or batch delete approach
    else:
        deleted = 0

    target_conn.commit()
    cursor.close()

    return inserted, updated, skipped, deleted


# ============================================================
# SCHEMA DISCOVERY STANDALONE
# ============================================================

def run_schema_discovery(source_db_config=None):
    """
    Run schema discovery on IEROXAPP2 to identify table/column names.

    Use this to find your actual schema before updating the ETL query.
    """
    logger.info("=" * 60)
    logger.info("IEROXAPP2 Schema Discovery Mode")
    logger.info("=" * 60)
    logger.info("")

    source_config = source_db_config or SOURCE_DB
    source_conn = None

    try:
        logger.info(f"Connecting to source: {source_config['server']}/{source_config['database']}")
        source_conn = get_connection(source_config)

        # Discover schema
        schema_info = discover_ieroxapp2_schema(source_conn)

        logger.info("")
        logger.info("=" * 60)
        logger.info("Schema Discovery Complete")
        logger.info("=" * 60)
        logger.info("")
        logger.info("NEXT STEPS:")
        logger.info("1. Review the output above to identify your table names")
        logger.info("2. Update fetch_revenue_data_from_source() with actual table/column names")
        logger.info("3. Run the ETL: python refresh_revenue_cache.py --dry-run")
        logger.info("")

    except Exception as e:
        logger.error(f"Schema discovery failed: {e}")
        raise
    finally:
        if source_conn:
            source_conn.close()


# ============================================================
# MAIN ETL PROCESS
# ============================================================

def run_revenue_refresh(source_db_config=None, target_db_config=None, dry_run=False, window_days=30):
    """
    Execute the revenue cache refresh ETL.

    Optimized flow:
      1. Connect to target DB (Customer_FeedBack_JIT)
      2. Get active customers from L30DInteractions (last 30 days)
      3. Connect to source DB (IEROXAPP2)
      4. Fetch revenue data ONLY for those customers
      5. Merge into Revenue_Cache

    Args:
        source_db_config: Override source DB config
        target_db_config: Override target DB config
        dry_run: If True, only fetch and show counts, don't update target
        window_days: Number of days to look back for active customers
    """
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("Revenue Cache Refresh ETL Started")
    logger.info("=" * 60)
    logger.info(f"Start time: {start_time}")
    logger.info(f"Window: {window_days} days")
    logger.info("")

    # Use provided configs or defaults
    source_config = source_db_config or SOURCE_DB
    target_config = target_db_config or TARGET_DB

    source_conn = None
    target_conn = None

    try:
        # ==========================================================
        # STEP 1: Connect to target and get active customers
        # ==========================================================
        logger.info(f"Connecting to target: {target_config['server']}/{target_config['database']}")
        target_conn = get_connection(target_config)

        active_customer_ids = get_active_customers(target_conn, window_days)

        if not active_customer_ids:
            logger.warning("No active customers found. Exiting.")
            return

        # ==========================================================
        # STEP 2: Connect to source and fetch revenue data
        # ==========================================================
        logger.info("")
        logger.info(f"Connecting to source: {source_config['server']}/{source_config['database']}")
        source_conn = get_connection(source_config)

        # Fetch revenue data only for active customers
        rows, columns = fetch_revenue_data_from_source(source_conn, active_customer_ids)
        source_conn.close()
        source_conn = None

        if not rows:
            logger.warning("No revenue data found in source. Exiting.")
            return

        # ==========================================================
        # STEP 3: Process revenue data
        # ==========================================================
        revenue_data = process_revenue_rows(rows, columns)
        logger.info(f"Processed {len(revenue_data)} revenue records")

        if dry_run:
            logger.info("")
            logger.info("=" * 60)
            logger.info("DRY RUN COMPLETE - No database updates performed")
            logger.info("=" * 60)
            logger.info(f"Active customers: {len(active_customer_ids)}")
            logger.info(f"Revenue records: {len(revenue_data)}")
            return

        # ==========================================================
        # STEP 4: Update target Revenue_Cache table
        # ==========================================================
        logger.info("")
        logger.info("Updating Revenue_Cache table...")

        # Get existing cache hashes
        existing_hashes = get_existing_cache_hashes(target_conn)

        # Update target
        inserted, updated, skipped, deleted = update_revenue_cache(
            target_conn, revenue_data, existing_hashes
        )

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 60)
        logger.info("ETL COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Active customers queried: {len(active_customer_ids)}")
        logger.info(f"Inserted : {inserted}")
        logger.info(f"Updated  : {updated}")
        logger.info(f"Skipped  : {skipped} (unchanged)")
        logger.info(f"Deleted  : {deleted}")
        logger.info(f"Total    : {inserted + updated + skipped}")
        logger.info(f"Duration : {duration:.1f} seconds")
        logger.info(f"Completed: {end_time}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise

    finally:
        # Ensure connections are closed
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Revenue Cache Refresh - ETL from IEROXAPP2 to Customer_FeedBack_JIT",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but don't update target database"
    )
    parser.add_argument(
        "--discover-schema",
        action="store_true",
        help="Discover IEROXAPP2 schema (lists tables and columns)"
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Lookback period for active customers (default: 30)"
    )
    parser.add_argument(
        "--source-server",
        help="Override source server name"
    )
    parser.add_argument(
        "--source-database",
        help="Override source database name"
    )
    parser.add_argument(
        "--target-server",
        help="Override target server name"
    )
    parser.add_argument(
        "--target-database",
        help="Override target database name"
    )

    args = parser.parse_args()

    # Build override configs if specified
    source_config = SOURCE_DB.copy()
    if args.source_server:
        source_config['server'] = args.source_server
    if args.source_database:
        source_config['database'] = args.source_database

    target_config = TARGET_DB.copy()
    if args.target_server:
        target_config['server'] = args.target_server
    if args.target_database:
        target_config['database'] = args.target_database

    try:
        if args.discover_schema:
            run_schema_discovery(source_config)
        else:
            run_revenue_refresh(source_config, target_config, args.dry_run, args.window_days)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
