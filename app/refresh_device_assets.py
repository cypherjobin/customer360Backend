"""
Device Assets Refresh - ETL from IEROXAPP2 to Customer_FeedBack_JIT
====================================================================

Pulls device contract data from IEROXAPP2 (CERILLION MVNO devices)
for customers with recent interactions, and populates the Customer_Device_Assets table.

SCHEMA:
    Source: CERILLION.dbo.cerillion_mvno_devices
    Target: Customer_FeedBack_JIT.dbo.Customer_Device_Assets

KEY FIELDS:
    - S_ACCOUNT_NO_ROOT_CUSTOMER_ID: Links to customer_id
    - TH2_MAKE_BRAND: Device brand (Samsung, Apple, etc.)
    - TH2_MODEL_MODEL: Device model
    - TSV_TARIFF_CHARGE_DEVICE_TOTAL_PRICE: Total device price
    - AN_STATUS_CD: Device status (CU = Active)
    - C_STATUS_CD_CONTRACT_STATUS: Contract status

OPTIMIZATION: Only fetches device data for customers with interactions in the
30-day window (from L30DInteractions), not all devices in the billing system.

USAGE:
    python refresh_device_assets.py                    # Full ETL run
    python refresh_device_assets.py --dry-run          # Test without updates
    python refresh_device_assets.py --window-days 7    # Custom lookback

Author: Data Engineering Team
Date: 2026-02-17
Version: 1.0
"""

import pyodbc
import logging
from datetime import datetime
import sys
import argparse

# ============================================================
# CONFIGURATION
# ============================================================

# Source: IEROXAPP2 (contains CERILLION database)
SOURCE_DB = {
    "server": "IEROXAPP2",
    "database": "CERILLION",
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": "yes",
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
LOG_FILE = "device_assets_refresh.log"

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
    Only customers with recent interactions need device data refreshed.
    """
    logger.info(f"Fetching active customers from L30DInteractions (last {window_days} days)...")

    query = """
        SELECT DISTINCT CAST([SelectedCustomerID] AS VARCHAR(20))
        FROM [dbo].[L30DInteractions]
        WHERE CAST([CreateDateTime] AS DATE) > DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
          AND [SelectedCustomerID] IS NOT NULL
    """

    cursor = target_conn.cursor()
    cursor.execute(query, window_days)

    # Return as integers for numeric comparison in source query
    customer_ids = []
    for row in cursor.fetchall():
        try:
            customer_ids.append(int(row[0]))
        except (ValueError, TypeError):
            # Skip invalid customer IDs
            continue

    cursor.close()

    logger.info(f"Found {len(customer_ids)} active customers in window")
    return customer_ids


# ============================================================
# SOURCE DATA EXTRACTION
# ============================================================

def fetch_device_data_from_source(conn, customer_ids):
    """
    Fetch device data from IEROXAPP2 for specific customers only.

    Returns list of device records with all relevant fields.
    """
    logger.info(f"Fetching device data from IEROXAPP2 for {len(customer_ids)} customers...")

    if not customer_ids:
        logger.warning("No customer IDs provided")
        return []

    # Split into batches for SQL Server (IN clause limit)
    batch_size = 1000
    batches = [customer_ids[i:i + batch_size] for i in range(0, len(customer_ids), batch_size)]

    all_devices = []
    cursor = conn.cursor()

    for batch_num, batch in enumerate(batches, 1):
        placeholders = ','.join(['?' for _ in batch])

        query = f"""
            SELECT
                STR(d.S_ACCOUNT_NO_ROOT_CUSTOMER_ID) AS customer_id,
                d.TH2_MAKE_BRAND AS device_brand,
                d.TH2_MODEL_MODEL AS device_model,
                d.TH2_COLOUR_COLOR AS device_colour,
                d.TH2_MEMORYSIZE_MEMORY AS device_memory,
                d.TSV_TARIFF_CHARGE_DEVICE_TOTAL_PRICE AS device_value,
                d.AMOUNT_BEFORE_TAX_DEVICE_TOTAL_PRICE_LESS_VAT AS device_value_ex_vat,
                d.DOWN_PAYMENT AS down_payment,
                d.MIC_AMOUNT AS mic_monthly,
                d.TSV_TARIFF_INSTALMENTS_NUMBER_OF_INSTALLMENTS AS installment_count,
                d.TSV_TARIFF_PERIOD AS installment_period,
                d.C_START_DATE_CONTRACT_START_DATE AS contract_start_date,
                d.C_EXPIRY_DATE_CONTRACT_END_DATE AS contract_end_date,
                d.E1_INITIAL_INSTALL_DT_WARRANTY_START_DATE AS warranty_start_date,
                d.D_WARRANTY_EXP_DATE_WARRANTY_END_DATE AS warranty_end_date,
                d.C_STATUS_CD_CONTRACT_STATUS AS contract_status,
                d.AN_STATUS_CD AS device_status,
                d.PACKAGE_CODE AS package_code,
                d.PACKAGE_NAME AS package_name,
                d.ISI_SERIAL_NO_IMEI AS imei,
                d.ONT_Serial_Number AS ont_serial_number,
                d.SALES_CHANNEL AS sales_channel,
                CASE WHEN d.C_EXPIRY_DATE_CONTRACT_END_DATE >= CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END AS is_contract_active
            FROM CERILLION.dbo.cerillion_mvno_devices d
            WHERE d.S_ACCOUNT_NO_ROOT_CUSTOMER_ID IN ({placeholders})
                AND d.AN_STATUS_CD = 'CU'
            ORDER BY d.S_ACCOUNT_NO_ROOT_CUSTOMER_ID, d.C_START_DATE_CONTRACT_START_DATE
        """

        try:
            cursor.execute(query, batch)
            rows = cursor.fetchall()

            # Get column names
            if batch_num == 1:
                columns = [desc[0] for desc in cursor.description]

            # Convert to list of dicts
            for row in rows:
                device_dict = dict(zip(columns, row))
                all_devices.append(device_dict)

            logger.info(f"Batch {batch_num}/{len(batches)}: Fetched {len(rows)} device records")
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise

    cursor.close()
    logger.info(f"Fetched {len(all_devices)} total device records from IEROXAPP2")
    return all_devices


# ============================================================
# TARGET DATABASE OPERATIONS
# ============================================================

def update_device_assets(target_conn, device_data):
    """
    Update Customer_Device_Assets table with new/changed device data.

    Strategy: Delete existing devices for these customers, then insert fresh data.
    This is simpler than upsert logic and devices don't change that frequently.

    Also updates Revenue_Cache summary fields (device_count, device_total_value).

    Returns: (inserted, updated, customers_processed) counts
    """
    logger.info("Updating Customer_Device_Assets table...")

    if not device_data:
        return 0, 0, 0

    cursor = target_conn.cursor()

    # Group devices by customer (convert to string for consistency)
    from collections import defaultdict
    devices_by_customer = defaultdict(list)
    for device in device_data:
        customer_id = str(device['customer_id']).strip()
        device['customer_id'] = customer_id
        devices_by_customer[customer_id].append(device)

    inserted = 0
    customers_processed = 0

    for customer_id, devices in devices_by_customer.items():
        try:
            # Delete existing devices for this customer
            cursor.execute(
                "DELETE FROM dbo.Customer_Device_Assets WHERE customer_id = ?",
                customer_id
            )

            # Insert fresh device records
            for device in devices:
                cursor.execute("""
                    INSERT INTO dbo.Customer_Device_Assets
                    (customer_id, device_brand, device_model, device_colour, device_memory,
                     device_value, device_value_ex_vat, down_payment, mic_monthly, installment_count,
                     contract_start_date, contract_end_date, warranty_start_date, warranty_end_date,
                     contract_status, device_status, package_code, package_name,
                     imei, ont_serial_number, is_contract_active, synced_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())
                """, (
                    device['customer_id'],
                    device['device_brand'],
                    device['device_model'],
                    device['device_colour'],
                    device['device_memory'],
                    float(device['device_value']) if device['device_value'] else None,
                    float(device['device_value_ex_vat']) if device['device_value_ex_vat'] else None,
                    float(device['down_payment']) if device['down_payment'] else None,
                    float(device['mic_monthly']) if device.get('mic_monthly') else None,
                    int(device['installment_count']) if device['installment_count'] else None,
                    device['contract_start_date'],
                    device['contract_end_date'],
                    device['warranty_start_date'],
                    device['warranty_end_date'],
                    device['contract_status'],
                    device['device_status'],
                    device['package_code'],
                    device['package_name'],
                    device['imei'],
                    device['ont_serial_number'],
                    device.get('is_contract_active')
                ))
                inserted += 1

            # Update Revenue_Cache summary
            total_value = sum(
                float(d['device_value']) if d['device_value'] else 0
                for d in devices
            )

            # MIC revenue = sum of MIC for active contracts only
            total_mic_revenue = sum(
                float(d['mic_monthly']) if (d.get('mic_monthly') and d.get('is_contract_active')) else 0
                for d in devices
            )

            cursor.execute("""
                UPDATE dbo.Revenue_Cache
                SET device_count = ?,
                    device_total_value = ?,
                    device_financing_revenue = ?,
                    cached_at = GETDATE()
                WHERE customer_id = ?
            """, len(devices), total_value, total_mic_revenue, customer_id)

            customers_processed += 1

            # Progress every 100 customers
            if customers_processed % 100 == 0:
                logger.info(f"Processed {customers_processed} customers...")

        except Exception as e:
            logger.error(f"Failed to process customer {customer_id}: {e}")
            continue

    target_conn.commit()
    cursor.close()

    return inserted, customers_processed, len(devices_by_customer)


# ============================================================
# MAIN ETL PROCESS
# ============================================================

def run_device_assets_refresh(dry_run=False, window_days=30):
    """
    Execute the device assets refresh ETL.

    Flow:
      1. Connect to target DB (Customer_FeedBack_JIT)
      2. Get active customers from L30DInteractions (last 30 days)
      3. Connect to source DB (IEROXAPP2)
      4. Fetch device data ONLY for those customers
      5. Load into Customer_Device_Assets + update Revenue_Cache summary

    Args:
        dry_run: If True, only fetch and show counts, don't update target
        window_days: Number of days to look back for active customers
    """
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("Device Assets Refresh ETL Started")
    logger.info("=" * 60)
    logger.info(f"Start time: {start_time}")
    logger.info(f"Window: {window_days} days")
    logger.info("")

    source_conn = None
    target_conn = None

    try:
        # ==========================================================
        # STEP 1: Connect to target and get active customers
        # ==========================================================
        logger.info(f"Connecting to target: {TARGET_DB['server']}/{TARGET_DB['database']}")
        target_conn = get_connection(TARGET_DB)

        active_customer_ids = get_active_customers(target_conn, window_days)

        if not active_customer_ids:
            logger.warning("No active customers found. Exiting.")
            return

        # ==========================================================
        # STEP 2: Connect to source and fetch device data
        # ==========================================================
        logger.info("")
        logger.info(f"Connecting to source: {SOURCE_DB['server']}/{SOURCE_DB['database']}")
        source_conn = get_connection(SOURCE_DB)

        device_data = fetch_device_data_from_source(source_conn, active_customer_ids)
        source_conn.close()
        source_conn = None

        if not device_data:
            logger.warning("No device data found in source. Exiting.")
            return

        # ==========================================================
        # STEP 3: Show summary
        # ==========================================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("DEVICE DATA SUMMARY")
        logger.info("=" * 60)

        # Count by brand
        from collections import Counter
        brands = [d['device_brand'] for d in device_data if d['device_brand']]
        brand_counts = Counter(brands)

        logger.info(f"Total device records: {len(device_data)}")
        logger.info(f"Unique customers: {len(set(d['customer_id'] for d in device_data))}")
        logger.info("")
        logger.info("Top device brands:")
        for brand, count in brand_counts.most_common(10):
            logger.info(f"  {brand}: {count}")

        if dry_run:
            logger.info("")
            logger.info("=" * 60)
            logger.info("DRY RUN COMPLETE - No database updates performed")
            logger.info("=" * 60)
            return

        # ==========================================================
        # STEP 4: Update target tables
        # ==========================================================
        logger.info("")
        logger.info("Updating target tables...")

        inserted, customers_processed, total_customers = update_device_assets(
            target_conn, device_data
        )

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 60)
        logger.info("ETL COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Customers processed: {customers_processed}/{total_customers}")
        logger.info(f"Device records inserted: {inserted}")
        logger.info(f"Duration: {duration:.1f} seconds")
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
        description="Device Assets Refresh - ETL from IEROXAPP2 to Customer_FeedBack_JIT",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but don't update target database"
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Lookback period for active customers (default: 30)"
    )

    args = parser.parse_args()

    try:
        run_device_assets_refresh(args.dry_run, args.window_days)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
