# Oracle Revenue Cache - Quick Start Guide

## Overview

This implementation allows pulling revenue data directly from **Oracle CRM PROD** database instead of going through the IEROXAPP2 linked server.

---

## Architecture Comparison

### Current Approach (IEROXAPP2)
```
Oracle CRM PROD (CRMPROD)
        ↓
   Linked Server
        ↓
IEROXAPP2 SQL Server
        ↓
cerillion_mobile_customers view
        ↓
Revenue_Cache table
```

### New Approach (Direct Oracle)
```
Oracle CRM PROD (CRMPROD)
        ↓
oracle_db_util.py (direct connection)
        ↓
Revenue_Cache_Oracle table
```

---

## Files Created

| File | Purpose |
|------|---------|
| `create_revenue_cache_oracle_table.sql` | SQL script to create new table |
| `refresh_revenue_cache_oracle.py` | Python script to pull Oracle data |
| `test_oracle_revenue.py` | Connection and data validation test |

---

## Step-by-Step Implementation

### Step 1: Create the Table (SQL Server)

```powershell
# On DBUATL01, run:
sqlcmd -S DBUATL01 -d Customer_FeedBack_JIT -i create_revenue_cache_oracle_table.sql
```

This creates:
- `Revenue_Cache_Oracle` table (new Oracle-sourced data)
- `vw_Revenue_Comparison` view (side-by-side comparison)

---

### Step 2: Test Oracle Connection

```powershell
# Test Oracle connectivity
python test_oracle_revenue.py
```

Expected output:
```
[1/4] Testing Oracle Connection...
  ✓ Connected to Oracle DSN
[2/4] Testing Customer Data Retrieval...
  ✓ Fetched 5 sample customers:
[3/4] Testing SQL Server Connection...
  ✓ Revenue_Cache_Oracle table exists
[4/4] Revenue Data Comparison...
  IEROXAPP2 Revenue_Cache:     7,508 customers
  Oracle Revenue_Cache_Oracle: 0 customers
```

---

### Step 3: Fetch Oracle Column Names

**IMPORTANT**: The column names in Oracle may differ from what we assumed. You need to verify:

```sql
-- In Oracle SQL Developer or SQL*Plus, run:
DESC SUPER.CUSTOMERS;

-- Or find revenue-related columns:
SELECT COLUMN_NAME
FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME = 'CUSTOMERS'
  AND OWNER = 'SUPER'
  AND (COLUMN_NAME LIKE '%CHARGE%'
       OR COLUMN_NAME LIKE '%REVENUE%'
       OR COLUMN_NAME LIKE '%BILL%'
       OR COLUMN_NAME LIKE '%BALANCE%'
ORDER BY COLUMN_NAME;
```

Look for columns like:
- `MONTHLY_CHARGE` or `BILL_AMOUNT`
- `ACCOUNT_BALANCE` or `CURRENT_BALANCE`
- `CREDIT_LIMIT` or `CREDIT_CLASS`

---

### Step 4: Update Column Names

Once you know the correct Oracle column names, update `refresh_revenue_cache_oracle.py`:

```python
# Line ~57, update the query:
query = """
    SELECT
        CUSTOMER_ID,
        BILLING_ACCOUNT_NO,
        ACCOUNT_STATUS,
        CREDIT_CLASS,
        ACTUAL_MONTHLY_CHARGE,    -- Use correct column name
        CURRENT_BALANCE,          -- Use correct column name
        ...
    FROM SUPER.CUSTOMERS
"""
```

And update `transform_oracle_data()`:

```python
# Line ~86, update field mappings:
return {
    'monthly_revenue_total': float(oracle_row.get('ACTUAL_MONTHLY_CHARGE', 0) or 0),
    ...
}
```

---

### Step 5: Run Dry Run Test

```powershell
# Test without writing to database
python refresh_revenue_cache_oracle.py --dry-run
```

This will:
- Connect to Oracle
- Fetch customer data
- Transform and validate
- **NOT write to SQL Server**

---

### Step 6: Full Refresh

```powershell
# Populate Oracle revenue cache
python refresh_revenue_cache_oracle.py
```

Expected output:
```
======================================================================
ORACLE REVENUE CACHE REFRESH
======================================================================
Customer ID: All mobile customers
Dry Run: False
Fetching customers from Oracle CRM PROD...
Fetched 7,508 customers from Oracle
Processed 100/7508 customers...
Processed 200/7508 customers...
...
======================================================================
REFRESH SUMMARY
======================================================================
  Customers Fetched    : 7508
  Customers Processed  : 7508
  Customers Updated    : 0
  Customers Inserted   : 7508
  Errors               : 0
  Duration             : 245.3 seconds
======================================================================
```

---

### Step 7: Compare Data

```powershell
# Generate comparison report
python refresh_revenue_cache_oracle.py --compare
```

Expected output:
```
REVENUE COMPARISON REPORT
------------------------------------------------------------
Match Status                     Count       Avg Diff
------------------------------------------------------------
Exact Match                      6234        €0.00
Close Match                       987        €2.34
Only in IEROXAPP2                 287        N/A
Only in Oracle                       0        N/A
Different                           12        €45.67

TOP DISCREPANCIES (>€10 difference):
------------------------------------------------------------
Customer        Oracle       IEROXAPP2    Diff         O_Segment      I_Segment
--------------------------------------------------------------------------------
10012525        €180.50      €31.84       €148.66      High Value     Low Value
...
```

---

## Validation Checks

### ✅ Data Quality Checks

| Check | Query | Expected Result |
|-------|-------|-----------------|
| Row count comparison | `SELECT COUNT(*) FROM vw_Revenue_Comparison WHERE match_status = 'Only in IEROXAPP2'` | Should be low (customers not in Oracle) |
| Revenue variance | `SELECT AVG(ABS(total_diff)) FROM vw_Revenue_Comparison` | Should be close to 0 |
| Null values | `SELECT COUNT(*) FROM Revenue_Cache_Oracle WHERE monthly_revenue_total IS NULL` | Should be 0 |

---

## Next Steps After Validation

### Option A: Switch to Oracle (Recommended)

If Oracle data is accurate:

```python
# Update llm_summariser_v4.py to use Oracle table
# Change query from:
FROM Revenue_Cache

# To:
FROM Revenue_Cache_Oracle
```

### Option B: Run Both Systems

Keep both tables and use comparison view for validation.

### Option C: Merge Data

Create a combined view that uses Oracle where available, falls back to IEROXAPP2.

---

## Troubleshooting

### Issue: "ORA-00904: invalid identifier"

**Cause**: Column name doesn't exist in Oracle CUSTOMERS table

**Solution**:
1. Run `DESC SUPER.CUSTOMERS` in Oracle SQL Developer
2. Find correct column names
3. Update `refresh_revenue_cache_oracle.py` query

### Issue: "No customers returned from Oracle"

**Cause**: Filter too restrictive or no mobile customers

**Solution**:
1. Test query in Oracle SQL Developer first
2. Remove `WHERE` clauses to see all customers
3. Add filters back incrementally

### Issue: "Revenue amounts differ significantly"

**Cause**: Different calculation methods or different source tables

**Solution**:
1. Compare sample customers manually
2. Check if Oracle has more recent data
3. Verify revenue calculation logic

---

## Scheduled Task Setup

Once validated, add to daily pipeline:

```python
# In run_daily_pipeline.py, add after refresh_revenue_cache.py:

# Step 3b: Refresh Oracle Revenue Cache (optional)
subprocess.run([
    sys.executable,
    "refresh_revenue_cache_oracle.py"
])
```

---

## Performance Considerations

| Factor | IEROXAPP2 | Oracle Direct |
|--------|-----------|---------------|
| **Network hops** | 2 (Oracle→SQL→SQL) | 1 (Oracle→SQL) |
| **Complexity** | Linked server overhead | Direct Python query |
| **Refresh time** | ~4 minutes (7,508 customers) | ~4 minutes (estimated) |
| **Reliability** | Linked server dependency | Direct connection |

---

## Contact

For questions or issues:
- Check `revenue_cache_oracle.log` for detailed error messages
- Run `test_oracle_revenue.py` to validate connections
- Review Oracle column names in SQL Developer
