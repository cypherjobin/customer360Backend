# Revenue Cache Logic Documentation

## Overview
The `Revenue_Cache` table in `[Customer_FeedBack_JIT].[dbo].[Revenue_Cache]` is populated by an ETL process that pulls customer revenue data from the billing system (IEROXAPP2) for customers with recent interactions.

---

## Monthly Revenue Calculation Logic

### Mobile Revenue
**Source Table:** `CERILLION.dbo.cerillion_mobile_customers`

| Column | Purpose | Logic |
|--------|---------|-------|
| `BH_CHARGE_TOTAL_LAST_BILL_AMOUNT` | Monthly recurring charges | Used for monthly revenue (actual bill amount) |
| `BH_CREDIT_TOTAL` | Monthly credits | Subtracted from charges |
| `S_LAST_PAY_AMOUNT` | **NOT USED** | Last payment includes one-time charges (device purchases, fees) |

**Formula:**
```
monthly_mobile_revenue = SUM(BH_CHARGE_TOTAL_LAST_BILL_AMOUNT) - SUM(BH_CREDIT_TOTAL)
```

**Key Points:**
- Only includes `AN_STATUS_CD_SUBS_STATUS = 'CU'` (Active subscriptions)
- **Aggregates multiple plans per customer** - if a customer has 2-3 mobile plans, all are summed
- Each plan's revenue is calculated separately, then totalled
- Uses the last bill amount (not last payment) to avoid one-time charges

**Why NOT use `S_LAST_PAY_AMOUNT`:**
- Last payment includes device purchases, activation fees, late fees, etc.
- We want **recurring monthly revenue (MRR)**, not one-time payments
- Example: If customer bought a €500 phone, last payment would be €550, but monthly revenue is only €50

---

### Fixed/Broadband Revenue
**Source Table:** `ITDEV.dbo.CUSTOMERS`

| Column | Purpose | Logic |
|--------|---------|-------|
| `LAST_PAY_AMT`, `LAST_PAY_AMT_2`, `LAST_PAY_AMT_3` | Recent payments | Averaged for monthly revenue |

**Formula:**
```
monthly_fixed_revenue = (LAST_PAY_AMT + LAST_PAY_AMT_2 + LAST_PAY_AMT_3) / 3
```

**Note:** The Fixed revenue query is currently disabled in the ETL to focus on Mobile customers first.

---

## Total Revenue Calculation

```python
monthly_total = monthly_mobile_revenue + monthly_fixed_revenue
annual_total = monthly_total * 12
```

---

## Revenue Segmentation

| Monthly Revenue | Segment |
|-----------------|---------|
| >= €100 | High Value |
| >= €50 | Medium Value |
| < €50 | Low Value |

---

## Customer Type Determination

| Has Mobile | Has Fixed | Customer Type |
|------------|-----------|---------------|
| Yes | No | Mobile Only |
| Yes | Yes | Mobile + Fixed |
| No | Yes | Fixed Only |

---

## Other Key Fields

| Field | Logic |
|-------|-------|
| `mobile_active` | 1 if mobile status = 'Active', else 0 |
| `fixed_active` | 1 if fixed status = 'Active', else 0 |
| `plan_count` | Number of active mobile plans for the customer |
| `account_category` | From `CU_CREATION_SOURCE_CD_CUST_TYPE` (default: 'Consumer') |
| `tenure_months` | Months since `customer_since_date` |
| `contract_end_fixed` | From `CUSTOMER.dbo.eecc_b2c.CONTRACT_END` |

---

## ETL Process Flow

```
1. Get active customers from L30DInteractions (last 30 days)
   ↓
2. Query IEROXAPP2 for those customers only (performance optimization)
   ↓
3. Calculate revenue using the formulas above
   ↓
4. Update Revenue_Cache table (INSERT new, UPDATE changed, SKIP unchanged)
```

---

## Important Notes

1. **Multi-plan customers**: If a customer has 3 mobile plans, all 3 are summed for total mobile revenue
2. **Only active plans**: `AN_STATUS_CD_SUBS_STATUS = 'CU'` (Current/Active)
3. **Use bill amount, not payment**: `BH_CHARGE_TOTAL_LAST_BILL_AMOUNT` gives true recurring revenue
4. **Batch processing**: Processes customers in batches of 1000 for SQL Server performance
5. **Change detection**: Uses SHA-256 hash to only update changed records

---

## Example

**Customer with 2 mobile plans:**
- Plan 1: Last bill = €30, Credits = €0
- Plan 2: Last bill = €20, Credits = €5

```
monthly_mobile_revenue = (€30 + €0) + (€20 - €5) = €45
revenue_segment = Low Value (< €50)
plan_count = 2
```

---

## Code Reference

**File:** `refresh_revenue_cache.py`
**Key Function:** `fetch_revenue_data_from_source()` (lines 229-388)

**SQL Query (simplified):**
```sql
WITH MobileRevenue AS (
    SELECT
        S_ACCOUNT_NO_ACC,
        SUM(BH_CHARGE_TOTAL_LAST_BILL_AMOUNT) AS total_charges,
        SUM(BH_CREDIT_TOTAL) AS total_credits
    FROM CERILLION.dbo.cerillion_mobile_customers
    WHERE AN_STATUS_CD_SUBS_STATUS = 'CU'  -- Active only
    GROUP BY S_ACCOUNT_NO_ACC
)
SELECT
    S_ACCOUNT_NO_ACC AS customer_id,
    (total_charges - total_credits) AS monthly_mobile_revenue
FROM MobileRevenue
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.0 | 2026-02-17 | **FIXED:** Use `BH_CHARGE_TOTAL` instead of `S_LAST_PAY_AMOUNT`, aggregate multi-plan customers |
| 1.0 | Earlier | Initial version (had issues with one-time charges being included) |

---

## Contact

For questions about this logic, contact the Data Engineering Team.
