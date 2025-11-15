# Deal-Company Association Workflow

## Overview

This workflow creates programmatic associations between HubSpot deals and their primary companies using both API and SQL approaches.

**Filter Criteria:** Deals with `icalps_deal_id` starting with '4'
**Association Type:** 6 (Deal with primary Company)
**Target Table:** `hubspot.association_company_deal`

## Workflow Strategy

```
┌─────────────────────────────────────────────────────────────┐
│                    WORKFLOW PROCESS                         │
└─────────────────────────────────────────────────────────────┘

1. EXPLORATION (SQL)
   ↓ Identify target deals & count records

2. SINGLE DEAL TEST (API)
   ↓ Test with one deal via HubSpot API

3. VALIDATION (SQL)
   ↓ Verify API association created

4. BATCH PROCESSING (SQL)
   ↓ Create associations for all filtered deals

5. FINAL VALIDATION (SQL)
   ↓ Verify all associations created correctly
```

## Prerequisites

- [x] PostgreSQL database access to `hubspot` schema
- [x] HubSpot Private App Token with associations scope
- [x] `postgres_connection_manager.py` configured
- [x] Python 3 with `psycopg2-binary` installed

## Files in This Workflow

| File | Purpose | Type |
|------|---------|------|
| `deal_company_association_workflow.sql` | SQL queries for exploration and batch processing | SQL |
| `test_deal_association_api.sh` | API test script for single deal association | Bash |
| `DEAL_COMPANY_ASSOCIATION_WORKFLOW.md` | This documentation | Markdown |

---

## Phase 1: Exploration (SQL - Read Only)

### Objective
Identify target deals and understand current state.

### Execute Queries

Open `deal_company_association_workflow.sql` in your SQL client.

**Query 1.1: Find all deals starting with '4'**
```sql
SELECT
    hs_object_id as deal_hubspot_id,
    icalps_deal_id as deal_legacy_id,
    dealname,
    associatedcompanyid as current_company_id
FROM hubspot.deals
WHERE icalps_deal_id::TEXT LIKE '4%'
ORDER BY icalps_deal_id
LIMIT 50;
```

**Expected Output:**
- List of deals with legacy ID starting with '4'
- Their HubSpot IDs and associated company IDs

**Query 1.2: Count target deals**
```sql
SELECT
    COUNT(*) as total_target_deals,
    COUNT(associatedcompanyid) as deals_with_company,
    COUNT(*) - COUNT(associatedcompanyid) as deals_without_company
FROM hubspot.deals
WHERE icalps_deal_id::TEXT LIKE '4%';
```

**Expected Output:**
```
total_target_deals | deals_with_company | deals_without_company
-------------------|--------------------|-----------------------
           150     |         148        |           2
```

**Query 1.3: Get test deal parameters**

Run the DO block (lines 104-136 in SQL file):

```sql
DO $$
DECLARE
    v_test_deal_id BIGINT;
    v_test_company_id BIGINT;
    v_test_deal_name TEXT;
BEGIN
    SELECT
        hs_object_id,
        associatedcompanyid,
        dealname
    INTO
        v_test_deal_id,
        v_test_company_id,
        v_test_deal_name
    FROM hubspot.deals
    WHERE icalps_deal_id::TEXT LIKE '4%'
      AND associatedcompanyid IS NOT NULL
    LIMIT 1;

    RAISE NOTICE 'Deal HubSpot ID:    %', v_test_deal_id;
    RAISE NOTICE 'Company HubSpot ID: %', v_test_company_id;
    RAISE NOTICE 'Deal Name:          %', v_test_deal_name;
END $$;
```

**Save the output values - you'll need them for API testing!**

Example output:
```
NOTICE:  Deal HubSpot ID:    12345678901
NOTICE:  Company HubSpot ID: 98765432109
NOTICE:  Deal Name:          Acme Corp - Q1 Deal
```

---

## Phase 2: Single Deal API Test

### Objective
Test HubSpot API with one deal before batch processing.

### Setup

1. **Set HubSpot Token**
   ```bash
   export HUBSPOT_TOKEN="your_private_app_token_here"
   ```

2. **Verify token is set**
   ```bash
   echo $HUBSPOT_TOKEN
   ```

### Option A: Auto-Query Database

Let the script query the database for a test deal:

```bash
./test_deal_association_api.sh
```

**Script will:**
- Query database for first eligible deal (icalps_deal_id starting with '4')
- Extract deal ID and company ID
- Create association via HubSpot API
- Verify response

**Expected Output:**
```
============================================================================
HubSpot Deal-Company Association API Test
============================================================================

[SUCCESS] HubSpot token found
[INFO] Querying database for test deal...
[SUCCESS] Retrieved test deal from database:
[INFO]   Deal ID (HubSpot): 12345678901
[INFO]   Deal ID (Legacy): 4001234
[INFO]   Deal Name: Acme Corp - Q1 Deal
[INFO]   Company ID: 98765432109

----------------------------------------------------------------------------
Creating Association
----------------------------------------------------------------------------

[INFO] Creating association:
[INFO]   Deal ID: 12345678901
[INFO]   Company ID: 98765432109
[INFO]   Association Type: 6 (Deal with primary Company)
[INFO] HTTP Status Code: 200
[SUCCESS] Association created successfully!

[INFO] Response:
{
  "fromObjectTypeId": "0-3",
  "fromObjectId": 12345678901,
  "toObjectTypeId": "0-2",
  "toObjectId": 98765432109,
  "labels": []
}

----------------------------------------------------------------------------
Verifying Association
----------------------------------------------------------------------------

✓ Association found in database:
  Company ID: 98765432109
  Deal ID: 12345678901
  Association Type: 6
  Created At: 2025-11-15 10:30:45

============================================================================
[SUCCESS] API TEST COMPLETE
============================================================================
```

### Option B: Specify Deal IDs

If you have specific deal and company IDs to test:

```bash
./test_deal_association_api.sh 12345678901 98765432109
```

### Possible Responses

| HTTP Code | Status | Meaning | Action |
|-----------|--------|---------|--------|
| 200 | Success | Association created | Proceed to batch |
| 201 | Created | Association created | Proceed to batch |
| 409 | Conflict | Association already exists | OK, proceed |
| 401 | Unauthorized | Invalid token | Check HUBSPOT_TOKEN |
| 403 | Forbidden | Missing scope | Update app permissions |
| 404 | Not Found | Deal/Company doesn't exist | Verify IDs |

### Troubleshooting API Test

**Error: "HUBSPOT_TOKEN environment variable not set"**
```bash
export HUBSPOT_TOKEN="your_token"
```

**Error: "No deals found with icalps_deal_id starting with '4'"**
- Verify filter criteria in database
- Run Query 1.1 manually to check deals exist

**Error: 401 Unauthorized**
- Verify token is correct
- Check token hasn't expired
- Ensure token has `crm.objects.deals.write` and `crm.schemas.deals.write` scopes

**Error: Association not found in database**
- This is OK - API association may not sync to database immediately
- Verify in HubSpot UI instead
- Proceed with batch if API returned 200/201

---

## Phase 3: Verify Single Association (SQL)

### Check Association in Database

Run this query to verify the test association:

```sql
SELECT
    from_object_id as company_id,
    to_object_id as deal_id,
    association_type_id,
    created_at
FROM hubspot.association_company_deal
WHERE to_object_id = 12345678901  -- Replace with your test deal ID
  AND association_type_id = 6;
```

**If association NOT found in database:**
- Check HubSpot UI directly
- Database sync may be delayed
- API association is still valid even if not in database yet

### Check in HubSpot UI

1. Navigate to deal in HubSpot
2. Look for "Associated Company" section
3. Verify company is listed with correct association type

---

## Phase 4: Batch SQL Processing

### ⚠️ IMPORTANT: Safety Checklist

Before running batch processing, verify:

- [x] Single deal API test succeeded
- [x] Test association verified in HubSpot UI
- [x] You understand the filter criteria (`icalps_deal_id LIKE '4%'`)
- [x] You have reviewed target deal count (Query 1.2)
- [x] You have database backup or can rollback

### Batch Insert Process

Open `deal_company_association_workflow.sql` and locate Step 5 (lines 180-216).

**Uncomment the batch insert block:**

```sql
BEGIN;

-- Insert associations for all deals starting with '4'
INSERT INTO hubspot.association_company_deal (
    from_object_id,    -- Company ID
    to_object_id,      -- Deal ID
    association_type_id,
    created_at
)
SELECT
    d.associatedcompanyid as from_object_id,
    d.hs_object_id as to_object_id,
    6 as association_type_id,
    NOW() as created_at
FROM hubspot.deals d
WHERE d.icalps_deal_id::TEXT LIKE '4%'
  AND d.associatedcompanyid IS NOT NULL
ON CONFLICT (from_object_id, to_object_id, association_type_id)
DO UPDATE SET
    created_at = NOW();

-- Get summary of insertions
SELECT
    COUNT(*) as total_associations_created
FROM hubspot.association_company_deal
WHERE association_type_id = 6
  AND DATE(created_at) = CURRENT_DATE;

-- Review the results before committing!
-- If everything looks good:
COMMIT;

-- If something is wrong:
-- ROLLBACK;
```

### Execute Step-by-Step

1. **Start transaction:**
   ```sql
   BEGIN;
   ```

2. **Run INSERT statement** (lines 189-205)

3. **Check results:**
   ```sql
   SELECT COUNT(*) as total_associations_created
   FROM hubspot.association_company_deal
   WHERE association_type_id = 6
     AND DATE(created_at) = CURRENT_DATE;
   ```

4. **Review output - does the count match expectations?**
   - Compare with Query 1.2 count
   - Should match `deals_with_company` count

5. **If correct, commit:**
   ```sql
   COMMIT;
   ```

6. **If incorrect, rollback:**
   ```sql
   ROLLBACK;
   ```

### Expected Results

Based on Query 1.2:
```
total_associations_created
--------------------------
         148
```

This should match the `deals_with_company` count from exploration phase.

---

## Phase 5: Final Validation (SQL)

### Validation Query 6.1: Count Associations

```sql
SELECT
    COUNT(*) as total_associations,
    COUNT(DISTINCT from_object_id) as unique_companies,
    COUNT(DISTINCT to_object_id) as unique_deals
FROM hubspot.association_company_deal
WHERE association_type_id = 6;
```

**Expected:**
- `total_associations`: 148 (or your target count)
- `unique_deals`: 148
- `unique_companies`: ≤ 148 (some companies may have multiple deals)

### Validation Query 6.2: Verify Deal Matches

```sql
SELECT
    d.hs_object_id as deal_id,
    d.icalps_deal_id,
    d.dealname,
    d.associatedcompanyid as deal_company_field,
    a.from_object_id as assoc_company_id,
    CASE
        WHEN d.associatedcompanyid = a.from_object_id THEN '✓ Match'
        ELSE '✗ Mismatch'
    END as company_match
FROM hubspot.deals d
INNER JOIN hubspot.association_company_deal a
    ON d.hs_object_id = a.to_object_id
    AND a.association_type_id = 6
WHERE d.icalps_deal_id::TEXT LIKE '4%'
ORDER BY d.icalps_deal_id
LIMIT 50;
```

**Expected:**
- All rows show `✓ Match` in `company_match` column
- `deal_company_field` = `assoc_company_id` for all records

### Validation Query 6.3: Check Unassociated Deals

```sql
SELECT
    d.hs_object_id as deal_id,
    d.icalps_deal_id,
    d.dealname,
    d.associatedcompanyid,
    CASE
        WHEN d.associatedcompanyid IS NULL THEN 'No company in deal record'
        ELSE 'Unknown reason'
    END as reason_not_associated
FROM hubspot.deals d
LEFT JOIN hubspot.association_company_deal a
    ON d.hs_object_id = a.to_object_id
    AND a.association_type_id = 6
WHERE d.icalps_deal_id::TEXT LIKE '4%'
  AND a.to_object_id IS NULL
LIMIT 20;
```

**Expected:**
- Only 2 rows (from Query 1.2: `deals_without_company`)
- All should show `reason_not_associated = 'No company in deal record'`

---

## Phase 6: Rollback (If Needed)

### When to Rollback

- Wrong deals were associated
- Data quality issues discovered
- Need to reprocess with different logic

### Rollback Procedure

**⚠️ WARNING: This will delete ALL associations for filtered deals**

```sql
BEGIN;

DELETE FROM hubspot.association_company_deal
WHERE association_type_id = 6
  AND to_object_id IN (
      SELECT hs_object_id
      FROM hubspot.deals
      WHERE icalps_deal_id::TEXT LIKE '4%'
  );

-- Verify deletion count
SELECT COUNT(*) as remaining_associations
FROM hubspot.association_company_deal
WHERE association_type_id = 6;

-- If count looks correct:
COMMIT;

-- If something is wrong:
-- ROLLBACK;
```

---

## Workflow Summary

### Complete Execution Checklist

- [ ] **Phase 1: Exploration**
  - [ ] Run Query 1.1 - Identify target deals
  - [ ] Run Query 1.2 - Count target deals
  - [ ] Run Query 1.3 - Get test deal parameters
  - [ ] Record test deal ID and company ID

- [ ] **Phase 2: API Test**
  - [ ] Set HUBSPOT_TOKEN environment variable
  - [ ] Run `./test_deal_association_api.sh`
  - [ ] Verify HTTP 200/201 response
  - [ ] Save API response for reference

- [ ] **Phase 3: Verify Single Association**
  - [ ] Check association in database (optional)
  - [ ] Verify in HubSpot UI
  - [ ] Confirm association type is correct

- [ ] **Phase 4: Batch Processing**
  - [ ] Review safety checklist
  - [ ] Start transaction (BEGIN)
  - [ ] Run batch INSERT
  - [ ] Verify count matches expectations
  - [ ] COMMIT or ROLLBACK

- [ ] **Phase 5: Final Validation**
  - [ ] Run Query 6.1 - Count total associations
  - [ ] Run Query 6.2 - Verify company matches
  - [ ] Run Query 6.3 - Check unassociated deals
  - [ ] Review validation results

- [ ] **Phase 6: Documentation**
  - [ ] Document total associations created
  - [ ] Note any issues or exceptions
  - [ ] Update project records

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA FLOW ARCHITECTURE                      │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│  PostgreSQL DB  │
│                 │
│ hubspot.deals   │
│  - hs_object_id │──────┐
│  - icalps_deal_id│      │
│  - associated   │      │  Filter: icalps_deal_id LIKE '4%'
│    companyid    │      │
└─────────────────┘      │
                         ↓
                  ┌──────────────┐
                  │ SQL QUERY    │
                  │ Extract IDs  │
                  └──────────────┘
                         │
         ┌───────────────┴───────────────┐
         ↓                               ↓
  ┌─────────────┐               ┌──────────────────┐
  │ API Test    │               │ Batch SQL Insert │
  │ (Single)    │               │ (All Deals)      │
  │             │               │                  │
  │ curl → API  │               │ INSERT INTO      │
  └─────────────┘               │ association_     │
         │                      │ company_deal     │
         ↓                      └──────────────────┘
  ┌─────────────┐                       │
  │ HubSpot API │                       ↓
  │             │               ┌──────────────────┐
  │ Association │←──────────────│ hubspot.         │
  │ Created     │               │ association_     │
  └─────────────┘               │ company_deal     │
                                │                  │
                                │ - from_object_id │
                                │   (company)      │
                                │ - to_object_id   │
                                │   (deal)         │
                                │ - association_   │
                                │   type_id = 6    │
                                └──────────────────┘
```

---

## Key Concepts

### Association Type ID = 6

In HubSpot's CRM:
- **6** = "Deal with primary Company"
- This is a standard HubSpot association type
- Creates a one-to-many relationship (Company → Deals)

### ON CONFLICT Strategy

The batch insert uses `ON CONFLICT DO UPDATE`:
```sql
ON CONFLICT (from_object_id, to_object_id, association_type_id)
DO UPDATE SET created_at = NOW();
```

**Benefits:**
- Idempotent operation (can re-run safely)
- Updates timestamp if association already exists
- No duplicate associations created

### Filter Strategy

`icalps_deal_id::TEXT LIKE '4%'` ensures:
- Only deals from specific pipeline/source
- Legacy ID pattern matching
- Excludes deals from other pipelines

---

## Performance Considerations

### Batch Size

For large datasets (>10,000 deals):
- Consider breaking into smaller batches
- Monitor transaction log size
- Use LIMIT with OFFSET for chunking

Example chunked processing:
```sql
-- Process in batches of 1000
INSERT INTO hubspot.association_company_deal (...)
SELECT ... FROM hubspot.deals d
WHERE d.icalps_deal_id::TEXT LIKE '4%'
  AND d.associatedcompanyid IS NOT NULL
LIMIT 1000 OFFSET 0;  -- Then 1000, 2000, etc.
```

### Index Performance

Ensure indexes exist:
```sql
-- Check indexes on deals table
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'deals';

-- Should have index on icalps_deal_id for filter performance
```

---

## Error Recovery

### Common Issues

**Issue: "deal already has an association to company with that type"**
- This is OK - use ON CONFLICT to handle
- Or skip if API test shows 409 response

**Issue: Database sync lag**
- API associations may not appear in database immediately
- Wait 5-10 minutes or verify in HubSpot UI directly

**Issue: Transaction timeout**
- Break batch into smaller chunks
- Increase statement_timeout if needed

### Recovery Steps

1. **Check transaction state:**
   ```sql
   SELECT state FROM pg_stat_activity WHERE pid = pg_backend_pid();
   ```

2. **Rollback if stuck:**
   ```sql
   ROLLBACK;
   ```

3. **Re-run from last successful point**

---

## Integration with Larger Project

This workflow is part of the HubSpot Activities Staging Decomposition project.

**Related Workflows:**
- Contact-Company associations
- Deal-Contact associations
- Activity-Deal associations

**Next Steps After Completion:**
- Apply similar pattern to other association types
- Build validation dashboard
- Create monitoring queries
- Document association type mappings

---

## References

### HubSpot API Documentation

- **Associations API v3**: https://developers.hubspot.com/docs/api/crm/associations
- **Private App Tokens**: https://developers.hubspot.com/docs/api/private-apps
- **Association Type IDs**: https://developers.hubspot.com/docs/api/crm/associations#association-type-id-values

### Internal Documentation

- `HUBSPOT_ACTIVITIES_STAGING_ARCHITECTURE.md` - Master architecture
- `PLAN.md` - Overall project plan
- `deal_company_association_workflow.sql` - SQL queries
- `test_deal_association_api.sh` - API test script

---

## Changelog

| Date | Version | Changes |
|------|---------|---------|
| 2025-11-15 | 1.0 | Initial workflow documentation |

---

## Support

For issues or questions:
1. Review troubleshooting section above
2. Check HubSpot API documentation
3. Verify database connectivity
4. Review PostgreSQL logs for errors

---

**END OF DOCUMENT**
