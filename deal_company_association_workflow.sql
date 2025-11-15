-- ============================================================================
-- Deal-Company Association Workflow
-- ============================================================================
-- Purpose: Create associations between deals and their primary companies
-- Filter: Deals with deal_id (icalps_deal_id) starting with '4'
-- Association Type: 6 (Deal with primary Company)
--
-- Workflow:
-- 1. EXPLORATION: Identify target deals
-- 2. TEST: Single deal association (via API/SQL)
-- 3. BATCH: All filtered deals association
-- ============================================================================

-- ============================================================================
-- STEP 1: EXPLORATION - Identify Target Deals
-- ============================================================================

-- Query 1.1: Find all deals starting with '4'
SELECT
    hs_object_id as deal_hubspot_id,
    icalps_deal_id as deal_legacy_id,
    dealname,
    amount,
    dealstage,
    associatedcompanyid as current_company_id,
    icalps_company_id as legacy_company_id,
    createdate,
    closedate
FROM hubspot.deals
WHERE icalps_deal_id::TEXT LIKE '4%'
ORDER BY icalps_deal_id
LIMIT 50;

-- Query 1.2: Count of target deals
SELECT
    COUNT(*) as total_target_deals,
    COUNT(associatedcompanyid) as deals_with_company,
    COUNT(*) - COUNT(associatedcompanyid) as deals_without_company
FROM hubspot.deals
WHERE icalps_deal_id::TEXT LIKE '4%';

-- Query 1.3: Sample deal for testing (pick first one)
SELECT
    hs_object_id as deal_hubspot_id,
    icalps_deal_id as deal_legacy_id,
    dealname,
    associatedcompanyid as company_hubspot_id,
    icalps_company_id as legacy_company_id
FROM hubspot.deals
WHERE icalps_deal_id::TEXT LIKE '4%'
  AND associatedcompanyid IS NOT NULL  -- Only deals that have a company
LIMIT 1;

-- ============================================================================
-- STEP 2: CHECK EXISTING ASSOCIATIONS
-- ============================================================================

-- Query 2.1: Check if association table exists and its structure
SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'hubspot'
  AND table_name = 'association_company_deal'
ORDER BY ordinal_position;

-- Query 2.2: Sample existing associations (if table exists)
-- Note: Uncomment after confirming table exists
/*
SELECT
    from_object_id as company_id,
    to_object_id as deal_id,
    association_type_id,
    created_at
FROM hubspot.association_company_deal
LIMIT 10;
*/

-- Query 2.3: Check if our target deals already have associations
-- Note: Uncomment after confirming table structure
/*
SELECT
    d.hs_object_id as deal_id,
    d.icalps_deal_id,
    d.dealname,
    d.associatedcompanyid,
    a.from_object_id as assoc_company_id,
    a.association_type_id
FROM hubspot.deals d
LEFT JOIN hubspot.association_company_deal a
    ON d.hs_object_id = a.to_object_id
    AND a.association_type_id = 6
WHERE d.icalps_deal_id::TEXT LIKE '4%'
LIMIT 20;
*/

-- ============================================================================
-- STEP 3: PREPARE TEST DATA
-- ============================================================================

-- Query 3.1: Get a single deal for API testing
-- Save these values for the API test
DO $$
DECLARE
    v_test_deal_id BIGINT;
    v_test_company_id BIGINT;
    v_test_deal_name TEXT;
BEGIN
    -- Get first eligible deal
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

    -- Output for API testing
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'TEST DEAL FOR API:';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Deal HubSpot ID:    %', v_test_deal_id;
    RAISE NOTICE 'Company HubSpot ID: %', v_test_company_id;
    RAISE NOTICE 'Deal Name:          %', v_test_deal_name;
    RAISE NOTICE '';
    RAISE NOTICE 'Use these values in the API test script!';
    RAISE NOTICE '============================================';
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- STEP 4: TEST ASSOCIATION (SQL - Single Record)
-- ============================================================================

-- Note: This is a DRY RUN example. Uncomment to execute.
-- This assumes the association table structure is:
-- (from_object_id, to_object_id, association_type_id, created_at)

/*
-- Test insert for a single deal-company association
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
LIMIT 1
ON CONFLICT (from_object_id, to_object_id, association_type_id)
DO UPDATE SET
    created_at = NOW();

-- Verify the test insert
SELECT
    from_object_id as company_id,
    to_object_id as deal_id,
    association_type_id,
    created_at
FROM hubspot.association_company_deal
WHERE association_type_id = 6
ORDER BY created_at DESC
LIMIT 5;
*/

-- ============================================================================
-- STEP 5: BATCH ASSOCIATION (SQL - All Filtered Deals)
-- ============================================================================

-- Note: Execute this only after testing with single deal succeeds

/*
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

COMMIT;
-- ROLLBACK;  -- Uncomment to rollback if needed
*/

-- ============================================================================
-- STEP 6: VALIDATION QUERIES
-- ============================================================================

-- Query 6.1: Verify associations created
/*
SELECT
    COUNT(*) as total_associations,
    COUNT(DISTINCT from_object_id) as unique_companies,
    COUNT(DISTINCT to_object_id) as unique_deals
FROM hubspot.association_company_deal
WHERE association_type_id = 6;
*/

-- Query 6.2: Check deals that got associated
/*
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
*/

-- Query 6.3: Check deals that were NOT associated (troubleshooting)
/*
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
*/

-- ============================================================================
-- STEP 7: ROLLBACK PROCEDURE (If Needed)
-- ============================================================================

-- Delete all associations created for deals starting with '4'
-- WARNING: Only use if you need to undo the batch operation
/*
BEGIN;

DELETE FROM hubspot.association_company_deal
WHERE association_type_id = 6
  AND to_object_id IN (
      SELECT hs_object_id
      FROM hubspot.deals
      WHERE icalps_deal_id::TEXT LIKE '4%'
  );

-- Verify deletion
SELECT COUNT(*) as deleted_count
FROM hubspot.association_company_deal
WHERE association_type_id = 6;

COMMIT;
-- ROLLBACK;
*/

-- ============================================================================
-- USAGE INSTRUCTIONS
-- ============================================================================

/*
WORKFLOW STEPS:

1. EXPLORATION (Safe - Read Only):
   - Run Query 1.1, 1.2, 1.3 to identify target deals
   - Run Query 3.1 (DO block) to get test deal parameters

2. API TESTING (Single Deal):
   - Use test_deal_association_api.sh script
   - Verify association created in HubSpot UI or via Query 6.1

3. SQL TESTING (Single Deal):
   - Uncomment and run Step 4 queries
   - Verify with Query 6.1

4. BATCH PROCESSING (All Deals):
   - Uncomment and run Step 5 (within transaction)
   - Validate with Step 6 queries
   - COMMIT if successful, ROLLBACK if issues found

5. ROLLBACK (If Needed):
   - Use Step 7 to undo batch associations

SAFETY CHECKS:
- Always run within a transaction (BEGIN...COMMIT/ROLLBACK)
- Test with LIMIT 1 first
- Verify with validation queries before COMMIT
- Keep rollback script ready
*/
