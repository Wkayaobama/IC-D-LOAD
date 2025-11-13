-- ============================================================================
-- Phase 4: Foreign Key Resolution
-- ============================================================================
-- Purpose: Resolve legacy IDs to HubSpot IDs via reconciliation tables
-- Prerequisites: Phase 3 complete (master table loaded)
-- Execution: Run as a single transaction or step-by-step
-- ============================================================================

BEGIN;

-- ============================================================================
-- STEP 1: Resolve Contact Foreign Keys
-- ============================================================================

DO $$
DECLARE
    v_total_with_person_id INTEGER;
    v_resolved_contact_id INTEGER;
    v_resolution_pct DECIMAL(5,2);
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'STEP 1: Resolving Contact Foreign Keys';
    RAISE NOTICE '============================================';
    RAISE NOTICE '';

    -- Count records with person_id
    SELECT COUNT(*)
    INTO v_total_with_person_id
    FROM staging.ic_communication_master
    WHERE person_id IS NOT NULL;

    RAISE NOTICE 'Records with person_id: %', v_total_with_person_id;

    -- Resolve contact IDs
    UPDATE staging.ic_communication_master m
    SET
        hubspot_contact_id = c.hubspot_contact_id,
        last_updated = NOW()
    FROM staging.contacts_reconciliation c
    WHERE m.person_id = c.legacy_contact_id
      AND c.hubspot_contact_id IS NOT NULL
      AND m.hubspot_contact_id IS NULL;  -- Only update if not already set

    -- Count resolved
    SELECT COUNT(*)
    INTO v_resolved_contact_id
    FROM staging.ic_communication_master
    WHERE person_id IS NOT NULL
      AND hubspot_contact_id IS NOT NULL;

    -- Calculate resolution rate
    v_resolution_pct := CASE
        WHEN v_total_with_person_id > 0 THEN
            ROUND(100.0 * v_resolved_contact_id / v_total_with_person_id, 2)
        ELSE 0
    END;

    RAISE NOTICE 'Resolved contact IDs: % (%.2f%%)',
        v_resolved_contact_id,
        v_resolution_pct;

    -- Log unresolved references
    INSERT INTO staging.reconciliation_log (
        operation,
        entity_type,
        legacy_id,
        status,
        error_message,
        timestamp
    )
    SELECT
        'FK_RESOLUTION',
        'contact',
        m.person_id,
        'unresolved',
        'Contact not found in staging.contacts_reconciliation',
        NOW()
    FROM staging.ic_communication_master m
    WHERE m.person_id IS NOT NULL
      AND m.hubspot_contact_id IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM staging.contacts_reconciliation c
          WHERE c.legacy_contact_id = m.person_id
      );

    RAISE NOTICE '✓ Contact FK resolution complete';
    RAISE NOTICE '';

END $$;

-- ============================================================================
-- STEP 2: Resolve Company Foreign Keys
-- ============================================================================

DO $$
DECLARE
    v_total_with_company_id INTEGER;
    v_resolved_company_id INTEGER;
    v_resolution_pct DECIMAL(5,2);
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'STEP 2: Resolving Company Foreign Keys';
    RAISE NOTICE '============================================';
    RAISE NOTICE '';

    -- Count records with company_id
    SELECT COUNT(*)
    INTO v_total_with_company_id
    FROM staging.ic_communication_master
    WHERE company_id IS NOT NULL;

    RAISE NOTICE 'Records with company_id: %', v_total_with_company_id;

    -- Resolve company IDs
    UPDATE staging.ic_communication_master m
    SET
        hubspot_company_id = c.hubspot_company_id,
        last_updated = NOW()
    FROM staging.companies_reconciliation c
    WHERE m.company_id = c.legacy_company_id
      AND c.hubspot_company_id IS NOT NULL
      AND m.hubspot_company_id IS NULL;

    -- Count resolved
    SELECT COUNT(*)
    INTO v_resolved_company_id
    FROM staging.ic_communication_master
    WHERE company_id IS NOT NULL
      AND hubspot_company_id IS NOT NULL;

    -- Calculate resolution rate
    v_resolution_pct := CASE
        WHEN v_total_with_company_id > 0 THEN
            ROUND(100.0 * v_resolved_company_id / v_total_with_company_id, 2)
        ELSE 0
    END;

    RAISE NOTICE 'Resolved company IDs: % (%.2f%%)',
        v_resolved_company_id,
        v_resolution_pct;

    -- Log unresolved references
    INSERT INTO staging.reconciliation_log (
        operation,
        entity_type,
        legacy_id,
        status,
        error_message,
        timestamp
    )
    SELECT
        'FK_RESOLUTION',
        'company',
        m.company_id,
        'unresolved',
        'Company not found in staging.companies_reconciliation',
        NOW()
    FROM staging.ic_communication_master m
    WHERE m.company_id IS NOT NULL
      AND m.hubspot_company_id IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM staging.companies_reconciliation c
          WHERE c.legacy_company_id = m.company_id
      );

    RAISE NOTICE '✓ Company FK resolution complete';
    RAISE NOTICE '';

END $$;

-- ============================================================================
-- STEP 3: Resolve Deal Foreign Keys
-- ============================================================================

DO $$
DECLARE
    v_total_with_opportunity_id INTEGER;
    v_resolved_deal_id INTEGER;
    v_resolution_pct DECIMAL(5,2);
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'STEP 3: Resolving Deal Foreign Keys';
    RAISE NOTICE '============================================';
    RAISE NOTICE '';

    -- Count records with comm_opportunityid
    SELECT COUNT(*)
    INTO v_total_with_opportunity_id
    FROM staging.ic_communication_master
    WHERE comm_opportunityid IS NOT NULL;

    RAISE NOTICE 'Records with comm_opportunityid: %', v_total_with_opportunity_id;

    -- Resolve deal IDs
    UPDATE staging.ic_communication_master m
    SET
        hubspot_deal_id = d.hubspot_deal_id,
        last_updated = NOW()
    FROM staging.deals_reconciliation d
    WHERE m.comm_opportunityid = d.legacy_deal_id
      AND d.hubspot_deal_id IS NOT NULL
      AND m.hubspot_deal_id IS NULL;

    -- Count resolved
    SELECT COUNT(*)
    INTO v_resolved_deal_id
    FROM staging.ic_communication_master
    WHERE comm_opportunityid IS NOT NULL
      AND hubspot_deal_id IS NOT NULL;

    -- Calculate resolution rate
    v_resolution_pct := CASE
        WHEN v_total_with_opportunity_id > 0 THEN
            ROUND(100.0 * v_resolved_deal_id / v_total_with_opportunity_id, 2)
        ELSE 0
    END;

    RAISE NOTICE 'Resolved deal IDs: % (%.2f%%)',
        v_resolved_deal_id,
        v_resolution_pct;

    -- Log unresolved references
    INSERT INTO staging.reconciliation_log (
        operation,
        entity_type,
        legacy_id,
        status,
        error_message,
        timestamp
    )
    SELECT
        'FK_RESOLUTION',
        'deal',
        m.comm_opportunityid,
        'unresolved',
        'Deal not found in staging.deals_reconciliation',
        NOW()
    FROM staging.ic_communication_master m
    WHERE m.comm_opportunityid IS NOT NULL
      AND m.hubspot_deal_id IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM staging.deals_reconciliation d
          WHERE d.legacy_deal_id = m.comm_opportunityid
      );

    RAISE NOTICE '✓ Deal FK resolution complete';
    RAISE NOTICE '';

END $$;

-- ============================================================================
-- STEP 4: Flag Orphaned Records
-- ============================================================================

DO $$
DECLARE
    v_total_records INTEGER;
    v_orphaned_records INTEGER;
    v_orphaned_pct DECIMAL(5,2);
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'STEP 4: Flagging Orphaned Records';
    RAISE NOTICE '============================================';
    RAISE NOTICE '';

    -- Get total count
    SELECT COUNT(*)
    INTO v_total_records
    FROM staging.ic_communication_master;

    -- Flag orphaned records (no contact, company, or deal)
    UPDATE staging.ic_communication_master
    SET
        is_orphaned = TRUE,
        last_updated = NOW()
    WHERE hubspot_contact_id IS NULL
      AND hubspot_company_id IS NULL
      AND hubspot_deal_id IS NULL;

    -- Count orphaned
    SELECT COUNT(*)
    INTO v_orphaned_records
    FROM staging.ic_communication_master
    WHERE is_orphaned = TRUE;

    -- Calculate orphaned percentage
    v_orphaned_pct := CASE
        WHEN v_total_records > 0 THEN
            ROUND(100.0 * v_orphaned_records / v_total_records, 2)
        ELSE 0
    END;

    RAISE NOTICE 'Total records: %', v_total_records;
    RAISE NOTICE 'Orphaned records: % (%.2f%%)',
        v_orphaned_records,
        v_orphaned_pct;

    IF v_orphaned_pct > 5.0 THEN
        RAISE WARNING '⚠ Orphaned rate (%.2f%%) exceeds threshold (5%%). Review unresolved references.',
            v_orphaned_pct;
    ELSE
        RAISE NOTICE '✓ Orphaned rate within acceptable threshold';
    END IF;

    RAISE NOTICE '';

END $$;

-- ============================================================================
-- STEP 5: Generate FK Resolution Summary
-- ============================================================================

DO $$
DECLARE
    v_contact_resolution_pct DECIMAL(5,2);
    v_company_resolution_pct DECIMAL(5,2);
    v_deal_resolution_pct DECIMAL(5,2);
    v_orphaned_pct DECIMAL(5,2);
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Phase 4: FK Resolution Summary';
    RAISE NOTICE '============================================';
    RAISE NOTICE '';

    -- Calculate resolution rates
    SELECT
        ROUND(100.0 * COUNT(hubspot_contact_id) / NULLIF(COUNT(person_id), 0), 2),
        ROUND(100.0 * COUNT(hubspot_company_id) / NULLIF(COUNT(company_id), 0), 2),
        ROUND(100.0 * COUNT(hubspot_deal_id) / NULLIF(COUNT(comm_opportunityid), 0), 2),
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_orphaned = TRUE) / COUNT(*), 2)
    INTO
        v_contact_resolution_pct,
        v_company_resolution_pct,
        v_deal_resolution_pct,
        v_orphaned_pct
    FROM staging.ic_communication_master;

    RAISE NOTICE 'FK Resolution Rates:';
    RAISE NOTICE '  Contact: %.2f%%', COALESCE(v_contact_resolution_pct, 0);
    RAISE NOTICE '  Company: %.2f%%', COALESCE(v_company_resolution_pct, 0);
    RAISE NOTICE '  Deal:    %.2f%%', COALESCE(v_deal_resolution_pct, 0);
    RAISE NOTICE '';
    RAISE NOTICE 'Orphaned Records: %.2f%%', COALESCE(v_orphaned_pct, 0);
    RAISE NOTICE '';

    -- Check if resolution rates meet target (>95%)
    IF COALESCE(v_contact_resolution_pct, 0) < 95.0 OR
       COALESCE(v_company_resolution_pct, 0) < 95.0 OR
       COALESCE(v_deal_resolution_pct, 0) < 95.0 THEN
        RAISE WARNING '⚠ Some FK resolution rates below target (95%%). Review reconciliation tables.';
    ELSE
        RAISE NOTICE '✓ All FK resolution rates meet or exceed target (95%%)';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE '✓ PHASE 4: FK RESOLUTION - COMPLETE';
    RAISE NOTICE '============================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Next Steps:';
    RAISE NOTICE '  1. Review any unresolved references in staging.reconciliation_log';
    RAISE NOTICE '  2. Execute Phase 5: Derivative Table Population';
    RAISE NOTICE '  3. Run population script in hubspot_activities_staging_views.sql';
    RAISE NOTICE '';

END $$;

COMMIT;

-- ============================================================================
-- OPTIONAL: Query to review unresolved references
-- ============================================================================

-- Uncomment to review unresolved references after FK resolution:

-- SELECT
--     entity_type,
--     COUNT(*) as unresolved_count
-- FROM staging.reconciliation_log
-- WHERE operation = 'FK_RESOLUTION'
--   AND status = 'unresolved'
--   AND DATE(timestamp) = CURRENT_DATE
-- GROUP BY entity_type
-- ORDER BY unresolved_count DESC;
