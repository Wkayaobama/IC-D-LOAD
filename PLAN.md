# HubSpot Activities Staging Decomposition - Implementation Plan

**Project:** IC-D-LOAD - HubSpot CRM Data Integration
**Phase:** Implementation
**Started:** 2025-11-13
**Status:** 🚧 IN PROGRESS

---

## 📋 Table of Contents

1. [Project Overview](#project-overview)
2. [Implementation Phases](#implementation-phases)
3. [Phase Status Tracking](#phase-status-tracking)
4. [Current Phase Details](#current-phase-details)
5. [Dependencies & Blockers](#dependencies--blockers)
6. [Decisions Log](#decisions-log)
7. [Validation Checkpoints](#validation-checkpoints)
8. [Rollback Procedures](#rollback-procedures)

---

## 🎯 Project Overview

### Objective
Transform monolithic communications table into 7 HubSpot-compliant activity staging tables for API integration.

### Architecture
- **Master Table:** `staging.ic_communication_master` (22 source columns)
- **Derivative Tables:** 7 tables (calls, emails, meetings, notes, tasks, communications, postal_mail)
- **Classification:** Intelligent type mapping via `classify_activity_type()` function
- **FK Resolution:** Link to existing reconciliation tables (contacts, companies, deals)
- **API Integration:** Private app token authentication (future phase)

### Key Deliverables
- ✅ Architecture documentation (HUBSPOT_ACTIVITIES_STAGING_ARCHITECTURE.md)
- ✅ Database schema DDL (hubspot_activities_staging_ddl.sql)
- ✅ SQL transformation views (hubspot_activities_staging_views.sql)
- ✅ GraphML ERD diagram (hubspot_activities_staging_erd.graphml)
- ✅ Classification decision tree (ACTIVITY_TYPE_CLASSIFICATION_DECISION_TREE.md)
- ✅ Project summary (HUBSPOT_ACTIVITIES_PROJECT_SUMMARY.md)
- 🚧 Implementation plan (PLAN.md - this file)

---

## 📊 Implementation Phases

### Phase 1: Planning & Design ✅ COMPLETE
**Duration:** Completed 2025-11-13
**Deliverables:** All architecture documents created and committed

### Phase 2: Database Setup ⏸️ READY (Awaiting Database Access)
**Started:** 2025-11-13
**Status:** Scripts created, awaiting database connection
**Tasks:**
- [x] Create DDL scripts for staging tables
- [x] Create SQL views for transformations
- [x] Create Phase 2 execution script (execute_phase2_database_setup.py)
- [ ] Execute DDL scripts to create staging tables (⚠️ **BLOCKED: Database connectivity**)
- [ ] Verify table creation (master + 7 derivatives)
- [ ] Verify indexes created (40+ indexes)
- [ ] Verify constraints (UNIQUE, CHECK, FK)
- [ ] Test classification function
- [ ] Verify validation views created

**Blocker:** PostgreSQL database not accessible from current environment
- Host: `2219-revops.pgm5k8mhg52j6k63k3dd54em0v.postgres.stacksync.com`
- Error: DNS resolution failure
- **Resolution:** Execute scripts from environment with database access

### Phase 3: Master Table Loading ⏸️ READY (Awaiting Data & DB Access)
**Estimated Duration:** 2-4 hours
**Prerequisites:** Phase 2 complete, communications CSV available
**Tasks:**
- [x] Create Python data loading script (load_communications_master.py)
- [ ] Prepare communications CSV (22 columns) (⚠️ **AWAITING USER**)
- [ ] Execute loading script (⚠️ **BLOCKED: Database connectivity**)
- [ ] Verify record counts
- [ ] Review `derived_activity_type` distribution
- [ ] Identify unrecognized `Comm_Type` values
- [ ] Quality check: NULL values in required fields

**Blocker:**
1. Communications CSV not yet provided
2. Database access required

**Usage:** `python load_communications_master.py communications_master.csv`

### Phase 4: FK Resolution ⏸️ READY (Awaiting Phase 3)
**Estimated Duration:** 1-2 hours
**Prerequisites:** Phase 3 complete
**Tasks:**
- [x] Create FK resolution SQL script (resolve_foreign_keys.sql)
- [ ] Execute contact FK resolution query (⚠️ **BLOCKED: Phase 3**)
- [ ] Execute company FK resolution query
- [ ] Execute deal FK resolution query
- [ ] Log unresolved references to reconciliation_log
- [ ] Calculate FK resolution rates (target: >95%)
- [ ] Flag orphaned records (target: <5%)
- [ ] Review and address low resolution rates

**Usage:** Execute SQL script via psql or PgAdmin
```bash
psql -h <host> -U postgres -d postgres -f resolve_foreign_keys.sql
```

### Phase 5: Derivative Table Population 📅 PENDING
**Estimated Duration:** 2-3 hours
**Prerequisites:** Phase 4 complete
**Tasks:**
- [ ] Execute population script for all 7 derivative tables
- [ ] Verify record counts match master table by type
- [ ] Run coverage validation query
- [ ] Check required fields populated
- [ ] Verify date logic (meeting end >= start)
- [ ] Sample record review for each activity type
- [ ] Fix any transformation issues

### Phase 6: Validation & QA 📅 PENDING
**Estimated Duration:** 2-3 hours
**Prerequisites:** Phase 5 complete
**Tasks:**
- [ ] Run all validation queries
- [ ] Generate data quality report
- [ ] Review orphaned record distribution
- [ ] Check enum value compliance
- [ ] Verify association counts
- [ ] Manual spot-check sample records (10 per type)
- [ ] Document any data quality issues

### Phase 7: API Preparation 📅 FUTURE
**Estimated Duration:** 4-6 hours
**Prerequisites:** Phase 6 complete, HubSpot private app token obtained
**Tasks:**
- [ ] Create Jinja templates for 7 activity types
- [ ] Build API payload JSONB in derivative tables
- [ ] Create Python API pusher script
- [ ] Test with HubSpot sandbox/test account
- [ ] Implement rate limiting (10 req/sec)
- [ ] Add error handling & retry logic
- [ ] Dry-run with small batch (100 records)

### Phase 8: Production API Push 📅 FUTURE
**Estimated Duration:** 2-4 hours (depends on volume)
**Prerequisites:** Phase 7 complete, stakeholder approval
**Tasks:**
- [ ] Final validation of staging data
- [ ] Backup staging tables
- [ ] Execute API push script
- [ ] Monitor progress & error rates
- [ ] Verify engagement records in HubSpot UI
- [ ] Generate completion report
- [ ] Update `api_push_status` in staging tables

---

## 🏁 Phase Status Tracking

| Phase | Status | Start Date | Completion Date | Duration | Notes |
|-------|--------|------------|-----------------|----------|-------|
| 1. Planning & Design | ✅ COMPLETE | 2025-11-13 | 2025-11-13 | ~4 hours | All docs created |
| 2. Database Setup | ⏸️ READY | 2025-11-13 | - | - | Scripts created, awaiting DB access |
| 3. Master Table Loading | ⏸️ READY | 2025-11-13 | - | - | Script created, awaiting CSV + DB |
| 4. FK Resolution | ⏸️ READY | 2025-11-13 | - | - | SQL script created |
| 5. Derivative Population | ⏸️ READY | 2025-11-13 | - | - | SQL views created |
| 6. Validation & QA | 📅 PENDING | - | - | - | - |
| 7. API Preparation | 📅 FUTURE | - | - | - | - |
| 8. Production API Push | 📅 FUTURE | - | - | - | - |

---

## 🔍 Current Phase Details

### Phase 2-5: Scripts Created - Awaiting Database Access

#### Current Status
All implementation scripts for Phases 2-5 have been created and are ready for execution once database access is available.

#### Steps Being Executed

**Step 1: Create staging schema**
```sql
CREATE SCHEMA IF NOT EXISTS staging;
```

**Step 2: Create master table**
- Table: `staging.ic_communication_master`
- Columns: 35 (22 source + 13 metadata)
- Constraints: UNIQUE(comm_communicationid), CHECK(derived_activity_type)
- Indexes: 13 indexes

**Step 3: Create derivative tables (7 tables)**
1. `staging.ic_calls` - 25 columns, 7 indexes
2. `staging.ic_emails` - 27 columns, 8 indexes
3. `staging.ic_meetings` - 30 columns, 8 indexes
4. `staging.ic_notes` - 17 columns, 6 indexes
5. `staging.ic_tasks` - 31 columns, 10 indexes
6. `staging.ic_communications` - 18 columns, 7 indexes
7. `staging.ic_postal_mail` - 18 columns, 6 indexes

**Step 4: Create helper functions**
- `staging.classify_activity_type(p_comm_type, p_comm_priority, p_comm_caseid)`

**Step 5: Create validation views**
- `staging.v_ic_activities_summary`
- `staging.v_ic_derivative_coverage`

**Step 6: Create transformation views (7 views)**
- `staging.v_ic_calls_staging`
- `staging.v_ic_emails_staging`
- `staging.v_ic_meetings_staging`
- `staging.v_ic_notes_staging`
- `staging.v_ic_tasks_staging`
- `staging.v_ic_communications_staging`
- `staging.v_ic_postal_mail_staging`

#### Success Criteria for Phase 2
- [ ] All 8 tables created (1 master + 7 derivatives)
- [ ] All 40+ indexes created
- [ ] All constraints enforced (UNIQUE, CHECK, FK)
- [ ] Classification function returns expected results
- [ ] All validation views return data (empty is OK)
- [ ] All transformation views are queryable

#### Validation Queries for Phase 2
```sql
-- Check tables exist
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'staging'
  AND table_name LIKE 'ic_%'
ORDER BY table_name;

-- Check indexes
SELECT tablename, indexname
FROM pg_indexes
WHERE schemaname = 'staging'
  AND tablename LIKE 'ic_%'
ORDER BY tablename, indexname;

-- Test classification function
SELECT
    staging.classify_activity_type('Call', NULL, NULL) as test_call,
    staging.classify_activity_type('Email', NULL, NULL) as test_email,
    staging.classify_activity_type('Meeting', NULL, NULL) as test_meeting,
    staging.classify_activity_type('Case', 'High', 123) as test_case_task,
    staging.classify_activity_type('Case', 'Low', 456) as test_case_note,
    staging.classify_activity_type('Unknown', NULL, NULL) as test_default;

-- Check views exist
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'staging'
  AND table_name LIKE '%ic_%'
ORDER BY table_name;
```

---

## 🔗 Dependencies & Blockers

### Current Dependencies (Phase 2)
- ✅ PostgreSQL database access
- ✅ `staging` schema permissions
- ✅ DDL scripts available (hubspot_activities_staging_ddl.sql, hubspot_activities_staging_views.sql)

### Future Dependencies

#### Phase 3: Master Table Loading
- ⏳ Communications CSV file (22 columns)
- ⏳ Python environment with pandas, psycopg2
- ⏳ PostgreSQL connection credentials

#### Phase 4: FK Resolution
- ✅ Existing `staging.contacts_reconciliation` table
- ✅ Existing `staging.companies_reconciliation` table
- ✅ Existing `staging.deals_reconciliation` table

#### Phase 7: API Preparation
- ⏳ HubSpot private app token
- ⏳ Python environment with requests, jinja2
- ⏳ HubSpot sandbox/test account (optional but recommended)

### Current Blockers

#### Blocker 1: Database Access (HIGH PRIORITY)
**Impact:** Phases 2-5 cannot execute
**Issue:** PostgreSQL database not accessible from current environment
- Host: `2219-revops.pgm5k8mhg52j6k63k3dd54em0v.postgres.stacksync.com`
- Error: DNS resolution failure ("Temporary failure in name resolution")
**Resolution:** Execute scripts from environment with database network access
- Option A: Run scripts from server with database connectivity
- Option B: Set up VPN/tunnel to database network
- Option C: Use port forwarding or proxy

#### Blocker 2: Communications CSV (MEDIUM PRIORITY)
**Impact:** Phase 3 cannot execute (after database access restored)
**Issue:** Communications master CSV not yet prepared
**Requirements:** CSV with 22 columns (see `load_communications_master.py` header for spec)
**Resolution:** User to extract and provide communications data

### Future Blockers
- Phase 7 blocked until: HubSpot private app token obtained

---

## 📝 Decisions Log

### Decision 1: User Assignment Fields
**Date:** 2025-11-13
**Question:** How to populate `activity_assigned_to` and `activity_created_by`?
**Decision:** Leave NULL for now, handled by external system
**Impact:** Fields remain NULL in staging, populated during/after API push by external system
**Documented in:** This plan

### Decision 2: Phone Number Extraction
**Date:** 2025-11-13
**Question:** How to extract `from_number` and `to_number` for calls?
**Decision:** Leave NULL, phone numbers fetched from external system
**Impact:** `from_number` and `to_number` fields NULL in staging.ic_calls
**Documented in:** This plan

### Decision 3: Meeting Location Extraction
**Date:** 2025-11-13
**Question:** Extract meeting location from `comm_note` or leave NULL?
**Decision:** Leave NULL (not extracting from comm_note)
**Impact:** `meeting_location` field NULL in staging.ic_meetings
**Documented in:** This plan

### Decision 4: Ambiguous Type Mappings
**Date:** 2025-11-13
**Question:** How to handle ambiguous types ('Mail', 'Text', 'Message')?
**Decision:** Use current mappings, subject to review at later stage
**Impact:**
- 'Text' → 'communications' (SMS)
- 'Message' → 'communications'
- 'Mail' → 'notes' (no explicit mapping, requires 'email' or 'postal')
**Documented in:** ACTIVITY_TYPE_CLASSIFICATION_DECISION_TREE.md

### Decision 5: API Authentication Method
**Date:** 2025-11-13
**Question:** Which HubSpot authentication method to use?
**Decision:** Private app token
**Impact:** API integration will use `Authorization: Bearer <token>` header
**Documented in:** This plan

---

## ✅ Validation Checkpoints

### Checkpoint 1: Phase 2 Complete (Database Setup)
**When:** After executing DDL scripts
**Criteria:**
- [ ] All 8 tables exist in staging schema
- [ ] All 40+ indexes created
- [ ] Classification function test returns expected results
- [ ] All 7 transformation views are queryable (SELECT * FROM view LIMIT 1 succeeds even if empty)
- [ ] No SQL errors in PostgreSQL logs

**Validation Script:** `validate_phase2.sql` (to be created)

---

### Checkpoint 2: Phase 3 Complete (Master Table Loading)
**When:** After loading communications CSV
**Criteria:**
- [ ] Record count in master table matches CSV row count
- [ ] No NULL values in required fields (comm_communicationid, comm_type, comm_datetime, comm_createddate, derived_activity_type)
- [ ] `derived_activity_type` distribution looks reasonable (no type has 0 records, no type dominates >90%)
- [ ] All `derived_activity_type` values are valid (calls, emails, meetings, notes, tasks, communications, postal_mail)
- [ ] Unrecognized `Comm_Type` values documented and reviewed

**Validation Script:** `validate_phase3.sql` (to be created)

**Distribution Query:**
```sql
SELECT
    derived_activity_type,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
FROM staging.ic_communication_master
GROUP BY derived_activity_type
ORDER BY count DESC;
```

---

### Checkpoint 3: Phase 4 Complete (FK Resolution)
**When:** After running FK resolution queries
**Criteria:**
- [ ] Contact FK resolution rate > 95% (for records with person_id IS NOT NULL)
- [ ] Company FK resolution rate > 95% (for records with company_id IS NOT NULL)
- [ ] Deal FK resolution rate > 95% (for records with comm_opportunityid IS NOT NULL)
- [ ] Orphaned record rate < 5% (records with all 3 HubSpot FKs NULL)
- [ ] Unresolved references logged to staging.reconciliation_log

**Validation Script:** `validate_phase4.sql` (to be created)

**FK Resolution Query:**
```sql
SELECT
    COUNT(*) as total_records,
    COUNT(person_id) as has_person_id,
    COUNT(hubspot_contact_id) as resolved_contact_id,
    ROUND(100.0 * COUNT(hubspot_contact_id) / NULLIF(COUNT(person_id), 0), 2) as contact_resolution_pct,

    COUNT(company_id) as has_company_id,
    COUNT(hubspot_company_id) as resolved_company_id,
    ROUND(100.0 * COUNT(hubspot_company_id) / NULLIF(COUNT(company_id), 0), 2) as company_resolution_pct,

    COUNT(comm_opportunityid) as has_opportunity_id,
    COUNT(hubspot_deal_id) as resolved_deal_id,
    ROUND(100.0 * COUNT(hubspot_deal_id) / NULLIF(COUNT(comm_opportunityid), 0), 2) as deal_resolution_pct,

    COUNT(*) FILTER (WHERE is_orphaned = TRUE) as orphaned_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_orphaned = TRUE) / COUNT(*), 2) as orphaned_pct
FROM staging.ic_communication_master;
```

---

### Checkpoint 4: Phase 5 Complete (Derivative Population)
**When:** After running population script
**Criteria:**
- [ ] Record counts match: `SELECT * FROM staging.v_ic_derivative_coverage WHERE missing_count > 0` returns 0 rows
- [ ] No NULL values in required fields per table
- [ ] Date logic constraints satisfied (e.g., meeting_end_time >= meeting_start_time)
- [ ] All enum values valid (call_direction IN ('INBOUND','OUTBOUND'), etc.)
- [ ] Sample record review passed (10 records per type manually reviewed)

**Validation Script:** `validate_phase5.sql` (to be created)

**Coverage Query:**
```sql
SELECT * FROM staging.v_ic_derivative_coverage;
```

**Required Fields Query:**
```sql
-- Check required fields for each table
SELECT
    'ic_calls' as table_name,
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE call_title IS NULL) as missing_title,
    COUNT(*) FILTER (WHERE activity_date IS NULL) as missing_date
FROM staging.ic_calls
UNION ALL
SELECT 'ic_emails', COUNT(*),
    COUNT(*) FILTER (WHERE email_subject IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL)
FROM staging.ic_emails
UNION ALL
SELECT 'ic_meetings', COUNT(*),
    COUNT(*) FILTER (WHERE meeting_name IS NULL),
    COUNT(*) FILTER (WHERE meeting_start_time IS NULL)
FROM staging.ic_meetings
UNION ALL
SELECT 'ic_notes', COUNT(*),
    COUNT(*) FILTER (WHERE note_body IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL)
FROM staging.ic_notes
UNION ALL
SELECT 'ic_tasks', COUNT(*),
    COUNT(*) FILTER (WHERE task_title IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL)
FROM staging.ic_tasks
UNION ALL
SELECT 'ic_communications', COUNT(*),
    COUNT(*) FILTER (WHERE communication_body IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL)
FROM staging.ic_communications
UNION ALL
SELECT 'ic_postal_mail', COUNT(*),
    COUNT(*) FILTER (WHERE postal_mail_body IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL)
FROM staging.ic_postal_mail;
```

---

### Checkpoint 5: Phase 6 Complete (Validation & QA)
**When:** After running all validation queries and manual review
**Criteria:**
- [ ] All validation queries pass (no data quality issues)
- [ ] Data quality report generated and reviewed
- [ ] All orphaned records reviewed and approved
- [ ] Sample records manually spot-checked (10 per type, 70 total)
- [ ] Stakeholder sign-off obtained for proceeding to API phase

**Validation Report:** `data_quality_report.md` (to be created in Phase 6)

---

### Checkpoint 6: Phase 7 Complete (API Preparation)
**When:** After API test push succeeds
**Criteria:**
- [ ] All 7 Jinja templates created and tested
- [ ] API payloads built and stored in `hubspot_api_payload` JSONB columns
- [ ] Test push to HubSpot sandbox succeeded (100 records)
- [ ] No API errors in test push
- [ ] Engagement records visible in HubSpot UI
- [ ] API pusher script handles errors gracefully
- [ ] Rate limiting working (10 req/sec max)

**Test Report:** `api_test_report.md` (to be created in Phase 7)

---

### Checkpoint 7: Phase 8 Complete (Production API Push)
**When:** After production push finishes
**Criteria:**
- [ ] All records pushed (api_push_status = 'pushed')
- [ ] Push success rate > 98%
- [ ] Failed records logged and reviewed
- [ ] `hubspot_engagement_id` populated for successful pushes
- [ ] Engagement records verified in HubSpot production UI
- [ ] Completion report generated

**Completion Report:** `production_push_report.md` (to be created in Phase 8)

---

## 🔄 Rollback Procedures

### Rollback Phase 2 (Database Setup)
**If:** DDL script execution fails or creates incorrect schema

**Steps:**
```sql
-- Drop all views
DROP VIEW IF EXISTS staging.v_ic_postal_mail_staging CASCADE;
DROP VIEW IF EXISTS staging.v_ic_communications_staging CASCADE;
DROP VIEW IF EXISTS staging.v_ic_tasks_staging CASCADE;
DROP VIEW IF EXISTS staging.v_ic_notes_staging CASCADE;
DROP VIEW IF EXISTS staging.v_ic_meetings_staging CASCADE;
DROP VIEW IF EXISTS staging.v_ic_emails_staging CASCADE;
DROP VIEW IF EXISTS staging.v_ic_calls_staging CASCADE;
DROP VIEW IF EXISTS staging.v_ic_derivative_coverage CASCADE;
DROP VIEW IF EXISTS staging.v_ic_activities_summary CASCADE;

-- Drop all derivative tables (CASCADE will remove FKs)
DROP TABLE IF EXISTS staging.ic_postal_mail CASCADE;
DROP TABLE IF EXISTS staging.ic_communications CASCADE;
DROP TABLE IF EXISTS staging.ic_tasks CASCADE;
DROP TABLE IF EXISTS staging.ic_notes CASCADE;
DROP TABLE IF EXISTS staging.ic_meetings CASCADE;
DROP TABLE IF EXISTS staging.ic_emails CASCADE;
DROP TABLE IF EXISTS staging.ic_calls CASCADE;

-- Drop master table
DROP TABLE IF EXISTS staging.ic_communication_master CASCADE;

-- Drop function
DROP FUNCTION IF EXISTS staging.classify_activity_type(VARCHAR, VARCHAR, INTEGER);
```

**Re-execute:** Run DDL scripts again after fixing issues.

---

### Rollback Phase 3 (Master Table Loading)
**If:** Data load fails or data quality issues discovered

**Steps:**
```sql
-- Truncate master table (preserves structure)
TRUNCATE TABLE staging.ic_communication_master CASCADE;

-- Verify empty
SELECT COUNT(*) FROM staging.ic_communication_master; -- Should return 0
```

**Re-execute:** Fix CSV data or loading script, then reload.

---

### Rollback Phase 4 (FK Resolution)
**If:** FK resolution produces incorrect mappings

**Steps:**
```sql
-- Reset HubSpot FK columns to NULL
UPDATE staging.ic_communication_master
SET
    hubspot_contact_id = NULL,
    hubspot_company_id = NULL,
    hubspot_deal_id = NULL,
    is_orphaned = FALSE,
    last_updated = NOW();

-- Clear reconciliation log
DELETE FROM staging.reconciliation_log
WHERE operation = 'FK_RESOLUTION'
  AND entity_type IN ('contact', 'company', 'deal');
```

**Re-execute:** Fix FK resolution queries, then re-run.

---

### Rollback Phase 5 (Derivative Population)
**If:** Transformation logic produces incorrect data

**Steps:**
```sql
-- Truncate all derivative tables
TRUNCATE TABLE staging.ic_calls CASCADE;
TRUNCATE TABLE staging.ic_emails CASCADE;
TRUNCATE TABLE staging.ic_meetings CASCADE;
TRUNCATE TABLE staging.ic_notes CASCADE;
TRUNCATE TABLE staging.ic_tasks CASCADE;
TRUNCATE TABLE staging.ic_communications CASCADE;
TRUNCATE TABLE staging.ic_postal_mail CASCADE;

-- Verify empty
SELECT
    (SELECT COUNT(*) FROM staging.ic_calls) as calls,
    (SELECT COUNT(*) FROM staging.ic_emails) as emails,
    (SELECT COUNT(*) FROM staging.ic_meetings) as meetings,
    (SELECT COUNT(*) FROM staging.ic_notes) as notes,
    (SELECT COUNT(*) FROM staging.ic_tasks) as tasks,
    (SELECT COUNT(*) FROM staging.ic_communications) as communications,
    (SELECT COUNT(*) FROM staging.ic_postal_mail) as postal_mail;
-- All should return 0
```

**Re-execute:** Fix transformation views, then re-run population script.

---

### Rollback Phase 7 (API Test Push)
**If:** Test push creates incorrect engagements in HubSpot

**Steps:**
1. **In HubSpot Sandbox UI:** Delete test engagement records manually or via API
   ```python
   # Python example using HubSpot API
   import requests

   # Get test engagement IDs
   test_engagement_ids = [...]  # List of IDs to delete

   for engagement_id in test_engagement_ids:
       response = requests.delete(
           f'https://api.hubapi.com/engagements/v1/engagements/{engagement_id}',
           headers={'Authorization': f'Bearer {HUBSPOT_TOKEN}'}
       )
   ```

2. **In PostgreSQL:** Reset API push status
   ```sql
   -- Reset api_push_status in derivative tables
   UPDATE staging.ic_calls
   SET api_push_status = 'pending',
       hubspot_engagement_id = NULL,
       pushed_at = NULL,
       error_message = NULL;

   -- Repeat for all 7 derivative tables
   ```

**Re-execute:** Fix Jinja templates or API pusher script, then re-test.

---

### Rollback Phase 8 (Production Push) ⚠️ CRITICAL
**If:** Production push creates incorrect engagements (AVOID IF POSSIBLE)

**WARNING:** Production rollback is complex and may not be fully reversible.

**Steps:**
1. **STOP IMMEDIATELY:** Kill the API pusher script
2. **Assess Damage:** Query how many records were pushed
   ```sql
   SELECT
       api_push_status,
       COUNT(*) as count
   FROM staging.ic_calls
   GROUP BY api_push_status;
   -- Repeat for all 7 tables
   ```
3. **Decision Point:**
   - If < 100 records pushed → Manual deletion in HubSpot UI feasible
   - If > 100 records pushed → Contact HubSpot support for bulk deletion assistance

4. **Partial Rollback:** Reset only failed/pending records
   ```sql
   -- DO NOT reset successfully pushed records
   UPDATE staging.ic_calls
   SET api_push_status = 'pending',
       error_message = 'Production push aborted - manual review required'
   WHERE api_push_status IN ('pending', 'failed');
   ```

5. **Post-Mortem:** Document what went wrong, fix root cause before retry.

**Prevention:** ALWAYS test thoroughly in sandbox (Phase 7) before production push.

---

## 📅 Timeline & Milestones

### Week 1 (Current)
- ✅ Day 1: Planning & Design complete
- 🚧 Day 1: Database setup (Phase 2)
- 📅 Day 2-3: Master table loading (Phase 3) - *awaiting CSV*
- 📅 Day 4: FK resolution (Phase 4)
- 📅 Day 5: Derivative population (Phase 5)

### Week 2
- 📅 Day 1-2: Validation & QA (Phase 6)
- 📅 Day 3-5: API preparation (Phase 7) - *awaiting HubSpot token*

### Week 3+
- 📅 TBD: Production API push (Phase 8) - *subject to stakeholder approval*

---

## 📊 Metrics & KPIs

### Data Quality Metrics
| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| Master table load success | 100% | - | 📅 Pending |
| FK resolution rate (contacts) | >95% | - | 📅 Pending |
| FK resolution rate (companies) | >95% | - | 📅 Pending |
| FK resolution rate (deals) | >95% | - | 📅 Pending |
| Orphaned record rate | <5% | - | 📅 Pending |
| Derivative table coverage | 100% | - | 📅 Pending |
| Required fields populated | >99% | - | 📅 Pending |

### API Push Metrics (Future)
| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| API test push success rate | 100% | - | 📅 Future |
| API production push success rate | >98% | - | 📅 Future |
| Average API response time | <500ms | - | 📅 Future |
| Rate limit compliance | 100% | - | 📅 Future |

---

## 🔧 Tools & Scripts

### Created Scripts

#### Phase 2: Database Setup
- `hubspot_activities_staging_ddl.sql` - ✅ Creates tables/indexes/constraints/functions
- `hubspot_activities_staging_views.sql` - ✅ Creates transformation views
- `execute_phase2_database_setup.py` - ✅ Automated Phase 2 execution & validation
- `validate_phase2.sql` - 📅 To be created (optional, validation in Python script)

#### Phase 3: Master Table Loading
- `load_communications_master.py` - ✅ CSV loading with classification & validation
- `validate_phase3.sql` - 📅 To be created (optional, validation in Python script)

#### Phase 4: FK Resolution
- `resolve_foreign_keys.sql` - ✅ Complete FK resolution with logging & validation
- `validate_phase4.sql` - 📅 To be created (optional, validation in SQL script)

#### Phase 5: Derivative Population
- Population script embedded in `hubspot_activities_staging_views.sql` ✅
- `validate_phase5.sql` - 📅 To be created (optional, views query validation)

#### Phase 6: Validation & QA
- `validate_all.sql` - 📅 To be created (combines all validation queries)
- `generate_data_quality_report.py` - 📅 To be created

#### Phase 7: API Preparation
- `templates/hubspot_call_payload.j2` - 📅 To be created
- `templates/hubspot_email_payload.j2` - 📅 To be created
- `templates/hubspot_meeting_payload.j2` - 📅 To be created
- `templates/hubspot_note_payload.j2` - 📅 To be created
- `templates/hubspot_task_payload.j2` - 📅 To be created
- `templates/hubspot_communication_payload.j2` - 📅 To be created
- `templates/hubspot_postal_mail_payload.j2` - 📅 To be created
- `build_api_payloads.py` - 📅 To be created
- `push_to_hubspot.py` - 📅 To be created

---

## 📞 Contacts & Escalation

### Project Team
- **Technical Lead:** [TBD]
- **Database Admin:** [TBD]
- **HubSpot Admin:** [TBD]
- **QA Lead:** [TBD]

### Escalation Path
1. **Data Quality Issues:** Technical Lead
2. **Database Access/Performance:** Database Admin
3. **HubSpot API Issues:** HubSpot Admin
4. **Timeline Concerns:** Project Manager

---

## 📚 References

### Documentation
- [HUBSPOT_ACTIVITIES_STAGING_ARCHITECTURE.md](./HUBSPOT_ACTIVITIES_STAGING_ARCHITECTURE.md)
- [HUBSPOT_ACTIVITIES_PROJECT_SUMMARY.md](./HUBSPOT_ACTIVITIES_PROJECT_SUMMARY.md)
- [ACTIVITY_TYPE_CLASSIFICATION_DECISION_TREE.md](./ACTIVITY_TYPE_CLASSIFICATION_DECISION_TREE.md)
- [hubspot_activities_staging_erd.graphml](./hubspot_activities_staging_erd.graphml)

### SQL Scripts
- [hubspot_activities_staging_ddl.sql](./hubspot_activities_staging_ddl.sql)
- [hubspot_activities_staging_views.sql](./hubspot_activities_staging_views.sql)

### External Resources
- [HubSpot Activities Documentation](https://knowledge.hubspot.com/records/manually-log-activities-on-records)
- [HubSpot Engagements API](https://developers.hubspot.com/docs/api/crm/engagements)
- [HubSpot Private Apps](https://developers.hubspot.com/docs/api/private-apps)

---

## 📝 Change Log

| Date | Phase | Change | Author |
|------|-------|--------|--------|
| 2025-11-13 | Planning | Initial PLAN.md created | Claude |
| 2025-11-13 | Phase 2 | Database setup initiated | Claude |

---

## ✅ Sign-Off

### Phase 1: Planning & Design
- **Completed By:** Claude
- **Date:** 2025-11-13
- **Approved By:** [Pending stakeholder review]

### Phase 2: Database Setup
- **Started By:** Claude
- **Date:** 2025-11-13
- **Status:** 🚧 IN PROGRESS

---

**Last Updated:** 2025-11-13
**Next Review:** After Phase 2 completion
**Document Owner:** Technical Lead
