# HubSpot Activities Staging Decomposition - Project Summary

**Project:** IC-D-LOAD - HubSpot CRM Data Integration
**Phase:** Staging Layer Design & Architecture
**Status:** ✅ Planning Complete - Ready for Implementation
**Date:** 2025-11-13

---

## Executive Summary

This project successfully designed a comprehensive staging architecture for decomposing a monolithic communications table into 7 distinct HubSpot activity types. The system transforms legacy CRM communication records into HubSpot-compliant engagement objects ready for API integration.

### Key Achievement

Designed a **master-derivative staging pattern** that:
- Preserves data integrity through cascade relationships
- Ensures HubSpot API schema compliance
- Enables intelligent activity type classification
- Supports complex data transformations via SQL views

---

## Deliverables

### 1. Architecture Documentation

**File:** `HUBSPOT_ACTIVITIES_STAGING_ARCHITECTURE.md` (45KB, 1,100+ lines)

Comprehensive system design covering:
- ✅ 8 staging table schemas (1 master + 7 derivatives)
- ✅ Property mapping strategies for all activity types
- ✅ Foreign key resolution patterns
- ✅ API integration architecture (Jinja templates)
- ✅ Data flow pipeline (5 stages)
- ✅ Validation queries and success metrics

**Highlights:**
- 22-column source schema mapped to 7 activity types
- Master-first loading pattern to ensure referential integrity
- JSONB columns for flexible API payload storage

---

### 2. Entity-Relationship Diagram

**File:** `hubspot_activities_staging_erd.graphml` (24KB GraphML)

Visual data model showing:
- ✅ Source → Master → Derivatives data flow
- ✅ 4 processing engines (Classification, FK Resolution, Property Mapping, API Builder)
- ✅ 38 relationships between 18 entities
- ✅ Color-coded by layer (source, staging, reference, process, API)

**Viewable in:** yEd, Gephi, Cytoscape, or any GraphML-compatible tool

---

### 3. SQL Database Schema

**File:** `hubspot_activities_staging_ddl.sql` (26KB, 800+ lines)

Production-ready PostgreSQL DDL:
- ✅ Master table: `staging.ic_communication_master`
- ✅ 7 derivative tables with HubSpot schemas
- ✅ 40+ indexes for query performance
- ✅ CHECK constraints for data validation
- ✅ CASCADE DELETE for referential integrity
- ✅ Helper function: `classify_activity_type()`
- ✅ 2 validation views

**Key Features:**
- UNIQUE constraints on master FK in derivatives (1:1 relationship)
- JSONB columns for API payloads
- Audit fields (created_at, last_updated, pushed_at)

---

### 4. SQL Transformation Views

**File:** `hubspot_activities_staging_views.sql` (19KB, 500+ lines)

SQL views for populating derivative tables:
- ✅ 7 staging views (one per activity type)
- ✅ Complex property transformations (date calculations, string concatenation, enum mapping)
- ✅ UPSERT-ready materialization queries
- ✅ Validation queries for data quality checks
- ✅ Transaction-wrapped population script

**Transformation Examples:**
- Email direction logic (inbound vs outbound)
- Call duration calculation (milliseconds from timestamp diff)
- Meeting end time inference (if NULL, add 1 hour)
- Task overdue flag computation

---

### 5. Classification Decision Tree

**File:** `ACTIVITY_TYPE_CLASSIFICATION_DECISION_TREE.md` (15KB)

Complete logic for activity type determination:
- ✅ Algorithm pseudocode in Python
- ✅ Visual decision tree diagram
- ✅ 50+ type mappings (case-insensitive, trimmed)
- ✅ Case-to-Task conversion rules (priority threshold)
- ✅ 30+ test cases with expected results
- ✅ Edge case handling (NULL, whitespace, multi-word)

**Priority Order:**
1. Explicit type mapping (e.g., 'call' → 'calls')
2. Case-to-Task conversion (if CaseId present + High priority)
3. Default fallback → 'notes'

---

## System Architecture Overview

### Data Flow Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│ Stage 1: Load Master Table                                   │
│ ────────────────────────────                                 │
│ Input:  Communications CSV (22 columns)                      │
│ Output: staging.ic_communication_master                      │
│ Logic:  Classify activity type, set task eligibility flag    │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│ Stage 2: Resolve Foreign Keys                                │
│ ─────────────────────────────                                │
│ Process: Join with reconciliation tables                     │
│ Update:  hubspot_contact_id, hubspot_company_id, hubspot_deal_id │
│ Log:     Unresolved references, orphaned records             │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│ Stage 3: Populate Derivative Tables                          │
│ ───────────────────────────────────                          │
│ Input:  Master table (WHERE derived_activity_type = X)       │
│ Views:  v_ic_calls_staging, v_ic_emails_staging, etc.        │
│ Output: 7 derivative staging tables                          │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│ Stage 4: Validation & QA                                     │
│ ────────────────────────                                     │
│ Check:  Record counts, required fields, date logic           │
│ Report: v_ic_derivative_coverage, v_ic_activities_summary    │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│ Stage 5: API Integration (Future Phase)                      │
│ ───────────────────────────────────────                      │
│ Build:  JSON payloads via Jinja templates                    │
│ Push:   POST /engagements/v1/engagements                     │
│ Update: hubspot_engagement_id, api_push_status               │
└──────────────────────────────────────────────────────────────┘
```

### Activity Type Breakdown

| Activity Type | HubSpot Object | Derivative Table | Use Cases |
|---------------|----------------|------------------|-----------|
| **calls** | Calls | `staging.ic_calls` | Phone calls, VoIP, Zoom |
| **emails** | Emails | `staging.ic_emails` | Email communications |
| **meetings** | Meetings | `staging.ic_meetings` | Appointments, demos, visits |
| **notes** | Notes | `staging.ic_notes` | General notes, comments |
| **tasks** | Tasks | `staging.ic_tasks` | To-dos, follow-ups, high-priority cases |
| **communications** | Communications | `staging.ic_communications` | SMS, LinkedIn, WhatsApp |
| **postal_mail** | Postal Mail | `staging.ic_postal_mail` | Physical mail correspondence |

---

## Database Schema Summary

### Master Table: `staging.ic_communication_master`

**Purpose:** Central staging table loaded FIRST, preserving all source data

**Key Columns:**
- 22 source fields (comm_*, person_*, company_*)
- `derived_activity_type` - Classified activity type
- `hubspot_contact_id`, `hubspot_company_id`, `hubspot_deal_id` - Resolved FKs
- `staging_status`, `api_push_status` - Processing metadata
- `is_orphaned` - Flag for records with no associations

**Constraints:**
- `UNIQUE(comm_communicationid)` - Prevent duplicates
- `CHECK(derived_activity_type IN (...))` - Valid activity types

---

### Derivative Tables (7 total)

**Common Pattern:**

```sql
CREATE TABLE staging.ic_{activity_type} (
    id SERIAL PRIMARY KEY,
    ic_communication_master_id INTEGER NOT NULL
        REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- General activity properties
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- Type-specific properties (HubSpot schema)
    ...

    -- Associations
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- API fields
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50),
    hubspot_engagement_id BIGINT,

    -- Constraints
    UNIQUE(ic_communication_master_id)
);
```

**Relationship:** 1:1 between master and each derivative (enforced by UNIQUE constraint)

---

## Property Mapping Examples

### Calls

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Subject` | `call_title` | Direct, with fallback |
| `Comm_Note` | `call_notes` | Direct |
| `Comm_Action` | `call_direction` | Map: 'Inbound'→INBOUND, 'Outbound'→OUTBOUND |
| `Comm_ToDateTime - Comm_DateTime` | `call_duration_ms` | Calculate milliseconds |
| `Comm_Status` | `call_status` | Map to HubSpot enum (COMPLETED, NO_ANSWER, etc.) |

### Emails

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Subject` | `email_subject` | Direct |
| `Comm_Note` | `email_body` | Direct |
| `Comm_Action` | `email_direction` | Map: 'Inbound'→INCOMING, 'Outbound'→OUTGOING |
| `Person_EmailAddress` / `Comm_Email_Clean` | `email_from_address` | Logic based on direction |
| `Comm_Email_Clean` / `Person_EmailAddress` | `email_to_address` | Logic based on direction |

### Tasks (Including Case Conversion)

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Subject` | `task_title` | Direct |
| `Comm_Priority` | `task_priority` | Map: 'High'→HIGH, 'Medium'→MEDIUM, 'Low'→LOW |
| `Comm_Status` | `task_status` | Map: 'Completed'→COMPLETED, else NOT_STARTED |
| `Comm_ToDateTime` OR `Comm_DateTime + 1 day` | `due_date` | Prefer ToDateTime, fallback +1 day |
| `due_date < NOW() AND status != COMPLETED` | `is_overdue` | Computed boolean |
| `Comm_CaseId` | `source_case_id` | Direct (for case tracking) |

---

## Key Design Patterns

### 1. Master-First Loading

**Rationale:** Derivative tables reference master via FK, so master MUST exist first.

**Process:**
1. Load CSV → Master table (with classification)
2. Resolve FKs in master table
3. Populate derivatives FROM master (via views)

**Benefit:** Single source of truth, referential integrity via CASCADE DELETE

---

### 2. Activity Type Classification Function

**SQL Function:** `staging.classify_activity_type(comm_type, comm_priority, comm_caseid)`

**Returns:** VARCHAR (one of 7 activity types)

**Logic:**
1. Exact match on normalized `comm_type` (e.g., 'call' → 'calls')
2. Case-to-Task conversion (if `comm_caseid` IS NOT NULL AND priority = High)
3. Default to 'notes'

**Usage:**
```sql
INSERT INTO staging.ic_communication_master (derived_activity_type, ...)
SELECT staging.classify_activity_type(Comm_Type, Comm_Priority, Comm_CaseId), ...
FROM source_csv;
```

---

### 3. SQL Views for Transformation

**Pattern:** Create VIEW for each activity type, then INSERT from VIEW

**Example:**
```sql
-- View transforms master data
CREATE VIEW staging.v_ic_calls_staging AS
SELECT
    m.id as ic_communication_master_id,
    m.comm_datetime as activity_date,
    COALESCE(m.comm_subject, 'Call - ' || m.person_firstname) as call_title,
    CASE WHEN m.comm_action = 'Inbound' THEN 'INBOUND' ELSE 'OUTBOUND' END as call_direction,
    ...
FROM staging.ic_communication_master m
WHERE m.derived_activity_type = 'calls';

-- Materialize into table
INSERT INTO staging.ic_calls SELECT * FROM staging.v_ic_calls_staging;
```

**Benefits:**
- Testable transformations (query view directly)
- Reusable logic
- UPSERT support (ON CONFLICT DO UPDATE)

---

### 4. Foreign Key Resolution via Reconciliation Tables

**Pattern:** Join master table with existing reconciliation tables to resolve HubSpot IDs

**Query:**
```sql
-- Resolve contact IDs
UPDATE staging.ic_communication_master m
SET hubspot_contact_id = c.hubspot_contact_id
FROM staging.contacts_reconciliation c
WHERE m.person_id = c.legacy_contact_id;

-- Similar for companies and deals
```

**Orphaned Records:** Communications without resolved FKs are flagged but NOT discarded

```sql
UPDATE staging.ic_communication_master
SET is_orphaned = TRUE
WHERE hubspot_contact_id IS NULL
  AND hubspot_company_id IS NULL
  AND hubspot_deal_id IS NULL;
```

**Decision:** Push orphaned records to HubSpot (they appear in timeline without associations)

---

### 5. JSONB API Payload Storage

**Pattern:** Pre-build HubSpot API JSON payloads in staging tables

**Column:** `hubspot_api_payload JSONB`

**Benefits:**
- Decouple transformation from API push
- Enable payload review before push
- Support retry logic (payload persisted)
- Audit trail (what was sent)

**Future Implementation:** Jinja templates to build payloads

---

## Validation Strategies

### 1. Coverage Check

**Query:** Ensure all master records have derivative records

```sql
SELECT * FROM staging.v_ic_derivative_coverage;
```

**Expected:** `missing_count = 0` for all activity types

---

### 2. Required Fields Check

**Query:** Verify required fields populated

```sql
SELECT
    'ic_calls' as table_name,
    COUNT(*) FILTER (WHERE call_title IS NULL) as missing_title,
    COUNT(*) FILTER (WHERE activity_date IS NULL) as missing_date
FROM staging.ic_calls;
```

**Expected:** All counts = 0

---

### 3. Association Coverage

**Query:** Check orphaned records percentage

```sql
SELECT
    derived_activity_type,
    COUNT(*) FILTER (WHERE is_orphaned = TRUE) as orphaned_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_orphaned = TRUE) / COUNT(*), 2) as orphaned_pct
FROM staging.ic_communication_master
GROUP BY derived_activity_type;
```

**Threshold:** < 5% orphaned acceptable (warn if higher)

---

### 4. Date Logic Validation

**Query:** Ensure meeting end time ≥ start time

```sql
SELECT COUNT(*)
FROM staging.ic_meetings
WHERE meeting_end_time < meeting_start_time;
```

**Expected:** 0 (enforced by CHECK constraint)

---

### 5. Distribution Analysis

**Query:** Activity type distribution

```sql
SELECT
    derived_activity_type,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
FROM staging.ic_communication_master
GROUP BY derived_activity_type
ORDER BY count DESC;
```

**Review:** Flag if one type dominates (>80%) or is missing (0%)

---

## Implementation Checklist

### Phase 1: Database Setup ✅ (Planning Complete)
- [x] Create master table DDL
- [x] Create 7 derivative table DDLs
- [x] Create classification function
- [x] Create validation views
- [x] Write transformation views

### Phase 2: Master Table Loading (Next Steps)
- [ ] Prepare communications CSV data
- [ ] Load CSV into master table
- [ ] Apply classification logic
- [ ] Validate master table data quality
- [ ] Review unrecognized `Comm_Type` values

### Phase 3: FK Resolution (Next Steps)
- [ ] Run contact FK resolution queries
- [ ] Run company FK resolution queries
- [ ] Run deal FK resolution queries
- [ ] Log unresolved references
- [ ] Review orphaned communications
- [ ] Update reconciliation tables if needed

### Phase 4: Derivative Population (Next Steps)
- [ ] Execute population script (all 7 tables)
- [ ] Run coverage validation queries
- [ ] Check required fields populated
- [ ] Verify association counts
- [ ] Review transformation accuracy (sample records)
- [ ] Fix any data quality issues

### Phase 5: API Preparation (Future)
- [ ] Create Jinja templates (7 activity types)
- [ ] Build API payloads (populate JSONB column)
- [ ] Test with HubSpot sandbox account
- [ ] Implement Python API pusher script
- [ ] Add error handling & retry logic
- [ ] Set up rate limiting (10 req/sec)

---

## File Structure

```
IC-D-LOAD/
├── HUBSPOT_ACTIVITIES_STAGING_ARCHITECTURE.md     # 📘 Comprehensive architecture doc
├── HUBSPOT_ACTIVITIES_PROJECT_SUMMARY.md          # 📋 This executive summary
├── ACTIVITY_TYPE_CLASSIFICATION_DECISION_TREE.md  # 🌳 Classification logic doc
├── hubspot_activities_staging_ddl.sql             # 🛢️  Database schema DDL
├── hubspot_activities_staging_views.sql           # 🔍 SQL transformation views
├── hubspot_activities_staging_erd.graphml         # 📊 Entity-relationship diagram
├── property_mapping_config.py                     # 🗺️  Existing property mappings
├── staging_schema_manager.py                      # 🛠️  Existing staging manager
└── ... (other project files)
```

**Total Documentation:** 130KB, 3,000+ lines across 6 files

---

## Success Metrics

| Metric | Target | Validation Query |
|--------|--------|------------------|
| Master table load | 100% | `SELECT COUNT(*) FROM ic_communication_master` |
| FK resolution rate | >95% | `SELECT COUNT(*) FILTER (WHERE hubspot_contact_id IS NOT NULL) / COUNT(*) FROM ic_communication_master` |
| Derivative coverage | 100% | `SELECT * FROM v_ic_derivative_coverage WHERE missing_count > 0` (should be empty) |
| Required fields populated | >99% | Check NULL counts for required fields per table |
| Orphaned records | <5% | `SELECT AVG(is_orphaned::INT) FROM ic_communication_master` |
| API push success (future) | >98% | `SELECT COUNT(*) FILTER (WHERE api_push_status = 'pushed') / COUNT(*) FROM ic_calls` |

---

## Technical Specifications

### Database
- **Platform:** PostgreSQL 12+
- **Schema:** `staging`
- **Tables:** 8 (1 master + 7 derivatives)
- **Indexes:** 40+
- **Constraints:** UNIQUE, CHECK, FK with CASCADE DELETE

### Data Volume (Estimates)
- **Source records:** ~10,000 - 100,000 communications
- **Master table:** 1:1 with source
- **Derivative tables:** Sum of derivatives = master count (1:1 split by type)
- **Storage:** ~500MB - 2GB (with indexes and JSONB)

### Performance
- **Load time:** ~5-10 minutes for 50k records (including FK resolution)
- **Query performance:** < 1 second for validation queries (with indexes)
- **API push:** ~90 minutes for 50k records (at 10 req/sec, batches of 100)

---

## Dependencies

### Existing Tables (Required)
- `staging.contacts_reconciliation` - For resolving `person_id` → `hubspot_contact_id`
- `staging.companies_reconciliation` - For resolving `company_id` → `hubspot_company_id`
- `staging.deals_reconciliation` - For resolving `comm_opportunityid` → `hubspot_deal_id`

### Future Dependencies (API Phase)
- HubSpot API credentials (API key or OAuth token)
- Jinja2 library (Python) for template rendering
- Rate limiting library (e.g., `ratelimit` or custom)
- Retry logic (e.g., `tenacity` library)

---

## Risk Mitigation

### Risk 1: Unrecognized Comm_Type Values
**Impact:** Records classified as 'notes' by default, may not match user intent

**Mitigation:**
- Query to find unrecognized types before migration
- Review with business stakeholders
- Add to TYPE_MAP as needed
- Worst case: Manually reclassify in HubSpot after push

---

### Risk 2: High Orphaned Record Rate
**Impact:** Engagements not associated with contacts/companies/deals, less useful in HubSpot

**Mitigation:**
- Target < 5% orphaned rate
- Review FK resolution queries
- Update reconciliation tables if needed
- HubSpot allows orphaned engagements (appear in timeline)

---

### Risk 3: Data Transformation Errors
**Impact:** Invalid data in derivative tables, API push failures

**Mitigation:**
- Extensive validation queries (required fields, date logic, enum values)
- Test with sample data before full load
- CHECK constraints prevent invalid enum values
- Review sample records manually

---

### Risk 4: HubSpot API Rate Limits
**Impact:** Slow API push, potential timeouts

**Mitigation:**
- Batch records (100 per request)
- Rate limiting (10 req/sec)
- Retry logic with exponential backoff
- Monitor API usage quotas

---

## Next Steps

### Immediate Actions
1. **Review & Approve Architecture** - Stakeholder sign-off on design
2. **Prepare Source Data** - Export communications CSV with 22 columns
3. **Execute DDL Scripts** - Create staging tables in PostgreSQL

### Short-Term (Phase 2-4)
4. **Load Master Table** - Import CSV, classify activity types
5. **Resolve Foreign Keys** - Update HubSpot IDs
6. **Populate Derivatives** - Run transformation views
7. **Validate Data** - Run all validation queries, review results

### Long-Term (Phase 5)
8. **Build API Integration** - Create Jinja templates, implement pusher script
9. **Test in Sandbox** - Push sample records to HubSpot test account
10. **Production Push** - Full migration to HubSpot production
11. **Monitor & Verify** - Check engagement records in HubSpot UI

---

## Questions & Clarifications Needed

Before implementation, clarify:

1. **User Assignment** - Who should be `activity_assigned_to` and `activity_created_by`? (currently NULL, requires user mapping)
2. **Phone Number Extraction** - How to extract `from_number` and `to_number` for calls? (currently NULL, may need regex or separate fields)
3. **Meeting Location** - Extract from `comm_note` or leave NULL? (currently NULL)
4. **Ambiguous Type Values** - Review 'Mail', 'Text', 'Message' mappings with stakeholders
5. **API Credentials** - Obtain HubSpot API key or OAuth token for production
6. **Migration Timeline** - When to execute? (recommend off-hours for production push)

---

## Conclusion

This project has successfully completed the planning and design phase for HubSpot activities staging decomposition. All architecture documents, database schemas, transformation logic, and validation queries are production-ready.

**Key Success Factors:**
- ✅ Master-first loading ensures data integrity
- ✅ HubSpot schema compliance via exact property naming
- ✅ Flexible classification logic handles edge cases
- ✅ SQL views enable testable, reusable transformations
- ✅ Comprehensive validation catches data quality issues
- ✅ JSONB payloads decouple staging from API push

**Ready for Implementation:** Database setup (Phase 1) can proceed immediately. Subsequent phases (load, FK resolution, population) await source data preparation.

---

**Project Status:** ✅ **READY FOR IMPLEMENTATION**

**Next Milestone:** Execute DDL scripts and load master table

**Contact:** IC-D-LOAD Project Team

**Last Updated:** 2025-11-13
