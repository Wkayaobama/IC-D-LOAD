# HubSpot Activities Staging Decomposition Architecture
## System Design for Communications-to-Activities Transformation

**Version:** 1.0
**Date:** 2025-11-13
**Status:** Design Phase

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Overview](#system-overview)
3. [Data Model Architecture](#data-model-architecture)
4. [Staging Layer Design](#staging-layer-design)
5. [Property Mapping Strategy](#property-mapping-strategy)
6. [Activity Type Classification Logic](#activity-type-classification-logic)
7. [Foreign Key & Association Strategy](#foreign-key--association-strategy)
8. [Data Flow & Processing Pipeline](#data-flow--processing-pipeline)
9. [SQL Implementation Strategy](#sql-implementation-strategy)
10. [API Integration Architecture](#api-integration-architecture)
11. [GraphML Entity-Relationship Diagram](#graphml-entity-relationship-diagram)

---

## Executive Summary

This document outlines the comprehensive architecture for decomposing a monolithic `communications` master table into multiple HubSpot-compliant activity staging tables. The system transforms a single denormalized communications dataset into 7 distinct activity types (Calls, Emails, Meetings, Notes, Tasks, Communications, Postal Mail), each conforming to HubSpot's API schema requirements.

### Key Design Principles

1. **Master-First Loading**: The master staging table (`staging.ic_communication_master`) MUST be loaded before any derivative tables
2. **Schema Compliance**: All derivative tables strictly adhere to HubSpot's activity property naming conventions
3. **Intelligent Foreign Keys**: Each activity maintains references to master table, contacts, companies, and deals
4. **Type-Based Decomposition**: Activity type determined by `Comm_Type` field with intelligent fallback logic
5. **API-Ready Design**: Staging tables designed to map directly to HubSpot API payloads via Jinja templates

---

## System Overview

### Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1: SOURCE DATA                                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Communications Master Table (Denormalized)         │   │
│  │  - Comm_CommunicationId (PK)                        │   │
│  │  - 22 source columns from legacy CRM                │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  LAYER 2: MASTER STAGING TABLE (PostgreSQL)                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  staging.ic_communication_master                    │   │
│  │  - All source columns preserved                     │   │
│  │  - Enriched with FK references                      │   │
│  │  - Activity type classification metadata            │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  LAYER 3: DERIVATIVE STAGING TABLES (PostgreSQL)           │
│  ┌──────────────┬──────────────┬──────────────┬─────────┐ │
│  │ ic_calls     │ ic_emails    │ ic_meetings  │ ic_notes│ │
│  ├──────────────┼──────────────┼──────────────┼─────────┤ │
│  │ ic_tasks     │ ic_comms     │ ic_postal_mail│         │ │
│  └──────────────┴──────────────┴──────────────┴─────────┘ │
│  Each table: HubSpot activity schema + FK to master        │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  LAYER 4: API INTEGRATION (Future - Post Staging)          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Jinja Templates → HubSpot API Payloads             │   │
│  │  - POST /engagements/v1/engagements                 │   │
│  │  - Batch processing with rate limiting              │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Model Architecture

### Source Schema: Communications Master Table

The source data contains denormalized communication records with the following schema:

| Column Name | Data Type | Description | Nullable |
|------------|-----------|-------------|----------|
| `Comm_CommunicationId` | INTEGER | Primary key, unique communication ID | NO |
| `Comm_OpportunityId` | INTEGER | Foreign key to Deal/Opportunity | YES |
| `Comm_CaseId` | INTEGER | Foreign key to Case/Ticket | YES |
| `Comm_Type` | VARCHAR(100) | Communication type (Call, Email, Meeting, etc.) | NO |
| `Comm_Action` | VARCHAR(100) | Action type (Inbound, Outbound, Follow-up) | YES |
| `Comm_Status` | VARCHAR(100) | Status (Completed, Pending, Cancelled) | YES |
| `Comm_Priority` | VARCHAR(50) | Priority level (Low, Medium, High) | YES |
| `Comm_DateTime` | TIMESTAMP | Primary activity timestamp | NO |
| `Comm_ToDateTime` | TIMESTAMP | End timestamp (for meetings/calls) | YES |
| `Comm_Note` | TEXT | Body content of communication | YES |
| `Comm_Subject` | TEXT | Subject line/title | YES |
| `Comm_Email_Clean` | VARCHAR(500) | Extracted email address | YES |
| `Comm_CreatedDate` | TIMESTAMP | Record creation date | NO |
| `Comm_UpdatedDate` | TIMESTAMP | Last update date | YES |
| `Comm_OriginalDateTime` | TIMESTAMP | Original scheduled date | YES |
| `Comm_OriginalToDateTime` | TIMESTAMP | Original scheduled end date | YES |
| `Person_Id` | INTEGER | Foreign key to Contact | YES |
| `Person_FirstName` | VARCHAR(200) | Contact first name | YES |
| `Person_LastName` | VARCHAR(200) | Contact last name | YES |
| `Person_EmailAddress` | VARCHAR(500) | Contact email | YES |
| `Company_Id` | INTEGER | Foreign key to Company | YES |
| `Company_Name` | VARCHAR(500) | Company name | YES |

**Total Source Columns:** 22

---

## Staging Layer Design

### Master Staging Table: `staging.ic_communication_master`

This is the **FIRST** table to be loaded. It preserves all source data and adds metadata for classification.

#### Table Structure

```sql
CREATE TABLE staging.ic_communication_master (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Legacy Communication Fields (Direct mapping from source)
    comm_communicationid INTEGER NOT NULL UNIQUE,
    comm_opportunityid INTEGER,
    comm_caseid INTEGER,
    comm_type VARCHAR(100) NOT NULL,
    comm_action VARCHAR(100),
    comm_status VARCHAR(100),
    comm_priority VARCHAR(50),
    comm_datetime TIMESTAMP NOT NULL,
    comm_todatetime TIMESTAMP,
    comm_note TEXT,
    comm_subject TEXT,
    comm_email_clean VARCHAR(500),
    comm_createddate TIMESTAMP NOT NULL,
    comm_updateddate TIMESTAMP,
    comm_originaldatetime TIMESTAMP,
    comm_originaltodatetime TIMESTAMP,

    -- Person/Contact Fields
    person_id INTEGER,
    person_firstname VARCHAR(200),
    person_lastname VARCHAR(200),
    person_emailaddress VARCHAR(500),

    -- Company Fields
    company_id INTEGER,
    company_name VARCHAR(500),

    -- Classification & Metadata (Computed)
    derived_activity_type VARCHAR(50) NOT NULL,  -- calls, emails, meetings, notes, tasks, communications, postal_mail
    is_task_eligible BOOLEAN DEFAULT FALSE,      -- TRUE if should create task (based on priority threshold)

    -- Foreign Key References (To be populated via lookups)
    hubspot_contact_id BIGINT,                   -- From staging.contacts_reconciliation
    hubspot_company_id BIGINT,                   -- From staging.companies_reconciliation
    hubspot_deal_id BIGINT,                      -- From staging.deals_reconciliation

    -- Processing Metadata
    staging_status VARCHAR(50) DEFAULT 'pending',  -- pending, processed, error
    api_push_status VARCHAR(50),                   -- not_pushed, pushed, failed
    api_engagement_id BIGINT,                      -- HubSpot engagement ID after API push
    error_message TEXT,

    -- Audit Fields
    created_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_ic_comm_master_comm_id ON staging.ic_communication_master(comm_communicationid);
CREATE INDEX idx_ic_comm_master_type ON staging.ic_communication_master(derived_activity_type);
CREATE INDEX idx_ic_comm_master_person ON staging.ic_communication_master(person_id);
CREATE INDEX idx_ic_comm_master_company ON staging.ic_communication_master(company_id);
CREATE INDEX idx_ic_comm_master_status ON staging.ic_communication_master(staging_status);
```

**Key Design Decisions:**

1. **Preserve All Source Fields**: No data loss during transformation
2. **Add Classification Field**: `derived_activity_type` computed during load
3. **HubSpot FK References**: Populated by joining with existing reconciliation tables
4. **Dual Status Tracking**: `staging_status` (internal) + `api_push_status` (external)
5. **Task Eligibility Flag**: Pre-computed to handle Case→Task conversion

---

### Derivative Staging Tables

Each derivative table follows HubSpot's activity schema and maintains a foreign key to the master table.

#### Common Design Pattern

All derivative tables share:

1. **Master FK**: `ic_communication_master_id INTEGER NOT NULL REFERENCES staging.ic_communication_master(id)`
2. **HubSpot General Properties**: Activity date, assigned to, created by, etc.
3. **Type-Specific Properties**: Defined by HubSpot schema
4. **Association Fields**: Contact, Company, Deal IDs
5. **API Payload JSONB**: Pre-built JSON for HubSpot API
6. **Processing Status**: Track API push status

---

### 1. `staging.ic_calls`

**Purpose**: Store call activity data (phone calls, VoIP, Zoom calls)

**HubSpot Object**: Calls

```sql
CREATE TABLE staging.ic_calls (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- HubSpot General Activity Properties
    activity_date TIMESTAMP NOT NULL,              -- hs_timestamp (Comm_DateTime)
    activity_assigned_to VARCHAR(500),             -- User email/name (TBD)
    activity_created_by VARCHAR(500),              -- User who created (TBD)
    create_date TIMESTAMP,                         -- hs_createdate (Comm_CreatedDate)
    last_modified_date TIMESTAMP,                  -- hs_lastmodifieddate (Comm_UpdatedDate)

    -- Call-Specific Properties (HubSpot Schema)
    call_title VARCHAR(500),                       -- hs_call_title (Comm_Subject)
    call_notes TEXT,                               -- hs_call_body (Comm_Note)
    call_direction VARCHAR(50),                    -- hs_call_direction (Inbound/Outbound from Comm_Action)
    call_duration_ms BIGINT,                       -- hs_call_duration (calculated from DateTime diff)
    call_status VARCHAR(100),                      -- hs_call_status (Comm_Status)
    call_outcome VARCHAR(100),                     -- hs_call_disposition (mapped from Comm_Status)
    call_source VARCHAR(100) DEFAULT 'Integration', -- hs_call_source
    from_number VARCHAR(100),                      -- hs_call_from_number (extracted from Note or separate field)
    to_number VARCHAR(100),                        -- hs_call_to_number (extracted from Note or separate field)

    -- Association Fields (HubSpot Associations)
    associated_contact_id BIGINT,                  -- HubSpot contact ID
    associated_company_id BIGINT,                  -- HubSpot company ID
    associated_deal_id BIGINT,                     -- HubSpot deal ID

    -- API Integration Fields
    hubspot_api_payload JSONB,                     -- Pre-built API JSON payload
    api_push_status VARCHAR(50) DEFAULT 'pending', -- pending, pushed, failed
    hubspot_engagement_id BIGINT,                  -- HubSpot engagement ID after push
    error_message TEXT,

    -- Audit Fields
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- Constraints
    UNIQUE(ic_communication_master_id)
);

CREATE INDEX idx_ic_calls_master_id ON staging.ic_calls(ic_communication_master_id);
CREATE INDEX idx_ic_calls_contact ON staging.ic_calls(associated_contact_id);
CREATE INDEX idx_ic_calls_company ON staging.ic_calls(associated_company_id);
CREATE INDEX idx_ic_calls_status ON staging.ic_calls(api_push_status);
```

---

### 2. `staging.ic_emails`

**Purpose**: Store email communication data

**HubSpot Object**: Emails

```sql
CREATE TABLE staging.ic_emails (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- HubSpot General Activity Properties
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- Email-Specific Properties (HubSpot Schema)
    email_subject TEXT,                            -- hs_email_subject (Comm_Subject)
    email_body TEXT,                               -- hs_email_text (Comm_Note)
    email_direction VARCHAR(50),                   -- hs_email_direction (INCOMING/OUTGOING from Comm_Action)
    email_from_address VARCHAR(500),               -- hs_email_from_email (Person_EmailAddress or Comm_Email_Clean)
    email_to_address VARCHAR(500),                 -- hs_email_to_email (Comm_Email_Clean or Person_EmailAddress)
    email_send_status VARCHAR(100),                -- hs_email_status (mapped from Comm_Status)
    email_open_rate DECIMAL(5,2),                  -- hs_email_open_rate (NULL for imported)
    email_click_rate DECIMAL(5,2),                 -- hs_email_click_rate (NULL for imported)
    email_reply_rate DECIMAL(5,2),                 -- hs_email_reply_rate (NULL for imported)

    -- Association Fields
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- API Integration Fields
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending',
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- Audit Fields
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    UNIQUE(ic_communication_master_id)
);

CREATE INDEX idx_ic_emails_master_id ON staging.ic_emails(ic_communication_master_id);
CREATE INDEX idx_ic_emails_contact ON staging.ic_emails(associated_contact_id);
CREATE INDEX idx_ic_emails_status ON staging.ic_emails(api_push_status);
```

---

### 3. `staging.ic_meetings`

**Purpose**: Store meeting/appointment data

**HubSpot Object**: Meetings

```sql
CREATE TABLE staging.ic_meetings (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- HubSpot General Activity Properties
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- Meeting-Specific Properties (HubSpot Schema)
    meeting_name VARCHAR(500),                     -- hs_meeting_title (Comm_Subject)
    meeting_description TEXT,                      -- hs_meeting_body (Comm_Note)
    meeting_start_time TIMESTAMP NOT NULL,         -- hs_meeting_start_time (Comm_DateTime)
    meeting_end_time TIMESTAMP,                    -- hs_meeting_end_time (Comm_ToDateTime)
    internal_meeting_notes TEXT,                   -- hs_internal_meeting_notes (Comm_Note duplicate for internal use)
    meeting_location VARCHAR(500),                 -- hs_meeting_location (extracted from Note)
    location_type VARCHAR(50),                     -- hs_meeting_location_type (Phone Call/Address/Video Conference)
    meeting_outcome VARCHAR(100),                  -- hs_meeting_outcome (mapped from Comm_Status)
    meeting_source VARCHAR(100) DEFAULT 'Import',  -- hs_meeting_source
    outcome_completed_count INTEGER DEFAULT 0,     -- hs_meeting_outcome_completed
    outcome_canceled_count INTEGER DEFAULT 0,      -- hs_meeting_outcome_canceled
    outcome_no_show_count INTEGER DEFAULT 0,       -- hs_meeting_outcome_no_show
    outcome_rescheduled_count INTEGER DEFAULT 0,   -- hs_meeting_outcome_rescheduled

    -- Association Fields
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- API Integration Fields
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending',
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- Audit Fields
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    UNIQUE(ic_communication_master_id)
);

CREATE INDEX idx_ic_meetings_master_id ON staging.ic_meetings(ic_communication_master_id);
CREATE INDEX idx_ic_meetings_start ON staging.ic_meetings(meeting_start_time);
CREATE INDEX idx_ic_meetings_contact ON staging.ic_meetings(associated_contact_id);
CREATE INDEX idx_ic_meetings_status ON staging.ic_meetings(api_push_status);
```

---

### 4. `staging.ic_notes`

**Purpose**: Store general notes/annotations

**HubSpot Object**: Notes

```sql
CREATE TABLE staging.ic_notes (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- HubSpot General Activity Properties
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- Note-Specific Properties (HubSpot Schema)
    note_body TEXT NOT NULL,                       -- hs_note_body (Comm_Note + Comm_Subject combined)

    -- Association Fields
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- API Integration Fields
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending',
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- Audit Fields
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    UNIQUE(ic_communication_master_id)
);

CREATE INDEX idx_ic_notes_master_id ON staging.ic_notes(ic_communication_master_id);
CREATE INDEX idx_ic_notes_contact ON staging.ic_notes(associated_contact_id);
CREATE INDEX idx_ic_notes_status ON staging.ic_notes(api_push_status);
```

---

### 5. `staging.ic_tasks`

**Purpose**: Store tasks/to-dos (including converted cases)

**HubSpot Object**: Tasks

```sql
CREATE TABLE staging.ic_tasks (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- HubSpot General Activity Properties
    activity_date TIMESTAMP NOT NULL,              -- hs_timestamp (due date)
    activity_assigned_to VARCHAR(500),             -- hs_task_assigned_to
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- Task-Specific Properties (HubSpot Schema)
    task_title VARCHAR(500) NOT NULL,              -- hs_task_subject (Comm_Subject)
    task_notes TEXT,                               -- hs_task_body (Comm_Note)
    due_date TIMESTAMP,                            -- hs_task_due_date (Comm_DateTime or Comm_ToDateTime)
    task_status VARCHAR(100) DEFAULT 'NOT_STARTED', -- hs_task_status (NOT_STARTED/COMPLETED from Comm_Status)
    completed_at TIMESTAMP,                        -- hs_task_completion_date (if Comm_Status = Completed)
    task_priority VARCHAR(50),                     -- hs_task_priority (LOW/MEDIUM/HIGH from Comm_Priority)
    task_type VARCHAR(100) DEFAULT 'TODO',         -- hs_task_type (TODO/EMAIL/CALL)
    is_overdue BOOLEAN DEFAULT FALSE,              -- hs_task_is_overdue (computed)
    reminder_date TIMESTAMP,                       -- hs_task_reminders (Comm_OriginalDateTime)

    -- Source Tracking
    source_type VARCHAR(50),                       -- 'task' or 'case' (if converted from Case)
    source_case_id INTEGER,                        -- Comm_CaseId if converted from case

    -- Association Fields
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- API Integration Fields
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending',
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- Audit Fields
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    UNIQUE(ic_communication_master_id)
);

CREATE INDEX idx_ic_tasks_master_id ON staging.ic_tasks(ic_communication_master_id);
CREATE INDEX idx_ic_tasks_due_date ON staging.ic_tasks(due_date);
CREATE INDEX idx_ic_tasks_priority ON staging.ic_tasks(task_priority);
CREATE INDEX idx_ic_tasks_status ON staging.ic_tasks(task_status);
CREATE INDEX idx_ic_tasks_api_status ON staging.ic_tasks(api_push_status);
```

---

### 6. `staging.ic_communications`

**Purpose**: Store SMS, LinkedIn, WhatsApp messages

**HubSpot Object**: Communications

```sql
CREATE TABLE staging.ic_communications (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- HubSpot General Activity Properties
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- Communication-Specific Properties (HubSpot Schema)
    communication_body TEXT NOT NULL,              -- hs_communication_body (Comm_Note)
    channel_type VARCHAR(100),                     -- hs_communication_channel_type (SMS/LINKEDIN_MESSAGE/WHATS_APP)
    logged_from VARCHAR(100) DEFAULT 'API',        -- hs_communication_logged_from

    -- Association Fields
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- API Integration Fields
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending',
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- Audit Fields
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    UNIQUE(ic_communication_master_id)
);

CREATE INDEX idx_ic_comms_master_id ON staging.ic_communications(ic_communication_master_id);
CREATE INDEX idx_ic_comms_channel ON staging.ic_communications(channel_type);
CREATE INDEX idx_ic_comms_contact ON staging.ic_communications(associated_contact_id);
CREATE INDEX idx_ic_comms_status ON staging.ic_communications(api_push_status);
```

---

### 7. `staging.ic_postal_mail`

**Purpose**: Store postal mail activities

**HubSpot Object**: Postal Mail

```sql
CREATE TABLE staging.ic_postal_mail (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- HubSpot General Activity Properties
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- Postal Mail-Specific Properties (HubSpot Schema)
    postal_mail_body TEXT NOT NULL,                -- hs_postal_mail_body (Comm_Note)
    body_preview VARCHAR(500),                     -- hs_postal_mail_body_preview (first 500 chars of Comm_Note)

    -- Association Fields
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- API Integration Fields
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending',
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- Audit Fields
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    UNIQUE(ic_communication_master_id)
);

CREATE INDEX idx_ic_postal_master_id ON staging.ic_postal_mail(ic_communication_master_id);
CREATE INDEX idx_ic_postal_contact ON staging.ic_postal_mail(associated_contact_id);
CREATE INDEX idx_ic_postal_status ON staging.ic_postal_mail(api_push_status);
```

---

## Property Mapping Strategy

### Mapping Logic Framework

Each source field maps to one or more HubSpot activity properties based on the activity type. The mapping follows this framework:

#### Universal Mappings (All Activity Types)

| Source Field | Target Property | Transformation | Notes |
|--------------|-----------------|----------------|-------|
| `Comm_DateTime` | `activity_date` | Direct | Primary timestamp |
| `Comm_CreatedDate` | `create_date` | Direct | Record creation |
| `Comm_UpdatedDate` | `last_modified_date` | Direct | Last modification |
| `Person_Id` → `hubspot_contact_id` | `associated_contact_id` | FK Lookup | Via contacts_reconciliation |
| `Company_Id` → `hubspot_company_id` | `associated_company_id` | FK Lookup | Via companies_reconciliation |
| `Comm_OpportunityId` → `hubspot_deal_id` | `associated_deal_id` | FK Lookup | Via deals_reconciliation |

#### Type-Specific Mappings

##### CALLS

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Subject` | `call_title` | Direct |
| `Comm_Note` | `call_notes` | Direct |
| `Comm_Action` | `call_direction` | Map: 'Inbound'→INBOUND, 'Outbound'→OUTBOUND |
| `Comm_ToDateTime - Comm_DateTime` | `call_duration_ms` | Calculate milliseconds |
| `Comm_Status` | `call_status` | Map to HubSpot call status values |
| `Comm_Status` | `call_outcome` | Map to call disposition values |

##### EMAILS

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Subject` | `email_subject` | Direct |
| `Comm_Note` | `email_body` | Direct |
| `Comm_Action` | `email_direction` | Map: 'Inbound'→INCOMING, 'Outbound'→OUTGOING |
| `Person_EmailAddress` / `Comm_Email_Clean` | `email_from_address` | Use Person email if Action=Outbound, else Comm_Email_Clean |
| `Comm_Email_Clean` / `Person_EmailAddress` | `email_to_address` | Use Comm_Email_Clean if Action=Outbound, else Person email |
| `Comm_Status` | `email_send_status` | Map to SENT/FAILED/SCHEDULED |

##### MEETINGS

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Subject` | `meeting_name` | Direct |
| `Comm_Note` | `meeting_description` | Direct |
| `Comm_DateTime` | `meeting_start_time` | Direct |
| `Comm_ToDateTime` | `meeting_end_time` | Direct (use Comm_DateTime + 1 hour if NULL) |
| `Comm_Note` | `internal_meeting_notes` | Direct |
| `Comm_Status` | `meeting_outcome` | Map to SCHEDULED/COMPLETED/CANCELLED/NO_SHOW |
| `Comm_Status` | `outcome_*_count` | Set appropriate outcome flag to 1 |

##### NOTES

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Subject + '\n\n' + Comm_Note` | `note_body` | Concatenate with separator |

##### TASKS

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Subject` | `task_title` | Direct |
| `Comm_Note` | `task_notes` | Direct |
| `Comm_DateTime` OR `Comm_ToDateTime` | `due_date` | Use ToDateTime if available, else DateTime |
| `Comm_Priority` | `task_priority` | Map: 'Low'→LOW, 'Medium'→MEDIUM, 'High'→HIGH |
| `Comm_Status` | `task_status` | Map: 'Completed'→COMPLETED, else NOT_STARTED |
| `Comm_Status='Completed'` | `completed_at` | Use Comm_UpdatedDate or Comm_DateTime |
| `due_date < NOW()` | `is_overdue` | Compute boolean |
| `Comm_CaseId IS NOT NULL` | `source_type` | Set to 'case' |
| `Comm_CaseId` | `source_case_id` | Direct |

##### COMMUNICATIONS

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Note` | `communication_body` | Direct |
| `Comm_Type` | `channel_type` | Map: 'SMS'→SMS, 'LinkedIn'→LINKEDIN_MESSAGE, 'WhatsApp'→WHATS_APP |

##### POSTAL_MAIL

| Source Field | Target Property | Transformation |
|--------------|-----------------|----------------|
| `Comm_Note` | `postal_mail_body` | Direct |
| `SUBSTRING(Comm_Note, 1, 500)` | `body_preview` | First 500 characters |

---

## Activity Type Classification Logic

The `derived_activity_type` field in the master table determines which derivative table(s) to populate.

### Classification Rules (Priority Order)

```python
def classify_activity_type(comm_type: str, comm_priority: str, comm_caseid: int) -> str:
    """
    Determine HubSpot activity type from communication data.

    Priority order:
    1. Explicit type mapping
    2. Case-to-Task conversion (if priority threshold met)
    3. Default fallback
    """

    # Normalize to lowercase for matching
    comm_type_lower = (comm_type or '').lower().strip()

    # Explicit type mappings
    TYPE_MAP = {
        # Calls
        'call': 'calls',
        'phone': 'calls',
        'voip': 'calls',
        'zoom': 'calls',
        'telephone': 'calls',
        'phonecall': 'calls',

        # Emails
        'email': 'emails',
        'e-mail': 'emails',
        'mail': 'emails',

        # Meetings
        'meeting': 'meetings',
        'appointment': 'meetings',
        'visit': 'meetings',
        'demo': 'meetings',
        'presentation': 'meetings',

        # Notes
        'note': 'notes',
        'comment': 'notes',
        'annotation': 'notes',
        'memo': 'notes',

        # Tasks
        'task': 'tasks',
        'todo': 'tasks',
        'to-do': 'tasks',
        'follow-up': 'tasks',
        'followup': 'tasks',
        'action': 'tasks',

        # Communications (messaging)
        'sms': 'communications',
        'text': 'communications',
        'linkedin': 'communications',
        'whatsapp': 'communications',
        'wa': 'communications',
        'message': 'communications',

        # Postal Mail
        'postal': 'postal_mail',
        'mail': 'postal_mail',
        'letter': 'postal_mail',
    }

    # Check explicit mapping
    if comm_type_lower in TYPE_MAP:
        return TYPE_MAP[comm_type_lower]

    # Case-to-Task conversion logic
    # If Comm_CaseId is populated AND priority is High, create Task
    if comm_caseid is not None:
        priority_lower = (comm_priority or '').lower().strip()
        if priority_lower in ['high', 'urgent', 'critical']:
            return 'tasks'
        else:
            # Low/Medium priority cases → Notes
            return 'notes'

    # Default fallback: Unknown types become Notes
    return 'notes'
```

### Classification Matrix

| Comm_Type | Comm_CaseId | Comm_Priority | Result | Reasoning |
|-----------|-------------|---------------|--------|-----------|
| 'Call' | NULL | Any | `calls` | Explicit mapping |
| 'Email' | NULL | Any | `emails` | Explicit mapping |
| 'Meeting' | NULL | Any | `meetings` | Explicit mapping |
| 'Note' | NULL | Any | `notes` | Explicit mapping |
| 'Task' | NULL | Any | `tasks` | Explicit mapping |
| 'SMS' | NULL | Any | `communications` | Explicit mapping |
| 'Case' | 12345 | 'High' | `tasks` | Case → Task conversion (high priority) |
| 'Case' | 12345 | 'Low' | `notes` | Case → Note (low priority) |
| 'Unknown' | NULL | Any | `notes` | Default fallback |
| NULL | NULL | Any | `notes` | Default fallback |

### Multi-Target Logic

**Special Case**: Some communications may create MULTIPLE activities

Example: A meeting may create BOTH a `meeting` record AND a `task` (follow-up)

```sql
-- Flag for multi-target creation
ALTER TABLE staging.ic_communication_master
ADD COLUMN create_followup_task BOOLEAN DEFAULT FALSE;

-- Business rule: Meetings with status 'Requires Follow-up' create both
UPDATE staging.ic_communication_master
SET create_followup_task = TRUE
WHERE derived_activity_type = 'meetings'
  AND comm_status ILIKE '%follow%';
```

---

## Foreign Key & Association Strategy

### FK Resolution Flow

```
┌─────────────────────────────────────────────────────────┐
│  STEP 1: Load Master Table with Legacy IDs             │
│  staging.ic_communication_master                        │
│  - person_id = 12345 (legacy)                           │
│  - company_id = 67890 (legacy)                          │
│  - comm_opportunityid = 54321 (legacy)                  │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  STEP 2: Resolve HubSpot IDs via Reconciliation Tables │
│                                                          │
│  UPDATE ic_communication_master m                       │
│  SET hubspot_contact_id = c.hubspot_contact_id          │
│  FROM staging.contacts_reconciliation c                 │
│  WHERE m.person_id = c.legacy_contact_id;               │
│                                                          │
│  UPDATE ic_communication_master m                       │
│  SET hubspot_company_id = c.hubspot_company_id          │
│  FROM staging.companies_reconciliation c                │
│  WHERE m.company_id = c.legacy_company_id;              │
│                                                          │
│  UPDATE ic_communication_master m                       │
│  SET hubspot_deal_id = d.hubspot_deal_id                │
│  FROM staging.deals_reconciliation d                    │
│  WHERE m.comm_opportunityid = d.legacy_deal_id;         │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  STEP 3: Populate Derivative Tables with Resolved FKs  │
│                                                          │
│  INSERT INTO staging.ic_calls (...)                     │
│  SELECT                                                  │
│    m.id,                    -- master FK                │
│    m.hubspot_contact_id,    -- resolved FK              │
│    m.hubspot_company_id,    -- resolved FK              │
│    m.hubspot_deal_id        -- resolved FK              │
│  FROM ic_communication_master m                         │
│  WHERE m.derived_activity_type = 'calls';               │
└─────────────────────────────────────────────────────────┘
```

### FK Lookup Queries

#### Contact FK Resolution

```sql
-- Resolve contact IDs
UPDATE staging.ic_communication_master m
SET
    hubspot_contact_id = c.hubspot_contact_id,
    last_updated = NOW()
FROM staging.contacts_reconciliation c
WHERE m.person_id = c.legacy_contact_id
  AND c.hubspot_contact_id IS NOT NULL;

-- Log unresolved contacts
INSERT INTO staging.reconciliation_log (operation, entity_type, legacy_id, status, error_message)
SELECT
    'FK_RESOLUTION',
    'contact',
    person_id,
    'unresolved',
    'Contact not found in reconciliation table'
FROM staging.ic_communication_master
WHERE person_id IS NOT NULL
  AND hubspot_contact_id IS NULL;
```

#### Company FK Resolution

```sql
-- Resolve company IDs
UPDATE staging.ic_communication_master m
SET
    hubspot_company_id = c.hubspot_company_id,
    last_updated = NOW()
FROM staging.companies_reconciliation c
WHERE m.company_id = c.legacy_company_id
  AND c.hubspot_company_id IS NOT NULL;

-- Log unresolved companies
INSERT INTO staging.reconciliation_log (operation, entity_type, legacy_id, status, error_message)
SELECT
    'FK_RESOLUTION',
    'company',
    company_id,
    'unresolved',
    'Company not found in reconciliation table'
FROM staging.ic_communication_master
WHERE company_id IS NOT NULL
  AND hubspot_company_id IS NULL;
```

#### Deal FK Resolution

```sql
-- Resolve deal IDs
UPDATE staging.ic_communication_master m
SET
    hubspot_deal_id = d.hubspot_deal_id,
    last_updated = NOW()
FROM staging.deals_reconciliation d
WHERE m.comm_opportunityid = d.legacy_deal_id
  AND d.hubspot_deal_id IS NOT NULL;

-- Log unresolved deals
INSERT INTO staging.reconciliation_log (operation, entity_type, legacy_id, status, error_message)
SELECT
    'FK_RESOLUTION',
    'deal',
    comm_opportunityid,
    'unresolved',
    'Deal not found in reconciliation table'
FROM staging.ic_communication_master
WHERE comm_opportunityid IS NOT NULL
  AND hubspot_deal_id IS NULL;
```

### Orphaned Record Handling

**Strategy**: Communications without resolved FKs are still valid but flagged

```sql
-- Flag orphaned communications (no contact/company/deal)
ALTER TABLE staging.ic_communication_master
ADD COLUMN is_orphaned BOOLEAN DEFAULT FALSE;

UPDATE staging.ic_communication_master
SET is_orphaned = TRUE
WHERE hubspot_contact_id IS NULL
  AND hubspot_company_id IS NULL
  AND hubspot_deal_id IS NULL;

-- Decision: Push orphaned records to HubSpot but log warnings
-- HubSpot allows engagements without associations (they appear in timeline)
```

---

## Data Flow & Processing Pipeline

### Pipeline Stages

```
STAGE 1: LOAD MASTER TABLE
┌─────────────────────────────────────────────────┐
│  Input: Communications CSV (denormalized)       │
│  Output: staging.ic_communication_master        │
│  ┌──────────────────────────────────────────┐  │
│  │ 1. Read CSV with 22 columns              │  │
│  │ 2. Classify activity type                │  │
│  │ 3. Set is_task_eligible flag             │  │
│  │ 4. INSERT with UPSERT on comm_id         │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
                    ↓
STAGE 2: RESOLVE FOREIGN KEYS
┌─────────────────────────────────────────────────┐
│  Update master table with HubSpot IDs           │
│  ┌──────────────────────────────────────────┐  │
│  │ 1. UPDATE hubspot_contact_id             │  │
│  │ 2. UPDATE hubspot_company_id             │  │
│  │ 3. UPDATE hubspot_deal_id                │  │
│  │ 4. Flag orphaned records                 │  │
│  │ 5. Log unresolved FKs                    │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
                    ↓
STAGE 3: POPULATE DERIVATIVE TABLES
┌─────────────────────────────────────────────────┐
│  Create type-specific activity records          │
│  ┌──────────────────────────────────────────┐  │
│  │ FOR EACH activity_type:                  │  │
│  │   SELECT from master WHERE type = X      │  │
│  │   TRANSFORM properties per mapping       │  │
│  │   INSERT INTO staging.ic_{type}          │  │
│  │   Calculate derived fields               │  │
│  │   Build API payload JSON                 │  │
│  └──────────────────────────────────────────┘  │
│                                                  │
│  Order: calls → emails → meetings → notes       │
│         → tasks → communications → postal_mail  │
└─────────────────────────────────────────────────┘
                    ↓
STAGE 4: VALIDATION & QUALITY CHECKS
┌─────────────────────────────────────────────────┐
│  Verify data integrity                          │
│  ┌──────────────────────────────────────────┐  │
│  │ 1. Check required fields NOT NULL        │  │
│  │ 2. Validate FK references exist          │  │
│  │ 3. Check date logic (start < end)        │  │
│  │ 4. Validate enum values                  │  │
│  │ 5. Test JSON payload structure           │  │
│  │ 6. Generate validation report            │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
                    ↓
STAGE 5: API PREPARATION (Future)
┌─────────────────────────────────────────────────┐
│  Prepare for HubSpot API push                   │
│  ┌──────────────────────────────────────────┐  │
│  │ 1. Render Jinja templates                │  │
│  │ 2. Batch records (100 per API call)      │  │
│  │ 3. Add rate limiting metadata            │  │
│  │ 4. Create retry queue                    │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

### Processing Scripts (Planned)

1. **`load_communication_master.py`**
   - Reads communications CSV
   - Applies classification logic
   - Loads master table

2. **`resolve_foreign_keys.py`**
   - Joins with reconciliation tables
   - Updates HubSpot ID fields
   - Logs unresolved references

3. **`populate_derivative_tables.py`**
   - Loops through 7 activity types
   - Applies property transformations
   - Inserts into derivative tables

4. **`validate_staging_data.py`**
   - Runs data quality checks
   - Generates validation report
   - Flags invalid records

5. **`build_api_payloads.py`** (Future)
   - Renders Jinja templates
   - Creates JSON payloads
   - Prepares for API push

---

## SQL Implementation Strategy

### SQL View for Each Activity Type

Instead of writing complex INSERT queries, use SQL VIEWS for clarity and reusability.

#### Example: `view_ic_calls_staging`

```sql
CREATE OR REPLACE VIEW staging.view_ic_calls_staging AS
SELECT
    -- Master table reference
    m.id as ic_communication_master_id,

    -- General activity properties
    m.comm_datetime as activity_date,
    NULL::VARCHAR(500) as activity_assigned_to,  -- TBD: Map from user table
    NULL::VARCHAR(500) as activity_created_by,   -- TBD: Map from user table
    m.comm_createddate as create_date,
    m.comm_updateddate as last_modified_date,

    -- Call-specific properties
    COALESCE(m.comm_subject, 'Call - ' || m.person_firstname || ' ' || m.person_lastname) as call_title,
    m.comm_note as call_notes,

    -- Call direction mapping
    CASE
        WHEN LOWER(m.comm_action) IN ('inbound', 'incoming', 'received') THEN 'INBOUND'
        WHEN LOWER(m.comm_action) IN ('outbound', 'outgoing', 'sent') THEN 'OUTBOUND'
        ELSE 'OUTBOUND'
    END as call_direction,

    -- Call duration (milliseconds)
    CASE
        WHEN m.comm_todatetime IS NOT NULL THEN
            EXTRACT(EPOCH FROM (m.comm_todatetime - m.comm_datetime)) * 1000
        ELSE NULL
    END::BIGINT as call_duration_ms,

    -- Call status mapping
    CASE
        WHEN LOWER(m.comm_status) IN ('completed', 'done', 'finished') THEN 'COMPLETED'
        WHEN LOWER(m.comm_status) IN ('missed', 'no answer') THEN 'NO_ANSWER'
        WHEN LOWER(m.comm_status) IN ('busy') THEN 'BUSY'
        WHEN LOWER(m.comm_status) IN ('cancelled', 'canceled') THEN 'CANCELED'
        ELSE 'COMPLETED'
    END as call_status,

    -- Call outcome
    COALESCE(m.comm_status, 'Connected') as call_outcome,

    'Integration' as call_source,

    -- Phone number extraction (placeholder - implement regex extraction)
    NULL::VARCHAR(100) as from_number,
    NULL::VARCHAR(100) as to_number,

    -- Associations (resolved FKs)
    m.hubspot_contact_id as associated_contact_id,
    m.hubspot_company_id as associated_company_id,
    m.hubspot_deal_id as associated_deal_id,

    -- Audit
    NOW() as created_at,
    NOW() as last_updated

FROM staging.ic_communication_master m
WHERE m.derived_activity_type = 'calls'
  AND m.staging_status = 'pending';
```

#### Materialization Query

```sql
-- Insert from view into staging table
INSERT INTO staging.ic_calls (
    ic_communication_master_id,
    activity_date,
    activity_assigned_to,
    activity_created_by,
    create_date,
    last_modified_date,
    call_title,
    call_notes,
    call_direction,
    call_duration_ms,
    call_status,
    call_outcome,
    call_source,
    from_number,
    to_number,
    associated_contact_id,
    associated_company_id,
    associated_deal_id
)
SELECT
    ic_communication_master_id,
    activity_date,
    activity_assigned_to,
    activity_created_by,
    create_date,
    last_modified_date,
    call_title,
    call_notes,
    call_direction,
    call_duration_ms,
    call_status,
    call_outcome,
    call_source,
    from_number,
    to_number,
    associated_contact_id,
    associated_company_id,
    associated_deal_id
FROM staging.view_ic_calls_staging
ON CONFLICT (ic_communication_master_id)
DO UPDATE SET
    activity_date = EXCLUDED.activity_date,
    last_modified_date = NOW();
```

### Validation Queries

```sql
-- Check 1: All master records have derivative records
SELECT
    m.derived_activity_type,
    COUNT(*) as master_count,
    COUNT(d.id) as derivative_count,
    COUNT(*) - COUNT(d.id) as missing_count
FROM staging.ic_communication_master m
LEFT JOIN staging.ic_calls d ON m.id = d.ic_communication_master_id
WHERE m.derived_activity_type = 'calls'
GROUP BY m.derived_activity_type;

-- Check 2: No orphaned derivative records
SELECT COUNT(*) as orphaned_count
FROM staging.ic_calls c
LEFT JOIN staging.ic_communication_master m ON c.ic_communication_master_id = m.id
WHERE m.id IS NULL;

-- Check 3: Required fields populated
SELECT
    'ic_calls' as table_name,
    COUNT(*) FILTER (WHERE call_title IS NULL) as missing_title,
    COUNT(*) FILTER (WHERE activity_date IS NULL) as missing_date,
    COUNT(*) FILTER (WHERE associated_contact_id IS NULL
                      AND associated_company_id IS NULL
                      AND associated_deal_id IS NULL) as missing_all_associations
FROM staging.ic_calls;
```

---

## API Integration Architecture

### HubSpot Engagements API

**Endpoint**: `POST https://api.hubapi.com/engagements/v1/engagements`

**Authentication**: API Key or OAuth token

### API Payload Structure

Each activity type has a specific payload structure:

#### Example: Call Engagement

```json
{
  "engagement": {
    "active": true,
    "type": "CALL",
    "timestamp": 1699900800000
  },
  "associations": {
    "contactIds": [123456],
    "companyIds": [789012],
    "dealIds": [345678]
  },
  "metadata": {
    "toNumber": "+1234567890",
    "fromNumber": "+0987654321",
    "status": "COMPLETED",
    "durationMilliseconds": 300000,
    "body": "Call notes here",
    "disposition": "Connected"
  }
}
```

### Jinja Template for Call Payloads

**File**: `templates/hubspot_call_payload.j2`

```jinja2
{
  "engagement": {
    "active": true,
    "type": "CALL",
    "timestamp": {{ (activity_date | to_epoch_ms) }},
    "ownerId": {{ owner_id | default('null') }}
  },
  "associations": {
    {% if associated_contact_id %}
    "contactIds": [{{ associated_contact_id }}],
    {% endif %}
    {% if associated_company_id %}
    "companyIds": [{{ associated_company_id }}],
    {% endif %}
    {% if associated_deal_id %}
    "dealIds": [{{ associated_deal_id }}]
    {% endif %}
  },
  "metadata": {
    {% if to_number %}
    "toNumber": "{{ to_number }}",
    {% endif %}
    {% if from_number %}
    "fromNumber": "{{ from_number }}",
    {% endif %}
    "status": "{{ call_status }}",
    {% if call_duration_ms %}
    "durationMilliseconds": {{ call_duration_ms }},
    {% endif %}
    "body": {{ call_notes | tojson }},
    "disposition": "{{ call_outcome }}"
  }
}
```

### Python API Integration Script (Skeleton)

```python
from jinja2 import Environment, FileSystemLoader
import requests
import time

class HubSpotActivityPusher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.hubapi.com/engagements/v1/engagements"
        self.jinja_env = Environment(loader=FileSystemLoader('templates'))

    def push_calls(self, batch_size: int = 100):
        """Push call activities to HubSpot"""
        # Fetch pending calls from staging
        calls = self.fetch_pending_activities('ic_calls', batch_size)

        for call in calls:
            try:
                # Render Jinja template
                template = self.jinja_env.get_template('hubspot_call_payload.j2')
                payload = template.render(**call)

                # POST to HubSpot API
                response = requests.post(
                    self.base_url,
                    headers={'Authorization': f'Bearer {self.api_key}'},
                    json=payload
                )

                if response.status_code == 200:
                    engagement_id = response.json()['engagement']['id']
                    self.mark_pushed(call['id'], engagement_id)
                else:
                    self.mark_failed(call['id'], response.text)

                # Rate limiting
                time.sleep(0.1)  # 10 requests/second

            except Exception as e:
                self.mark_failed(call['id'], str(e))
```

---

## GraphML Entity-Relationship Diagram

### Diagram Description

The GraphML below models:

1. **Source Entity**: Communications Master Table
2. **Master Staging Entity**: ic_communication_master
3. **7 Derivative Entities**: Calls, Emails, Meetings, Notes, Tasks, Communications, Postal Mail
4. **Association Entities**: Contacts, Companies, Deals (from reconciliation tables)
5. **Relationships**: FK references, data flow, API associations

### Key Relationships

- **1:1** between Master and each Derivative (unique constraint on master FK)
- **N:1** from Master to Contact/Company/Deal
- **1:1** from Derivative to HubSpot Engagement (after API push)

---

## Implementation Checklist

### Phase 1: Master Table Setup
- [ ] Create `staging.ic_communication_master` table
- [ ] Load communications CSV into master table
- [ ] Implement activity type classification logic
- [ ] Set `is_task_eligible` flags
- [ ] Validate master table data quality

### Phase 2: FK Resolution
- [ ] Run contact FK resolution queries
- [ ] Run company FK resolution queries
- [ ] Run deal FK resolution queries
- [ ] Log unresolved references
- [ ] Review orphaned communications

### Phase 3: Derivative Tables
- [ ] Create all 7 derivative staging tables
- [ ] Create SQL views for each activity type
- [ ] Run materialization queries (view → table)
- [ ] Validate data integrity
- [ ] Review property transformations

### Phase 4: Validation & QA
- [ ] Run validation SQL queries
- [ ] Check record counts match
- [ ] Verify required fields populated
- [ ] Test date logic (meeting start < end)
- [ ] Review sample records for each type

### Phase 5: API Preparation (Future)
- [ ] Create Jinja templates for each activity type
- [ ] Build API payload JSON in derivative tables
- [ ] Implement Python API pusher script
- [ ] Test with HubSpot sandbox account
- [ ] Implement error handling & retry logic

---

## Success Metrics

| Metric | Target | Validation Query |
|--------|--------|------------------|
| Master table load success | 100% | `COUNT(*) FROM ic_communication_master` |
| FK resolution rate | >95% | `COUNT(hubspot_contact_id IS NOT NULL)` |
| Derivative table coverage | 100% | Compare counts by activity type |
| Required fields populated | >99% | Check NULL counts for required fields |
| API push success rate | >98% | Track `api_push_status = 'pushed'` |

---

## Glossary

| Term | Definition |
|------|------------|
| **Master Table** | `staging.ic_communication_master` - First staging table loaded with all source data |
| **Derivative Table** | Activity-specific staging tables (calls, emails, etc.) populated FROM master table |
| **Activity Type** | HubSpot engagement type: Call, Email, Meeting, Note, Task, Communication, Postal Mail |
| **FK Resolution** | Process of converting legacy IDs to HubSpot IDs via reconciliation tables |
| **Orphaned Record** | Communication with no associated contact, company, or deal |
| **API Payload** | JSON structure conforming to HubSpot Engagements API schema |
| **Engagement** | HubSpot's term for activity/interaction records |
| **Association** | Link between activity and CRM object (contact, company, deal) |

---

## References

- HubSpot Activities Documentation: https://knowledge.hubspot.com/records/manually-log-activities-on-records
- HubSpot Engagements API: https://developers.hubspot.com/docs/api/crm/engagements
- Project Repository: `/home/user/IC-D-LOAD/`
- Existing Staging Schema: `staging_schema_manager.py`
- Property Mappings: `property_mapping_config.py`

---

**Document Status**: Draft - Ready for Review
**Next Steps**: Generate GraphML, Create SQL DDL, Implement Stage 1 (Master Table Load)
