-- ============================================================================
-- HubSpot Activities Staging Tables DDL
-- ============================================================================
-- Purpose: Create staging tables for HubSpot activities decomposition
-- Author: IC-D-LOAD Project
-- Date: 2025-11-13
-- Version: 1.0
--
-- IMPORTANT: Execute tables in order:
--   1. Master table FIRST
--   2. Derivative tables (in any order)
--
-- Dependencies:
--   - staging.contacts_reconciliation (must exist)
--   - staging.companies_reconciliation (must exist)
--   - staging.deals_reconciliation (must exist)
-- ============================================================================

-- Create staging schema if not exists
CREATE SCHEMA IF NOT EXISTS staging;

-- ============================================================================
-- 1. MASTER STAGING TABLE (Load First!)
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.ic_communication_master (
    -- ========================================================================
    -- Primary Key
    -- ========================================================================
    id SERIAL PRIMARY KEY,

    -- ========================================================================
    -- Legacy Communication Fields (Direct mapping from source)
    -- ========================================================================
    comm_communicationid INTEGER NOT NULL,
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

    -- ========================================================================
    -- Person/Contact Fields (Denormalized)
    -- ========================================================================
    person_id INTEGER,
    person_firstname VARCHAR(200),
    person_lastname VARCHAR(200),
    person_emailaddress VARCHAR(500),

    -- ========================================================================
    -- Company Fields (Denormalized)
    -- ========================================================================
    company_id INTEGER,
    company_name VARCHAR(500),

    -- ========================================================================
    -- Classification & Metadata (Computed during load)
    -- ========================================================================
    derived_activity_type VARCHAR(50) NOT NULL
        CHECK (derived_activity_type IN ('calls', 'emails', 'meetings', 'notes', 'tasks', 'communications', 'postal_mail')),
    is_task_eligible BOOLEAN DEFAULT FALSE,

    -- ========================================================================
    -- Foreign Key References (Resolved via reconciliation tables)
    -- ========================================================================
    hubspot_contact_id BIGINT,
    hubspot_company_id BIGINT,
    hubspot_deal_id BIGINT,

    -- ========================================================================
    -- Processing Metadata
    -- ========================================================================
    staging_status VARCHAR(50) DEFAULT 'pending'
        CHECK (staging_status IN ('pending', 'processed', 'error')),
    api_push_status VARCHAR(50)
        CHECK (api_push_status IN ('not_pushed', 'pushed', 'failed')),
    api_engagement_id BIGINT,
    is_orphaned BOOLEAN DEFAULT FALSE,
    error_message TEXT,

    -- ========================================================================
    -- Audit Fields
    -- ========================================================================
    created_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- ========================================================================
    -- Constraints
    -- ========================================================================
    CONSTRAINT uq_ic_comm_master_comm_id UNIQUE (comm_communicationid)
);

-- Indexes for Master Table
CREATE INDEX IF NOT EXISTS idx_ic_comm_master_comm_id
    ON staging.ic_communication_master(comm_communicationid);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_derived_type
    ON staging.ic_communication_master(derived_activity_type);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_staging_status
    ON staging.ic_communication_master(staging_status);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_api_status
    ON staging.ic_communication_master(api_push_status);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_person_id
    ON staging.ic_communication_master(person_id);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_company_id
    ON staging.ic_communication_master(company_id);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_opportunity_id
    ON staging.ic_communication_master(comm_opportunityid);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_case_id
    ON staging.ic_communication_master(comm_caseid);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_datetime
    ON staging.ic_communication_master(comm_datetime);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_hubspot_contact
    ON staging.ic_communication_master(hubspot_contact_id);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_hubspot_company
    ON staging.ic_communication_master(hubspot_company_id);

CREATE INDEX IF NOT EXISTS idx_ic_comm_master_hubspot_deal
    ON staging.ic_communication_master(hubspot_deal_id);

-- Comment
COMMENT ON TABLE staging.ic_communication_master IS
'Master staging table for communications data. MUST be loaded FIRST before derivative tables. Preserves all source data and adds classification metadata.';

-- ============================================================================
-- 2. DERIVATIVE STAGING TABLE: CALLS
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.ic_calls (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL
        REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- ========================================================================
    -- HubSpot General Activity Properties
    -- ========================================================================
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- ========================================================================
    -- Call-Specific Properties (HubSpot Schema)
    -- ========================================================================
    call_title VARCHAR(500),
    call_notes TEXT,
    call_direction VARCHAR(50) CHECK (call_direction IN ('INBOUND', 'OUTBOUND')),
    call_duration_ms BIGINT,
    call_status VARCHAR(100) CHECK (call_status IN ('BUSY', 'CANCELED', 'COMPLETED', 'CONNECTING', 'FAILED', 'IN_PROGRESS', 'MISSED', 'NO_ANSWER', 'QUEUED', 'RINGING')),
    call_outcome VARCHAR(100),
    call_source VARCHAR(100) DEFAULT 'Integration',
    from_number VARCHAR(100),
    to_number VARCHAR(100),

    -- ========================================================================
    -- Association Fields (HubSpot Associations)
    -- ========================================================================
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- ========================================================================
    -- API Integration Fields
    -- ========================================================================
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending'
        CHECK (api_push_status IN ('pending', 'pushed', 'failed')),
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- ========================================================================
    -- Audit Fields
    -- ========================================================================
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- ========================================================================
    -- Constraints
    -- ========================================================================
    CONSTRAINT uq_ic_calls_master_id UNIQUE (ic_communication_master_id)
);

-- Indexes for Calls
CREATE INDEX IF NOT EXISTS idx_ic_calls_master_id
    ON staging.ic_calls(ic_communication_master_id);

CREATE INDEX IF NOT EXISTS idx_ic_calls_activity_date
    ON staging.ic_calls(activity_date);

CREATE INDEX IF NOT EXISTS idx_ic_calls_contact
    ON staging.ic_calls(associated_contact_id);

CREATE INDEX IF NOT EXISTS idx_ic_calls_company
    ON staging.ic_calls(associated_company_id);

CREATE INDEX IF NOT EXISTS idx_ic_calls_deal
    ON staging.ic_calls(associated_deal_id);

CREATE INDEX IF NOT EXISTS idx_ic_calls_api_status
    ON staging.ic_calls(api_push_status);

CREATE INDEX IF NOT EXISTS idx_ic_calls_hubspot_engagement
    ON staging.ic_calls(hubspot_engagement_id);

COMMENT ON TABLE staging.ic_calls IS
'Call activity staging table conforming to HubSpot Calls schema. Populated FROM ic_communication_master WHERE derived_activity_type = ''calls''.';

-- ============================================================================
-- 3. DERIVATIVE STAGING TABLE: EMAILS
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.ic_emails (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL
        REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- ========================================================================
    -- HubSpot General Activity Properties
    -- ========================================================================
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- ========================================================================
    -- Email-Specific Properties (HubSpot Schema)
    -- ========================================================================
    email_subject TEXT,
    email_body TEXT,
    email_direction VARCHAR(50) CHECK (email_direction IN ('INCOMING', 'OUTGOING')),
    email_from_address VARCHAR(500),
    email_to_address VARCHAR(500),
    email_send_status VARCHAR(100) CHECK (email_send_status IN ('BOUNCED', 'FAILED', 'SCHEDULED', 'SENDING', 'SENT')),
    email_open_rate DECIMAL(5,2),
    email_click_rate DECIMAL(5,2),
    email_reply_rate DECIMAL(5,2),
    number_of_email_opens INTEGER DEFAULT 0,
    number_of_email_clicks INTEGER DEFAULT 0,
    number_of_email_replies INTEGER DEFAULT 0,

    -- ========================================================================
    -- Association Fields
    -- ========================================================================
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- ========================================================================
    -- API Integration Fields
    -- ========================================================================
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending'
        CHECK (api_push_status IN ('pending', 'pushed', 'failed')),
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- ========================================================================
    -- Audit Fields
    -- ========================================================================
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- ========================================================================
    -- Constraints
    -- ========================================================================
    CONSTRAINT uq_ic_emails_master_id UNIQUE (ic_communication_master_id)
);

-- Indexes for Emails
CREATE INDEX IF NOT EXISTS idx_ic_emails_master_id
    ON staging.ic_emails(ic_communication_master_id);

CREATE INDEX IF NOT EXISTS idx_ic_emails_activity_date
    ON staging.ic_emails(activity_date);

CREATE INDEX IF NOT EXISTS idx_ic_emails_contact
    ON staging.ic_emails(associated_contact_id);

CREATE INDEX IF NOT EXISTS idx_ic_emails_company
    ON staging.ic_emails(associated_company_id);

CREATE INDEX IF NOT EXISTS idx_ic_emails_deal
    ON staging.ic_emails(associated_deal_id);

CREATE INDEX IF NOT EXISTS idx_ic_emails_api_status
    ON staging.ic_emails(api_push_status);

CREATE INDEX IF NOT EXISTS idx_ic_emails_from_address
    ON staging.ic_emails(email_from_address);

CREATE INDEX IF NOT EXISTS idx_ic_emails_to_address
    ON staging.ic_emails(email_to_address);

COMMENT ON TABLE staging.ic_emails IS
'Email activity staging table conforming to HubSpot Emails schema. Populated FROM ic_communication_master WHERE derived_activity_type = ''emails''.';

-- ============================================================================
-- 4. DERIVATIVE STAGING TABLE: MEETINGS
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.ic_meetings (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL
        REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- ========================================================================
    -- HubSpot General Activity Properties
    -- ========================================================================
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- ========================================================================
    -- Meeting-Specific Properties (HubSpot Schema)
    -- ========================================================================
    meeting_name VARCHAR(500),
    meeting_description TEXT,
    meeting_start_time TIMESTAMP NOT NULL,
    meeting_end_time TIMESTAMP,
    internal_meeting_notes TEXT,
    meeting_location VARCHAR(500),
    location_type VARCHAR(50) CHECK (location_type IN ('PHONE_CALL', 'ADDRESS', 'VIDEO_CONFERENCE')),
    meeting_outcome VARCHAR(100) CHECK (meeting_outcome IN ('SCHEDULED', 'COMPLETED', 'CANCELLED', 'NO_SHOW', 'RESCHEDULED')),
    meeting_source VARCHAR(100) DEFAULT 'Import',
    outcome_completed_count INTEGER DEFAULT 0,
    outcome_canceled_count INTEGER DEFAULT 0,
    outcome_no_show_count INTEGER DEFAULT 0,
    outcome_rescheduled_count INTEGER DEFAULT 0,

    -- ========================================================================
    -- Association Fields
    -- ========================================================================
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- ========================================================================
    -- API Integration Fields
    -- ========================================================================
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending'
        CHECK (api_push_status IN ('pending', 'pushed', 'failed')),
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- ========================================================================
    -- Audit Fields
    -- ========================================================================
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- ========================================================================
    -- Constraints
    -- ========================================================================
    CONSTRAINT uq_ic_meetings_master_id UNIQUE (ic_communication_master_id),
    CONSTRAINT chk_ic_meetings_time_order CHECK (meeting_end_time IS NULL OR meeting_end_time >= meeting_start_time)
);

-- Indexes for Meetings
CREATE INDEX IF NOT EXISTS idx_ic_meetings_master_id
    ON staging.ic_meetings(ic_communication_master_id);

CREATE INDEX IF NOT EXISTS idx_ic_meetings_start_time
    ON staging.ic_meetings(meeting_start_time);

CREATE INDEX IF NOT EXISTS idx_ic_meetings_end_time
    ON staging.ic_meetings(meeting_end_time);

CREATE INDEX IF NOT EXISTS idx_ic_meetings_contact
    ON staging.ic_meetings(associated_contact_id);

CREATE INDEX IF NOT EXISTS idx_ic_meetings_company
    ON staging.ic_meetings(associated_company_id);

CREATE INDEX IF NOT EXISTS idx_ic_meetings_deal
    ON staging.ic_meetings(associated_deal_id);

CREATE INDEX IF NOT EXISTS idx_ic_meetings_api_status
    ON staging.ic_meetings(api_push_status);

CREATE INDEX IF NOT EXISTS idx_ic_meetings_outcome
    ON staging.ic_meetings(meeting_outcome);

COMMENT ON TABLE staging.ic_meetings IS
'Meeting activity staging table conforming to HubSpot Meetings schema. Populated FROM ic_communication_master WHERE derived_activity_type = ''meetings''.';

-- ============================================================================
-- 5. DERIVATIVE STAGING TABLE: NOTES
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.ic_notes (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL
        REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- ========================================================================
    -- HubSpot General Activity Properties
    -- ========================================================================
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- ========================================================================
    -- Note-Specific Properties (HubSpot Schema)
    -- ========================================================================
    note_body TEXT NOT NULL,

    -- ========================================================================
    -- Association Fields
    -- ========================================================================
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- ========================================================================
    -- API Integration Fields
    -- ========================================================================
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending'
        CHECK (api_push_status IN ('pending', 'pushed', 'failed')),
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- ========================================================================
    -- Audit Fields
    -- ========================================================================
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- ========================================================================
    -- Constraints
    -- ========================================================================
    CONSTRAINT uq_ic_notes_master_id UNIQUE (ic_communication_master_id)
);

-- Indexes for Notes
CREATE INDEX IF NOT EXISTS idx_ic_notes_master_id
    ON staging.ic_notes(ic_communication_master_id);

CREATE INDEX IF NOT EXISTS idx_ic_notes_activity_date
    ON staging.ic_notes(activity_date);

CREATE INDEX IF NOT EXISTS idx_ic_notes_contact
    ON staging.ic_notes(associated_contact_id);

CREATE INDEX IF NOT EXISTS idx_ic_notes_company
    ON staging.ic_notes(associated_company_id);

CREATE INDEX IF NOT EXISTS idx_ic_notes_deal
    ON staging.ic_notes(associated_deal_id);

CREATE INDEX IF NOT EXISTS idx_ic_notes_api_status
    ON staging.ic_notes(api_push_status);

COMMENT ON TABLE staging.ic_notes IS
'Note activity staging table conforming to HubSpot Notes schema. Populated FROM ic_communication_master WHERE derived_activity_type = ''notes''.';

-- ============================================================================
-- 6. DERIVATIVE STAGING TABLE: TASKS
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.ic_tasks (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL
        REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- ========================================================================
    -- HubSpot General Activity Properties
    -- ========================================================================
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- ========================================================================
    -- Task-Specific Properties (HubSpot Schema)
    -- ========================================================================
    task_title VARCHAR(500) NOT NULL,
    task_notes TEXT,
    due_date TIMESTAMP,
    task_status VARCHAR(100) DEFAULT 'NOT_STARTED'
        CHECK (task_status IN ('NOT_STARTED', 'COMPLETED', 'IN_PROGRESS', 'WAITING', 'DEFERRED')),
    completed_at TIMESTAMP,
    task_priority VARCHAR(50) CHECK (task_priority IN ('LOW', 'MEDIUM', 'HIGH')),
    task_type VARCHAR(100) DEFAULT 'TODO'
        CHECK (task_type IN ('TODO', 'EMAIL', 'CALL')),
    is_overdue BOOLEAN DEFAULT FALSE,
    reminder_date TIMESTAMP,

    -- ========================================================================
    -- Source Tracking (for Case-to-Task conversion)
    -- ========================================================================
    source_type VARCHAR(50) CHECK (source_type IN ('task', 'case')),
    source_case_id INTEGER,

    -- ========================================================================
    -- Association Fields
    -- ========================================================================
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- ========================================================================
    -- API Integration Fields
    -- ========================================================================
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending'
        CHECK (api_push_status IN ('pending', 'pushed', 'failed')),
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- ========================================================================
    -- Audit Fields
    -- ========================================================================
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- ========================================================================
    -- Constraints
    -- ========================================================================
    CONSTRAINT uq_ic_tasks_master_id UNIQUE (ic_communication_master_id),
    CONSTRAINT chk_ic_tasks_completed_at CHECK (task_status != 'COMPLETED' OR completed_at IS NOT NULL)
);

-- Indexes for Tasks
CREATE INDEX IF NOT EXISTS idx_ic_tasks_master_id
    ON staging.ic_tasks(ic_communication_master_id);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_due_date
    ON staging.ic_tasks(due_date);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_status
    ON staging.ic_tasks(task_status);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_priority
    ON staging.ic_tasks(task_priority);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_contact
    ON staging.ic_tasks(associated_contact_id);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_company
    ON staging.ic_tasks(associated_company_id);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_deal
    ON staging.ic_tasks(associated_deal_id);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_api_status
    ON staging.ic_tasks(api_push_status);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_source_type
    ON staging.ic_tasks(source_type);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_source_case
    ON staging.ic_tasks(source_case_id);

CREATE INDEX IF NOT EXISTS idx_ic_tasks_is_overdue
    ON staging.ic_tasks(is_overdue);

COMMENT ON TABLE staging.ic_tasks IS
'Task activity staging table conforming to HubSpot Tasks schema. Populated FROM ic_communication_master WHERE derived_activity_type = ''tasks''. Includes Case-to-Task conversions.';

-- ============================================================================
-- 7. DERIVATIVE STAGING TABLE: COMMUNICATIONS (SMS/LinkedIn/WhatsApp)
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.ic_communications (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL
        REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- ========================================================================
    -- HubSpot General Activity Properties
    -- ========================================================================
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- ========================================================================
    -- Communication-Specific Properties (HubSpot Schema)
    -- ========================================================================
    communication_body TEXT NOT NULL,
    channel_type VARCHAR(100) CHECK (channel_type IN ('SMS', 'LINKEDIN_MESSAGE', 'WHATS_APP', 'CUSTOM')),
    logged_from VARCHAR(100) DEFAULT 'API',

    -- ========================================================================
    -- Association Fields
    -- ========================================================================
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- ========================================================================
    -- API Integration Fields
    -- ========================================================================
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending'
        CHECK (api_push_status IN ('pending', 'pushed', 'failed')),
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- ========================================================================
    -- Audit Fields
    -- ========================================================================
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- ========================================================================
    -- Constraints
    -- ========================================================================
    CONSTRAINT uq_ic_communications_master_id UNIQUE (ic_communication_master_id)
);

-- Indexes for Communications
CREATE INDEX IF NOT EXISTS idx_ic_comms_master_id
    ON staging.ic_communications(ic_communication_master_id);

CREATE INDEX IF NOT EXISTS idx_ic_comms_activity_date
    ON staging.ic_communications(activity_date);

CREATE INDEX IF NOT EXISTS idx_ic_comms_channel
    ON staging.ic_communications(channel_type);

CREATE INDEX IF NOT EXISTS idx_ic_comms_contact
    ON staging.ic_communications(associated_contact_id);

CREATE INDEX IF NOT EXISTS idx_ic_comms_company
    ON staging.ic_communications(associated_company_id);

CREATE INDEX IF NOT EXISTS idx_ic_comms_deal
    ON staging.ic_communications(associated_deal_id);

CREATE INDEX IF NOT EXISTS idx_ic_comms_api_status
    ON staging.ic_communications(api_push_status);

COMMENT ON TABLE staging.ic_communications IS
'Communication activity staging table for messaging channels (SMS, LinkedIn, WhatsApp). Populated FROM ic_communication_master WHERE derived_activity_type = ''communications''.';

-- ============================================================================
-- 8. DERIVATIVE STAGING TABLE: POSTAL_MAIL
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.ic_postal_mail (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key to Master Table
    ic_communication_master_id INTEGER NOT NULL
        REFERENCES staging.ic_communication_master(id) ON DELETE CASCADE,

    -- ========================================================================
    -- HubSpot General Activity Properties
    -- ========================================================================
    activity_date TIMESTAMP NOT NULL,
    activity_assigned_to VARCHAR(500),
    activity_created_by VARCHAR(500),
    create_date TIMESTAMP,
    last_modified_date TIMESTAMP,

    -- ========================================================================
    -- Postal Mail-Specific Properties (HubSpot Schema)
    -- ========================================================================
    postal_mail_body TEXT NOT NULL,
    body_preview VARCHAR(500),

    -- ========================================================================
    -- Association Fields
    -- ========================================================================
    associated_contact_id BIGINT,
    associated_company_id BIGINT,
    associated_deal_id BIGINT,

    -- ========================================================================
    -- API Integration Fields
    -- ========================================================================
    hubspot_api_payload JSONB,
    api_push_status VARCHAR(50) DEFAULT 'pending'
        CHECK (api_push_status IN ('pending', 'pushed', 'failed')),
    hubspot_engagement_id BIGINT,
    error_message TEXT,

    -- ========================================================================
    -- Audit Fields
    -- ========================================================================
    created_at TIMESTAMP DEFAULT NOW(),
    pushed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- ========================================================================
    -- Constraints
    -- ========================================================================
    CONSTRAINT uq_ic_postal_mail_master_id UNIQUE (ic_communication_master_id)
);

-- Indexes for Postal Mail
CREATE INDEX IF NOT EXISTS idx_ic_postal_master_id
    ON staging.ic_postal_mail(ic_communication_master_id);

CREATE INDEX IF NOT EXISTS idx_ic_postal_activity_date
    ON staging.ic_postal_mail(activity_date);

CREATE INDEX IF NOT EXISTS idx_ic_postal_contact
    ON staging.ic_postal_mail(associated_contact_id);

CREATE INDEX IF NOT EXISTS idx_ic_postal_company
    ON staging.ic_postal_mail(associated_company_id);

CREATE INDEX IF NOT EXISTS idx_ic_postal_deal
    ON staging.ic_postal_mail(associated_deal_id);

CREATE INDEX IF NOT EXISTS idx_ic_postal_api_status
    ON staging.ic_postal_mail(api_push_status);

COMMENT ON TABLE staging.ic_postal_mail IS
'Postal mail activity staging table conforming to HubSpot Postal Mail schema. Populated FROM ic_communication_master WHERE derived_activity_type = ''postal_mail''.';

-- ============================================================================
-- 9. HELPER FUNCTIONS
-- ============================================================================

-- Function: Classify activity type from Comm_Type
CREATE OR REPLACE FUNCTION staging.classify_activity_type(
    p_comm_type VARCHAR,
    p_comm_priority VARCHAR,
    p_comm_caseid INTEGER
) RETURNS VARCHAR AS $$
DECLARE
    v_type_lower VARCHAR;
    v_priority_lower VARCHAR;
BEGIN
    -- Normalize inputs
    v_type_lower := LOWER(TRIM(COALESCE(p_comm_type, '')));
    v_priority_lower := LOWER(TRIM(COALESCE(p_comm_priority, '')));

    -- Explicit type mappings
    CASE v_type_lower
        -- Calls
        WHEN 'call', 'phone', 'voip', 'zoom', 'telephone', 'phonecall' THEN
            RETURN 'calls';

        -- Emails
        WHEN 'email', 'e-mail' THEN
            RETURN 'emails';

        -- Meetings
        WHEN 'meeting', 'appointment', 'visit', 'demo', 'presentation' THEN
            RETURN 'meetings';

        -- Notes
        WHEN 'note', 'comment', 'annotation', 'memo' THEN
            RETURN 'notes';

        -- Tasks
        WHEN 'task', 'todo', 'to-do', 'follow-up', 'followup', 'action' THEN
            RETURN 'tasks';

        -- Communications
        WHEN 'sms', 'text', 'linkedin', 'whatsapp', 'wa', 'message' THEN
            RETURN 'communications';

        -- Postal Mail
        WHEN 'postal', 'letter' THEN
            RETURN 'postal_mail';

        -- Case-to-Task conversion
        WHEN 'case' THEN
            IF p_comm_caseid IS NOT NULL AND v_priority_lower IN ('high', 'urgent', 'critical') THEN
                RETURN 'tasks';
            ELSE
                RETURN 'notes';
            END IF;

        -- Default fallback
        ELSE
            RETURN 'notes';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION staging.classify_activity_type IS
'Determines HubSpot activity type from legacy communication type, priority, and case ID. Returns one of: calls, emails, meetings, notes, tasks, communications, postal_mail.';

-- ============================================================================
-- 10. VIEWS FOR DATA VALIDATION
-- ============================================================================

-- View: Count records by activity type
CREATE OR REPLACE VIEW staging.v_ic_activities_summary AS
SELECT
    derived_activity_type,
    COUNT(*) as total_count,
    COUNT(hubspot_contact_id) as with_contact,
    COUNT(hubspot_company_id) as with_company,
    COUNT(hubspot_deal_id) as with_deal,
    COUNT(CASE WHEN hubspot_contact_id IS NULL
                AND hubspot_company_id IS NULL
                AND hubspot_deal_id IS NULL
           THEN 1 END) as orphaned_count,
    COUNT(CASE WHEN staging_status = 'processed' THEN 1 END) as processed_count,
    COUNT(CASE WHEN staging_status = 'error' THEN 1 END) as error_count,
    MIN(comm_datetime) as earliest_date,
    MAX(comm_datetime) as latest_date
FROM staging.ic_communication_master
GROUP BY derived_activity_type
ORDER BY derived_activity_type;

COMMENT ON VIEW staging.v_ic_activities_summary IS
'Summary statistics for communications master table grouped by activity type.';

-- View: Check derivative table coverage
CREATE OR REPLACE VIEW staging.v_ic_derivative_coverage AS
SELECT
    'calls' as activity_type,
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'calls') as master_count,
    (SELECT COUNT(*) FROM staging.ic_calls) as derivative_count,
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'calls') -
    (SELECT COUNT(*) FROM staging.ic_calls) as missing_count
UNION ALL
SELECT
    'emails',
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'emails'),
    (SELECT COUNT(*) FROM staging.ic_emails),
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'emails') -
    (SELECT COUNT(*) FROM staging.ic_emails)
UNION ALL
SELECT
    'meetings',
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'meetings'),
    (SELECT COUNT(*) FROM staging.ic_meetings),
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'meetings') -
    (SELECT COUNT(*) FROM staging.ic_meetings)
UNION ALL
SELECT
    'notes',
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'notes'),
    (SELECT COUNT(*) FROM staging.ic_notes),
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'notes') -
    (SELECT COUNT(*) FROM staging.ic_notes)
UNION ALL
SELECT
    'tasks',
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'tasks'),
    (SELECT COUNT(*) FROM staging.ic_tasks),
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'tasks') -
    (SELECT COUNT(*) FROM staging.ic_tasks)
UNION ALL
SELECT
    'communications',
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'communications'),
    (SELECT COUNT(*) FROM staging.ic_communications),
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'communications') -
    (SELECT COUNT(*) FROM staging.ic_communications)
UNION ALL
SELECT
    'postal_mail',
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'postal_mail'),
    (SELECT COUNT(*) FROM staging.ic_postal_mail),
    (SELECT COUNT(*) FROM staging.ic_communication_master WHERE derived_activity_type = 'postal_mail') -
    (SELECT COUNT(*) FROM staging.ic_postal_mail);

COMMENT ON VIEW staging.v_ic_derivative_coverage IS
'Validates that all master records have corresponding derivative records.';

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '✓ HubSpot Activities Staging Tables Created Successfully!';
    RAISE NOTICE '';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  1. staging.ic_communication_master (MASTER - Load First)';
    RAISE NOTICE '  2. staging.ic_calls';
    RAISE NOTICE '  3. staging.ic_emails';
    RAISE NOTICE '  4. staging.ic_meetings';
    RAISE NOTICE '  5. staging.ic_notes';
    RAISE NOTICE '  6. staging.ic_tasks';
    RAISE NOTICE '  7. staging.ic_communications';
    RAISE NOTICE '  8. staging.ic_postal_mail';
    RAISE NOTICE '';
    RAISE NOTICE 'Helper functions created:';
    RAISE NOTICE '  - staging.classify_activity_type()';
    RAISE NOTICE '';
    RAISE NOTICE 'Validation views created:';
    RAISE NOTICE '  - staging.v_ic_activities_summary';
    RAISE NOTICE '  - staging.v_ic_derivative_coverage';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '  1. Load communications master table';
    RAISE NOTICE '  2. Resolve foreign keys';
    RAISE NOTICE '  3. Populate derivative tables';
    RAISE NOTICE '  4. Run validation queries';
END $$;
