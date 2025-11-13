-- ============================================================================
-- HubSpot Activities Staging Views & Population Queries
-- ============================================================================
-- Purpose: SQL views for transforming master table data into derivative tables
-- Author: IC-D-LOAD Project
-- Date: 2025-11-13
-- Version: 1.0
--
-- Usage:
--   1. Run DDL script first (hubspot_activities_staging_ddl.sql)
--   2. Load master table with communications data
--   3. Resolve foreign keys
--   4. Use these views to populate derivative tables
-- ============================================================================

-- ============================================================================
-- SECTION 1: SQL VIEWS FOR EACH ACTIVITY TYPE
-- ============================================================================

-- ============================================================================
-- 1. VIEW: Calls Staging Data
-- ============================================================================

CREATE OR REPLACE VIEW staging.v_ic_calls_staging AS
SELECT
    -- Master table reference
    m.id as ic_communication_master_id,

    -- General activity properties
    m.comm_datetime as activity_date,
    NULL::VARCHAR(500) as activity_assigned_to,  -- TODO: Map from user table
    NULL::VARCHAR(500) as activity_created_by,   -- TODO: Map from user table
    m.comm_createddate as create_date,
    m.comm_updateddate as last_modified_date,

    -- Call-specific properties
    COALESCE(
        m.comm_subject,
        'Call - ' || COALESCE(m.person_firstname || ' ' || m.person_lastname, 'Unknown')
    ) as call_title,

    m.comm_note as call_notes,

    -- Call direction mapping
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_action, ''))) IN ('inbound', 'incoming', 'received', 'in') THEN 'INBOUND'
        WHEN LOWER(TRIM(COALESCE(m.comm_action, ''))) IN ('outbound', 'outgoing', 'sent', 'out') THEN 'OUTBOUND'
        ELSE 'OUTBOUND'  -- Default to outbound
    END as call_direction,

    -- Call duration (milliseconds) - calculated from DateTime difference
    CASE
        WHEN m.comm_todatetime IS NOT NULL AND m.comm_todatetime > m.comm_datetime THEN
            EXTRACT(EPOCH FROM (m.comm_todatetime - m.comm_datetime))::BIGINT * 1000
        ELSE NULL
    END as call_duration_ms,

    -- Call status mapping
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('completed', 'done', 'finished', 'successful') THEN 'COMPLETED'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('missed', 'no answer', 'noanswer', 'unanswered') THEN 'NO_ANSWER'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('busy') THEN 'BUSY'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('cancelled', 'canceled') THEN 'CANCELED'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('failed', 'error') THEN 'FAILED'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('in progress', 'ongoing', 'active') THEN 'IN_PROGRESS'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('ringing', 'calling') THEN 'RINGING'
        ELSE 'COMPLETED'  -- Default to completed for legacy data
    END as call_status,

    -- Call outcome (more descriptive than status)
    COALESCE(m.comm_status, 'Connected') as call_outcome,

    'Integration' as call_source,

    -- Phone number extraction (placeholder - implement regex or lookup logic)
    -- TODO: Extract from comm_note or separate phone fields
    NULL::VARCHAR(100) as from_number,
    NULL::VARCHAR(100) as to_number,

    -- Associations (resolved HubSpot IDs)
    m.hubspot_contact_id as associated_contact_id,
    m.hubspot_company_id as associated_company_id,
    m.hubspot_deal_id as associated_deal_id,

    -- API payload (will be built separately)
    NULL::JSONB as hubspot_api_payload,

    -- Audit
    NOW() as created_at,
    NOW() as last_updated

FROM staging.ic_communication_master m
WHERE m.derived_activity_type = 'calls'
  AND m.staging_status = 'pending';

COMMENT ON VIEW staging.v_ic_calls_staging IS
'Transforms master table data for call activities. Use this view to INSERT INTO staging.ic_calls.';

-- ============================================================================
-- 2. VIEW: Emails Staging Data
-- ============================================================================

CREATE OR REPLACE VIEW staging.v_ic_emails_staging AS
SELECT
    -- Master table reference
    m.id as ic_communication_master_id,

    -- General activity properties
    m.comm_datetime as activity_date,
    NULL::VARCHAR(500) as activity_assigned_to,
    NULL::VARCHAR(500) as activity_created_by,
    m.comm_createddate as create_date,
    m.comm_updateddate as last_modified_date,

    -- Email-specific properties
    COALESCE(m.comm_subject, 'Email - No Subject') as email_subject,
    m.comm_note as email_body,

    -- Email direction mapping
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_action, ''))) IN ('inbound', 'incoming', 'received', 'in') THEN 'INCOMING'
        WHEN LOWER(TRIM(COALESCE(m.comm_action, ''))) IN ('outbound', 'outgoing', 'sent', 'out') THEN 'OUTGOING'
        ELSE 'OUTGOING'  -- Default to outgoing
    END as email_direction,

    -- Email from/to address logic
    -- If OUTGOING: from = person email (internal), to = comm_email_clean (external)
    -- If INCOMING: from = comm_email_clean (external), to = person email (internal)
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_action, ''))) IN ('outbound', 'outgoing', 'sent', 'out')
        THEN COALESCE(m.person_emailaddress, 'noreply@company.com')  -- Internal sender
        ELSE COALESCE(m.comm_email_clean, m.person_emailaddress)     -- External sender
    END as email_from_address,

    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_action, ''))) IN ('outbound', 'outgoing', 'sent', 'out')
        THEN COALESCE(m.comm_email_clean, m.person_emailaddress)     -- External recipient
        ELSE COALESCE(m.person_emailaddress, 'info@company.com')    -- Internal recipient
    END as email_to_address,

    -- Email send status mapping
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('sent', 'delivered', 'completed', 'successful') THEN 'SENT'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('failed', 'error', 'bounced') THEN 'FAILED'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('scheduled', 'pending') THEN 'SCHEDULED'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('sending', 'in progress') THEN 'SENDING'
        ELSE 'SENT'  -- Default to sent for legacy data
    END as email_send_status,

    -- Email metrics (NULL for imported emails - no tracking data)
    NULL::DECIMAL(5,2) as email_open_rate,
    NULL::DECIMAL(5,2) as email_click_rate,
    NULL::DECIMAL(5,2) as email_reply_rate,
    0 as number_of_email_opens,
    0 as number_of_email_clicks,
    0 as number_of_email_replies,

    -- Associations
    m.hubspot_contact_id as associated_contact_id,
    m.hubspot_company_id as associated_company_id,
    m.hubspot_deal_id as associated_deal_id,

    -- API payload
    NULL::JSONB as hubspot_api_payload,

    -- Audit
    NOW() as created_at,
    NOW() as last_updated

FROM staging.ic_communication_master m
WHERE m.derived_activity_type = 'emails'
  AND m.staging_status = 'pending';

COMMENT ON VIEW staging.v_ic_emails_staging IS
'Transforms master table data for email activities. Use this view to INSERT INTO staging.ic_emails.';

-- ============================================================================
-- 3. VIEW: Meetings Staging Data
-- ============================================================================

CREATE OR REPLACE VIEW staging.v_ic_meetings_staging AS
SELECT
    -- Master table reference
    m.id as ic_communication_master_id,

    -- General activity properties
    m.comm_datetime as activity_date,
    NULL::VARCHAR(500) as activity_assigned_to,
    NULL::VARCHAR(500) as activity_created_by,
    m.comm_createddate as create_date,
    m.comm_updateddate as last_modified_date,

    -- Meeting-specific properties
    COALESCE(
        m.comm_subject,
        'Meeting with ' || COALESCE(m.person_firstname || ' ' || m.person_lastname, m.company_name, 'Unknown')
    ) as meeting_name,

    m.comm_note as meeting_description,

    m.comm_datetime as meeting_start_time,

    -- Meeting end time: use comm_todatetime if available, else add 1 hour to start
    COALESCE(
        m.comm_todatetime,
        m.comm_datetime + INTERVAL '1 hour'
    ) as meeting_end_time,

    m.comm_note as internal_meeting_notes,

    -- Meeting location (placeholder - could extract from note)
    NULL::VARCHAR(500) as meeting_location,

    -- Location type (default to video conference for modern data)
    'VIDEO_CONFERENCE'::VARCHAR(50) as location_type,

    -- Meeting outcome mapping
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('scheduled', 'planned', 'upcoming') THEN 'SCHEDULED'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('completed', 'done', 'finished', 'attended') THEN 'COMPLETED'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('cancelled', 'canceled') THEN 'CANCELLED'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('no show', 'noshow', 'missed') THEN 'NO_SHOW'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('rescheduled', 'postponed') THEN 'RESCHEDULED'
        ELSE 'COMPLETED'  -- Default
    END as meeting_outcome,

    'Import' as meeting_source,

    -- Outcome count flags (set to 1 for the matching outcome)
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('completed', 'done', 'finished', 'attended') THEN 1
        ELSE 0
    END as outcome_completed_count,

    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('cancelled', 'canceled') THEN 1
        ELSE 0
    END as outcome_canceled_count,

    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('no show', 'noshow', 'missed') THEN 1
        ELSE 0
    END as outcome_no_show_count,

    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('rescheduled', 'postponed') THEN 1
        ELSE 0
    END as outcome_rescheduled_count,

    -- Associations
    m.hubspot_contact_id as associated_contact_id,
    m.hubspot_company_id as associated_company_id,
    m.hubspot_deal_id as associated_deal_id,

    -- API payload
    NULL::JSONB as hubspot_api_payload,

    -- Audit
    NOW() as created_at,
    NOW() as last_updated

FROM staging.ic_communication_master m
WHERE m.derived_activity_type = 'meetings'
  AND m.staging_status = 'pending';

COMMENT ON VIEW staging.v_ic_meetings_staging IS
'Transforms master table data for meeting activities. Use this view to INSERT INTO staging.ic_meetings.';

-- ============================================================================
-- 4. VIEW: Notes Staging Data
-- ============================================================================

CREATE OR REPLACE VIEW staging.v_ic_notes_staging AS
SELECT
    -- Master table reference
    m.id as ic_communication_master_id,

    -- General activity properties
    m.comm_datetime as activity_date,
    NULL::VARCHAR(500) as activity_assigned_to,
    NULL::VARCHAR(500) as activity_created_by,
    m.comm_createddate as create_date,
    m.comm_updateddate as last_modified_date,

    -- Note body: Combine subject and note with separator
    CASE
        WHEN m.comm_subject IS NOT NULL AND m.comm_note IS NOT NULL
        THEN m.comm_subject || E'\n\n' || m.comm_note
        WHEN m.comm_subject IS NOT NULL
        THEN m.comm_subject
        WHEN m.comm_note IS NOT NULL
        THEN m.comm_note
        ELSE 'Note (no content)'
    END as note_body,

    -- Associations
    m.hubspot_contact_id as associated_contact_id,
    m.hubspot_company_id as associated_company_id,
    m.hubspot_deal_id as associated_deal_id,

    -- API payload
    NULL::JSONB as hubspot_api_payload,

    -- Audit
    NOW() as created_at,
    NOW() as last_updated

FROM staging.ic_communication_master m
WHERE m.derived_activity_type = 'notes'
  AND m.staging_status = 'pending';

COMMENT ON VIEW staging.v_ic_notes_staging IS
'Transforms master table data for note activities. Use this view to INSERT INTO staging.ic_notes.';

-- ============================================================================
-- 5. VIEW: Tasks Staging Data
-- ============================================================================

CREATE OR REPLACE VIEW staging.v_ic_tasks_staging AS
SELECT
    -- Master table reference
    m.id as ic_communication_master_id,

    -- General activity properties
    m.comm_datetime as activity_date,
    NULL::VARCHAR(500) as activity_assigned_to,
    NULL::VARCHAR(500) as activity_created_by,
    m.comm_createddate as create_date,
    m.comm_updateddate as last_modified_date,

    -- Task-specific properties
    COALESCE(
        m.comm_subject,
        'Task - ' || COALESCE(m.company_name, m.person_firstname || ' ' || m.person_lastname, 'General')
    ) as task_title,

    m.comm_note as task_notes,

    -- Due date: prefer comm_todatetime, fallback to comm_datetime + 1 day
    COALESCE(
        m.comm_todatetime,
        m.comm_datetime + INTERVAL '1 day'
    ) as due_date,

    -- Task status mapping
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('completed', 'done', 'finished', 'closed') THEN 'COMPLETED'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('in progress', 'ongoing', 'active', 'started') THEN 'IN_PROGRESS'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('waiting', 'on hold', 'blocked') THEN 'WAITING'
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('deferred', 'postponed') THEN 'DEFERRED'
        ELSE 'NOT_STARTED'  -- Default
    END as task_status,

    -- Completed at: set if status is completed
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_status, ''))) IN ('completed', 'done', 'finished', 'closed')
        THEN COALESCE(m.comm_updateddate, m.comm_datetime)
        ELSE NULL
    END as completed_at,

    -- Task priority mapping
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_priority, ''))) IN ('high', 'urgent', 'critical', '1') THEN 'HIGH'
        WHEN LOWER(TRIM(COALESCE(m.comm_priority, ''))) IN ('medium', 'normal', '2') THEN 'MEDIUM'
        WHEN LOWER(TRIM(COALESCE(m.comm_priority, ''))) IN ('low', '3') THEN 'LOW'
        ELSE 'MEDIUM'  -- Default
    END as task_priority,

    -- Task type (default to TODO)
    'TODO' as task_type,

    -- Is overdue: due date < now AND not completed
    CASE
        WHEN COALESCE(m.comm_todatetime, m.comm_datetime + INTERVAL '1 day') < NOW()
         AND LOWER(TRIM(COALESCE(m.comm_status, ''))) NOT IN ('completed', 'done', 'finished', 'closed')
        THEN TRUE
        ELSE FALSE
    END as is_overdue,

    -- Reminder date: use original datetime if different from actual
    CASE
        WHEN m.comm_originaldatetime IS NOT NULL
         AND m.comm_originaldatetime != m.comm_datetime
        THEN m.comm_originaldatetime
        ELSE NULL
    END as reminder_date,

    -- Source tracking (for Case-to-Task conversion)
    CASE
        WHEN m.comm_caseid IS NOT NULL THEN 'case'
        ELSE 'task'
    END as source_type,

    m.comm_caseid as source_case_id,

    -- Associations
    m.hubspot_contact_id as associated_contact_id,
    m.hubspot_company_id as associated_company_id,
    m.hubspot_deal_id as associated_deal_id,

    -- API payload
    NULL::JSONB as hubspot_api_payload,

    -- Audit
    NOW() as created_at,
    NOW() as last_updated

FROM staging.ic_communication_master m
WHERE m.derived_activity_type = 'tasks'
  AND m.staging_status = 'pending';

COMMENT ON VIEW staging.v_ic_tasks_staging IS
'Transforms master table data for task activities (including Case-to-Task conversions). Use this view to INSERT INTO staging.ic_tasks.';

-- ============================================================================
-- 6. VIEW: Communications Staging Data (SMS/LinkedIn/WhatsApp)
-- ============================================================================

CREATE OR REPLACE VIEW staging.v_ic_communications_staging AS
SELECT
    -- Master table reference
    m.id as ic_communication_master_id,

    -- General activity properties
    m.comm_datetime as activity_date,
    NULL::VARCHAR(500) as activity_assigned_to,
    NULL::VARCHAR(500) as activity_created_by,
    m.comm_createddate as create_date,
    m.comm_updateddate as last_modified_date,

    -- Communication-specific properties
    COALESCE(m.comm_note, m.comm_subject, 'Message') as communication_body,

    -- Channel type mapping from comm_type
    CASE
        WHEN LOWER(TRIM(COALESCE(m.comm_type, ''))) IN ('sms', 'text') THEN 'SMS'
        WHEN LOWER(TRIM(COALESCE(m.comm_type, ''))) IN ('linkedin', 'linkedin message') THEN 'LINKEDIN_MESSAGE'
        WHEN LOWER(TRIM(COALESCE(m.comm_type, ''))) IN ('whatsapp', 'wa') THEN 'WHATS_APP'
        ELSE 'CUSTOM'
    END as channel_type,

    'API' as logged_from,

    -- Associations
    m.hubspot_contact_id as associated_contact_id,
    m.hubspot_company_id as associated_company_id,
    m.hubspot_deal_id as associated_deal_id,

    -- API payload
    NULL::JSONB as hubspot_api_payload,

    -- Audit
    NOW() as created_at,
    NOW() as last_updated

FROM staging.ic_communication_master m
WHERE m.derived_activity_type = 'communications'
  AND m.staging_status = 'pending';

COMMENT ON VIEW staging.v_ic_communications_staging IS
'Transforms master table data for communication activities (SMS, LinkedIn, WhatsApp). Use this view to INSERT INTO staging.ic_communications.';

-- ============================================================================
-- 7. VIEW: Postal Mail Staging Data
-- ============================================================================

CREATE OR REPLACE VIEW staging.v_ic_postal_mail_staging AS
SELECT
    -- Master table reference
    m.id as ic_communication_master_id,

    -- General activity properties
    m.comm_datetime as activity_date,
    NULL::VARCHAR(500) as activity_assigned_to,
    NULL::VARCHAR(500) as activity_created_by,
    m.comm_createddate as create_date,
    m.comm_updateddate as last_modified_date,

    -- Postal mail-specific properties
    COALESCE(m.comm_note, m.comm_subject, 'Postal mail sent') as postal_mail_body,

    -- Body preview (first 500 characters)
    SUBSTRING(COALESCE(m.comm_note, m.comm_subject, 'Postal mail sent'), 1, 500) as body_preview,

    -- Associations
    m.hubspot_contact_id as associated_contact_id,
    m.hubspot_company_id as associated_company_id,
    m.hubspot_deal_id as associated_deal_id,

    -- API payload
    NULL::JSONB as hubspot_api_payload,

    -- Audit
    NOW() as created_at,
    NOW() as last_updated

FROM staging.ic_communication_master m
WHERE m.derived_activity_type = 'postal_mail'
  AND m.staging_status = 'pending';

COMMENT ON VIEW staging.v_ic_postal_mail_staging IS
'Transforms master table data for postal mail activities. Use this view to INSERT INTO staging.ic_postal_mail.';

-- ============================================================================
-- SECTION 2: MATERIALIZATION QUERIES (INSERT FROM VIEWS)
-- ============================================================================

-- ============================================================================
-- POPULATE ALL DERIVATIVE TABLES FROM VIEWS
-- ============================================================================

-- Transaction wrapper for all inserts
DO $$
DECLARE
    v_calls_count INTEGER;
    v_emails_count INTEGER;
    v_meetings_count INTEGER;
    v_notes_count INTEGER;
    v_tasks_count INTEGER;
    v_comms_count INTEGER;
    v_postal_count INTEGER;
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Populating HubSpot Activities Staging Tables';
    RAISE NOTICE '============================================';
    RAISE NOTICE '';

    -- 1. Populate Calls
    RAISE NOTICE '1. Populating staging.ic_calls...';
    INSERT INTO staging.ic_calls (
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, call_title, call_notes, call_direction,
        call_duration_ms, call_status, call_outcome, call_source, from_number, to_number,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    )
    SELECT
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, call_title, call_notes, call_direction,
        call_duration_ms, call_status, call_outcome, call_source, from_number, to_number,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    FROM staging.v_ic_calls_staging
    ON CONFLICT (ic_communication_master_id) DO UPDATE SET
        activity_date = EXCLUDED.activity_date,
        call_title = EXCLUDED.call_title,
        call_notes = EXCLUDED.call_notes,
        call_direction = EXCLUDED.call_direction,
        call_duration_ms = EXCLUDED.call_duration_ms,
        call_status = EXCLUDED.call_status,
        last_updated = NOW();

    GET DIAGNOSTICS v_calls_count = ROW_COUNT;
    RAISE NOTICE '   ✓ Inserted/Updated % call records', v_calls_count;

    -- 2. Populate Emails
    RAISE NOTICE '2. Populating staging.ic_emails...';
    INSERT INTO staging.ic_emails (
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, email_subject, email_body, email_direction,
        email_from_address, email_to_address, email_send_status,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    )
    SELECT
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, email_subject, email_body, email_direction,
        email_from_address, email_to_address, email_send_status,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    FROM staging.v_ic_emails_staging
    ON CONFLICT (ic_communication_master_id) DO UPDATE SET
        activity_date = EXCLUDED.activity_date,
        email_subject = EXCLUDED.email_subject,
        email_body = EXCLUDED.email_body,
        last_updated = NOW();

    GET DIAGNOSTICS v_emails_count = ROW_COUNT;
    RAISE NOTICE '   ✓ Inserted/Updated % email records', v_emails_count;

    -- 3. Populate Meetings
    RAISE NOTICE '3. Populating staging.ic_meetings...';
    INSERT INTO staging.ic_meetings (
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, meeting_name, meeting_description,
        meeting_start_time, meeting_end_time, internal_meeting_notes, meeting_location,
        location_type, meeting_outcome, meeting_source,
        outcome_completed_count, outcome_canceled_count, outcome_no_show_count, outcome_rescheduled_count,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    )
    SELECT
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, meeting_name, meeting_description,
        meeting_start_time, meeting_end_time, internal_meeting_notes, meeting_location,
        location_type, meeting_outcome, meeting_source,
        outcome_completed_count, outcome_canceled_count, outcome_no_show_count, outcome_rescheduled_count,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    FROM staging.v_ic_meetings_staging
    ON CONFLICT (ic_communication_master_id) DO UPDATE SET
        activity_date = EXCLUDED.activity_date,
        meeting_name = EXCLUDED.meeting_name,
        meeting_start_time = EXCLUDED.meeting_start_time,
        meeting_end_time = EXCLUDED.meeting_end_time,
        last_updated = NOW();

    GET DIAGNOSTICS v_meetings_count = ROW_COUNT;
    RAISE NOTICE '   ✓ Inserted/Updated % meeting records', v_meetings_count;

    -- 4. Populate Notes
    RAISE NOTICE '4. Populating staging.ic_notes...';
    INSERT INTO staging.ic_notes (
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, note_body,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    )
    SELECT
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, note_body,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    FROM staging.v_ic_notes_staging
    ON CONFLICT (ic_communication_master_id) DO UPDATE SET
        activity_date = EXCLUDED.activity_date,
        note_body = EXCLUDED.note_body,
        last_updated = NOW();

    GET DIAGNOSTICS v_notes_count = ROW_COUNT;
    RAISE NOTICE '   ✓ Inserted/Updated % note records', v_notes_count;

    -- 5. Populate Tasks
    RAISE NOTICE '5. Populating staging.ic_tasks...';
    INSERT INTO staging.ic_tasks (
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, task_title, task_notes, due_date,
        task_status, completed_at, task_priority, task_type, is_overdue, reminder_date,
        source_type, source_case_id,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    )
    SELECT
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, task_title, task_notes, due_date,
        task_status, completed_at, task_priority, task_type, is_overdue, reminder_date,
        source_type, source_case_id,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    FROM staging.v_ic_tasks_staging
    ON CONFLICT (ic_communication_master_id) DO UPDATE SET
        activity_date = EXCLUDED.activity_date,
        task_title = EXCLUDED.task_title,
        due_date = EXCLUDED.due_date,
        task_status = EXCLUDED.task_status,
        is_overdue = EXCLUDED.is_overdue,
        last_updated = NOW();

    GET DIAGNOSTICS v_tasks_count = ROW_COUNT;
    RAISE NOTICE '   ✓ Inserted/Updated % task records', v_tasks_count;

    -- 6. Populate Communications
    RAISE NOTICE '6. Populating staging.ic_communications...';
    INSERT INTO staging.ic_communications (
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, communication_body, channel_type, logged_from,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    )
    SELECT
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, communication_body, channel_type, logged_from,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    FROM staging.v_ic_communications_staging
    ON CONFLICT (ic_communication_master_id) DO UPDATE SET
        activity_date = EXCLUDED.activity_date,
        communication_body = EXCLUDED.communication_body,
        channel_type = EXCLUDED.channel_type,
        last_updated = NOW();

    GET DIAGNOSTICS v_comms_count = ROW_COUNT;
    RAISE NOTICE '   ✓ Inserted/Updated % communication records', v_comms_count;

    -- 7. Populate Postal Mail
    RAISE NOTICE '7. Populating staging.ic_postal_mail...';
    INSERT INTO staging.ic_postal_mail (
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, postal_mail_body, body_preview,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    )
    SELECT
        ic_communication_master_id, activity_date, activity_assigned_to, activity_created_by,
        create_date, last_modified_date, postal_mail_body, body_preview,
        associated_contact_id, associated_company_id, associated_deal_id,
        hubspot_api_payload
    FROM staging.v_ic_postal_mail_staging
    ON CONFLICT (ic_communication_master_id) DO UPDATE SET
        activity_date = EXCLUDED.activity_date,
        postal_mail_body = EXCLUDED.postal_mail_body,
        body_preview = EXCLUDED.body_preview,
        last_updated = NOW();

    GET DIAGNOSTICS v_postal_count = ROW_COUNT;
    RAISE NOTICE '   ✓ Inserted/Updated % postal mail records', v_postal_count;

    -- Summary
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Population Complete!';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Total records processed:';
    RAISE NOTICE '  Calls:          %', v_calls_count;
    RAISE NOTICE '  Emails:         %', v_emails_count;
    RAISE NOTICE '  Meetings:       %', v_meetings_count;
    RAISE NOTICE '  Notes:          %', v_notes_count;
    RAISE NOTICE '  Tasks:          %', v_tasks_count;
    RAISE NOTICE '  Communications: %', v_comms_count;
    RAISE NOTICE '  Postal Mail:    %', v_postal_count;
    RAISE NOTICE '  TOTAL:          %', (v_calls_count + v_emails_count + v_meetings_count + v_notes_count + v_tasks_count + v_comms_count + v_postal_count);
    RAISE NOTICE '============================================';
END $$;

-- ============================================================================
-- SECTION 3: VALIDATION QUERIES
-- ============================================================================

-- Query: Check coverage (all master records have derivatives)
SELECT * FROM staging.v_ic_derivative_coverage;

-- Query: Summary by activity type
SELECT * FROM staging.v_ic_activities_summary;

-- Query: Check for orphaned records (no associations)
SELECT
    derived_activity_type,
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE is_orphaned = TRUE) as orphaned_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_orphaned = TRUE) / COUNT(*), 2) as orphaned_pct
FROM staging.ic_communication_master
GROUP BY derived_activity_type
ORDER BY derived_activity_type;

-- Query: Check required fields populated in derivative tables
SELECT
    'calls' as table_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE call_title IS NULL) as missing_title,
    COUNT(*) FILTER (WHERE activity_date IS NULL) as missing_date,
    COUNT(*) FILTER (WHERE associated_contact_id IS NULL
                      AND associated_company_id IS NULL
                      AND associated_deal_id IS NULL) as no_associations
FROM staging.ic_calls
UNION ALL
SELECT
    'emails',
    COUNT(*),
    COUNT(*) FILTER (WHERE email_subject IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL),
    COUNT(*) FILTER (WHERE associated_contact_id IS NULL
                      AND associated_company_id IS NULL
                      AND associated_deal_id IS NULL)
FROM staging.ic_emails
UNION ALL
SELECT
    'meetings',
    COUNT(*),
    COUNT(*) FILTER (WHERE meeting_name IS NULL),
    COUNT(*) FILTER (WHERE meeting_start_time IS NULL),
    COUNT(*) FILTER (WHERE associated_contact_id IS NULL
                      AND associated_company_id IS NULL
                      AND associated_deal_id IS NULL)
FROM staging.ic_meetings
UNION ALL
SELECT
    'notes',
    COUNT(*),
    COUNT(*) FILTER (WHERE note_body IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL),
    COUNT(*) FILTER (WHERE associated_contact_id IS NULL
                      AND associated_company_id IS NULL
                      AND associated_deal_id IS NULL)
FROM staging.ic_notes
UNION ALL
SELECT
    'tasks',
    COUNT(*),
    COUNT(*) FILTER (WHERE task_title IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL),
    COUNT(*) FILTER (WHERE associated_contact_id IS NULL
                      AND associated_company_id IS NULL
                      AND associated_deal_id IS NULL)
FROM staging.ic_tasks
UNION ALL
SELECT
    'communications',
    COUNT(*),
    COUNT(*) FILTER (WHERE communication_body IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL),
    COUNT(*) FILTER (WHERE associated_contact_id IS NULL
                      AND associated_company_id IS NULL
                      AND associated_deal_id IS NULL)
FROM staging.ic_communications
UNION ALL
SELECT
    'postal_mail',
    COUNT(*),
    COUNT(*) FILTER (WHERE postal_mail_body IS NULL),
    COUNT(*) FILTER (WHERE activity_date IS NULL),
    COUNT(*) FILTER (WHERE associated_contact_id IS NULL
                      AND associated_company_id IS NULL
                      AND associated_deal_id IS NULL)
FROM staging.ic_postal_mail;

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '✓ HubSpot Activities Staging Views Created Successfully!';
    RAISE NOTICE '';
    RAISE NOTICE 'Views created:';
    RAISE NOTICE '  - staging.v_ic_calls_staging';
    RAISE NOTICE '  - staging.v_ic_emails_staging';
    RAISE NOTICE '  - staging.v_ic_meetings_staging';
    RAISE NOTICE '  - staging.v_ic_notes_staging';
    RAISE NOTICE '  - staging.v_ic_tasks_staging';
    RAISE NOTICE '  - staging.v_ic_communications_staging';
    RAISE NOTICE '  - staging.v_ic_postal_mail_staging';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '  1. Review validation query results above';
    RAISE NOTICE '  2. Check for missing required fields';
    RAISE NOTICE '  3. Resolve any data quality issues';
    RAISE NOTICE '  4. Proceed to API payload generation (future phase)';
END $$;
