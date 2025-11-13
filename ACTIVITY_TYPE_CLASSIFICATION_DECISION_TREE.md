# Activity Type Classification Decision Tree
## HubSpot Activities Staging Decomposition

**Version:** 1.0
**Date:** 2025-11-13
**Purpose:** Define the complete logic for classifying legacy communications into HubSpot activity types

---

## Table of Contents

1. [Overview](#overview)
2. [Classification Algorithm](#classification-algorithm)
3. [Decision Tree Diagram](#decision-tree-diagram)
4. [Type Mapping Reference](#type-mapping-reference)
5. [Edge Cases & Special Rules](#edge-cases--special-rules)
6. [Testing Scenarios](#testing-scenarios)
7. [SQL Implementation](#sql-implementation)

---

## Overview

### Purpose

Transform a single `Comm_Type` field (and supporting fields) from the legacy CRM into one of 7 HubSpot activity types:

1. **calls** - Phone calls, VoIP calls
2. **emails** - Email communications
3. **meetings** - Appointments, meetings, visits
4. **notes** - General notes, comments, annotations
5. **tasks** - To-dos, follow-ups, action items
6. **communications** - SMS, LinkedIn, WhatsApp messages
7. **postal_mail** - Physical mail correspondence

### Input Fields

The classification logic uses three input fields from the communications master table:

| Field | Type | Description | Example Values |
|-------|------|-------------|----------------|
| `Comm_Type` | VARCHAR(100) | Primary type indicator | 'Call', 'Email', 'Meeting', 'Note', 'Case' |
| `Comm_Priority` | VARCHAR(50) | Priority level | 'Low', 'Medium', 'High', 'Urgent' |
| `Comm_CaseId` | INTEGER | Case/Ticket ID (if applicable) | 12345, NULL |

### Output

| Field | Type | Description | Example Value |
|-------|------|-------------|---------------|
| `derived_activity_type` | VARCHAR(50) | Determined activity type | 'calls' |

---

## Classification Algorithm

### High-Level Logic Flow

```
START
  ↓
1. Normalize Comm_Type to lowercase, trim whitespace
  ↓
2. Check for explicit type mapping in TYPE_MAP dictionary
  ↓
  YES → Return mapped activity type
  NO  → Continue
  ↓
3. Check if Comm_CaseId IS NOT NULL (Case-to-Task/Note conversion)
  ↓
  YES → Check Comm_Priority
     ↓
     High/Urgent/Critical → Return 'tasks'
     Low/Medium/NULL      → Return 'notes'
  NO  → Continue
  ↓
4. DEFAULT FALLBACK → Return 'notes'
  ↓
END
```

### Pseudocode

```python
def classify_activity_type(comm_type: str, comm_priority: str, comm_caseid: int) -> str:
    """
    Classify legacy communication into HubSpot activity type.

    Priority order:
    1. Explicit type mapping (highest priority)
    2. Case-to-Task/Note conversion
    3. Default fallback to 'notes'
    """
    # Step 1: Normalize inputs
    type_lower = (comm_type or '').lower().strip()
    priority_lower = (comm_priority or '').lower().strip()

    # Step 2: Explicit type mappings
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

        # Communications
        'sms': 'communications',
        'text': 'communications',
        'linkedin': 'communications',
        'whatsapp': 'communications',
        'wa': 'communications',
        'message': 'communications',

        # Postal Mail
        'postal': 'postal_mail',
        'letter': 'postal_mail',
    }

    # Check explicit mapping
    if type_lower in TYPE_MAP:
        return TYPE_MAP[type_lower]

    # Step 3: Case-to-Task/Note conversion
    if comm_caseid is not None:
        if priority_lower in ['high', 'urgent', 'critical']:
            return 'tasks'
        else:
            return 'notes'

    # Step 4: Default fallback
    return 'notes'
```

---

## Decision Tree Diagram

### Visual Representation

```
                            ┌──────────────────┐
                            │  Comm_Type       │
                            │  Comm_Priority   │
                            │  Comm_CaseId     │
                            └────────┬─────────┘
                                     │
                                     ▼
                    ┌────────────────────────────────┐
                    │ Normalize Comm_Type to         │
                    │ lowercase & trim               │
                    └────────────┬───────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────────────┐
                    │ Is Comm_Type in TYPE_MAP?      │
                    └────────────┬───────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
                   YES                       NO
                    │                         │
                    ▼                         ▼
        ┌───────────────────────┐   ┌─────────────────────────┐
        │ Return mapped type:   │   │ Is Comm_CaseId NOT NULL?│
        │ - calls               │   └──────────┬──────────────┘
        │ - emails              │              │
        │ - meetings            │   ┌──────────┴──────────┐
        │ - notes               │   │                     │
        │ - tasks               │  YES                   NO
        │ - communications      │   │                     │
        │ - postal_mail         │   ▼                     ▼
        └───────────────────────┘   │            ┌────────────────┐
                                    │            │ Return 'notes' │
                                    │            │  (default)     │
                                    │            └────────────────┘
                                    ▼
                    ┌─────────────────────────────────┐
                    │ Check Comm_Priority             │
                    └─────────────┬───────────────────┘
                                  │
                    ┌─────────────┴──────────────┐
                    │                            │
          Priority = High/Urgent/Critical      Other
                    │                            │
                    ▼                            ▼
        ┌───────────────────────┐    ┌──────────────────┐
        │ Return 'tasks'        │    │ Return 'notes'   │
        │ (Case → Task)         │    │ (Case → Note)    │
        └───────────────────────┘    └──────────────────┘
```

---

## Type Mapping Reference

### Complete TYPE_MAP Dictionary

#### Calls (calls)
| Legacy Value | Match Pattern | Case Sensitive? |
|--------------|---------------|-----------------|
| Call | `call` | No |
| Phone | `phone` | No |
| VoIP | `voip` | No |
| Zoom | `zoom` | No |
| Telephone | `telephone` | No |
| PhoneCall | `phonecall` | No |

**Examples:**
- `'Call'` → `'calls'`
- `'PHONE'` → `'calls'`
- `'zoom call'` → Does NOT match (contains space, not exact match)

---

#### Emails (emails)
| Legacy Value | Match Pattern | Case Sensitive? |
|--------------|---------------|-----------------|
| Email | `email` | No |
| E-mail | `e-mail` | No |

**Examples:**
- `'Email'` → `'emails'`
- `'E-MAIL'` → `'emails'`
- `'email message'` → Does NOT match (contains space)

---

#### Meetings (meetings)
| Legacy Value | Match Pattern | Case Sensitive? |
|--------------|---------------|-----------------|
| Meeting | `meeting` | No |
| Appointment | `appointment` | No |
| Visit | `visit` | No |
| Demo | `demo` | No |
| Presentation | `presentation` | No |

**Examples:**
- `'Meeting'` → `'meetings'`
- `'CLIENT VISIT'` → Does NOT match (contains space)
- `'Appointment'` → `'meetings'`

---

#### Notes (notes)
| Legacy Value | Match Pattern | Case Sensitive? |
|--------------|---------------|-----------------|
| Note | `note` | No |
| Comment | `comment` | No |
| Annotation | `annotation` | No |
| Memo | `memo` | No |

**Examples:**
- `'Note'` → `'notes'`
- `'COMMENT'` → `'notes'`
- `'note to file'` → Does NOT match

---

#### Tasks (tasks)
| Legacy Value | Match Pattern | Case Sensitive? |
|--------------|---------------|-----------------|
| Task | `task` | No |
| Todo | `todo` | No |
| To-Do | `to-do` | No |
| Follow-up | `follow-up` | No |
| Followup | `followup` | No |
| Action | `action` | No |

**Special Case:** Also includes Case → Task conversion (see below)

**Examples:**
- `'Task'` → `'tasks'`
- `'TODO'` → `'tasks'`
- `'follow-up'` → `'tasks'`

---

#### Communications (communications)
| Legacy Value | Match Pattern | Case Sensitive? |
|--------------|---------------|-----------------|
| SMS | `sms` | No |
| Text | `text` | No |
| LinkedIn | `linkedin` | No |
| WhatsApp | `whatsapp` | No |
| WA | `wa` | No |
| Message | `message` | No |

**Examples:**
- `'SMS'` → `'communications'`
- `'whatsapp'` → `'communications'`
- `'text message'` → Does NOT match (contains space)

---

#### Postal Mail (postal_mail)
| Legacy Value | Match Pattern | Case Sensitive? |
|--------------|---------------|-----------------|
| Postal | `postal` | No |
| Letter | `letter` | No |

**Note:** Ambiguity with 'mail' - could be email or postal. Requires explicit 'Postal' or 'Letter'.

**Examples:**
- `'Postal'` → `'postal_mail'`
- `'LETTER'` → `'postal_mail'`
- `'Mail'` → Does NOT match (see Email mapping for 'e-mail')

---

## Edge Cases & Special Rules

### 1. Case-to-Task Conversion

**Rule:** Communications with `Comm_CaseId IS NOT NULL` represent support tickets/cases. These should convert to either Tasks or Notes based on priority.

**Priority Threshold:**
- **High Priority** → Create Task (requires action)
  - Priority values: 'High', 'Urgent', 'Critical'
- **Low/Medium Priority** → Create Note (informational)
  - Priority values: 'Low', 'Medium', NULL, or any other value

**Examples:**

| Comm_Type | Comm_CaseId | Comm_Priority | Result | Reasoning |
|-----------|-------------|---------------|--------|-----------|
| 'Case' | 12345 | 'High' | `tasks` | High priority case requires action |
| 'Case' | 67890 | 'Low' | `notes` | Low priority case is informational |
| 'Case' | 54321 | NULL | `notes` | No priority = default to note |
| 'Case' | 11111 | 'Urgent' | `tasks` | Urgent case requires action |

**Source Tracking:** Tasks created from cases include:
```sql
source_type = 'case'
source_case_id = Comm_CaseId
```

---

### 2. NULL or Empty Comm_Type

**Rule:** If `Comm_Type` is NULL, empty string, or whitespace-only, default to `'notes'`.

**Examples:**
- `Comm_Type = NULL` → `'notes'`
- `Comm_Type = ''` → `'notes'`
- `Comm_Type = '   '` → `'notes'` (whitespace trimmed)

---

### 3. Unrecognized Comm_Type Values

**Rule:** Any value NOT in the TYPE_MAP and NOT matching the Case logic defaults to `'notes'`.

**Examples:**
- `'Unknown'` → `'notes'`
- `'General Communication'` → `'notes'`
- `'Follow-up Email'` → `'notes'` (multi-word, no match)
- `'123'` → `'notes'`

**Recommendation:** Review unrecognized types before migration:

```sql
-- Query to find unrecognized types
SELECT
    comm_type,
    COUNT(*) as count,
    staging.classify_activity_type(comm_type, NULL, NULL) as derived_type
FROM staging.ic_communication_master
GROUP BY comm_type
HAVING staging.classify_activity_type(comm_type, NULL, NULL) = 'notes'
  AND LOWER(TRIM(comm_type)) NOT IN ('note', 'comment', 'annotation', 'memo', 'case')
ORDER BY count DESC;
```

---

### 4. Multi-Word Type Values

**Rule:** The TYPE_MAP uses exact matching on the ENTIRE normalized string. Multi-word values do NOT match.

**Examples:**
- `'Phone Call'` → Does NOT match 'phone' or 'call' → `'notes'`
- `'Email Message'` → Does NOT match 'email' → `'notes'`
- `'Team Meeting'` → Does NOT match 'meeting' → `'notes'`

**Solution:** Either:
1. Add multi-word variations to TYPE_MAP
2. Pre-process `Comm_Type` to extract keywords
3. Accept as notes and manually reclassify

---

### 5. Ambiguous Types

**Potential Conflicts:**

1. **'Mail'** - Could be Email or Postal Mail
   - **Resolution:** 'mail' not in TYPE_MAP, requires explicit 'email'/'e-mail' or 'postal'
   - Default: `'notes'`

2. **'Message'** - Could be Email, SMS, or Note
   - **Resolution:** Maps to `'communications'` (assumes messaging channel)

3. **'Text'** - Could be SMS or general text note
   - **Resolution:** Maps to `'communications'` (assumes SMS)

**Recommendation:** Review these ambiguous mappings with business stakeholders.

---

### 6. Case Sensitivity

**Rule:** ALL matching is case-insensitive after normalization to lowercase.

**Examples:**
- `'CALL'` = `'Call'` = `'call'` = `'cAlL'` → All match → `'calls'`

---

### 7. Leading/Trailing Whitespace

**Rule:** Whitespace is trimmed before matching.

**Examples:**
- `'  Call  '` → Trimmed to `'call'` → Matches → `'calls'`
- `'\tEmail\n'` → Trimmed to `'email'` → Matches → `'emails'`

---

## Testing Scenarios

### Test Cases for Classification Logic

```sql
-- Test Case 1: Exact matches (case insensitive)
SELECT
    staging.classify_activity_type('Call', NULL, NULL) as test1,  -- Expected: calls
    staging.classify_activity_type('EMAIL', NULL, NULL) as test2,  -- Expected: emails
    staging.classify_activity_type('meeting', NULL, NULL) as test3, -- Expected: meetings
    staging.classify_activity_type('Note', NULL, NULL) as test4,   -- Expected: notes
    staging.classify_activity_type('Task', NULL, NULL) as test5,   -- Expected: tasks
    staging.classify_activity_type('SMS', NULL, NULL) as test6,    -- Expected: communications
    staging.classify_activity_type('Postal', NULL, NULL) as test7; -- Expected: postal_mail

-- Test Case 2: Whitespace trimming
SELECT
    staging.classify_activity_type('  Call  ', NULL, NULL) as test1,  -- Expected: calls
    staging.classify_activity_type('\tEmail\n', NULL, NULL) as test2; -- Expected: emails

-- Test Case 3: Case-to-Task conversion (high priority)
SELECT
    staging.classify_activity_type('Case', 'High', 123) as test1,    -- Expected: tasks
    staging.classify_activity_type('Case', 'Urgent', 456) as test2,  -- Expected: tasks
    staging.classify_activity_type('Case', 'Critical', 789) as test3; -- Expected: tasks

-- Test Case 4: Case-to-Note conversion (low priority)
SELECT
    staging.classify_activity_type('Case', 'Low', 123) as test1,     -- Expected: notes
    staging.classify_activity_type('Case', 'Medium', 456) as test2,  -- Expected: notes
    staging.classify_activity_type('Case', NULL, 789) as test3;      -- Expected: notes

-- Test Case 5: Default fallback
SELECT
    staging.classify_activity_type(NULL, NULL, NULL) as test1,           -- Expected: notes
    staging.classify_activity_type('', NULL, NULL) as test2,             -- Expected: notes
    staging.classify_activity_type('Unknown Type', NULL, NULL) as test3, -- Expected: notes
    staging.classify_activity_type('Phone Call', NULL, NULL) as test4;   -- Expected: notes (no match for multi-word)

-- Test Case 6: Variations
SELECT
    staging.classify_activity_type('phone', NULL, NULL) as test1,       -- Expected: calls
    staging.classify_activity_type('voip', NULL, NULL) as test2,        -- Expected: calls
    staging.classify_activity_type('e-mail', NULL, NULL) as test3,      -- Expected: emails
    staging.classify_activity_type('appointment', NULL, NULL) as test4, -- Expected: meetings
    staging.classify_activity_type('todo', NULL, NULL) as test5,        -- Expected: tasks
    staging.classify_activity_type('whatsapp', NULL, NULL) as test6,    -- Expected: communications
    staging.classify_activity_type('letter', NULL, NULL) as test7;      -- Expected: postal_mail
```

### Expected Results

| Test | Input | Expected Output | Pass/Fail |
|------|-------|-----------------|-----------|
| 1.1 | `('Call', NULL, NULL)` | `'calls'` | |
| 1.2 | `('EMAIL', NULL, NULL)` | `'emails'` | |
| 1.3 | `('meeting', NULL, NULL)` | `'meetings'` | |
| 1.4 | `('Note', NULL, NULL)` | `'notes'` | |
| 1.5 | `('Task', NULL, NULL)` | `'tasks'` | |
| 1.6 | `('SMS', NULL, NULL)` | `'communications'` | |
| 1.7 | `('Postal', NULL, NULL)` | `'postal_mail'` | |
| 2.1 | `('  Call  ', NULL, NULL)` | `'calls'` | |
| 2.2 | `('\tEmail\n', NULL, NULL)` | `'emails'` | |
| 3.1 | `('Case', 'High', 123)` | `'tasks'` | |
| 3.2 | `('Case', 'Urgent', 456)` | `'tasks'` | |
| 3.3 | `('Case', 'Critical', 789)` | `'tasks'` | |
| 4.1 | `('Case', 'Low', 123)` | `'notes'` | |
| 4.2 | `('Case', 'Medium', 456)` | `'notes'` | |
| 4.3 | `('Case', NULL, 789)` | `'notes'` | |
| 5.1 | `(NULL, NULL, NULL)` | `'notes'` | |
| 5.2 | `('', NULL, NULL)` | `'notes'` | |
| 5.3 | `('Unknown Type', NULL, NULL)` | `'notes'` | |
| 5.4 | `('Phone Call', NULL, NULL)` | `'notes'` | |
| 6.1 | `('phone', NULL, NULL)` | `'calls'` | |
| 6.2 | `('voip', NULL, NULL)` | `'calls'` | |
| 6.3 | `('e-mail', NULL, NULL)` | `'emails'` | |
| 6.4 | `('appointment', NULL, NULL)` | `'meetings'` | |
| 6.5 | `('todo', NULL, NULL)` | `'tasks'` | |
| 6.6 | `('whatsapp', NULL, NULL)` | `'communications'` | |
| 6.7 | `('letter', NULL, NULL)` | `'postal_mail'` | |

---

## SQL Implementation

### Function: `staging.classify_activity_type()`

Located in: `hubspot_activities_staging_ddl.sql`

```sql
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
```

### Usage in Master Table Load

```sql
-- Classify activity type during insert/update
INSERT INTO staging.ic_communication_master (
    comm_communicationid,
    comm_type,
    comm_priority,
    comm_caseid,
    derived_activity_type,
    -- ... other fields
)
SELECT
    Comm_CommunicationId,
    Comm_Type,
    Comm_Priority,
    Comm_CaseId,
    staging.classify_activity_type(Comm_Type, Comm_Priority, Comm_CaseId) as derived_activity_type,
    -- ... other fields
FROM source_communications_csv;
```

---

## Summary Statistics Query

```sql
-- Distribution of activity types after classification
SELECT
    derived_activity_type,
    COUNT(*) as record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM staging.ic_communication_master
GROUP BY derived_activity_type
ORDER BY record_count DESC;
```

### Expected Output Example

| derived_activity_type | record_count | percentage |
|-----------------------|--------------|------------|
| notes | 12,453 | 45.23% |
| calls | 8,721 | 31.67% |
| emails | 3,456 | 12.55% |
| meetings | 1,892 | 6.87% |
| tasks | 678 | 2.46% |
| communications | 234 | 0.85% |
| postal_mail | 102 | 0.37% |

---

## Maintenance & Updates

### Adding New Type Mappings

To add support for a new `Comm_Type` value:

1. **Update SQL Function** (`hubspot_activities_staging_ddl.sql`):
   ```sql
   -- Add to CASE statement
   WHEN 'newtype', 'newtypealias' THEN
       RETURN 'target_activity_type';
   ```

2. **Update This Document**:
   - Add to [Type Mapping Reference](#type-mapping-reference)
   - Add test case to [Testing Scenarios](#testing-scenarios)

3. **Retest**:
   ```sql
   SELECT staging.classify_activity_type('newtype', NULL, NULL);
   -- Verify expected output
   ```

---

## Document Revision History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2025-11-13 | Initial release | IC-D-LOAD Project |

---

**End of Document**
