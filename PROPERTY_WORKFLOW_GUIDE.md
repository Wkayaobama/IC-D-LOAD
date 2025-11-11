# Property & Workflow Configuration Guide

## Where Properties Are Defined

### **1. Property Mappings** (`property_mapping_config.py`)

```python
COMPANY_MAPPING = ObjectMapping(
    object_type="companies",
    legacy_table="Company",                          # Legacy CRM table
    legacy_id_field="Comp_CompanyId",               # Legacy ID field
    hubspot_id_field="hs_object_id",                # HubSpot record ID
    hubspot_legacy_id_field="icalps_company_id",    # HubSpot join field

    property_mappings=[
        # Legacy Property → HubSpot Property
        PropertyMapping("Comp_Name", "name", is_required=True),
        PropertyMapping("Comp_WebSite", "domain"),
        PropertyMapping("Addr_City", "city"),
    ],

    required_properties=[
        "hs_object_id",        # Always query
        "icalps_company_id",   # Always query (join key)
    ]
)
```

### **2. How Properties Generate PostgreSQL Queries**

```python
from property_mapping_config import get_hubspot_properties

# Get properties to query from PostgreSQL
props = get_hubspot_properties('companies')
# Returns: ['hs_object_id', 'name', 'domain', 'city', 'icalps_company_id', ...]

# Generates SQL like:
"""
SELECT
    hs_object_id,
    name,
    domain,
    city,
    icalps_company_id
FROM hubspot.companies
WHERE icalps_company_id IS NOT NULL
"""
```

### **3. Property Usage Flow**

```
┌─────────────────────────────────────────────────────────────┐
│        property_mapping_config.py                           │
│                                                             │
│  COMPANY_MAPPING:                                           │
│    - legacy_id_field: "Comp_CompanyId"                     │
│    - hubspot_id_field: "hs_object_id"                      │
│    - hubspot_legacy_id_field: "icalps_company_id"          │
│    - property_mappings: [...]                              │
│    - required_properties: [...]                            │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│     reconciliation_query_builder.py                         │
│                                                             │
│  Uses mappings to generate SQL:                            │
│                                                             │
│  SELECT                                                     │
│    leg.Comp_CompanyId as legacy_id,                        │
│    hs.hs_object_id as hubspot_id,                          │
│    leg.Comp_Name, leg.Comp_WebSite, ...,                   │
│    hs.name, hs.domain, ...                                 │
│  FROM bronze_companies leg                                  │
│  INNER JOIN hubspot.companies hs                            │
│    ON leg.Comp_CompanyId = hs.icalps_company_id            │
│  WHERE hs.icalps_company_id IS NOT NULL                    │
└─────────────────────────────────────────────────────────────┘
```

---

## Where Workflow Is Configured

### **Workflow Configuration Location**

Workflows are configured in **StackSync Workflow UI**, not in this codebase.

**Access:**
- URL: `https://besg.api.workflows.stacksync.com`
- Workspace: `2219`
- Current Workflow ID: `e4d4b296-deb5-4075-bb5f-20c77054d86f`

### **What's in a Workflow Configuration**

Each workflow in StackSync contains:

```yaml
Workflow Configuration:
  - Name: "Query HubSpot Contacts"
  - Trigger: HTTP POST/GET
    - Path: /triggers/post_trigger/run_wait_result
    - Method: GET

  - SQL Query: |
      SELECT
        hs_object_id,
        firstname,
        lastname,
        email,
        icalps_contact_id
      FROM hubspot.contacts
      WHERE
        lower(company) LIKE lower('%{{ input.query_parameters.company }}%')
        AND icalps_contact_id IS NOT NULL
      LIMIT 100

  - Response: JSON
```

### **Workflow Templates to Configure**

Copy these SQL queries into StackSync workflow configuration:

#### **Template 1: Query Contacts**

```sql
SELECT
    hs_object_id,
    firstname,
    lastname,
    email,
    phone,
    mobilephone,
    jobtitle,
    department,
    company,
    associatedcompanyid,
    icalps_contact_id,
    city,
    country
FROM hubspot.contacts
WHERE (
    -- Support multiple filter options
    (lower(company) LIKE lower('%{{ input.query_parameters.company }}%'))
    OR (email = '{{ input.query_parameters.email }}')
    OR (icalps_contact_id = '{{ input.query_parameters.icalps_contact_id }}')
    OR (firstname ILIKE '%{{ input.query_parameters.firstname }}%'
        AND lastname ILIKE '%{{ input.query_parameters.lastname }}%')
)
AND icalps_contact_id IS NOT NULL
LIMIT {{ input.query_parameters.limit | default(100) }};
```

**Test:**
```bash
curl 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/WORKFLOW_ID:latest_draft/triggers/post_trigger/run_wait_result?company=ACME&limit=10'
```

#### **Template 2: Query Companies**

```sql
SELECT
    hs_object_id,
    name,
    domain,
    website,
    phone,
    city,
    state,
    country,
    zip,
    industry,
    numberofemployees,
    annualrevenue,
    icalps_company_id
FROM hubspot.companies
WHERE (
    (lower(name) LIKE lower('%{{ input.query_parameters.name }}%'))
    OR (domain = '{{ input.query_parameters.domain }}')
    OR (icalps_company_id = '{{ input.query_parameters.icalps_company_id }}')
)
AND icalps_company_id IS NOT NULL
LIMIT {{ input.query_parameters.limit | default(100) }};
```

**Test:**
```bash
curl 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/WORKFLOW_ID:latest_draft/triggers/post_trigger/run_wait_result?icalps_company_id=123'
```

#### **Template 3: Query Deals**

```sql
SELECT
    hs_object_id,
    dealname,
    amount,
    dealstage,
    closedate,
    createdate,
    pipeline,
    deal_status,
    deal_certainty,
    deal_type,
    deal_source,
    icalps_deal_id,
    associatedcompanyid
FROM hubspot.deals
WHERE (
    (lower(dealname) LIKE lower('%{{ input.query_parameters.dealname }}%'))
    OR (icalps_deal_id = '{{ input.query_parameters.icalps_deal_id }}')
    OR (dealstage = '{{ input.query_parameters.dealstage }}')
)
AND icalps_deal_id IS NOT NULL
LIMIT {{ input.query_parameters.limit | default(100) }};
```

#### **Template 4: Query Communications**

```sql
SELECT
    hs_object_id,
    hs_engagement_subject,
    hs_note_body,
    hs_timestamp,
    hs_engagement_type,
    hs_engagement_status,
    associated_company_id,
    associated_contact_id,
    icalps_communication_id
FROM hubspot.engagements
WHERE (
    (icalps_communication_id = '{{ input.query_parameters.icalps_communication_id }}')
    OR (associated_company_id = '{{ input.query_parameters.company_id }}')
)
AND icalps_communication_id IS NOT NULL
LIMIT {{ input.query_parameters.limit | default(100) }};
```

#### **Template 5: Get All Records by Property Group**

```sql
-- This query finds all records with IcAlps group properties
SELECT
    hs_object_id,
    firstname,
    lastname,
    email,
    icalps_contact_id
FROM hubspot.contacts
WHERE
    icalps_contact_id IS NOT NULL
    OR icalps_company_id IS NOT NULL
    OR icalps_deal_id IS NOT NULL
LIMIT {{ input.query_parameters.limit | default(1000) }};
```

---

## How Properties & Workflows Connect

### **End-to-End Flow**

```
┌─────────────────────────────────────────────────────────────┐
│  1. PROPERTY DEFINITION (property_mapping_config.py)       │
│                                                             │
│     Defines:                                                │
│     - Legacy fields: Comp_Name, Pers_FirstName             │
│     - HubSpot fields: name, firstname                      │
│     - Join keys: icalps_company_id, icalps_contact_id      │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  2. POSTGRESQL DIRECT QUERY (reconciliation pipeline)      │
│                                                             │
│     Generated SQL:                                          │
│     SELECT hs_object_id, name, domain, icalps_company_id   │
│     FROM hubspot.companies                                  │
│     WHERE icalps_company_id IS NOT NULL                    │
│                                                             │
│     Executed via: postgres_connection_manager.py            │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ OR (Alternative)
                   │
┌──────────────────┴──────────────────────────────────────────┐
│  3. WORKFLOW API QUERY (workflow_api_client.py)            │
│                                                             │
│     HTTP Request:                                           │
│     GET /workflows/ID/run_wait_result?icalps_company_id=123│
│                                                             │
│     Workflow SQL Template (in StackSync):                   │
│     SELECT * FROM hubspot.companies                         │
│     WHERE icalps_company_id =                               │
│       '{{ input.query_parameters.icalps_company_id }}'     │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  4. RESULTS RETURNED                                        │
│                                                             │
│     JSON:                                                   │
│     {                                                       │
│       "hs_object_id": 456789,                              │
│       "name": "ACME Corp",                                 │
│       "icalps_company_id": 123                             │
│     }                                                       │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  5. STAGING TABLE INSERT (crm_reconciliation_pipeline.py) │
│                                                             │
│     INSERT INTO staging.companies_reconciliation            │
│     (legacy_company_id, hubspot_company_id, ...)           │
│     VALUES (123, 456789, ...)                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Reference

### **To Add/Modify Properties**

1. **Edit**: `property_mapping_config.py`
2. **Add mapping**:
   ```python
   PropertyMapping("Comp_NewField", "hubspot_newfield")
   ```
3. **Update required properties** if needed
4. **Re-run**: Pipeline will automatically use new properties

### **To Create New Workflow**

1. **Access**: StackSync Workflow UI
2. **Create**: New workflow with HTTP trigger
3. **Add SQL**: Copy template from this guide
4. **Test**: Use curl command
5. **Update**: `workflow_api_client.py` with new workflow ID

### **To Query HubSpot Data**

**Option A - Direct PostgreSQL** (Best for bulk reconciliation):
```python
from postgres_connection_manager import PostgreSQLManager
pg = PostgreSQLManager()
df = pg.execute_query_df("SELECT * FROM hubspot.companies WHERE icalps_company_id IS NOT NULL")
```

**Option B - Workflow API** (Best for dynamic queries):
```python
from workflow_api_client import WorkflowAPIClient
client = WorkflowAPIClient()
company = client.query_by_icalps_id('companies', icalps_id=123)
```

**Option C - Curl** (Testing/debugging):
```bash
curl 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/WORKFLOW_ID:latest_draft/triggers/post_trigger/run_wait_result?icalps_company_id=123'
```

---

## Configuration Checklist

### **Before Running Reconciliation**

- [ ] Properties defined in `property_mapping_config.py`
- [ ] PostgreSQL connection tested (`postgres_connection_manager.py`)
- [ ] Staging tables created (`staging_schema_manager.py`)
- [ ] Workflows configured in StackSync (optional, for API queries)
- [ ] Workflow IDs updated in `workflow_api_client.py` (if using workflows)
- [ ] Bronze CSV files available in `bronze_layer/`

### **Workflow Configuration Required?**

**NO** - If using direct PostgreSQL queries (default approach)
**YES** - If using workflow API for dynamic queries

---

## Examples

### **Example 1: Add New Property**

**Step 1 - Add to mapping:**
```python
# In property_mapping_config.py
COMPANY_MAPPING = ObjectMapping(
    property_mappings=[
        # ... existing mappings
        PropertyMapping("Comp_LinkedInURL", "linkedin_url"),  # NEW
    ]
)
```

**Step 2 - Automatic:**
The pipeline will now:
- Query `linkedin_url` from `hubspot.companies`
- Map `Comp_LinkedInURL` → `linkedin_url` for updates
- Include in staging table property updates

### **Example 2: Create Custom Workflow**

**StackSync Configuration:**
```sql
-- Workflow Name: Find High-Value Deals
SELECT
    hs_object_id,
    dealname,
    amount,
    icalps_deal_id
FROM hubspot.deals
WHERE
    amount > {{ input.query_parameters.min_amount }}
    AND icalps_deal_id IS NOT NULL
ORDER BY amount DESC
LIMIT 50;
```

**Python Usage:**
```python
from workflow_api_client import WorkflowAPIClient

client = WorkflowAPIClient()
deals = client.execute_workflow(
    workflow_id='your-new-workflow-id',
    query_params={'min_amount': 50000}
)
```

---

## Troubleshooting

### **Properties Not Appearing in Queries?**

**Check:**
1. Property name matches HubSpot exactly (case-sensitive)
2. Property added to `property_mappings` list
3. Property exists in PostgreSQL: `\d hubspot.companies`

### **Workflow Returns Empty Results?**

**Check:**
1. Jinja template syntax: `{{ input.query_parameters.param_name }}`
2. Parameter passed in URL: `?param_name=value`
3. WHERE clause: Records have `icalps_*` fields populated
4. Test SQL directly in PostgreSQL first

### **Property Mapping Errors?**

**Check:**
1. Legacy field exists in Bronze CSV
2. HubSpot field exists in PostgreSQL
3. Data types compatible (string → string, int → int)
4. NULL handling: Use `Optional[str]` for nullable fields

---

**Configuration Complete!** 🎉

All properties and workflows are now documented and ready to use.
