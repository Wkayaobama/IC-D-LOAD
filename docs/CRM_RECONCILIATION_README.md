# CRM Reconciliation Pipeline - Complete Guide

## Overview

This pipeline reconciles legacy CRM data (IC'ALPS) with the new HubSpot CRM data stored in PostgreSQL.

**Goal**: Map legacy record IDs to HubSpot record IDs to enable property updates and data synchronization.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Legacy CRM)                  │
│                                                             │
│  CSV Files:                                                 │
│  - Bronze_Company.csv                                       │
│  - Bronze_Person.csv                                        │
│  - Bronze_Opportunity.csv                                   │
│  - Bronze_Communication.csv                                 │
│                                                             │
│  Legacy IDs: Comp_CompanyId, Pers_PersonId, etc.          │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ JOIN ON
                   │ Legacy ID = icalps_* ID
                   │
┌──────────────────┴──────────────────────────────────────────┐
│              HUBSPOT CRM (PostgreSQL)                       │
│                                                             │
│  Tables:                                                    │
│  - hubspot.contacts                                         │
│  - hubspot.companies                                        │
│  - hubspot.deals                                            │
│  - hubspot.engagements                                      │
│                                                             │
│  Fields:                                                    │
│  - hs_object_id (HubSpot record ID)                        │
│  - icalps_company_id (Legacy company ID)                   │
│  - icalps_contact_id (Legacy contact ID)                   │
│  - icalps_deal_id (Legacy deal ID)                         │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              STAGING TABLES (PostgreSQL)                    │
│                                                             │
│  - staging.companies_reconciliation                         │
│  - staging.contacts_reconciliation                          │
│  - staging.deals_reconciliation                             │
│  - staging.communications_reconciliation                    │
│                                                             │
│  Contains:                                                  │
│  - Legacy ID                                                │
│  - HubSpot ID                                               │
│  - Properties to update                                     │
│  - Reconciliation status                                    │
└─────────────────────────────────────────────────────────────┘
```

## Database Connection

### PostgreSQL (HubSpot Sync)

**Connection String:**
```
jdbc:postgresql://2219-revops.pgm5k8mhg52j6k63k3dd54em0v.postgres.stacksync.com:5432/postgres?user=postgres&password=4LjdtD27gKxc5bptfFZp
```

**Python Connection:**
```python
from postgres_connection_manager import PostgreSQLManager

pg = PostgreSQLManager(
    host="2219-revops.pgm5k8mhg52j6k63k3dd54em0v.postgres.stacksync.com",
    port=5432,
    database="postgres",
    user="postgres",
    password="4LjdtD27gKxc5bptfFZp"
)
```

## Components

### 1. PostgreSQL Connection Manager
**File**: `postgres_connection_manager.py`

Manages connections to PostgreSQL with:
- Connection pooling
- Automatic retry
- Query execution
- DataFrame conversion

**Example:**
```python
from postgres_connection_manager import PostgreSQLManager

pg = PostgreSQLManager()

# Test connection
if pg.test_connection():
    print("✓ Connected!")

# Execute query
results = pg.execute_query("SELECT * FROM hubspot.contacts LIMIT 10")

# Get as DataFrame
df = pg.execute_query_df("SELECT * FROM hubspot.companies WHERE icalps_company_id IS NOT NULL")
```

### 2. Staging Schema Manager
**File**: `staging_schema_manager.py`

Creates and manages staging tables for reconciliation.

**Staging Tables:**
- `staging.companies_reconciliation`
- `staging.contacts_reconciliation`
- `staging.deals_reconciliation`
- `staging.communications_reconciliation`
- `staging.reconciliation_log`

**Example:**
```python
from staging_schema_manager import StagingSchemaManager

staging_mgr = StagingSchemaManager()

# Create all staging tables
staging_mgr.create_all_staging_tables()

# Get stats
stats = staging_mgr.get_staging_table_stats()
print(stats)
```

### 3. Property Mapping Configuration
**File**: `property_mapping_config.py`

Maps legacy CRM properties to HubSpot properties.

**Property Mappings:**
- **Companies**: `Comp_Name` → `name`, `Comp_WebSite` → `domain`
- **Contacts**: `Pers_FirstName` → `firstname`, `Pers_EmailAddress` → `email`
- **Deals**: `Oppo_Description` → `dealname`, `Oppo_Forecast` → `amount`
- **Communications**: `Comm_Subject` → `hs_engagement_subject`

**Example:**
```python
from property_mapping_config import get_object_mapping, get_hubspot_properties

# Get mapping for contacts
mapping = get_object_mapping('contacts')
print(f"Legacy ID field: {mapping.legacy_id_field}")
print(f"HubSpot ID field: {mapping.hubspot_id_field}")

# Get HubSpot properties to query
props = get_hubspot_properties('contacts')
print(props)
```

### 4. Reconciliation Query Builder
**File**: `reconciliation_query_builder.py`

Builds SQL queries for reconciliation.

**Query Types:**
- **Match queries**: Join legacy data with HubSpot data
- **Unmatched queries**: Find legacy records not in HubSpot
- **Staging inserts**: Store reconciliation results
- **Property updates**: Generate HubSpot update payloads

**Example:**
```python
from reconciliation_query_builder import ReconciliationQueryBuilder

builder = ReconciliationQueryBuilder()

# Build match query
query = builder.build_match_query('companies', limit=100)
print(query)

# Build staging insert
query, params = builder.build_staging_insert_query(
    'companies',
    legacy_id=123,
    hubspot_id=456789,
    legacy_properties={'Comp_Name': 'ACME'},
    properties_to_update={'name': 'ACME'},
    reconciliation_status='matched'
)
```

### 5. Workflow API Client
**File**: `workflow_api_client.py`

Executes queries via HubSpot workflow webhooks.

**Workflow URL:**
```
https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/{workflow_id}:latest_draft/triggers/post_trigger/run_wait_result
```

**Example:**
```python
from workflow_api_client import WorkflowAPIClient

client = WorkflowAPIClient()

# Query contacts by company
contacts = client.query_contacts(company='ACME')

# Query companies by icalps ID
company = client.query_by_icalps_id('companies', icalps_id=123)
```

**Curl Example:**
```bash
curl --request 'GET' \
  --url 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/e4d4b296-deb5-4075-bb5f-20c77054d86f:latest_draft/triggers/post_trigger/run_wait_result?company=ACME'
```

### 6. CRM Reconciliation Pipeline
**File**: `crm_reconciliation_pipeline.py`

Main orchestrator that coordinates all components.

**Example:**
```python
from crm_reconciliation_pipeline import CRMReconciliationPipeline

pipeline = CRMReconciliationPipeline()

# Setup staging environment
pipeline.setup_staging_environment()

# Reconcile companies
stats = pipeline.reconcile_companies(
    bronze_csv_path="bronze_layer/Bronze_Company.csv",
    limit=100
)

# Reconcile all objects
all_stats = pipeline.reconcile_all(
    bronze_layer_path="bronze_layer",
    limit_per_object=100
)
```

## Workflow Steps

### Step 1: Setup Environment

```python
from crm_reconciliation_pipeline import CRMReconciliationPipeline

pipeline = CRMReconciliationPipeline()
pipeline.setup_staging_environment()
```

This creates:
- `staging` schema
- Reconciliation tables for each object type
- Reconciliation log table

### Step 2: Load Bronze Layer Data (Optional)

If you want to join Bronze CSV data with HubSpot data:

```python
pipeline.load_bronze_csv_to_postgres(
    csv_path="bronze_layer/Bronze_Company.csv",
    table_name="bronze_companies"
)
```

Or query HubSpot directly for records with icalps_* IDs.

### Step 3: Reconcile Each Object Type

```python
# Reconcile companies
company_stats = pipeline.reconcile_companies(
    bronze_csv_path="bronze_layer/Bronze_Company.csv",
    limit=None  # None = all records
)

# Reconcile contacts
contact_stats = pipeline.reconcile_contacts(
    bronze_csv_path="bronze_layer/Bronze_Person.csv",
    limit=None
)

# Reconcile deals
deal_stats = pipeline.reconcile_deals(
    bronze_csv_path="bronze_layer/Bronze_Opportunity.csv",
    limit=None
)
```

### Step 4: Review Staging Tables

```python
# Get stats
stats = pipeline.get_reconciliation_stats()
print(stats)

# Query staging table directly
query = "SELECT * FROM staging.companies_reconciliation LIMIT 10;"
df = pipeline.pg.execute_query_df(query)
print(df)
```

### Step 5: Export for HubSpot Update

```python
# Get records ready for update
query = """
SELECT
    hubspot_company_id,
    properties_to_update
FROM staging.companies_reconciliation
WHERE reconciliation_status = 'matched'
  AND hubspot_company_id IS NOT NULL;
"""

updates = pipeline.pg.execute_query(query)

# Each update contains:
# - hubspot_company_id: HubSpot record ID for upsert
# - properties_to_update: JSON of properties to update
```

## SQL Query Examples

### Query 1: Match Legacy Companies with HubSpot

```sql
SELECT
    leg.Comp_CompanyId as legacy_id,
    hs.hs_object_id as hubspot_id,
    leg.Comp_Name as legacy_name,
    hs.name as hubspot_name,
    leg.Comp_WebSite as legacy_domain,
    hs.domain as hubspot_domain
FROM bronze_companies leg
INNER JOIN hubspot.companies hs
    ON leg.Comp_CompanyId::VARCHAR = hs.icalps_company_id::VARCHAR
WHERE hs.icalps_company_id IS NOT NULL
LIMIT 100;
```

### Query 2: Find Unmatched Legacy Contacts

```sql
SELECT
    leg.Pers_PersonId as legacy_id,
    leg.Pers_FirstName,
    leg.Pers_LastName,
    leg.Pers_EmailAddress
FROM bronze_persons leg
LEFT JOIN hubspot.contacts hs
    ON leg.Pers_PersonId::VARCHAR = hs.icalps_contact_id::VARCHAR
WHERE hs.hs_object_id IS NULL;
```

### Query 3: Get Reconciliation Statistics

```sql
SELECT
    reconciliation_status,
    COUNT(*) as count
FROM staging.companies_reconciliation
GROUP BY reconciliation_status;
```

### Query 4: Get Properties to Update

```sql
SELECT
    legacy_company_id,
    hubspot_company_id,
    properties_to_update,
    match_confidence
FROM staging.companies_reconciliation
WHERE reconciliation_status = 'matched'
ORDER BY last_updated DESC;
```

## Property Filter Rules

HubSpot records are filtered by:

1. **icalps_* ID not null**: Only records with legacy IDs
2. **Property Group "IcAlps"**: Records in the IcAlps property group (optional)

**SQL WHERE Clause:**
```sql
WHERE icalps_company_id IS NOT NULL
  OR icalps_contact_id IS NOT NULL
  OR icalps_deal_id IS NOT NULL
```

## Workflow API Query Templates

### Template 1: Query Contacts by Company

```sql
SELECT * FROM hubspot.contacts
WHERE lower(company) LIKE lower('%{{ input.query_parameters.company }}%')
```

**Curl:**
```bash
curl 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/{workflow_id}:latest_draft/triggers/post_trigger/run_wait_result?company=ACME'
```

### Template 2: Query Companies by Legacy ID

```sql
SELECT * FROM hubspot.companies
WHERE icalps_company_id = '{{ input.query_parameters.icalps_company_id }}'
```

**Curl:**
```bash
curl 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/{workflow_id}:latest_draft/triggers/post_trigger/run_wait_result?icalps_company_id=123'
```

### Template 3: Query Deals by Property

```sql
SELECT * FROM hubspot.deals
WHERE icalps_deal_id IS NOT NULL
  AND dealstage = '{{ input.query_parameters.dealstage }}'
```

## Complete Example: Reconcile Companies

```python
#!/usr/bin/env python3
"""
Complete example: Reconcile legacy companies with HubSpot companies.
"""

from crm_reconciliation_pipeline import CRMReconciliationPipeline
from loguru import logger

def main():
    # Initialize pipeline
    logger.info("Initializing CRM Reconciliation Pipeline...")
    pipeline = CRMReconciliationPipeline()

    # Step 1: Setup staging environment
    logger.info("Setting up staging environment...")
    pipeline.setup_staging_environment()

    # Step 2: Test connection
    logger.info("Testing PostgreSQL connection...")
    if not pipeline.pg.test_connection():
        logger.error("Failed to connect to PostgreSQL!")
        return

    # Step 3: Load Bronze CSV (optional)
    logger.info("Loading Bronze layer CSV...")
    pipeline.load_bronze_csv_to_postgres(
        csv_path="bronze_layer/Bronze_Company.csv",
        table_name="bronze_companies"
    )

    # Step 4: Reconcile companies
    logger.info("Reconciling companies...")
    stats = pipeline.reconcile_companies(
        bronze_csv_path="bronze_layer/Bronze_Company.csv",
        limit=None  # Process all records
    )

    # Step 5: Review results
    logger.info("Reconciliation complete!")
    logger.info(f"  Total: {stats['total']}")
    logger.info(f"  Matched: {stats['matched']}")
    logger.info(f"  Unmatched: {stats['unmatched']}")
    logger.info(f"  Errors: {stats['errors']}")

    # Step 6: Query staging table
    query = """
    SELECT
        legacy_company_id,
        hubspot_company_id,
        legacy_name,
        hubspot_name,
        reconciliation_status,
        match_confidence
    FROM staging.companies_reconciliation
    LIMIT 10;
    """
    df = pipeline.pg.execute_query_df(query)
    print("\nSample reconciliation results:")
    print(df.to_string(index=False))

if __name__ == "__main__":
    main()
```

## Troubleshooting

### Issue: Connection Timeout

**Error:** `could not translate host name ... to address`

**Solution:** Check network connectivity and firewall rules. Ensure PostgreSQL port 5432 is accessible.

### Issue: icalps_* Field Not Found

**Error:** `column "icalps_company_id" does not exist`

**Solution:** Verify that HubSpot custom properties are synced to PostgreSQL. Check property names in HubSpot settings.

### Issue: No Matches Found

**Error:** All records have status 'unmatched'

**Solution:**
1. Verify icalps_* fields are populated in HubSpot
2. Check that legacy IDs match between systems
3. Query HubSpot directly to verify data

```sql
SELECT icalps_company_id, name FROM hubspot.companies WHERE icalps_company_id IS NOT NULL LIMIT 10;
```

### Issue: Staging Table Conflicts

**Error:** `duplicate key value violates unique constraint`

**Solution:** Clear staging tables before re-running:

```python
staging_mgr.clear_staging_table('companies_reconciliation')
```

## File Structure

```
IC-D-LOAD/
├── postgres_connection_manager.py      # PostgreSQL connection management
├── staging_schema_manager.py           # Staging table creation
├── property_mapping_config.py          # Property mappings
├── reconciliation_query_builder.py     # SQL query generation
├── workflow_api_client.py              # Workflow API client
├── crm_reconciliation_pipeline.py      # Main orchestrator
├── CRM_RECONCILIATION_README.md        # This file
│
├── bronze_layer/                       # Bronze layer CSV files
│   ├── Bronze_Company.csv
│   ├── Bronze_Person.csv
│   ├── Bronze_Opportunity.csv
│   └── Bronze_Communication.csv
│
└── examples/                           # Example scripts
    ├── reconcile_companies.py
    ├── reconcile_contacts.py
    └── reconcile_all.py
```

## Next Steps

1. **Verify PostgreSQL Connection**: Test connection from your environment
2. **Inspect HubSpot Schema**: Query HubSpot tables to verify property names
3. **Run Reconciliation**: Start with small limits, then scale up
4. **Review Staging Tables**: Verify reconciliation results before updating HubSpot
5. **Update HubSpot**: Use HubSpot record IDs from staging tables for upserts

## Support

For issues or questions:
1. Check PostgreSQL connection and credentials
2. Verify HubSpot property names match configuration
3. Review reconciliation logs in `staging.reconciliation_log`
4. Check staging table data for debugging

---

**CRM Reconciliation Pipeline** - Map legacy CRM to HubSpot for seamless data synchronization.
