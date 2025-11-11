# CRM Reconciliation - Basic Usage Code Snippets

**Quick reference for executing the CRM reconciliation pipeline**

---

## Installation

```bash
# Install required packages
pip install psycopg2-binary pandas loguru requests

# Verify installation
python3 -c "import psycopg2, pandas, loguru, requests; print('✓ All packages installed')"
```

---

## 1. Test PostgreSQL Connection

```python
from postgres_connection_manager import PostgreSQLManager

# Initialize connection
pg = PostgreSQLManager(
    host="2219-revops.pgm5k8mhg52j6k63k3dd54em0v.postgres.stacksync.com",
    port=5432,
    database="postgres",
    user="postgres",
    password="4LjdtD27gKxc5bptfFZp"
)

# Test connection
if pg.test_connection():
    print("✓ Connected to HubSpot database!")

    # List available tables
    tables = pg.get_hubspot_tables()
    print(f"Found {len(tables)} HubSpot tables:")
    for table in tables[:5]:
        print(f"  - hubspot.{table}")

pg.close()
```

**Expected Output:**
```
✓ PostgreSQL connection test successful
  Version: PostgreSQL 14.x...
✓ Connected to HubSpot database!
Found 10 HubSpot tables:
  - hubspot.contacts
  - hubspot.companies
  - hubspot.deals
  - hubspot.engagements
  - hubspot.properties
```

---

## 2. Setup Staging Environment

```python
from staging_schema_manager import StagingSchemaManager
from postgres_connection_manager import PostgreSQLManager

# Initialize
pg = PostgreSQLManager()
staging_mgr = StagingSchemaManager(pg)

# Create all staging tables
staging_mgr.create_all_staging_tables()

# Verify creation
stats = staging_mgr.get_staging_table_stats()
print("\nStaging tables created:")
for table, count in stats.items():
    print(f"  {table}: {count} rows")

pg.close()
```

**Expected Output:**
```
✓ Staging schema 'staging' ready
✓ Created staging table: staging.companies_reconciliation
✓ Created staging table: staging.contacts_reconciliation
✓ Created staging table: staging.deals_reconciliation
✓ Created staging table: staging.communications_reconciliation
✓ Created staging table: staging.reconciliation_log
✓ All staging tables created successfully!

Staging tables created:
  companies_reconciliation: 0 rows
  contacts_reconciliation: 0 rows
  deals_reconciliation: 0 rows
  communications_reconciliation: 0 rows
```

---

## 3. Query HubSpot Data (Direct PostgreSQL)

```python
from postgres_connection_manager import PostgreSQLManager

pg = PostgreSQLManager()

# Query companies with legacy IDs
query = """
SELECT
    hs_object_id,
    name,
    domain,
    city,
    country,
    icalps_company_id
FROM hubspot.companies
WHERE icalps_company_id IS NOT NULL
LIMIT 10;
"""

df = pg.execute_query_df(query)
print(f"Found {len(df)} companies with legacy IDs:")
print(df.to_string(index=False))

pg.close()
```

**Expected Output:**
```
Found 10 companies with legacy IDs:
hs_object_id  name           domain      city        country  icalps_company_id
456789        ACME Corp      acme.com    New York    USA      123
456790        Tech Inc       tech.io     Paris       France   124
...
```

---

## 4. Reconcile Companies (Simple)

```python
from crm_reconciliation_pipeline import CRMReconciliationPipeline

# Initialize pipeline
pipeline = CRMReconciliationPipeline()

# Setup staging
pipeline.setup_staging_environment()

# Reconcile companies (limit to 10 for testing)
stats = pipeline.reconcile_companies(
    bronze_csv_path="bronze_layer/Bronze_Company.csv",
    limit=10
)

# Print results
print(f"\n✓ Reconciliation Complete!")
print(f"  Total: {stats['total']}")
print(f"  Matched: {stats['matched']}")
print(f"  Unmatched: {stats['unmatched']}")
print(f"  Errors: {stats['errors']}")
```

**Expected Output:**
```
============================================================
RECONCILING COMPANIES
============================================================
  Read 10 rows from CSV
✓ Loaded 10 rows to public.bronze_companies
Executing match query...
  Found 8 records
✓ Company reconciliation complete
  Total: 8
  Matched: 8
  Unmatched: 0
  Errors: 0
  Time: 1234ms

✓ Reconciliation Complete!
  Total: 8
  Matched: 8
  Unmatched: 0
  Errors: 0
```

---

## 5. Review Staging Table Results

```python
from postgres_connection_manager import PostgreSQLManager

pg = PostgreSQLManager()

# Query staging table
query = """
SELECT
    legacy_company_id,
    hubspot_company_id,
    legacy_name,
    hubspot_name,
    reconciliation_status,
    match_confidence,
    last_updated
FROM staging.companies_reconciliation
ORDER BY last_updated DESC
LIMIT 10;
"""

df = pg.execute_query_df(query)
print("Reconciliation Results:")
print(df.to_string(index=False))

pg.close()
```

**Expected Output:**
```
Reconciliation Results:
legacy_company_id  hubspot_company_id  legacy_name    hubspot_name   reconciliation_status  match_confidence  last_updated
123                456789              ACME Corp      ACME Corp      matched                100.0            2025-11-11 10:30:00
124                456790              Tech Inc       Tech Inc       matched                100.0            2025-11-11 10:30:01
...
```

---

## 6. Get Property Update Payloads

```python
from postgres_connection_manager import PostgreSQLManager
import json

pg = PostgreSQLManager()

# Get records ready for HubSpot update
query = """
SELECT
    hubspot_company_id,
    properties_to_update
FROM staging.companies_reconciliation
WHERE reconciliation_status = 'matched'
  AND hubspot_company_id IS NOT NULL
LIMIT 5;
"""

results = pg.execute_query(query)

print("Property updates ready for HubSpot:")
for row in results:
    print(f"\nCompany ID: {row['hubspot_company_id']}")
    print(f"Properties: {json.dumps(row['properties_to_update'], indent=2)}")

pg.close()
```

**Expected Output:**
```
Property updates ready for HubSpot:

Company ID: 456789
Properties: {
  "name": "ACME Corp",
  "domain": "acme.com",
  "phone": "+1-555-0100",
  "city": "New York",
  "country": "USA"
}

Company ID: 456790
Properties: {
  "name": "Tech Inc",
  "domain": "tech.io",
  "city": "Paris",
  "country": "France"
}
```

---

## 7. Query via Workflow API

```python
from workflow_api_client import WorkflowAPIClient

# Initialize client
client = WorkflowAPIClient()

# Query company by legacy ID
company = client.query_by_icalps_id('companies', icalps_id=123)

if company:
    print(f"Found company:")
    print(f"  HubSpot ID: {company['hs_object_id']}")
    print(f"  Name: {company.get('name')}")
    print(f"  Legacy ID: {company.get('icalps_company_id')}")
else:
    print("Company not found")

# Query contacts by company name
contacts = client.query_contacts(company='ACME')
print(f"\nFound {len(contacts)} contacts at ACME")
```

**Expected Output:**
```
Executing workflow: e4d4b296-deb5-4075-bb5f-20c77054d86f
✓ Workflow executed successfully
Found company:
  HubSpot ID: 456789
  Name: ACME Corp
  Legacy ID: 123

Found 5 contacts at ACME
```

---

## 8. Reconcile All Objects

```python
from crm_reconciliation_pipeline import CRMReconciliationPipeline

# Initialize
pipeline = CRMReconciliationPipeline()
pipeline.setup_staging_environment()

# Reconcile all objects (companies, contacts, deals, communications)
all_stats = pipeline.reconcile_all(
    bronze_layer_path="bronze_layer",
    limit_per_object=100  # Limit for testing, use None for all
)

# Print summary
print("\n" + "="*60)
print("RECONCILIATION SUMMARY")
print("="*60)

for object_type, stats in all_stats.items():
    print(f"\n{object_type.upper()}:")
    print(f"  Total: {stats.get('total', 0)}")
    print(f"  Matched: {stats.get('matched', 0)}")
    print(f"  Unmatched: {stats.get('unmatched', 0)}")
    print(f"  Errors: {stats.get('errors', 0)}")
```

**Expected Output:**
```
============================================================
STARTING FULL CRM RECONCILIATION
============================================================

COMPANIES:
  Total: 100
  Matched: 95
  Unmatched: 5
  Errors: 0

CONTACTS:
  Total: 100
  Matched: 92
  Unmatched: 8
  Errors: 0

DEALS:
  Total: 100
  Matched: 88
  Unmatched: 12
  Errors: 0
```

---

## 9. Generate Curl Commands

```python
from workflow_api_client import CurlCommandGenerator

# Print example curl commands
CurlCommandGenerator.print_examples()
```

**Expected Output:**
```
======================================================================
Curl Command Examples
======================================================================

1. Query contacts by company:
curl --request 'GET' --url 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/e4d4b296-deb5-4075-bb5f-20c77054d86f:latest_draft/triggers/post_trigger/run_wait_result?company=ACME'

2. Query companies by icalps_company_id:
curl --request 'GET' --url 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/e4d4b296-deb5-4075-bb5f-20c77054d86f:latest_draft/triggers/post_trigger/run_wait_result?icalps_company_id=123'

3. Query deals by icalps_deal_id:
curl --request 'GET' --url 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/e4d4b296-deb5-4075-bb5f-20c77054d86f:latest_draft/triggers/post_trigger/run_wait_result?icalps_deal_id=789'
```

---

## 10. Complete Example Script

Save as `reconcile_example.py`:

```python
#!/usr/bin/env python3
"""
Complete CRM reconciliation example.
"""

from crm_reconciliation_pipeline import CRMReconciliationPipeline
from postgres_connection_manager import PostgreSQLManager
from loguru import logger

def main():
    # Step 1: Test connection
    logger.info("Testing PostgreSQL connection...")
    pg = PostgreSQLManager()
    if not pg.test_connection():
        logger.error("Failed to connect!")
        return
    pg.close()

    # Step 2: Initialize pipeline
    logger.info("Initializing pipeline...")
    pipeline = CRMReconciliationPipeline()

    # Step 3: Setup staging
    logger.info("Setting up staging environment...")
    pipeline.setup_staging_environment()

    # Step 4: Reconcile companies
    logger.info("Reconciling companies...")
    stats = pipeline.reconcile_companies(
        bronze_csv_path="bronze_layer/Bronze_Company.csv",
        limit=10  # Start small
    )

    # Step 5: Review results
    logger.info(f"✓ Complete! Matched: {stats['matched']}, Unmatched: {stats['unmatched']}")

    # Step 6: Query staging table
    query = """
    SELECT
        legacy_company_id,
        hubspot_company_id,
        legacy_name,
        reconciliation_status
    FROM staging.companies_reconciliation
    LIMIT 5;
    """

    df = pipeline.pg.execute_query_df(query)
    print("\nSample Results:")
    print(df.to_string(index=False))

if __name__ == "__main__":
    main()
```

**Run it:**
```bash
python3 reconcile_example.py
```

---

## Quick Reference Commands

### **List HubSpot Tables**
```python
from postgres_connection_manager import PostgreSQLManager
pg = PostgreSQLManager()
print(pg.get_hubspot_tables())
pg.close()
```

### **Check Staging Stats**
```python
from staging_schema_manager import StagingSchemaManager
staging_mgr = StagingSchemaManager()
print(staging_mgr.get_staging_table_stats())
```

### **View Property Mappings**
```python
from property_mapping_config import print_mapping_summary
print_mapping_summary()
```

### **Test Workflow API**
```python
from workflow_api_client import WorkflowAPIClient
client = WorkflowAPIClient()
company = client.query_by_icalps_id('companies', icalps_id=123)
print(company)
```

### **Clear Staging Table**
```python
from staging_schema_manager import StagingSchemaManager
staging_mgr = StagingSchemaManager()
staging_mgr.clear_staging_table('companies_reconciliation')
```

---

## Common SQL Queries

### **Get All Matched Records**
```sql
SELECT * FROM staging.companies_reconciliation
WHERE reconciliation_status = 'matched'
ORDER BY match_confidence DESC;
```

### **Get Unmatched Records**
```sql
SELECT * FROM staging.companies_reconciliation
WHERE reconciliation_status = 'new'
ORDER BY legacy_company_id;
```

### **Get Reconciliation Statistics**
```sql
SELECT
    reconciliation_status,
    COUNT(*) as count,
    AVG(match_confidence) as avg_confidence
FROM staging.companies_reconciliation
GROUP BY reconciliation_status;
```

### **Get Recent Operations Log**
```sql
SELECT
    operation,
    entity_type,
    status,
    COUNT(*) as count
FROM staging.reconciliation_log
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY operation, entity_type, status;
```

---

## Troubleshooting

### **Connection Issues**
```python
# Test with verbose output
from postgres_connection_manager import PostgreSQLManager
import logging
logging.basicConfig(level=logging.DEBUG)

pg = PostgreSQLManager()
pg.test_connection()
```

### **Check if Table Exists**
```python
from postgres_connection_manager import PostgreSQLManager
pg = PostgreSQLManager()
exists = pg.table_exists('companies', schema='hubspot')
print(f"hubspot.companies exists: {exists}")
```

### **Inspect Table Columns**
```python
from postgres_connection_manager import PostgreSQLManager
pg = PostgreSQLManager()
columns = pg.get_table_columns('companies', schema='hubspot')
print(f"Columns: {columns}")
```

---

## Next Steps

1. **Test Connection**: Run snippet #1
2. **Setup Staging**: Run snippet #2
3. **Test Query**: Run snippet #3
4. **Reconcile Sample**: Run snippet #4 with `limit=10`
5. **Review Results**: Run snippet #5
6. **Scale Up**: Remove limits, reconcile all records
7. **Update HubSpot**: Use snippet #6 to get payloads

---

**Ready to reconcile!** 🚀

Copy and paste any snippet to get started.
