# CRM Reconciliation - Quick Start Guide

## Prerequisites

1. **PostgreSQL Access**: Connection to HubSpot synced database
2. **Bronze Layer**: CSV files from legacy CRM extraction
3. **Python Packages**: Install required dependencies

```bash
pip install psycopg2-binary pandas loguru requests
```

## 5-Minute Quick Start

### Step 1: Test PostgreSQL Connection

```python
from postgres_connection_manager import PostgreSQLManager

# Test connection
pg = PostgreSQLManager()
if pg.test_connection():
    print("✓ Connected to HubSpot database!")

    # List available tables
    tables = pg.get_hubspot_tables()
    print(f"Found {len(tables)} HubSpot tables")

pg.close()
```

### Step 2: Setup Staging Environment

```python
from staging_schema_manager import StagingSchemaManager

# Create staging tables
staging_mgr = StagingSchemaManager()
staging_mgr.create_all_staging_tables()
print("✓ Staging tables created!")
```

### Step 3: Run Reconciliation

```python
from crm_reconciliation_pipeline import CRMReconciliationPipeline

# Initialize pipeline
pipeline = CRMReconciliationPipeline()
pipeline.setup_staging_environment()

# Reconcile companies (limit to 10 for testing)
stats = pipeline.reconcile_companies(
    bronze_csv_path="bronze_layer/Bronze_Company.csv",
    limit=10
)

print(f"Matched: {stats['matched']}")
print(f"Unmatched: {stats['unmatched']}")
```

### Step 4: Review Results

```python
# Query staging table
query = """
SELECT
    legacy_company_id,
    hubspot_company_id,
    legacy_name,
    hubspot_name,
    reconciliation_status
FROM staging.companies_reconciliation
LIMIT 10;
"""

df = pipeline.pg.execute_query_df(query)
print(df)
```

## Complete Example Script

Save as `reconcile_example.py`:

```python
#!/usr/bin/env python3
"""Quick start example for CRM reconciliation."""

from crm_reconciliation_pipeline import CRMReconciliationPipeline
from loguru import logger

def main():
    # Initialize
    logger.info("Starting CRM reconciliation...")
    pipeline = CRMReconciliationPipeline()

    # Setup
    pipeline.setup_staging_environment()

    # Reconcile (start with small limit)
    stats = pipeline.reconcile_companies(
        bronze_csv_path="bronze_layer/Bronze_Company.csv",
        limit=10
    )

    # Results
    logger.info(f"✓ Reconciliation complete!")
    logger.info(f"  Matched: {stats['matched']}")
    logger.info(f"  Unmatched: {stats['unmatched']}")

    # View staging data
    query = "SELECT * FROM staging.companies_reconciliation LIMIT 5;"
    df = pipeline.pg.execute_query_df(query)
    print(df)

if __name__ == "__main__":
    main()
```

Run it:
```bash
python reconcile_example.py
```

## Next Steps

1. **Increase Limits**: Remove `limit` parameter to process all records
2. **Reconcile All Objects**: Use `pipeline.reconcile_all()`
3. **Export for HubSpot**: Query staging tables for update payloads
4. **Verify Results**: Review reconciliation status and confidence scores

## Common Queries

### Get Matched Records Ready for Update

```sql
SELECT
    hubspot_company_id,
    properties_to_update::json
FROM staging.companies_reconciliation
WHERE reconciliation_status = 'matched'
  AND hubspot_company_id IS NOT NULL;
```

### Get Unmatched Records (New in HubSpot)

```sql
SELECT
    legacy_company_id,
    legacy_name,
    legacy_properties::json
FROM staging.companies_reconciliation
WHERE reconciliation_status = 'new';
```

### Get Reconciliation Statistics

```sql
SELECT
    reconciliation_status,
    COUNT(*) as count,
    AVG(match_confidence) as avg_confidence
FROM staging.companies_reconciliation
GROUP BY reconciliation_status;
```

## Workflow API Example

Query HubSpot via workflow:

```python
from workflow_api_client import WorkflowAPIClient

client = WorkflowAPIClient()

# Find company by name
companies = client.query_companies(name='ACME')
print(f"Found {len(companies)} companies")

# Find company by legacy ID
company = client.query_by_icalps_id('companies', icalps_id=123)
if company:
    print(f"HubSpot ID: {company['hs_object_id']}")
```

Or using curl:

```bash
# Query companies by name
curl --request 'GET' \
  --url 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/e4d4b296-deb5-4075-bb5f-20c77054d86f:latest_draft/triggers/post_trigger/run_wait_result?company=ACME'

# Query by legacy ID
curl --request 'GET' \
  --url 'https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/e4d4b296-deb5-4075-bb5f-20c77054d86f:latest_draft/triggers/post_trigger/run_wait_result?icalps_company_id=123'
```

## Troubleshooting

### Can't connect to PostgreSQL?
- Check host, port, credentials
- Verify network connectivity
- Test with `psql` command line tool

### No matches found?
- Verify icalps_* fields are populated in HubSpot
- Check legacy IDs in Bronze CSV files
- Query HubSpot tables directly to verify data

### Errors during reconciliation?
- Check reconciliation log: `SELECT * FROM staging.reconciliation_log ORDER BY timestamp DESC;`
- Review error messages and stack traces
- Start with small limits (10-100 records)

## What's Next?

After successful reconciliation:

1. **Review Staging Tables**: Verify data quality and match confidence
2. **Generate Update Payloads**: Extract properties_to_update for each record
3. **Update HubSpot**: Use HubSpot API with record IDs from staging
4. **Monitor**: Track updates and handle conflicts

---

**Ready to reconcile your CRM data!** 🚀
