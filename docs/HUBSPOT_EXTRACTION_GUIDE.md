# HubSpot Data Extraction Guide

Complete guide to extracting HubSpot data from PostgreSQL for validation before reconciliation.

---

## Overview

Before running the reconciliation pipeline, you need to extract HubSpot data from PostgreSQL to CSV files. This allows you to:

1. **Verify properties** - Confirm all expected properties are retrievable
2. **Validate data model** - Check that data matches expected structure
3. **Monitor quality** - Review sample data before reconciliation
4. **Debug issues** - Identify missing or malformed data

---

## Quick Start

```bash
# Extract all HubSpot entities (1000 records each)
python3 extract_hubspot_data.py

# Extract specific entity
python3 extract_hubspot_data.py --entity contacts

# Preview data without saving
python3 extract_hubspot_data.py --preview contacts --limit 10
```

---

## Architecture

### Components

1. **`hubspot_entity_config.py`** - Entity definitions (properties, tables, queries)
2. **`hubspot_generic_extractor.py`** - Generic extraction logic
3. **`extract_hubspot_data.py`** - Main extraction script

### Entity Configurations

Each entity is defined with:
- **Name**: contacts, companies, deals, engagements
- **Table**: PostgreSQL table in `hubspot` schema
- **Properties**: List of fields to extract
- **Legacy ID field**: `icalps_*` field for matching
- **WHERE clause**: Filter for records with legacy IDs
- **ORDER BY**: Sort order for extraction

### Extraction Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Load Entity Config                                       │
│    - Get properties list                                    │
│    - Get table name (hubspot.contacts)                      │
│    - Get WHERE clause (icalps_contact_id IS NOT NULL)       │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Build SQL Query                                          │
│    SELECT firstname, lastname, email, ...                   │
│    FROM hubspot.contacts                                    │
│    WHERE icalps_contact_id IS NOT NULL                      │
│    ORDER BY hs_object_id                                    │
│    LIMIT 1000;                                              │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Execute Query                                            │
│    - Connect to PostgreSQL                                  │
│    - Execute query                                          │
│    - Load results into pandas DataFrame                     │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Validate Columns (Optional)                              │
│    - Compare extracted columns vs config                    │
│    - Report missing/extra columns                           │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Export to CSV                                            │
│    - Add timestamp to filename                              │
│    - Save to output directory                               │
│    - Log file size and record count                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Usage Examples

### 1. List Available Entities

```bash
python3 extract_hubspot_data.py --list
```

**Output:**
```
Available HubSpot Entities:
======================================================================
  - contacts: 19 properties
    Table: hubspot.contacts
    Legacy ID: icalps_contact_id

  - companies: 20 properties
    Table: hubspot.companies
    Legacy ID: icalps_company_id

  - deals: 18 properties
    Table: hubspot.deals
    Legacy ID: icalps_deal_id

  - engagements: 17 properties
    Table: hubspot.engagements
    Legacy ID: icalps_communication_id
======================================================================
```

### 2. Preview Entity Data

Preview data without saving to CSV:

```bash
python3 extract_hubspot_data.py --preview contacts --limit 10
```

**Output:**
```
======================================================================
PREVIEW: CONTACTS
======================================================================

Shape: 10 rows × 19 columns

Columns: ['hs_object_id', 'firstname', 'lastname', 'email', 'phone', ...]

First 10 records:

   hs_object_id  firstname  lastname          email         phone
0      12345     John       Smith         john@...    555-1234
1      12346     Jane       Doe           jane@...    555-5678
...

======================================================================

Summary Statistics:
  Total Records: 10
  Total Columns: 19
  Memory Usage: 0.02 MB

Legacy ID (icalps_contact_id):
  Non-null: 10 (100.0%)
  Null: 0
  Unique: 10
```

### 3. Extract Single Entity

Extract contacts to CSV:

```bash
python3 extract_hubspot_data.py --entity contacts
```

**Output:**
```
======================================================================
EXTRACTION RESULTS
======================================================================
Entity: contacts
Status: success
Records Extracted: 1000
CSV File: ./output/hubspot/contacts_20250111_143052.csv

Validation:
  Valid: True
  Expected Columns: 19
  Actual Columns: 19
======================================================================
```

### 4. Extract All Entities

Extract all entities sequentially:

```bash
python3 extract_hubspot_data.py
```

**Output:**
```
======================================================================
Extracting entity 1/4: contacts
======================================================================
✓ Extracted 1000 contacts records with 19 columns
✓ Saved 1000 records to ./output/hubspot/contacts_20250111_143052.csv

======================================================================
Extracting entity 2/4: companies
======================================================================
✓ Extracted 1000 companies records with 20 columns
✓ Saved 1000 records to ./output/hubspot/companies_20250111_143053.csv

======================================================================
Extracting entity 3/4: deals
======================================================================
✓ Extracted 850 deals records with 18 columns
✓ Saved 850 records to ./output/hubspot/deals_20250111_143054.csv

======================================================================
Extracting entity 4/4: engagements
======================================================================
✓ Extracted 2500 engagements records with 17 columns
✓ Saved 2500 records to ./output/hubspot/engagements_20250111_143055.csv

======================================================================
BATCH EXTRACTION COMPLETE
======================================================================

EXTRACTION SUMMARY
======================================================================
Total Entities: 4
Successful: 4
Failed: 0
Total Records: 5350

Details:
  ✓ contacts: 1000 records
    → ./output/hubspot/contacts_20250111_143052.csv
  ✓ companies: 1000 records
    → ./output/hubspot/companies_20250111_143053.csv
  ✓ deals: 850 records
    → ./output/hubspot/deals_20250111_143054.csv
  ✓ engagements: 2500 records
    → ./output/hubspot/engagements_20250111_143055.csv
======================================================================
```

### 5. Extract Full Data (No Limit)

Extract all records without row limit:

```bash
python3 extract_hubspot_data.py --no-limit
```

### 6. Custom Output Directory

Save CSVs to custom directory:

```bash
python3 extract_hubspot_data.py --output ./my_exports
```

### 7. Skip Validation

Extract without column validation:

```bash
python3 extract_hubspot_data.py --no-validate
```

### 8. Verbose Logging

Enable detailed logs:

```bash
python3 extract_hubspot_data.py --verbose
```

---

## Command Line Options

```
usage: extract_hubspot_data.py [-h] [--entity ENTITY] [--output OUTPUT]
                                [--limit LIMIT] [--no-limit] [--no-validate]
                                [--preview ENTITY] [--list] [--verbose]

Extract HubSpot data from PostgreSQL to CSV

optional arguments:
  -h, --help         show this help message and exit
  --entity ENTITY    Extract specific entity (contacts, companies, deals, engagements)
  --output OUTPUT    Output directory for CSV files (default: ./output/hubspot)
  --limit LIMIT      Row limit per entity (default: 1000)
  --no-limit         Extract all records (no limit)
  --no-validate      Skip column validation
  --preview ENTITY   Preview entity data without saving to CSV
  --list             List available entities and exit
  --verbose          Enable verbose logging
```

---

## Extracted Data Structure

### Contacts

**File**: `contacts_YYYYMMDD_HHMMSS.csv`

**Columns** (19):
```
hs_object_id, firstname, lastname, email, phone, mobilephone,
jobtitle, department, salutation, company, associatedcompanyid,
address, city, state, country, zip, icalps_contact_id,
createdate, lastmodifieddate, hs_lastmodifieddate
```

### Companies

**File**: `companies_YYYYMMDD_HHMMSS.csv`

**Columns** (20):
```
hs_object_id, name, domain, website, phone, address, city, state,
country, zip, industry, type, numberofemployees, annualrevenue,
hs_lead_source, territory, icalps_company_id, createdate,
hs_lastmodifieddate
```

### Deals

**File**: `deals_YYYYMMDD_HHMMSS.csv`

**Columns** (18):
```
hs_object_id, dealname, amount, dealstage, pipeline, closedate,
createdate, hs_lastmodifieddate, dealtype, deal_status,
deal_certainty, deal_priority, deal_source, deal_brand,
deal_notes, company, associatedcompanyid, icalps_deal_id
```

### Engagements

**File**: `engagements_YYYYMMDD_HHMMSS.csv`

**Columns** (17):
```
hs_object_id, hs_engagement_type, hs_timestamp,
hs_engagement_subject, hs_note_body, hs_engagement_status,
hs_call_direction, hs_call_duration, hs_meeting_title,
hs_email_subject, associated_company_id, associated_contact_id,
associated_deal_id, icalps_communication_id, createdate,
hs_lastmodifieddate
```

---

## Validation

### Column Validation

The extractor validates that extracted columns match the configuration:

```python
# Expected columns from config
expected = ['hs_object_id', 'firstname', 'lastname', 'email', ...]

# Actual columns from query
actual = df.columns.tolist()

# Validation checks
missing = expected - actual   # Columns in config but not extracted
extra = actual - expected     # Columns extracted but not in config
```

**Validation Results:**
- ✓ **Valid**: All expected columns present
- ⚠️ **Warning**: Missing or extra columns detected

### Data Quality Checks

Each extraction includes:

1. **Row Count**: Total records extracted
2. **Column Count**: Number of columns
3. **Null Counts**: Missing values per column
4. **Legacy ID Stats**:
   - Total records
   - Non-null legacy IDs
   - Null legacy IDs
   - Unique legacy IDs

---

## Output Files

### CSV Files

**Location**: `./output/hubspot/`

**Naming**: `{entity_name}_{timestamp}.csv`

**Examples**:
- `contacts_20250111_143052.csv`
- `companies_20250111_143053.csv`
- `deals_20250111_143054.csv`
- `engagements_20250111_143055.csv`

### Log Files

**Location**: `./logs/`

**Naming**: `hubspot_extraction_{timestamp}.log`

**Example**: `hubspot_extraction_20250111_143052.log`

**Contents**:
- Timestamp for each operation
- SQL queries executed
- Row counts and file sizes
- Validation results
- Errors and warnings

---

## Python API Usage

### Single Entity Extraction

```python
from postgres_connection_manager import PostgreSQLManager
from hubspot_entity_config import get_hubspot_entity_config
from hubspot_generic_extractor import HubSpotExtractor

# Initialize
pg = PostgreSQLManager()
config = get_hubspot_entity_config('contacts')
extractor = HubSpotExtractor(config, pg)

# Extract to DataFrame
df = extractor.extract_to_dataframe(limit=1000)

# Save to CSV
csv_path = extractor.save_to_csv(df, './output/contacts.csv')

# Get summary
summary = extractor.get_summary(df)
print(summary)

pg.close()
```

### Batch Extraction

```python
from postgres_connection_manager import PostgreSQLManager
from hubspot_entity_config import HUBSPOT_ENTITY_CONFIGS
from hubspot_generic_extractor import HubSpotBatchExtractor

# Initialize
pg = PostgreSQLManager()
batch_extractor = HubSpotBatchExtractor(pg)

# Extract all entities
all_configs = list(HUBSPOT_ENTITY_CONFIGS.values())
results = batch_extractor.extract_all(
    entity_configs=all_configs,
    output_dir='./output/hubspot',
    limit=1000,
    validate=True
)

# Check results
for result in results:
    print(f"{result['entity']}: {result['status']}")

pg.close()
```

### Custom Extraction

```python
from postgres_connection_manager import PostgreSQLManager
from hubspot_entity_config import HubSpotEntityConfig
from hubspot_generic_extractor import HubSpotExtractor

# Create custom config
custom_config = HubSpotEntityConfig(
    name="contacts_custom",
    table_name="contacts",
    schema="hubspot",
    properties=["hs_object_id", "email", "firstname", "lastname"],
    where_clause="email IS NOT NULL AND icalps_contact_id IS NOT NULL",
    order_by="createdate DESC"
)

# Extract
pg = PostgreSQLManager()
extractor = HubSpotExtractor(custom_config, pg)
df = extractor.extract_to_dataframe()

pg.close()
```

---

## Monitoring & Debugging

### Check Row Counts

```python
from postgres_connection_manager import PostgreSQLManager
from hubspot_entity_config import get_hubspot_entity_config
from hubspot_generic_extractor import HubSpotExtractor

pg = PostgreSQLManager()

for entity_name in ['contacts', 'companies', 'deals', 'engagements']:
    config = get_hubspot_entity_config(entity_name)
    extractor = HubSpotExtractor(config, pg)
    count = extractor.get_row_count()
    print(f"{entity_name}: {count} records")

pg.close()
```

### Preview Sample Data

```bash
# Preview contacts
python3 extract_hubspot_data.py --preview contacts --limit 5

# Preview companies
python3 extract_hubspot_data.py --preview companies --limit 5
```

### Check Extracted Files

```bash
# List all extracted CSVs
ls -lh ./output/hubspot/*.csv

# Count rows in CSV
wc -l ./output/hubspot/contacts_*.csv

# Preview CSV content
head -n 10 ./output/hubspot/contacts_*.csv
```

---

## Troubleshooting

### Issue: No data extracted

**Symptoms**: `Records Extracted: 0`

**Causes**:
1. No records match WHERE clause
2. Legacy ID field is NULL for all records
3. Table is empty

**Solutions**:
```python
# Check if table has data
from postgres_connection_manager import PostgreSQLManager
pg = PostgreSQLManager()

# Count all records (no WHERE clause)
result = pg.execute_query("SELECT COUNT(*) FROM hubspot.contacts;")
print(f"Total contacts: {result[0]['count']}")

# Count records with legacy IDs
result = pg.execute_query("""
    SELECT COUNT(*) FROM hubspot.contacts
    WHERE icalps_contact_id IS NOT NULL;
""")
print(f"Contacts with icalps_contact_id: {result[0]['count']}")

pg.close()
```

### Issue: Column validation fails

**Symptoms**: `Validation: Valid: False`

**Causes**:
1. Config properties don't match database columns
2. Database schema changed
3. Typo in property names

**Solutions**:
```python
# Check actual table columns
from postgres_connection_manager import PostgreSQLManager
pg = PostgreSQLManager()

columns = pg.execute_query("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'hubspot'
      AND table_name = 'contacts'
    ORDER BY ordinal_position;
""")

print("Actual columns in hubspot.contacts:")
for col in columns:
    print(f"  - {col['column_name']}")

pg.close()
```

### Issue: CSV file not found

**Causes**:
1. Extraction failed
2. Wrong output directory
3. Timestamp in filename

**Solutions**:
```bash
# Find all extracted CSVs
find ./output -name "*.csv" -type f

# Check specific entity
ls -lt ./output/hubspot/contacts_*.csv | head -n 1

# Use fixed output path (no timestamp)
# Modify save_to_csv call with include_timestamp=False
```

### Issue: Memory error on large extraction

**Symptoms**: `MemoryError` or system slowdown

**Solutions**:
```bash
# Use limit parameter
python3 extract_hubspot_data.py --limit 10000

# Extract one entity at a time
python3 extract_hubspot_data.py --entity contacts
python3 extract_hubspot_data.py --entity companies
```

---

## Next Steps

After extracting HubSpot data:

1. **Review CSV files** - Open in Excel/spreadsheet to validate
2. **Check property mappings** - Compare columns with `property_mapping_config.py`
3. **Verify legacy IDs** - Ensure `icalps_*` fields are populated
4. **Run reconciliation** - Use `crm_reconciliation_pipeline.py`

---

## Related Documentation

- **[BASIC_USAGE_SNIPPETS.md](BASIC_USAGE_SNIPPETS.md)** - Basic usage examples
- **[PROPERTY_WORKFLOW_GUIDE.md](PROPERTY_WORKFLOW_GUIDE.md)** - Property management
- **[CRM_RECONCILIATION_README.md](CRM_RECONCILIATION_README.md)** - Full pipeline documentation

---

**Happy Extracting!** 🎉
