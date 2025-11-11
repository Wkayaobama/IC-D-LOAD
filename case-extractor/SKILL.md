---
name: case-extractor
description: Extract Case/Support Ticket data using the async pipeline with denormalized medallion architecture. Use this skill to extract case data into Bronze layer with proper linkages to Company and Person entities.
---

# Case Extractor

## Overview

Extracts Case/Support Ticket data from SQL Server CRM database using the **async pipeline** and **denormalized medallion architecture**. The extraction follows Bronze → Silver → Gold pattern where Case data is denormalized with Company and Person information through SQL JOINs at extraction time (Bronze layer).

## Architecture Pattern

This extractor is part of the **modular async extraction pipeline** which provides:

- **Configuration-driven extraction**: Entity definitions in `entity_config.py`
- **Generic extractor pattern**: Single `GenericExtractor` class for all entities
- **Async task execution**: Task-based execution with progress tracking
- **Denormalized Bronze layer**: Foreign key relationships preserved through JOINs
- **Model separation**: Clear separation between properties (WHAT) and parameters (WHERE)
- **Character encoding**: UTF-8-sig encoding with special character sanitization

## When to Use This Skill

- **Extract case/support ticket data** from CRM database
- **Denormalize company and person** information into case records
- **Generate Bronze layer CSV** with "Bronze_Case.csv" naming
- **Track case lifecycle** (opened, closed, status, priority)
- **Full dataset extraction** (no row limits in production)

## Entity Properties

### Core Case Fields
- `Case_CaseId` - Unique case identifier (Primary Key)
- `Case_PrimaryCompanyId` - FK to Company
- `Case_PrimaryPersonId` - FK to Person (Contact)
- `Case_AssignedUserId` - Assigned user/owner
- `Case_Description` - Case description/summary
- `Case_Status` - Case status (Open, Closed, Pending)
- `Case_Stage` - Current stage in workflow
- `Case_Priority` - Priority level (High, Medium, Low)
- `Case_Opened` - Date/time opened
- `Case_Closed` - Date/time closed

### Denormalized Company Fields (from JOIN)
- `Company_Name` - Company name (from Company table)
- `Company_WebSite` - Company website (from Company table)

### Denormalized Person Fields (from JOIN)
- `Person_FirstName` - Contact first name (from Person table)
- `Person_LastName` - Contact last name (from Person table)
- `Person_EmailAddress` - Contact email (from vEmailCompanyAndPerson view)

## Cardinality Relationships

- **Case → Company**: many:1 (via `Case_PrimaryCompanyId`)
- **Case → Person**: many:1 (via `Case_PrimaryPersonId`)
- **Case → User**: many:1 (via `Case_AssignedUserId`)

## Entity Configuration

The Case entity is defined in `pipeline_async/entity_config.py`:

```python
CASE_SIMPLE_PROPERTIES = {
    'base': [
        'Case_CaseId',
        'Case_PrimaryCompanyId',
        'Case_PrimaryPersonId',
        'Case_AssignedUserId',
        'Case_Description',
        'Case_Status',
        'Case_Stage',
        'Case_Priority',
        'Case_Opened',
        'Case_Closed'
    ],
    'denormalized': [
        'comp.[Comp_Name] AS Company_Name',
        'comp.[Comp_WebSite] AS Company_WebSite',
        'p.[Pers_FirstName] AS Person_FirstName',
        'p.[Pers_LastName] AS Person_LastName',
        'v.[Emai_EmailAddress] AS Person_EmailAddress'
    ],
    'metadata': []
}

CASE_CONFIG = EntityConfig(
    name="Case",
    properties=CASE_SIMPLE_PROPERTIES,
    base_table="vCases",  # Using vCases view
    joins=[
        "LEFT JOIN [CRMICALPS].[dbo].[Company] comp ON base.[Case_PrimaryCompanyId] = comp.[Comp_CompanyId]",
        "LEFT JOIN [CRMICALPS].[dbo].[Person] p ON base.[Case_PrimaryPersonId] = p.[Pers_PersonId]",
        "LEFT JOIN [CRMICALPS].[dbo].[vEmailCompanyAndPerson] v ON base.[Case_PrimaryPersonId] = v.[Pers_PersonId]"
    ],
    where_clause=None
)
```

## Generated SQL Query

The EntityConfig automatically generates this SQL query:

```sql
SELECT
    base.[Case_CaseId],
    base.[Case_PrimaryCompanyId],
    base.[Case_PrimaryPersonId],
    base.[Case_AssignedUserId],
    base.[Case_Description],
    base.[Case_Status],
    base.[Case_Stage],
    base.[Case_Priority],
    base.[Case_Opened],
    base.[Case_Closed],
    comp.[Comp_Name] AS Company_Name,
    comp.[Comp_WebSite] AS Company_WebSite,
    p.[Pers_FirstName] AS Person_FirstName,
    p.[Pers_LastName] AS Person_LastName,
    v.[Emai_EmailAddress] AS Person_EmailAddress
FROM [CRMICALPS].[dbo].[vCases] base
LEFT JOIN [CRMICALPS].[dbo].[Company] comp ON base.[Case_PrimaryCompanyId] = comp.[Comp_CompanyId]
LEFT JOIN [CRMICALPS].[dbo].[Person] p ON base.[Case_PrimaryPersonId] = p.[Pers_PersonId]
LEFT JOIN [CRMICALPS].[dbo].[vEmailCompanyAndPerson] v ON base.[Case_PrimaryPersonId] = v.[Pers_PersonId]
```

## Usage Examples

### Example 1: Extract Full Case Dataset

```python
from pipeline_async.entity_config import get_entity_config
from pipeline_async.generic_extractor import GenericExtractor
from config import get_connection_string

# Get configuration and connection
case_config = get_entity_config('Case')
connection_string = get_connection_string()

# Create extractor
extractor = GenericExtractor(case_config, connection_string)

# Extract all cases (no limit)
df = extractor.extract_to_dataframe()

# Save to Bronze layer
path = extractor.save_to_bronze(df)
print(f"Extracted {len(df)} cases to {path}")
```

### Example 2: Extract with Filter

```python
# Extract only open cases
df = extractor.extract_to_dataframe(
    filter_clause="AND base.[Case_Status] = 'Open'"
)

path = extractor.save_to_bronze(df)
```

### Example 3: Preview Data

```python
# Preview first 10 cases
preview_df = extractor.preview(limit=10)
print(preview_df[['Case_CaseId', 'Case_Description', 'Company_Name', 'Person_FirstName']])
```

### Example 4: Get Row Count

```python
# Get total case count without extracting
count = extractor.get_row_count()
print(f"Total cases in database: {count}")
```

### Example 5: Extract and Save in One Call

```python
# Convenience method
path = extractor.extract_and_save()
print(f"Cases saved to {path}")
```

## Multi-Entity Extraction

The async pipeline supports extracting ALL entities with their linkages preserved:

```python
from pipeline_async.entity_config import get_entity_config
from pipeline_async.generic_extractor import GenericExtractor
from config import get_connection_string

# Single connection string reused across all entities
connection_string = get_connection_string()

# Extract all 5 entities
entity_names = ['Company', 'Person', 'Address', 'Case', 'Communication']

for entity_name in entity_names:
    print(f"\nExtracting {entity_name}...")

    # Get entity configuration
    config = get_entity_config(entity_name)

    # Create extractor (reuses connection string)
    extractor = GenericExtractor(config, connection_string)

    # Extract and save
    path = extractor.extract_and_save()

    print(f"  Saved to {path}")
```

## Bronze Layer Output

The Bronze layer CSV file includes:

1. **All base properties** from the vCases view
2. **All denormalized properties** from JOINed tables
3. **UTF-8-sig encoding** for proper character handling
4. **Special character sanitization**:
   - `\uf02d` (private use bullet) → `-`
   - `\uf0a7` (private use marker) → `*`

Example Bronze_Case.csv structure:

```csv
Case_CaseId,Case_PrimaryCompanyId,Case_Description,Company_Name,Person_FirstName,Person_LastName,Person_EmailAddress
1234,5678,"Customer inquiry about product",ACME Corp,John,Doe,john.doe@acme.com
```

## Medallion Architecture Pattern

### Bronze Layer (Raw Extraction)
- **Purpose**: Raw data from source system
- **Location**: `bronze_layer/Bronze_Case.csv`
- **Transformations**: None (except denormalization via JOINs)
- **Encoding**: UTF-8-sig with character sanitization

### Silver Layer (Cleaned/Enriched)
- **Purpose**: Cleaned, standardized, business logic applied
- **Transformations**:
  - Data type conversions
  - Null handling
  - Status mapping
  - Computed columns

### Gold Layer (Analytics-Ready)
- **Purpose**: Aggregated, ready for reporting/analytics
- **Transformations**:
  - Case resolution time calculations
  - Status counts by company
  - Priority distribution
  - SLA metrics

## Integration with Other Skills

This extractor integrates with:

- **sql-schema-discovery**: Discover actual database columns
- **dataclass-generator**: Generate type-safe dataclasses from queries
- **duckdb-transformer**: Transform Bronze → Silver → Gold
- **pipeline-stage-mapper**: Map case stages/statuses
- **dataframe-dataclass-converter**: Convert between DataFrames and dataclasses

## Character Encoding

The extractor handles special characters correctly:

```python
# In GenericExtractor.save_to_bronze()
# Clean text columns - remove special characters
for col in df.select_dtypes(include=['object']).columns:
    if df[col].dtype == 'object':
        df[col] = df[col].apply(
            lambda x: str(x).replace('\uf02d', '-').replace('\uf0a7', '*')
            if pd.notna(x) else x
        )

# Save with UTF-8 BOM for Excel compatibility
df.to_csv(output_path, index=False, encoding='utf-8-sig')
```

## Error Handling

The extractor includes comprehensive error handling:

```python
try:
    conn = pyodbc.connect(self.connection_string)
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Extracted {len(df)} rows for {self.entity_config.name}")
    return df
except Exception as e:
    logger.error(f"Extraction failed for {self.entity_config.name}: {str(e)}")
    raise
```

## Performance Considerations

- **No row limits in production**: Extract full dataset
- **Connection reuse**: Single connection string across all entities
- **Async execution**: Tasks can run concurrently
- **Memory efficient**: Pandas DataFrame streaming for large datasets
- **Logging**: Structured logging with loguru

## Adding New Properties

To add new properties to Case extraction:

1. Update `CASE_SIMPLE_PROPERTIES` in `entity_config.py`
2. Add to 'base' list if from vCases table
3. Add to 'denormalized' list if from JOINed table (with alias)
4. Re-run extraction - query auto-generates

Example:

```python
CASE_SIMPLE_PROPERTIES = {
    'base': [
        'Case_CaseId',
        # ... existing fields ...
        'Case_Source',  # ← Add new base property
    ],
    'denormalized': [
        # ... existing fields ...
        'u.[User_Name] AS AssignedUser_Name'  # ← Add new denormalized property
    ]
}

# Add corresponding JOIN
CASE_CONFIG = EntityConfig(
    # ... existing config ...
    joins=[
        # ... existing joins ...
        "LEFT JOIN [CRMICALPS].[dbo].[Users] u ON base.[Case_AssignedUserId] = u.[User_UserId]"
    ]
)
```

## Verification

After extraction, verify linkages with:

```python
import pandas as pd

# Load Bronze layer
case_df = pd.read_csv("bronze_layer/Bronze_Case.csv")

# Verify Company linkage
print(f"Cases with Company: {case_df['Case_PrimaryCompanyId'].notna().sum()}")
print(f"Cases with Company Name: {case_df['Company_Name'].notna().sum()}")

# Verify Person linkage
print(f"Cases with Person: {case_df['Case_PrimaryPersonId'].notna().sum()}")
print(f"Cases with Person Name: {case_df['Person_FirstName'].notna().sum()}")

# Sample case with linkages
sample = case_df[case_df['Company_Name'].notna()].iloc[0]
print(f"\nSample Case:")
print(f"  Description: {sample['Case_Description']}")
print(f"  Company: {sample['Company_Name']}")
print(f"  Contact: {sample['Person_FirstName']} {sample['Person_LastName']}")
print(f"  Email: {sample['Person_EmailAddress']}")
```

## Related Skills

- `sql-schema-discovery` - Discover database schema
- `dataclass-generator` - Generate dataclasses from queries
- `generic-extractor` - Generic extraction engine
- `entity-config` - Entity configuration system
- `duckdb-transformer` - Silver/Gold transformations
- `company-extractor` - Extract Company entities
- `person-extractor` - Extract Person entities
- `communication-extractor` - Extract Communication entities

## References

- [IC_Load Project Overview](../README.md)
- [Quick Start Guide](../QUICK_START.md)
- [Skills Summary](../SKILLS_SUMMARY.md)
- [Architecture Documentation](../ARCHITECTURE.md)
- [Entity Properties Documentation](../ENTITY_PROPERTIES.md)
