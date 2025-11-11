# Running the Case Extraction Pipeline

## Prerequisites

```bash
# Install required packages
pip install pandas pyodbc
```

## Quick Test (Recommended for First Run)

Run the simple extraction script that guides you through setup:

```bash
python run_case_extraction.py
```

This script will:
1. Ask for your SQL Server connection details (or use config.py)
2. Test the database connection
3. Discover the Cases table schema
4. Extract 100 sample records
5. Export to Bronze layer CSV

**Output**: `bronze_layer/Bronze_Cases_YYYYMMDD_HHMMSS.csv`

---

## Full Pipeline Test

For a complete test of all skills:

### Step 1: Create Configuration

```bash
# Copy the template
copy config_template.py config.py

# Edit config.py with your details
notepad config.py
```

Update these values in `config.py`:
```python
SQL_SERVER = "your_server_name"  # e.g., "localhost" or "SERVER\\INSTANCE"
SQL_DATABASE = "CRMICALPS"
SQL_TRUSTED_CONNECTION = True
```

### Step 2: Run Full Test

```bash
python test_case_extraction.py
```

This will test:
- ✅ sql-connection-manager
- ✅ sql-schema-discovery
- ✅ dataclass-generator
- ✅ dataframe-dataclass-converter
- ✅ Case extraction with denormalized Company/Person fields
- ✅ Bronze layer CSV export with metadata

**Outputs**:
- `models/case.py` - Generated Case dataclass
- `bronze_layer/Bronze_Cases_YYYYMMDD_HHMMSS.csv` - Bronze layer data

---

## What the Pipeline Does

### 1. Connection (sql-connection-manager)
Establishes connection to SQL Server with retry logic

### 2. Schema Discovery (sql-schema-discovery)
```python
from schema_discovery import SchemaDiscovery

discovery = SchemaDiscovery(connection_string)
columns = discovery.discover_columns("Cases")
relationships = discovery.discover_relationships("Cases")
```

**Output**: Column metadata, data types, foreign key relationships

### 3. Dataclass Generation (dataclass-generator)
```python
from dataclass_generator import DataclassGenerator

generator = DataclassGenerator()
code = generator.generate_from_query(case_query, "Case", discovery)
```

**Output**: `models/case.py` with type-safe Case dataclass

**Example Generated Class**:
```python
@dataclass
class Case:
    case_id: int
    status: Optional[str]
    company_name: Optional[str]  # Denormalized from JOIN
    person_first_name: Optional[str]  # Denormalized from JOIN
    opened: Optional[datetime]
```

### 4. Data Extraction
Executes SQL query with JOINs to denormalize Company and Person data:

```sql
SELECT
    c.Case_CaseId,
    c.Case_Status,
    comp.Comp_Name AS Company_Name,  -- Denormalized
    p.Pers_FirstName AS Person_FirstName  -- Denormalized
FROM Cases c
LEFT JOIN Company comp ON c.Case_PrimaryCompanyId = comp.Comp_CompanyId
LEFT JOIN Person p ON c.Case_PrimaryPersonId = p.Pers_PersonId
```

### 5. DataFrame to Dataclass (dataframe-dataclass-converter)
```python
from dataframe_converter import DataFrameConverter

converter = DataFrameConverter()
cases = converter.dataframe_to_dataclasses(df, Case)
```

**Output**: List[Case] - Type-safe Case instances

### 6. Bronze Layer Export
```python
df_export = converter.dataclasses_to_dataframe(cases)
df_export['_extracted_at'] = datetime.now()
df_export.to_csv("bronze_layer/Bronze_Cases.csv", index=False)
```

**Output**: CSV with metadata columns:
- `_extracted_at` - Extraction timestamp
- `_extraction_source` - Source table
- `_record_count` - Total records

---

## Troubleshooting

### Connection Errors

**Error**: "Login failed for user"
```bash
# Check SQL Server authentication mode
# Enable Windows Authentication or use SQL Server auth
```

**Error**: "Cannot open database"
```bash
# Verify database name
# Check user has permissions
```

### Import Errors

**Error**: "No module named 'pyodbc'"
```bash
pip install pyodbc
```

**Error**: "No module named 'pandas'"
```bash
pip install pandas
```

### Table Not Found

**Error**: "Cases table not found"
```bash
# Check table name in SQL Server
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%Case%'

# If table is in different schema:
# Update query to use correct schema (e.g., dbo.Cases)
```

### Permission Issues

**Error**: "SELECT permission denied"
```bash
# Grant SELECT permission on Cases table
GRANT SELECT ON Cases TO [your_user]
```

---

## Verifying Results

### 1. Check Generated Dataclass

```bash
cat models/case.py
# or
type models\case.py
```

Verify:
- ✅ Has all expected fields (case_id, status, etc.)
- ✅ Includes denormalized fields (company_name, person_first_name)
- ✅ Proper type hints (Optional[str], datetime, etc.)

### 2. Check Bronze CSV

```bash
# View first few rows
head bronze_layer/Bronze_Cases_*.csv
# or in Python:
import pandas as pd
df = pd.read_csv("bronze_layer/Bronze_Cases_YYYYMMDD_HHMMSS.csv")
print(df.head())
print(df.dtypes)
```

Verify:
- ✅ Has records (non-empty)
- ✅ Has metadata columns (_extracted_at, _source)
- ✅ Denormalized fields populated (Company_Name, Person_FirstName)
- ✅ No encoding issues with special characters

---

## Expected Output Structure

### Bronze Layer CSV Columns

```
Case_CaseId                  int64
Case_PrimaryCompanyId        object
Company_Name                 object  ← Denormalized
Company_WebSite              object  ← Denormalized
Case_PrimaryPersonId         object
Person_FirstName             object  ← Denormalized
Person_LastName              object  ← Denormalized
Person_EmailAddress          object  ← Denormalized
Case_Status                  object
Case_Stage                   object
Case_Priority                object
Case_Opened                  datetime64[ns]
Case_Closed                  datetime64[ns]
_extracted_at                datetime64[ns]  ← Metadata
_extraction_source           object          ← Metadata
_record_count                int64           ← Metadata
```

---

## Next Steps After Successful Test

### 1. Create Additional Entity Extractors

Follow the case-extractor pattern:
```bash
# Copy case-extractor template
cp -r case-extractor contact-extractor

# Update contact-extractor/SKILL.md
# Update contact-extractor/scripts/contact_extractor.py
```

### 2. Transform with DuckDB (Silver Layer)

```python
from duckdb_transformer.scripts.duckdb_processor import DuckDBProcessor

processor = DuckDBProcessor()
processor.load_csv("bronze_layer/Bronze_Cases.csv", "cases")
processor.load_csv("bronze_layer/Bronze_Company.csv", "companies")

result = processor.to_dataframe("""
    SELECT
        c.*,
        COUNT(*) OVER (PARTITION BY c.Case_Status) as status_count
    FROM cases c
""")

result.to_csv("silver_layer/Silver_Cases.csv", index=False)
```

### 3. Apply Business Logic (Gold Layer)

```python
from pipeline_stage_mapper.scripts.stage_mapper import StageMapper
from computed_columns_calculator.scripts.computed_calculator import ComputedColumnsCalculator

# Map stages
mapper = StageMapper()
df['final_stage'] = df.apply(
    lambda row: mapper.map_stage(row['pipeline'], row['stage'], row['outcome']),
    axis=1
)

# Calculate computed columns
calculator = ComputedColumnsCalculator()
df = calculator.calculate_all(df)

df.to_csv("gold_layer/Gold_Cases.csv", index=False)
```

---

## Performance Optimization

### For Large Datasets

1. **Add WHERE clause** to limit records during testing:
```python
query = """
SELECT TOP 10000 *
FROM Cases c
WHERE c.Case_CreatedDate >= DATEADD(month, -6, GETDATE())
"""
```

2. **Use chunked processing**:
```python
# In config.py
MAX_RECORDS_PER_EXTRACTION = 10000

# Process in chunks
for chunk in pd.read_sql(query, conn, chunksize=1000):
    # Process each chunk
    pass
```

3. **Add indexes** to SQL Server:
```sql
CREATE INDEX IX_Cases_CreatedDate ON Cases(Case_CreatedDate)
CREATE INDEX IX_Cases_Status ON Cases(Case_Status)
```

---

## Getting Help

1. **Review skill documentation**:
   - `sql-schema-discovery/SKILL.md`
   - `dataclass-generator/SKILL.md`
   - `case-extractor/SKILL.md`

2. **Check complete documentation**:
   - `SKILLS_SUMMARY.md` - Complete reference
   - `ARCHITECTURE.md` - Architecture diagrams
   - `QUICK_START.md` - Quick tutorial

3. **Common issues**:
   - Connection: Check SQL Server is running and accessible
   - Permissions: Verify user has SELECT permission on tables
   - Schema: Confirm table and column names match your database

---

## Success Indicators

You'll know it worked when you see:

```
======================================================================
✅ Pipeline Execution Complete!
======================================================================

Summary:
  ✅ Database connection established
  ✅ Schema discovered (50+ columns, 3 FKs)
  ✅ Case dataclass generated (models/case.py)
  ✅ Data extracted (100 records)
  ✅ Converted to type-safe dataclass instances
  ✅ Exported to Bronze layer CSV (bronze_layer/Bronze_Cases_YYYYMMDD_HHMMSS.csv)

🎉 All skills working correctly!
```
