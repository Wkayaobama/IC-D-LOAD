# IC_Load Skills - Quick Start Guide

## Installation

```bash
pip install pandas pyodbc duckdb
```

## 5-Minute Quick Start

### Step 1: Discover Your Database

```python
from sql-schema-discovery.scripts.schema_discovery import SchemaDiscovery

# Connect to your database
discovery = SchemaDiscovery(
    connection_string="DRIVER={SQL Server};SERVER=your_server;DATABASE=CRMICALPS;Trusted_Connection=yes"
)

# See all tables
tables = discovery.discover_tables("CRMICALPS")
for table in tables:
    print(table.name)

# Inspect a table
columns = discovery.discover_columns("Cases")
for col in columns:
    print(f"{col.name}: {col.data_type}")
```

### Step 2: Generate a Dataclass

```python
from dataclass_generator.scripts.dataclass_generator import DataclassGenerator

generator = DataclassGenerator()

# Your SQL query (with JOINs if needed)
query = """
SELECT
    c.Case_CaseId,
    c.Case_Status,
    comp.Comp_Name AS Company_Name
FROM Cases c
LEFT JOIN Company comp ON c.Case_PrimaryCompanyId = comp.Comp_CompanyId
"""

# Generate dataclass code
code = generator.generate_from_query(query, "Case", discovery)
print(code)

# Save it
generator.save_to_file(code, "models/case.py")
```

### Step 3: Extract Data

```python
from case_extractor.scripts.case_extractor import CaseExtractor

# Extract cases
extractor = CaseExtractor(connection_string)
cases = extractor.extract()

print(f"Extracted {len(cases)} cases")

# Save to Bronze CSV
extractor.save_to_bronze(cases, "Bronze_Cases.csv")
```

### Step 4: Transform with DuckDB

```python
from duckdb_transformer.scripts.duckdb_processor import DuckDBProcessor

processor = DuckDBProcessor()

# Load Bronze data
processor.load_csv("Bronze_Cases.csv", "cases")

# Transform
result = processor.to_dataframe("""
    SELECT
        status,
        COUNT(*) as count,
        AVG(CASE WHEN closed IS NOT NULL
            THEN EXTRACT(EPOCH FROM (closed - opened))/86400
            END) as avg_days_to_close
    FROM cases
    GROUP BY status
""")

print(result)
```

### Step 5: Apply Business Logic

```python
from pipeline_stage_mapper.scripts.stage_mapper import StageMapper
from computed_columns_calculator.scripts.computed_calculator import ComputedColumnsCalculator

# Map pipeline stages
mapper = StageMapper()
result['final_stage'] = result.apply(
    lambda row: mapper.map_stage(row['pipeline'], row['stage'], row['outcome']),
    axis=1
)

# Calculate computed columns
calculator = ComputedColumnsCalculator()
result = calculator.calculate_all(result)

# Save to Silver layer
result.to_csv("Silver_Cases.csv", index=False)
```

## Common Patterns

### Pattern 1: Add a New Entity

```python
# 1. Discover schema
columns = discovery.discover_columns("Address")

# 2. Filter to properties you care about
properties = ["Addr_AddressId", "Addr_Street1", "Addr_City"]
filtered = [c for c in columns if c.name in properties]

# 3. Create query with JOINs
query = """
SELECT a.*, comp.Comp_Name AS Company_Name
FROM Address a
LEFT JOIN Company comp ON a.Addr_CompanyId = comp.Comp_CompanyId
"""

# 4. Generate dataclass
code = generator.generate_from_query(query, "Address", discovery)

# 5. Create extractor (follow case_extractor.py pattern)
```

### Pattern 2: Convert Between Formats

```python
from dataframe_dataclass_converter.scripts.dataframe_converter import DataFrameConverter

converter = DataFrameConverter()

# DataFrame → Dataclasses
cases = converter.dataframe_to_dataclasses(df, Case)

# Dataclasses → DataFrame
df = converter.dataclasses_to_dataframe(cases)
```

### Pattern 3: Complex Transformations

```python
processor = DuckDBProcessor()

# Load multiple Bronze files
processor.load_csv("Bronze_Cases.csv", "cases")
processor.load_csv("Bronze_Company.csv", "companies")
processor.load_csv("Bronze_Contact.csv", "contacts")

# Complex JOIN and aggregation
result = processor.to_dataframe("""
    WITH case_metrics AS (
        SELECT
            company_id,
            COUNT(*) as total_cases,
            SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) as closed_cases
        FROM cases
        GROUP BY company_id
    )
    SELECT
        comp.Comp_Name,
        cm.total_cases,
        cm.closed_cases,
        ROUND(cm.closed_cases * 100.0 / cm.total_cases, 2) as closure_rate
    FROM case_metrics cm
    JOIN companies comp ON cm.company_id = comp.company_id
    ORDER BY total_cases DESC
""")
```

## Troubleshooting

### Connection Issues
```python
# Test connection
manager = ConnectionManager(server="your_server", database="CRMICALPS")
with manager.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    print("Connection successful!")
```

### Column Name Mismatches
```python
# Check DataFrame columns
print(df.columns.tolist())

# Check dataclass fields
from dataclasses import fields
print([f.name for f in fields(Case)])

# Use converter with auto-mapping
converter = DataFrameConverter(auto_map_names=True)
```

### Query Parsing Errors
```python
# Manually specify columns if generator fails
columns = ["Case_CaseId", "Case_Status", "Company_Name"]
code = generator.generate_from_columns("Case",
    [ColumnMetadata(name=c, data_type="varchar", python_type=str, is_nullable=True)
     for c in columns]
)
```

## Next Steps

1. **Read SKILLS_SUMMARY.md** for complete documentation
2. **Review individual SKILL.md files** for detailed usage
3. **Follow case-extractor pattern** to create new extractors
4. **Extend existing skills** or create new ones as needed

## File Structure

```
IC_Load/
├── QUICK_START.md          ← You are here
├── SKILLS_SUMMARY.md       ← Complete documentation
├── sql-schema-discovery/   ← Skill 1
├── dataclass-generator/    ← Skill 2
├── sql-connection-manager/ ← Skill 3
├── case-extractor/         ← Skill 4 (example)
├── dataframe-dataclass-converter/  ← Skill 5
├── duckdb-transformer/     ← Skill 6
├── pipeline-stage-mapper/  ← Skill 7
└── computed-columns-calculator/    ← Skill 8
```

## Key Concepts

1. **Dataclass = Query Result**: Your dataclass includes ALL columns from the query, including JOINs
2. **Schema Discovery First**: Always discover schema before creating extractors
3. **Properties Filter**: Use properties lists to filter columns to business-relevant ones
4. **Bronze → Silver → Gold**: Bronze = raw extracts, Silver = transformed, Gold = aggregated
5. **Modular Skills**: Use skills independently or compose into pipelines

Happy extracting! 🚀
