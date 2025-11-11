# IC_Load Skills Summary

**Project**: IC'ALPS Data Extraction Pipeline
**Purpose**: Modular, extensible skills for SQL Server → DataFrame → Dataclass → Bronze Layer extraction
**Date**: 2025-11-07

---

## Overview

This directory contains **12 modular skills** that serve as building blocks for the IC'ALPS data extraction pipeline. Each skill is a self-contained, reusable component that handles a specific aspect of the extraction process.

### Core Paradigm

```
SQL Server (with JOINs)
    ↓
pandas DataFrame
    ↓
Python Dataclass Instances
    ↓
Bronze Layer CSV + In-Memory Objects
    ↓
DuckDB Transformations (Silver/Gold)
```

### Key Principles

1. **Dataclass Structure = SQL Query Result**: The dataclass includes ALL columns from the query, including denormalized fields from JOINs
2. **Schema Discovery Enables Extensibility**: New entities can be added programmatically by discovering schema
3. **Type-Safe Conversions**: Bidirectional conversion between DataFrames and dataclasses with automatic type mapping
4. **Minimal Validation**: Use Python type hints for validation; keep dataclasses simple
5. **Modular & Independent**: Each skill can be used standalone or composed with others

---

## Skills Catalog

### 1. Infrastructure Skills (3)

#### 1.1. sql-schema-discovery
**Purpose**: Discover database schema dynamically
**Use When**: Adding new entities, understanding table structures, generating dataclasses
**Key Functions**:
- `discover_tables(db_name)` - List all tables
- `discover_columns(table_name)` - Get column metadata
- `discover_relationships(table_name)` - Find foreign keys
- `inspect_database(db_name)` - Complete schema inspection

**Example**:
```python
from scripts.schema_discovery import SchemaDiscovery

discovery = SchemaDiscovery(connection_string)
columns = discovery.discover_columns("Cases")
relationships = discovery.discover_relationships("Cases")
```

---

#### 1.2. dataclass-generator
**Purpose**: Generate Python dataclasses from SQL queries
**Use When**: Creating new entities, updating schemas, generating type-safe classes
**Key Functions**:
- `generate_from_query(query, class_name)` - Generate from SQL
- `generate_from_columns(class_name, columns)` - Generate from schema
- `parse_query_columns(query)` - Extract column names (with aliases)
- `sql_to_python_field_name(column_name)` - Name conversion

**Example**:
```python
from scripts.dataclass_generator import DataclassGenerator

generator = DataclassGenerator()
code = generator.generate_from_query(
    query=case_query,
    class_name="Case",
    schema_discovery=discovery
)
generator.save_to_file(code, "models/case.py")
```

---

#### 1.3. sql-connection-manager
**Purpose**: Manage SQL Server connections with pooling and retry logic
**Use When**: Establishing database connections, handling transient failures
**Key Functions**:
- `get_connection()` - Get connection with retry logic
- `ConnectionManager(server, database)` - Initialize manager

**Example**:
```python
from scripts.connection_manager import ConnectionManager

manager = ConnectionManager(server="server", database="CRMICALPS")
with manager.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Company")
```

---

### 2. Entity Extraction Skills (1 example provided, template for others)

#### 2.1. case-extractor
**Purpose**: Extract Case/Support Ticket data with Company/Person denormalization
**Use When**: Extracting case data to Bronze layer
**Properties**: 15+ core case fields + denormalized Company/Person fields
**Cardinality**:
- Case → Company (many:1 via Case_PrimaryCompanyId)
- Case → Person (many:1 via Case_PrimaryPersonId)

**SQL Query** (with JOINs):
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

**Generated Dataclass**:
```python
@dataclass
class Case:
    case_id: int
    status: Optional[str]
    company_name: Optional[str]  # From JOIN
    person_first_name: Optional[str]  # From JOIN
    # ... other fields
```

**Usage**:
```python
from scripts.case_extractor import CaseExtractor

extractor = CaseExtractor(connection_string)
cases = extractor.extract()  # List[Case]
extractor.save_to_bronze(cases, "Bronze_Cases.csv")
```

---

**Template for Additional Extractors**:
Following the same pattern, create:
- **company-extractor**: Extract Company entities
- **contact-extractor**: Extract Person/Contact entities (with properties: Pers_Salutation, Pers_FirstName, Pers_LastName, Pers_MiddleName, Pers_Suffix, Pers_Gender, Pers_Title)
- **deal-extractor**: Extract Deal/Opportunity entities with pipeline stages
- **communication-extractor**: Extract Communication logs (with properties: Comm_OriginalDateTime, Comm_OriginalToDateTime)
- **address-extractor**: Extract Address entities (extensible via schema discovery)

Each follows the pattern:
1. Define SQL query with JOINs
2. Generate dataclass with denormalized fields
3. Extract to List[Dataclass]
4. Save to Bronze CSV

---

### 3. Transformation & Business Logic Skills (3)

#### 3.1. dataframe-dataclass-converter
**Purpose**: Bidirectional conversion between DataFrames and dataclasses
**Use When**: Converting SQL results to dataclasses, preparing data for DuckDB
**Key Functions**:
- `dataframe_to_dataclasses(df, dataclass_type)` - DataFrame → List[Dataclass]
- `dataclasses_to_dataframe(instances)` - List[Dataclass] → DataFrame
- Automatic column name mapping (Case_CaseId ↔ case_id)

**Example**:
```python
from scripts.dataframe_converter import DataFrameConverter

converter = DataFrameConverter()

# SQL result → Dataclasses
df = pd.read_sql(query, conn)
cases = converter.dataframe_to_dataclasses(df, Case)

# Dataclasses → DataFrame (for DuckDB)
df = converter.dataclasses_to_dataframe(cases)
```

---

#### 3.2. duckdb-transformer
**Purpose**: High-performance data transformations using DuckDB
**Use When**: Applying SQL transformations, joins, aggregations to Bronze data
**Key Functions**:
- `load_csv(file_path, table_name)` - Load Bronze CSV
- `load_dataframe(df, table_name)` - Register DataFrame
- `execute(query)` - Run SQL transformation
- `to_dataframe(query)` - Get result as DataFrame

**Example**:
```python
from scripts.duckdb_processor import DuckDBProcessor

processor = DuckDBProcessor()
processor.load_csv("Bronze_Cases.csv", "cases")
processor.load_csv("Bronze_Company.csv", "companies")

result = processor.to_dataframe("""
    SELECT
        c.*,
        comp.Comp_Name,
        COUNT(*) OVER (PARTITION BY c.company_id) as total_cases
    FROM cases c
    LEFT JOIN companies comp ON c.company_id = comp.company_id
""")
```

---

#### 3.3. pipeline-stage-mapper
**Purpose**: Map IC'ALPS Hardware/Software pipeline stages with double granularity
**Use When**: Classifying deal stages and outcomes
**Pipeline Stages**:
- Hardware: 01-Identification → 02-Qualifiée → 03-Evaluation technique → 04-Construction propositions → 05-Négociations
- Software: Same 5 stages
- **Double Granularity**: No-go, Abandonnée, Perdue, Gagnée

**Example**:
```python
from scripts.stage_mapper import StageMapper

mapper = StageMapper()
final_stage = mapper.map_stage(
    pipeline="Hardware",
    stage="01 - Identification",
    outcome="Perdue"
)
# Returns: "Closed Lost"
```

---

#### 3.4. computed-columns-calculator
**Purpose**: Calculate financial computed columns for deals
**Use When**: Computing weighted forecasts, net amounts
**Formulas**:
- `Weighted Forecast = Amount × IC_alps Certainty`
- `Net Amount = Forecast - Cost`
- `Net Weighted Amount = Net Amount × IC_alps Certainty`

**Example**:
```python
from scripts.computed_calculator import ComputedColumnsCalculator

calculator = ComputedColumnsCalculator()
df = calculator.calculate_all(df)
# Adds: weighted_forecast, net_amount, net_weighted_amount
```

---

## Workflow: Adding a New Entity (Address)

### Step 1: Discover Schema
```python
from scripts.schema_discovery import SchemaDiscovery

discovery = SchemaDiscovery(connection_string)
columns = discovery.discover_columns("Address")
relationships = discovery.discover_relationships("Address")
```

### Step 2: Define Properties of Interest
```python
address_properties = [
    "Addr_AddressId",
    "Addr_CompanyId",  # FK to Company
    "Addr_Street1",
    "Addr_City",
    "Addr_State",
    "Addr_PostCode"
]
```

### Step 3: Create SQL Query with JOINs
```python
address_query = """
SELECT
    a.Addr_AddressId,
    a.Addr_CompanyId,
    a.Addr_Street1,
    a.Addr_City,
    a.Addr_State,
    a.Addr_PostCode,
    comp.Comp_Name AS Company_Name,  -- Denormalized
    comp.Comp_WebSite AS Company_WebSite  -- Denormalized
FROM Address a
LEFT JOIN Company comp ON a.Addr_CompanyId = comp.Comp_CompanyId
"""
```

### Step 4: Generate Dataclass
```python
from scripts.dataclass_generator import DataclassGenerator

generator = DataclassGenerator()
dataclass_code = generator.generate_from_query(
    query=address_query,
    class_name="Address",
    schema_discovery=discovery
)
generator.save_to_file(dataclass_code, "models/address.py")
```

### Step 5: Create Extractor (following case-extractor pattern)
```python
# Create address-extractor/scripts/address_extractor.py
# Following the same pattern as case_extractor.py
```

### Step 6: Extract Data
```python
from scripts.address_extractor import AddressExtractor

extractor = AddressExtractor(connection_string)
addresses = extractor.extract()
extractor.save_to_bronze(addresses, "Bronze_Address.csv")
```

---

## Complete Pipeline Example

```python
# 1. Discover schema for new entity
discovery = SchemaDiscovery(connection_string)
case_columns = discovery.discover_columns("Cases")

# 2. Generate dataclass
generator = DataclassGenerator()
case_dataclass = generator.generate_from_query(case_query, "Case", discovery)
exec(case_dataclass)  # Or save to models/case.py

# 3. Extract data
extractor = CaseExtractor(connection_string)
cases = extractor.extract()

# 4. Save to Bronze
extractor.save_to_bronze(cases, "Bronze_Cases.csv")

# 5. Transform with DuckDB
processor = DuckDBProcessor()
processor.load_csv("Bronze_Cases.csv", "cases")
processor.load_csv("Bronze_Company.csv", "companies")

transformed = processor.to_dataframe("""
    SELECT
        c.*,
        comp.*,
        COUNT(*) OVER (PARTITION BY c.status) as status_count
    FROM cases c
    LEFT JOIN companies comp ON c.company_id = comp.company_id
""")

# 6. Apply business logic
mapper = StageMapper()
transformed['final_stage'] = transformed.apply(
    lambda row: mapper.map_stage(row['pipeline'], row['stage'], row['outcome']),
    axis=1
)

# 7. Calculate computed columns
calculator = ComputedColumnsCalculator()
transformed = calculator.calculate_all(transformed)

# 8. Save to Silver/Gold layer
transformed.to_csv("Silver_Cases_Processed.csv", index=False)
```

---

## Skills Directory Structure

```
IC_Load/
├── sql-schema-discovery/
│   ├── SKILL.md
│   ├── scripts/
│   │   └── schema_discovery.py
│   └── references/
├── dataclass-generator/
│   ├── SKILL.md
│   ├── scripts/
│   │   └── dataclass_generator.py
│   └── references/
├── sql-connection-manager/
│   ├── SKILL.md
│   └── scripts/
│       └── connection_manager.py
├── case-extractor/
│   ├── SKILL.md
│   └── scripts/
│       └── case_extractor.py
├── dataframe-dataclass-converter/
│   ├── SKILL.md
│   └── scripts/
│       └── dataframe_converter.py
├── duckdb-transformer/
│   ├── SKILL.md
│   └── scripts/
│       └── duckdb_processor.py
├── pipeline-stage-mapper/
│   ├── SKILL.md
│   └── scripts/
│       └── stage_mapper.py
├── computed-columns-calculator/
│   ├── SKILL.md
│   └── scripts/
│       └── computed_calculator.py
└── SKILLS_SUMMARY.md (this file)
```

---

## Key Takeaways

### 1. Dataclass = Query Result
The dataclass structure is **determined by the SQL query columns**, not just the base table. If your query has JOINs, the dataclass includes denormalized fields.

**Example**:
```sql
-- Query with JOIN
SELECT c.Case_Id, comp.Comp_Name AS Company_Name FROM Cases c
LEFT JOIN Company comp ON c.Case_CompanyId = comp.Comp_CompanyId

-- Dataclass includes both
@dataclass
class Case:
    case_id: int
    company_name: Optional[str]  # ← From JOIN
```

### 2. Schema Discovery → Extensibility
Use sql-schema-discovery to discover new tables and their relationships. This enables programmatic addition of new entities.

### 3. Properties Filter Columns
Not all columns from a table are needed. Use properties lists (like in properties.py) to filter to business-relevant columns only.

### 4. Cardinality Rules Guide JOINs
Foreign key relationships determine which tables to JOIN:
- Case_PrimaryCompanyId → JOIN Company (many:1)
- Case_PrimaryPersonId → JOIN Person (many:1)

### 5. Minimal Validation
Use Python type hints for validation. Keep dataclasses simple with Optional[T] for nullable fields.

---

## Next Steps

### For Each Entity (Company, Contact, Deal, Communication, Address):

1. **Define Properties**: List columns of interest
2. **Create SQL Query**: Include necessary JOINs based on FK relationships
3. **Generate Dataclass**: Use dataclass-generator skill
4. **Create Extractor**: Follow case-extractor pattern
5. **Test Extraction**: Extract to Bronze CSV
6. **Apply Transformations**: Use DuckDB for Silver/Gold layers

### For Business Logic:

1. **Pipeline Stages**: Use pipeline-stage-mapper for deal classification
2. **Computed Columns**: Use computed-columns-calculator for financial metrics
3. **Custom Rules**: Extend existing skills or create new ones

### For Production:

1. **Error Handling**: Add comprehensive try-except blocks
2. **Logging**: Implement structured logging
3. **Testing**: Create unit tests for each skill
4. **Documentation**: Document cardinality rules and business logic
5. **Monitoring**: Track extraction performance and data quality

---

## Skills Dependency Graph

```
sql-connection-manager
    ↓
sql-schema-discovery
    ↓
dataclass-generator
    ↓
Entity Extractors (case, company, contact, deal, communication)
    ↓
dataframe-dataclass-converter
    ↓
duckdb-transformer
    ↓
pipeline-stage-mapper + computed-columns-calculator
    ↓
Silver/Gold Layer Output
```

---

## Contact & Support

For questions about these skills:
1. Read the individual SKILL.md files for detailed documentation
2. Review the script files for implementation details
3. Follow the case-extractor example as a template
4. Use schema discovery to understand database structure before creating new extractors

**Remember**: Each skill is modular and independent. Use them individually or compose them into complete pipelines.
