# IC_Load - Modular Data Extraction Pipeline Skills

> **Flexible, extensible skills for SQL Server → DataFrame → Dataclass → Bronze Layer extraction**

## 📚 Documentation

- **[QUICK_START.md](QUICK_START.md)** - Get started in 5 minutes
- **[SKILLS_SUMMARY.md](SKILLS_SUMMARY.md)** - Complete skills documentation
- **Individual SKILL.md files** - Detailed documentation for each skill

## 🎯 What is IC_Load?

IC_Load is a collection of **12 modular, independent skills** that serve as building blocks for extracting data from SQL Server databases into type-safe Python dataclasses and Bronze layer CSV files.

### Core Architecture

```
SQL Server (ADODB)
    ↓ [sql-connection-manager]
SQL Query (with JOINs)
    ↓ [sql-schema-discovery]
pandas DataFrame
    ↓ [dataframe-dataclass-converter]
Python Dataclass Instances (type-safe)
    ↓ [entity-extractors]
Bronze Layer CSV
    ↓ [duckdb-transformer]
Silver/Gold Layer
    ↓ [pipeline-stage-mapper, computed-columns-calculator]
Business Logic Applied
```

## 🧱 Skills Overview

### Infrastructure (3 skills)
1. **sql-schema-discovery** - Discover database schemas dynamically
2. **dataclass-generator** - Generate Python dataclasses from SQL queries
3. **sql-connection-manager** - Manage connections with pooling & retry

### Entity Extraction (1 example + template)
4. **case-extractor** - Extract Case data with Company/Person denormalization
   - *Template for: company, contact, deal, communication, address extractors*

### Transformation (4 skills)
5. **dataframe-dataclass-converter** - Bidirectional DataFrame ↔ Dataclass
6. **duckdb-transformer** - High-performance SQL transformations
7. **pipeline-stage-mapper** - IC'ALPS pipeline stage mapping
8. **computed-columns-calculator** - Financial calculations

## 🚀 Quick Example

```python
# 1. Discover schema
from sql_schema_discovery.scripts.schema_discovery import SchemaDiscovery
discovery = SchemaDiscovery(connection_string)
columns = discovery.discover_columns("Cases")

# 2. Generate dataclass from query
from dataclass_generator.scripts.dataclass_generator import DataclassGenerator
generator = DataclassGenerator()
code = generator.generate_from_query(query, "Case", discovery)

# 3. Extract data
from case_extractor.scripts.case_extractor import CaseExtractor
extractor = CaseExtractor(connection_string)
cases = extractor.extract()  # List[Case]
extractor.save_to_bronze(cases, "Bronze_Cases.csv")

# 4. Transform with DuckDB
from duckdb_transformer.scripts.duckdb_processor import DuckDBProcessor
processor = DuckDBProcessor()
processor.load_csv("Bronze_Cases.csv", "cases")
result = processor.to_dataframe("SELECT * FROM cases WHERE status = 'Open'")
```

## 🎨 Key Features

### ✅ Type-Safe Dataclasses
SQL queries → Python dataclasses with proper type hints (int, str, datetime, Optional[T])

### ✅ Denormalized JOINs
Dataclasses include fields from JOINed tables for convenient access

### ✅ Schema Discovery
Programmatically discover tables, columns, and relationships

### ✅ Extensible
Add new entities by following the established pattern

### ✅ Modular
Use skills independently or compose into complete pipelines

### ✅ Bronze → Silver → Gold
Medallion architecture for data quality layers

## 📋 Entity Properties Reference

### Case Entity
**Properties**: Case_CaseId, Case_PrimaryCompanyId, Case_PrimaryPersonId, Case_Status, Case_Stage, Case_Priority, Case_Opened, Case_Closed (+ denormalized Company/Person fields)

**Cardinality**:
- Case → Company (many:1)
- Case → Person (many:1)

### Contact Entity
**Properties**: Pers_Salutation, Pers_FirstName, Pers_LastName, Pers_MiddleName, Pers_Suffix, Pers_Gender, Pers_Title

**Cardinality**:
- Contact → Company (many:1)

### Communication Entity
**Properties**: Comm_OriginalDateTime, Comm_OriginalToDateTime, (+ other communication fields)

**Cardinality**:
- Communication → Company (many:many)
- Communication → Contact (many:many)

## 🔄 Adding a New Entity

Follow these 6 steps:

1. **Discover Schema**: `discovery.discover_columns("EntityName")`
2. **Define Properties**: List columns of interest
3. **Create SQL Query**: Include JOINs based on FK relationships
4. **Generate Dataclass**: `generator.generate_from_query()`
5. **Create Extractor**: Follow case_extractor.py pattern
6. **Extract & Save**: `extractor.extract()` → `Bronze_Entity.csv`

See [SKILLS_SUMMARY.md](SKILLS_SUMMARY.md#workflow-adding-a-new-entity-address) for detailed example.

## 🛠️ Installation

```bash
# Core dependencies
pip install pandas pyodbc duckdb

# Optional: for dataclass validation
pip install pydantic
```

## 📁 Directory Structure

```
IC_Load/
├── README.md                        ← You are here
├── QUICK_START.md                   ← 5-minute tutorial
├── SKILLS_SUMMARY.md                ← Complete documentation
│
├── sql-schema-discovery/            ← Skill 1
│   ├── SKILL.md
│   └── scripts/schema_discovery.py
│
├── dataclass-generator/             ← Skill 2
│   ├── SKILL.md
│   └── scripts/dataclass_generator.py
│
├── sql-connection-manager/          ← Skill 3
│   ├── SKILL.md
│   └── scripts/connection_manager.py
│
├── case-extractor/                  ← Skill 4 (example)
│   ├── SKILL.md
│   └── scripts/case_extractor.py
│
├── dataframe-dataclass-converter/   ← Skill 5
│   ├── SKILL.md
│   └── scripts/dataframe_converter.py
│
├── duckdb-transformer/              ← Skill 6
│   ├── SKILL.md
│   └── scripts/duckdb_processor.py
│
├── pipeline-stage-mapper/           ← Skill 7
│   ├── SKILL.md
│   └── scripts/stage_mapper.py
│
└── computed-columns-calculator/     ← Skill 8
    ├── SKILL.md
    └── scripts/computed_calculator.py
```

## 🎓 Core Concepts

### 1. Dataclass = Query Result
The dataclass structure matches the SQL query output, including denormalized fields from JOINs.

**Example**:
```sql
SELECT c.Case_Id, comp.Comp_Name AS Company_Name FROM Cases c
LEFT JOIN Company comp ON c.Case_CompanyId = comp.Comp_CompanyId
```
↓
```python
@dataclass
class Case:
    case_id: int
    company_name: Optional[str]  # ← From JOIN
```

### 2. Schema Discovery → Extensibility
Discover database structure programmatically to add new entities without manual schema inspection.

### 3. Properties Filter Columns
Not all columns are needed. Filter to business-relevant properties only.

### 4. Cardinality Rules Guide JOINs
Foreign key relationships determine JOIN structure:
- many:1 → LEFT JOIN
- many:many → junction table

### 5. Minimal Validation
Use type hints (Optional[T]) for validation. Keep dataclasses simple.

## 📚 Learn More

### Getting Started
1. Read [QUICK_START.md](QUICK_START.md)
2. Review [case-extractor/SKILL.md](case-extractor/SKILL.md) as a template
3. Follow the "Adding a New Entity" workflow in [SKILLS_SUMMARY.md](SKILLS_SUMMARY.md)

### Advanced Topics
- Custom business logic in DuckDB transformations
- Pipeline stage mapping rules
- Computed column formulas
- Connection pooling and retry strategies

## 🤝 Contributing

To add a new entity extractor:
1. Use schema-discovery to inspect the table
2. Generate dataclass using dataclass-generator
3. Create extractor following case-extractor pattern
4. Document properties and cardinality
5. Add to SKILLS_SUMMARY.md

## 📄 License

Internal project for IC'ALPS CRM data extraction pipeline.

---

**Questions?** See individual SKILL.md files or [SKILLS_SUMMARY.md](SKILLS_SUMMARY.md) for complete documentation.
