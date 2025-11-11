# IC_Load Architecture & Data Flow

## System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      SQL Server Database                         │
│                        (CRMICALPS)                               │
│                                                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Company  │  │  Person  │  │  Cases   │  │ Address  │       │
│  │  Table   │  │  Table   │  │  Table   │  │  Table   │       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
└───────┼─────────────┼─────────────┼─────────────┼──────────────┘
        │             │             │             │
        └─────────────┴─────────────┴─────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  sql-connection-manager     │
        │  (ADODB Connection Pool)    │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  sql-schema-discovery       │
        │  (information_schema)       │
        │  - discover_tables()        │
        │  - discover_columns()       │
        │  - discover_relationships() │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  dataclass-generator        │
        │  - Parse SQL queries        │
        │  - Generate Python classes  │
        │  - Type mapping             │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │     SQL Query (with JOINs)  │
        │                             │
        │  SELECT                     │
        │    c.Case_CaseId,           │
        │    comp.Comp_Name AS        │
        │      Company_Name,          │
        │    p.Pers_FirstName AS      │
        │      Person_FirstName       │
        │  FROM Cases c               │
        │  LEFT JOIN Company comp     │
        │  LEFT JOIN Person p         │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │    pandas DataFrame         │
        │    (Query Results)          │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  dataframe-dataclass-       │
        │  converter                  │
        │  - DataFrame → Dataclass    │
        │  - Dataclass → DataFrame    │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  Python Dataclass Instances │
        │                             │
        │  @dataclass                 │
        │  class Case:                │
        │    case_id: int             │
        │    company_name: str        │
        │    person_first_name: str   │
        │    # ... (type-safe)        │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  Entity Extractors          │
        │  - case-extractor           │
        │  - company-extractor        │
        │  - contact-extractor        │
        │  - deal-extractor           │
        │  - communication-extractor  │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │    BRONZE LAYER             │
        │  (Raw CSV Extracts)         │
        │                             │
        │  - Bronze_Cases.csv         │
        │  - Bronze_Company.csv       │
        │  - Bronze_Contact.csv       │
        │  - Bronze_Deal.csv          │
        │  - Bronze_Communication.csv │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  duckdb-transformer         │
        │  (SQL Transformations)      │
        │  - Load CSVs                │
        │  - JOIN operations          │
        │  - Aggregations             │
        │  - Complex queries          │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │    SILVER LAYER             │
        │  (Transformed Data)         │
        └─────────────┬───────────────┘
                      │
        ┌─────────────┴─────────────┐
        │                           │
        ▼                           ▼
┌───────────────────┐   ┌───────────────────────┐
│ pipeline-stage-   │   │ computed-columns-     │
│ mapper            │   │ calculator            │
│                   │   │                       │
│ - IC'ALPS stages  │   │ - Weighted Forecast   │
│ - Double          │   │ - Net Amount          │
│   granularity     │   │ - Net Weighted Amount │
└─────────┬─────────┘   └─────────┬─────────────┘
          │                       │
          └───────────┬───────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │      GOLD LAYER             │
        │  (Business Logic Applied)   │
        │                             │
        │  - Final stage mapped       │
        │  - Computed columns         │
        │  - Ready for analytics      │
        └─────────────────────────────┘
```

## Data Flow Sequence

### Phase 1: Schema Discovery & Dataclass Generation

```
1. [sql-connection-manager] → Connect to SQL Server
2. [sql-schema-discovery] → Query information_schema
3. [sql-schema-discovery] → Return TableMetadata, ColumnMetadata, ForeignKeyRelationship
4. [dataclass-generator] → Parse SQL query with JOINs
5. [dataclass-generator] → Generate Python dataclass code
6. [dataclass-generator] → Save to models/entity.py
```

### Phase 2: Data Extraction (Bronze Layer)

```
1. [entity-extractor] → Execute SQL query with JOINs
2. [SQL Server] → Return result set
3. [entity-extractor] → Convert to pandas DataFrame
4. [dataframe-dataclass-converter] → DataFrame → List[Dataclass]
5. [entity-extractor] → Validate dataclass instances
6. [entity-extractor] → Save to Bronze_Entity.csv
```

### Phase 3: Transformation (Silver Layer)

```
1. [duckdb-transformer] → Load Bronze CSV files
2. [duckdb-transformer] → Register as DuckDB tables
3. [duckdb-transformer] → Execute SQL transformations
4. [duckdb-transformer] → Apply business rules
5. [duckdb-transformer] → Export to Silver layer
```

### Phase 4: Business Logic (Gold Layer)

```
1. [pipeline-stage-mapper] → Map deal stages to final outcomes
2. [computed-columns-calculator] → Calculate financial metrics
3. [Business Logic] → Apply cardinality rules
4. [Output] → Gold_Entity.csv (analytics-ready)
```

## Component Interactions

### Dataclass ↔ SQL Query Relationship

```
SQL Query:
┌─────────────────────────────────────────┐
│ SELECT                                  │
│   c.Case_CaseId,                        │
│   c.Case_Status,                        │
│   comp.Comp_Name AS Company_Name,  ◄──┐ │
│   p.Pers_FirstName AS Person_First  ◄─┤ │ Denormalized
│ FROM Cases c                         │ │ from JOINs
│ LEFT JOIN Company comp               │ │
│ LEFT JOIN Person p                   ├─┘
└─────────────────────────────────────────┘
           │
           ▼
Python Dataclass:
┌─────────────────────────────────────────┐
│ @dataclass                              │
│ class Case:                             │
│   case_id: int                          │ From base table
│   status: Optional[str]                 │
│   company_name: Optional[str]  ◄────────┤ From JOIN
│   person_first_name: Optional[str] ◄────┤ From JOIN
└─────────────────────────────────────────┘
```

### Entity Relationships (Cardinality)

```
┌──────────┐                    ┌──────────┐
│ Company  │                    │  Person  │
│          │                    │ (Contact)│
└────┬─────┘                    └────┬─────┘
     │ 1                             │ 1
     │                               │
     │ many                          │ many
     ▼                               ▼
┌────────────────────────────────────────┐
│              Cases                     │
│                                        │
│  Case_PrimaryCompanyId (FK) ──────┐   │
│  Case_PrimaryPersonId (FK) ────────┤   │
└────────────────────────────────────────┘
           │ many
           │
           ▼ many
┌──────────────────┐
│  Communication   │
└──────────────────┘
```

## Skill Dependencies

```
sql-connection-manager (independent)
           │
           ▼
sql-schema-discovery (requires connection)
           │
           ▼
dataclass-generator (requires schema metadata)
           │
           ├─────────────────┐
           ▼                 ▼
  Entity Extractors    dataframe-dataclass-converter
  (require both)            │
           │                │
           └────────┬───────┘
                    ▼
         duckdb-transformer (requires DataFrame)
                    │
           ┌────────┴─────────┐
           ▼                  ▼
  pipeline-stage-mapper  computed-columns-calculator
       (independent)          (independent)
```

## Type Mapping Flow

```
SQL Server Types          Python Types           Pandas Types
─────────────────────────────────────────────────────────────
int, bigint           →   int                →   int64
varchar, nvarchar     →   str                →   object
datetime, datetime2   →   datetime           →   datetime64
bit                   →   bool               →   bool
decimal, money        →   Decimal            →   float64
NULL values           →   Optional[T]        →   NaN

Conversions handled by:
1. sql-schema-discovery (SQL → Python)
2. dataframe-dataclass-converter (DataFrame → Dataclass)
3. dataclass-generator (Type hint generation)
```

## Medallion Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                    BRONZE LAYER                         │
│  - Raw SQL extracts                                     │
│  - Denormalized (includes JOINs)                        │
│  - Minimal transformation                               │
│  - CSV + Python dataclasses                             │
│  - Prefix: Bronze_*                                     │
│  - Tools: entity-extractors                             │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                    SILVER LAYER                         │
│  - DuckDB transformations applied                       │
│  - Joins, aggregations, filtering                       │
│  - Data quality checks                                  │
│  - Prefix: Silver_*                                     │
│  - Tools: duckdb-transformer                            │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                     GOLD LAYER                          │
│  - Business logic applied                               │
│  - Pipeline stages mapped                               │
│  - Computed columns calculated                          │
│  - Analytics-ready                                      │
│  - Prefix: Gold_*                                       │
│  - Tools: pipeline-stage-mapper,                        │
│           computed-columns-calculator                   │
└─────────────────────────────────────────────────────────┘
```

## Extension Pattern: Adding New Entity

```
Step 1: Schema Discovery
┌──────────────────────────┐
│ discovery.discover_      │
│   columns("NewEntity")   │
└──────────┬───────────────┘
           │
Step 2: Properties Filter
           ▼
┌──────────────────────────┐
│ properties = [           │
│   "Entity_Field1",       │
│   "Entity_Field2"        │
│ ]                        │
└──────────┬───────────────┘
           │
Step 3: Build Query
           ▼
┌──────────────────────────┐
│ query = """              │
│ SELECT e.*, comp.*       │
│ FROM NewEntity e         │
│ LEFT JOIN Company        │
│ """                      │
└──────────┬───────────────┘
           │
Step 4: Generate Dataclass
           ▼
┌──────────────────────────┐
│ generator.generate_      │
│   from_query()           │
└──────────┬───────────────┘
           │
Step 5: Create Extractor
           ▼
┌──────────────────────────┐
│ class NewEntityExtractor │
│   (follow pattern)       │
└──────────┬───────────────┘
           │
Step 6: Extract & Save
           ▼
┌──────────────────────────┐
│ extractor.extract()      │
│ extractor.save_to_       │
│   bronze()               │
└──────────────────────────┘
```

## Performance Considerations

### Connection Pooling
```
┌──────────────────────────────────────┐
│   sql-connection-manager             │
│                                      │
│   Pool Size: 5                       │
│   Max Overflow: 10                   │
│   ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐│
│   │Conn│ │Conn│ │Conn│ │Conn│ │Conn││
│   └────┘ └────┘ └────┘ └────┘ └────┘│
└──────────────────────────────────────┘
         │       │       │       │
         ▼       ▼       ▼       ▼
    Extractor Extractor Schema Discovery
```

### DuckDB In-Memory Processing
```
Bronze CSVs (Disk)
        │
        ▼
DuckDB (In-Memory) ◄── Fast SQL operations
        │              - Columnar storage
        │              - Parallel execution
        ▼              - SIMD operations
Silver Layer (Disk)
```

## Error Handling Flow

```
┌─────────────────────────────────────────────┐
│  Connection Error                           │
│  ├─ Retry with exponential backoff          │
│  ├─ Max 3 retries                           │
│  └─ Log failure after exhaustion            │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│  Schema Discovery Error                     │
│  ├─ Table not found → return empty list     │
│  ├─ Column not found → return empty list    │
│  └─ Log warning                             │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│  Dataclass Conversion Error                 │
│  ├─ Column mismatch → auto-mapping attempt  │
│  ├─ Type mismatch → coerce or None          │
│  └─ Log warning with field name             │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│  Extraction Error                           │
│  ├─ SQL error → log query and retry         │
│  ├─ Data validation → log invalid rows      │
│  └─ File write error → retry with backoff   │
└─────────────────────────────────────────────┘
```

---

**For more details, see:**
- [README.md](README.md) - Project overview
- [QUICK_START.md](QUICK_START.md) - Getting started
- [SKILLS_SUMMARY.md](SKILLS_SUMMARY.md) - Complete documentation
