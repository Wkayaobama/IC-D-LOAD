# Pipeline Async - Modular Extraction Framework

## Overview

**Pipeline Async** is a modular, extensible data extraction framework that separates concerns between:

- **Entity Configuration** (properties + SQL queries) - WHAT to extract
- **Connection Parameters** (connection strings, database labels) - WHERE to extract from
- **Generic Extractors** (reusable extraction logic) - HOW to extract
- **Async Tasks** (execution coordination) - WHEN to extract
- **Models** (data storage) - WHERE to store

This architecture solves the key problems with the previous single-extractor approach:

✅ **Reusable connection strings** across all entity extractions
✅ **Centralized property definitions** in `properties.py`
✅ **Declarative entity configuration** linking properties to SQL
✅ **Generic extractor** works for ANY entity
✅ **Async task execution** for parallel/sequential extraction
✅ **Model integration** for Memory/SQL/Chain storage

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PIPELINE ASYNC                            │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Entity     │    │   Generic    │    │  Extraction  │
│   Config     │───▶│  Extractor   │───▶│     Task     │
│              │    │              │    │              │
│ Properties   │    │ SQL Query    │    │ Async Exec   │
│ + JOINs      │    │ Execution    │    │ + Progress   │
└──────────────┘    └──────────────┘    └──────────────┘
        │                   │                   │
        │                   └───────────────────┤
        │                                       │
        ▼                                       ▼
┌──────────────┐                        ┌──────────────┐
│ properties.py│                        │    Model     │
│              │                        │              │
│ CASE_PROPS   │                        │ Memory/SQL/  │
│ COMPANY_PROPS│                        │   Chain      │
└──────────────┘                        └──────────────┘
```

### Key Components

#### 1. **properties.py** - Property Definitions

Centralized property definitions for all entities:

```python
CASE_PROPERTIES = {
    'base': ['Case_CaseId', 'Case_Status', ...],
    'denormalized': ['Company_Name', 'Person_FirstName', ...],
    'metadata': ['bronze_extracted_at', ...]
}
```

- **Base properties**: From the primary table
- **Denormalized properties**: From JOINs
- **Computed properties**: Calculated fields
- **Metadata**: Extraction timestamps, source files

#### 2. **entity_config.py** - Entity Configuration

Links properties to SQL queries:

```python
CASE_CONFIG = EntityConfig(
    name="Case",
    properties=CASE_PROPERTIES,
    base_table="vCases",
    joins=[
        "LEFT JOIN Company comp ON base.Case_PrimaryCompanyId = comp.Comp_CompanyId",
        "LEFT JOIN Person p ON base.Case_PrimaryPersonId = p.Pers_PersonId"
    ],
    where_clause=None
)
```

**Key Features:**
- Declarative entity definitions
- Auto-generates SQL queries from properties
- Supports JOINs for denormalization
- Configurable WHERE clauses

#### 3. **generic_extractor.py** - Generic Extractor

Reusable extractor that works with ANY `EntityConfig`:

```python
from pipeline_async import GenericExtractor

extractor = GenericExtractor(case_config, connection_string)

# Extract to DataFrame
df = extractor.extract_to_dataframe(limit=100)

# Extract to dataclasses
cases = extractor.extract_to_dataclasses(Case, limit=100)

# Save to Bronze layer
extractor.save_to_bronze(df, "Bronze_Cases.csv")
```

**Key Features:**
- Works with any EntityConfig
- Returns pandas DataFrames or dataclasses
- Supports filtering and limits
- Saves to Bronze layer CSV
- Connection string reused across extractions

#### 4. **extraction_task.py** - Async Tasks

Task wrapper for async execution:

```python
from pipeline_async import ExtractionTask
from pipeline_async.model.memory import MemoryModel

# Create task
task = ExtractionTask(
    entity_config=case_config,
    connection_string=connection_string,
    model=MemoryModel(),
    limit=100
)

# Run asynchronously
await task.run()

# Get stats
stats = task.get_stats()
# {'entity': 'Case', 'extracted': 100, 'added_to_model': 95, ...}
```

**Multi-Entity Extraction:**

```python
from pipeline_async import MultiEntityExtractionTask

task = MultiEntityExtractionTask(
    entity_names=['Company', 'Person', 'Case'],
    connection_string=connection_string,
    model=MemoryModel(),
    limits={'Company': 100, 'Person': 100, 'Case': 50},
    parallel=False  # Sequential for FK dependencies
)

await task.run()
```

#### 5. **Models** - Data Storage

Three model types for different use cases:

**MemoryModel** - In-memory storage:
```python
from pipeline_async.model.memory import MemoryModel

model = MemoryModel()
# Data stored in model.storage dict
# Fast, good for testing
```

**SqlModel** - SQL database persistence:
```python
from pipeline_async.model.sql import SqlModel

model = SqlModel(engine)
# Data persisted to SQL database
# Auto-creates tables from dataclasses
```

**ChainModel** - Chain multiple models:
```python
from pipeline_async.model.chain import ChainModel

model = ChainModel(MemoryModel(), SqlModel(engine))
# Data added to all models in chain
# Combine in-memory + persistence
```

---

## Quick Start

### 1. Installation

```bash
cd IC_Load
pip install pandas pyodbc loguru
```

### 2. Configure Connection

Update `config.py` with your database settings:

```python
SQL_SERVER = r"(localdb)\MSSQLLocalDB"
SQL_DATABASE = "CRMICALPS"
SQL_TRUSTED_CONNECTION = True
```

### 3. Run Examples

```bash
python example_pipeline.py
```

This runs 6 examples demonstrating:
- Single entity extraction
- Multiple entity extraction
- ChainModel usage
- Filtered extraction
- All entities extraction
- Direct extractor usage

---

## Usage Patterns

### Pattern 1: Quick Single Entity Extraction

```python
import asyncio
from pipeline_async import extract_entity
from pipeline_async.model.memory import MemoryModel
from config import get_connection_string

async def main():
    model = MemoryModel()

    task = await extract_entity(
        entity_name='Case',
        connection_string=get_connection_string(),
        model=model,
        limit=100
    )

    print(task.get_stats())

asyncio.run(main())
```

### Pattern 2: Multiple Entities with Shared Connection

```python
import asyncio
from pipeline_async import extract_multiple_entities
from pipeline_async.model.memory import MemoryModel
from config import get_connection_string

async def main():
    model = MemoryModel()
    connection_string = get_connection_string()

    task = await extract_multiple_entities(
        entity_names=['Company', 'Person', 'Case'],
        connection_string=connection_string,  # Reused!
        model=model,
        limits={'Company': 1000, 'Person': 1000, 'Case': 500},
        save_bronze=True,
        parallel=False  # Sequential for FK dependencies
    )

    print(f"Extracted {task.get_stats()['total_extracted']} total rows")

asyncio.run(main())
```

### Pattern 3: Direct Extractor (No Task Wrapper)

```python
from pipeline_async import GenericExtractor, get_entity_config
from config import get_connection_string

# Setup
case_config = get_entity_config('Case')
extractor = GenericExtractor(case_config, get_connection_string())

# Preview
preview = extractor.preview(limit=5)
print(preview)

# Extract
df = extractor.extract_to_dataframe(
    filter_clause="AND Case_Status = 'Open'",
    limit=100
)

# Save
path = extractor.save_to_bronze(df)
print(f"Saved to {path}")
```

### Pattern 4: Custom Task with ChainModel

```python
import asyncio
from pipeline_async import ExtractionTask, get_entity_config
from pipeline_async.model.chain import ChainModel
from pipeline_async.model.memory import MemoryModel
from pipeline_async.model.sql import SqlModel
from config import get_connection_string

async def main():
    # Chain in-memory + SQL persistence
    model = ChainModel(
        MemoryModel(),
        SqlModel.from_filepath("bronze_layer/data.db")
    )

    # Create task
    task = ExtractionTask(
        entity_config=get_entity_config('Case'),
        connection_string=get_connection_string(),
        model=model,
        save_bronze=True
    )

    # Run
    success = await task.run()
    print(f"Success: {success}")
    print(f"Stats: {task.get_stats()}")

asyncio.run(main())
```

---

## Adding New Entities

To add a new entity (e.g., `Invoice`):

### Step 1: Define Properties in `properties.py`

```python
INVOICE_PROPERTIES = {
    'base': [
        'Invo_InvoiceId',
        'Invo_Number',
        'Invo_Date',
        'Invo_Amount',
        'Invo_Status'
    ],
    'denormalized': [
        'Comp_CompanyId',
        'Company_Name',
        'Company_Website'
    ],
    'metadata': [
        'bronze_extracted_at',
        'bronze_source_file'
    ]
}
```

### Step 2: Add EntityConfig in `entity_config.py`

```python
INVOICE_CONFIG = EntityConfig(
    name="Invoice",
    properties=INVOICE_PROPERTIES,
    base_table="Invoice",
    joins=[
        "LEFT JOIN Company c ON base.Invo_CompanyId = c.Comp_CompanyId"
    ],
    where_clause="base.Invo_InvoiceId IS NOT NULL"
)

# Add to registry
ENTITY_CONFIGS['Invoice'] = INVOICE_CONFIG
```

### Step 3: Use It!

```python
import asyncio
from pipeline_async import extract_entity
from pipeline_async.model.memory import MemoryModel
from config import get_connection_string

async def main():
    task = await extract_entity(
        entity_name='Invoice',
        connection_string=get_connection_string(),
        model=MemoryModel(),
        limit=100
    )
    print(task.get_stats())

asyncio.run(main())
```

**That's it!** No need to create a custom extractor - `GenericExtractor` handles everything.

---

## Benefits Over Previous Approach

### Before (Single Extractor Pattern)

```python
# case_extractor.py
class CaseExtractor:
    QUERY = """SELECT ..."""  # Hardcoded query

    def __init__(self, connection_string):
        self.conn_manager = ConnectionManager(connection_string)

    def extract(self):
        # Extract logic

    def save_to_bronze(self, cases):
        # Save logic

# Usage
extractor1 = CaseExtractor(conn_str)  # New connection
extractor2 = CompanyExtractor(conn_str)  # New connection
extractor3 = PersonExtractor(conn_str)  # New connection

cases = extractor1.extract()
companies = extractor2.extract()
persons = extractor3.extract()
```

**Problems:**
- ❌ New extractor class for EVERY entity
- ❌ Duplicate connection logic
- ❌ Duplicate extraction logic
- ❌ No coordination between extractors
- ❌ Properties scattered across files
- ❌ Hard to maintain consistency

### After (Modular Pipeline)

```python
# One extractor for ALL entities
from pipeline_async import extract_multiple_entities
from pipeline_async.model.memory import MemoryModel
from config import get_connection_string

async def main():
    task = await extract_multiple_entities(
        entity_names=['Case', 'Company', 'Person'],
        connection_string=get_connection_string(),  # Reused!
        model=MemoryModel(),
        parallel=False
    )

asyncio.run(main())
```

**Benefits:**
- ✅ **One generic extractor** for ALL entities
- ✅ **Shared connection string** across all extractions
- ✅ **Centralized properties** in `properties.py`
- ✅ **Declarative configuration** in `entity_config.py`
- ✅ **Async coordination** with progress tracking
- ✅ **Model integration** (Memory/SQL/Chain)
- ✅ **Easy to add new entities** (3 lines of code)

---

## Project Structure

```
IC_Load/
├── properties.py                    # Property definitions
├── config.py                        # Connection settings
├── example_pipeline.py              # Complete examples
│
├── pipeline_async/
│   ├── __init__.py                  # Main exports
│   ├── entity_config.py             # EntityConfig definitions
│   ├── generic_extractor.py         # GenericExtractor
│   ├── extraction_task.py           # Async tasks
│   │
│   ├── model/
│   │   ├── base.py                  # ModelBase
│   │   ├── memory.py                # MemoryModel
│   │   ├── sql.py                   # SqlModel (user provided)
│   │   ├── chain.py                 # ChainModel
│   │   └── passthrough.py           # PassThroughModel
│   │
│   └── task/
│       ├── base.py                  # Task base class
│       └── __init__.py
│
├── sql-connection-manager/          # Existing skill
│   └── scripts/
│       └── connection_manager.py
│
├── dataframe-dataclass-converter/   # Existing skill
│   └── scripts/
│       └── dataframe_converter.py
│
└── case-extractor/                  # Legacy (for reference)
    └── scripts/
        └── case_extractor.py
```

---

## API Reference

### EntityConfig

```python
class EntityConfig:
    name: str                        # Entity name
    properties: Dict[str, List[str]] # Property dictionary
    base_table: str                  # Primary table/view
    database: str                    # Database name
    schema: str                      # Schema name
    joins: List[str]                 # JOIN clauses
    where_clause: Optional[str]      # WHERE filter

    def get_all_properties() -> List[str]
    def get_base_properties() -> List[str]
    def build_query(include_metadata: bool) -> str
    def get_primary_key() -> str
```

### GenericExtractor

```python
class GenericExtractor:
    def __init__(self, entity_config: EntityConfig, connection_string: str)

    def extract_to_dataframe(
        filter_clause: Optional[str] = None,
        include_metadata: bool = False,
        limit: Optional[int] = None
    ) -> pd.DataFrame

    def extract_to_dataclasses(
        dataclass_type: Type,
        filter_clause: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Any]

    def save_to_bronze(
        data: Any,
        output_path: Optional[str] = None
    ) -> str

    def get_row_count() -> int
    def preview(limit: int = 5) -> pd.DataFrame
```

### ExtractionTask

```python
class ExtractionTask(Task):
    def __init__(
        entity_config: EntityConfig,
        connection_string: str,
        model: ModelBase,
        dataclass_type: Optional[Type] = None,
        filter_clause: Optional[str] = None,
        limit: Optional[int] = None
    )

    async def run(progress: bool = True) -> bool
    def get_stats() -> dict
```

### MultiEntityExtractionTask

```python
class MultiEntityExtractionTask(Task):
    def __init__(
        entity_names: List[str],
        connection_string: str,
        model: ModelBase,
        limits: Optional[dict] = None,
        parallel: bool = False
    )

    async def run(progress: bool = True) -> bool
    def get_stats() -> dict
```

---

## FAQ

**Q: How do I extract just one entity quickly?**

```python
import asyncio
from pipeline_async import extract_entity
from pipeline_async.model.memory import MemoryModel
from config import get_connection_string

task = await extract_entity('Case', get_connection_string(), MemoryModel())
```

**Q: How do I extract multiple entities with the same connection?**

Use `extract_multiple_entities()` - connection string is reused automatically.

**Q: How do I add a new entity?**

1. Add properties to `properties.py`
2. Add EntityConfig to `entity_config.py`
3. Use it with `extract_entity()` or `extract_multiple_entities()`

**Q: Can I use the extractor without async?**

Yes, use `GenericExtractor` directly:

```python
from pipeline_async import GenericExtractor, get_entity_config
extractor = GenericExtractor(get_entity_config('Case'), conn_str)
df = extractor.extract_to_dataframe()
```

**Q: How do I filter the extraction?**

```python
task = await extract_entity(
    'Case',
    conn_str,
    model,
    filter_clause="AND Case_Status = 'Open'"
)
```

**Q: How do I extract to SQL database?**

```python
from pipeline_async.model.sql import SqlModel
model = SqlModel.from_filepath("data.db")
task = await extract_entity('Case', conn_str, model)
```

---

## Next Steps

1. **Run the examples**: `python example_pipeline.py`
2. **Add your entities**: Follow "Adding New Entities" section
3. **Customize extraction**: Use filter_clause, limits, etc.
4. **Production deployment**: Use ChainModel(MemoryModel, SqlModel)

---

## Support

For issues or questions:
- Check `example_pipeline.py` for usage patterns
- Review entity configurations in `entity_config.py`
- Refer to inline documentation in source files

---

**Pipeline Async** - Modular, extensible, production-ready extraction framework for IC_Load.
