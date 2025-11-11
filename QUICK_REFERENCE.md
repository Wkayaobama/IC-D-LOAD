# Pipeline Async - Quick Reference

## Installation

```bash
pip install pandas pyodbc loguru
```

## Quick Start

### Extract Single Entity

```python
import asyncio
from pipeline_async import extract_entity
from pipeline_async.model.memory import MemoryModel
from config import get_connection_string

async def main():
    task = await extract_entity(
        entity_name='Case',
        connection_string=get_connection_string(),
        model=MemoryModel(),
        limit=100
    )
    print(task.get_stats())

asyncio.run(main())
```

### Extract Multiple Entities

```python
import asyncio
from pipeline_async import extract_multiple_entities
from pipeline_async.model.memory import MemoryModel
from config import get_connection_string

async def main():
    task = await extract_multiple_entities(
        entity_names=['Company', 'Person', 'Case'],
        connection_string=get_connection_string(),
        model=MemoryModel(),
        limits={'Company': 100, 'Person': 100, 'Case': 50},
        save_bronze=True
    )
    print(task.get_stats())

asyncio.run(main())
```

## Available Entities

```python
from pipeline_async import list_entities

print(list_entities())
# ['Case', 'Company', 'Person', 'Opportunity', 'Communication', 'SocialNetwork', 'Address']
```

## Common Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `entity_name` | str | Entity to extract ('Case', 'Company', etc.) |
| `entity_names` | List[str] | List of entities to extract |
| `connection_string` | str | SQL Server connection string (from config) |
| `model` | ModelBase | Storage model (Memory/SQL/Chain) |
| `limit` | int | Limit number of rows (optional) |
| `filter_clause` | str | Additional WHERE clause (optional) |
| `save_bronze` | bool | Save to Bronze layer CSV (default True) |
| `parallel` | bool | Run extractions in parallel (default False) |

## Filtering

```python
# Extract only open cases
task = await extract_entity(
    'Case',
    connection_string,
    model,
    filter_clause="AND Case_Status = 'Open'"
)

# Extract recent opportunities
task = await extract_entity(
    'Opportunity',
    connection_string,
    model,
    filter_clause="AND Oppo_CreatedDate >= '2024-01-01'"
)
```

## Direct Extractor Usage (No Async)

```python
from pipeline_async import GenericExtractor, get_entity_config
from config import get_connection_string

# Create extractor
config = get_entity_config('Case')
extractor = GenericExtractor(config, get_connection_string())

# Preview data
preview = extractor.preview(limit=5)

# Extract to DataFrame
df = extractor.extract_to_dataframe(limit=100)

# Save to Bronze
path = extractor.save_to_bronze(df)
```

## Model Options

### MemoryModel (In-Memory)

```python
from pipeline_async.model.memory import MemoryModel

model = MemoryModel()
# Fast, good for testing
# Data stored in model.storage dict
```

### SqlModel (Database Persistence)

```python
from pipeline_async.model.sql import SqlModel

model = SqlModel.from_filepath("data.db")
# Persistent storage
# Auto-creates tables
```

### ChainModel (Multiple Models)

```python
from pipeline_async.model.chain import ChainModel
from pipeline_async.model.memory import MemoryModel
from pipeline_async.model.sql import SqlModel

model = ChainModel(
    MemoryModel(),
    SqlModel.from_filepath("data.db")
)
# Data added to all models in chain
```

## Adding New Entity

### 1. Define Properties (properties.py)

```python
INVOICE_PROPERTIES = {
    'base': [
        'Invo_InvoiceId',
        'Invo_Number',
        'Invo_Date',
        'Invo_Amount'
    ],
    'denormalized': [
        'Company_Name',
        'Company_Website'
    ],
    'metadata': [
        'bronze_extracted_at'
    ]
}
```

### 2. Add Configuration (entity_config.py)

```python
INVOICE_CONFIG = EntityConfig(
    name="Invoice",
    properties=INVOICE_PROPERTIES,
    base_table="Invoice",
    joins=[
        "LEFT JOIN Company c ON base.Invo_CompanyId = c.Comp_CompanyId"
    ]
)

ENTITY_CONFIGS['Invoice'] = INVOICE_CONFIG
```

### 3. Use It!

```python
task = await extract_entity('Invoice', connection_string, model)
```

## Statistics

```python
# Single entity stats
stats = task.get_stats()
# {
#     'entity': 'Case',
#     'extracted': 100,
#     'added_to_model': 95,
#     'duplicates_skipped': 5,
#     'bronze_path': 'bronze_layer/Bronze_Case.csv'
# }

# Multi-entity stats
stats = task.get_stats()
# {
#     'entities': ['Company', 'Person', 'Case'],
#     'total_extracted': 270,
#     'total_added': 265,
#     'individual_stats': {...}
# }
```

## Examples

See [example_pipeline.py](example_pipeline.py) for 6 complete examples:

1. Single entity extraction
2. Multiple entity extraction
3. ChainModel usage
4. Filtered extraction
5. All entities extraction
6. Direct extractor usage

Run examples:
```bash
python example_pipeline.py
```

## Documentation

- [PIPELINE_ASYNC_README.md](PIPELINE_ASYNC_README.md) - Complete documentation
- [PIPELINE_IMPROVEMENT_SUMMARY.md](PIPELINE_IMPROVEMENT_SUMMARY.md) - What changed
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - This file

## Common Issues

**Issue:** `ModuleNotFoundError: No module named 'pipeline_async'`
**Solution:** Run from IC_Load directory: `cd IC_Load && python example_pipeline.py`

**Issue:** Connection fails
**Solution:** Check [config.py](config.py) connection settings

**Issue:** No data extracted
**Solution:** Check base table exists in database, verify WHERE clause

## Tips

- Use `limit` parameter for testing (e.g., `limit=10`)
- Use `parallel=False` for multi-entity extraction (respects FK dependencies)
- Use `preview()` to inspect data before full extraction
- Check `get_row_count()` to know total rows before extracting
- Use `filter_clause` to extract subsets of data
- Use `ChainModel` for production (MemoryModel + SqlModel)

## Performance

| Entities | Rows Each | Time (Sequential) | Time (Parallel) |
|----------|-----------|-------------------|-----------------|
| 1 | 1000 | ~2s | N/A |
| 3 | 1000 | ~6s | ~3s* |
| 7 | 1000 | ~14s | ~5s* |

*Parallel extraction may violate FK constraints - use with caution

## Architecture Summary

```
properties.py → entity_config.py → GenericExtractor → ExtractionTask → Model
    (WHAT)          (HOW)              (EXECUTE)         (COORDINATE)   (STORE)
```

**One extractor, infinite entities. That's the power of configuration-driven architecture.** 🚀
