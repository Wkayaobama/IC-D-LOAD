---
name: duckdb-transformer
description: Transform Bronze layer data using DuckDB for joins, aggregations, and business logic. Use this skill for high-performance data transformations in the pipeline.
---

# DuckDB Transformer

## Overview

Provides high-performance data transformation using DuckDB in-memory database. Processes Bronze layer CSV files and dataframes through SQL transformations, joins, aggregations, and business logic application.

## When to Use This Skill

- **Transform Bronze layer data** (raw extracts)
- **Apply SQL joins and aggregations** for analytics
- **Execute business logic** via SQL transformations
- **High-performance processing** of large datasets
- **Generate Silver/Gold layer** datasets

## Core Capabilities

### 1. Load and Transform

```python
from scripts.duckdb_processor import DuckDBProcessor

processor = DuckDBProcessor()

# Load Bronze CSV
processor.load_csv("Bronze_Cases.csv", table_name="cases")
processor.load_csv("Bronze_Company.csv", table_name="companies")

# Execute transformation
result = processor.execute("""
    SELECT
        c.*,
        comp.Comp_Name,
        COUNT(*) OVER (PARTITION BY c.Company_Id) as total_cases
    FROM cases c
    LEFT JOIN companies comp ON c.company_id = comp.company_id
""")

# Get as DataFrame
df = result.df()
```

### 2. Business Logic Application

```python
# Apply computed columns
processor.execute("""
    CREATE TABLE silver_cases AS
    SELECT
        *,
        CASE
            WHEN priority = 'High' THEN 1
            WHEN priority = 'Medium' THEN 2
            ELSE 3
        END as priority_score
    FROM cases
""")
```

## Resources

See `scripts/duckdb_processor.py` for implementation.
