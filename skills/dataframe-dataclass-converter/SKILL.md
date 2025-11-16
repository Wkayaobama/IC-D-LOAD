---
name: dataframe-dataclass-converter
description: Bidirectional conversion between pandas DataFrames and Python dataclasses. Use this skill for type-safe conversion between SQL results, DataFrames, and dataclass instances.
---

# DataFrame Dataclass Converter

## Overview

Provides bidirectional, type-safe conversion between pandas DataFrames and Python dataclasses. Essential utility for the extraction pipeline to convert SQL query results (DataFrames) into dataclass instances and vice versa.

## When to Use This Skill

- **Convert SQL ResultSet → DataFrame → List[Dataclass]**
- **Convert List[Dataclass] → DataFrame** (for DuckDB processing)
- **Type-safe data transformation** between representations
- **Automated field mapping** between DataFrame columns and dataclass fields

## Core Capabilities

### 1. DataFrame to Dataclasses

```python
from scripts.dataframe_converter import DataFrameConverter
import pandas as pd

# SQL query result as DataFrame
df = pd.read_sql("SELECT * FROM Cases", conn)

# Convert to dataclass instances
converter = DataFrameConverter()
cases = converter.dataframe_to_dataclasses(df, Case)

# Returns: List[Case]
```

### 2. Dataclasses to DataFrame

```python
# Convert dataclass instances back to DataFrame
df = converter.dataclasses_to_dataframe(cases)

# Use with DuckDB
import duckdb
conn = duckdb.connect()
conn.register("cases_df", df)
result = conn.execute("SELECT * FROM cases_df WHERE status = 'Open'").df()
```

### 3. Column Name Mapping

```python
# Automatic mapping between SQL naming and Python naming
# Case_CaseId (SQL) ← → case_id (Python dataclass)
# Company_Name (SQL) ← → company_name (Python dataclass)

converter = DataFrameConverter(auto_map_names=True)
```

## Resources

See `scripts/dataframe_converter.py` for implementation.
