---
name: sql-schema-discovery
description: Discover and introspect SQL Server database schemas dynamically. Use this skill when you need to understand table structures, column types, primary/foreign key relationships, or when adding new entities to the extraction pipeline.
---

# SQL Schema Discovery

## Overview

This skill provides automated database schema introspection for SQL Server databases. It queries `information_schema` system views to discover tables, columns, data types, constraints, and relationships. Essential for building dynamic data extraction pipelines where new entities can be added programmatically.

## When to Use This Skill

Use this skill when you need to:
- **Discover available tables** in a SQL Server database
- **Understand column structures** (names, types, nullability)
- **Identify relationships** between tables (foreign keys)
- **Add new entities** to the extraction pipeline dynamically
- **Generate dataclasses** based on actual database schema
- **Validate SQL queries** against existing schema
- **Map SQL types to Python types** for dataclass generation

## Core Capabilities

### 1. Table Discovery

Discover all tables in a database or specific schema:

```python
from scripts.schema_discovery import SchemaDiscovery

# Initialize with connection string
discovery = SchemaDiscovery(connection_string="your_connection_string")

# Discover all tables
tables = discovery.discover_tables(database_name="CRMICALPS")

# Output: List of TableMetadata objects
for table in tables:
    print(f"Table: {table.catalog}.{table.schema}.{table.name}")
```

### 2. Column Discovery

Get detailed column information for any table:

```python
# Discover columns for a specific table
columns = discovery.discover_columns(table_name="Company")

# Output: List of ColumnMetadata objects
for col in columns:
    print(f"{col.name}: {col.data_type} {'NULL' if col.is_nullable else 'NOT NULL'}")
```

### 3. Relationship Discovery

Identify foreign key relationships:

```python
# Discover foreign key relationships for a table
relationships = discovery.discover_relationships(table_name="Cases")

# Output: List of ForeignKeyRelationship objects
for fk in relationships:
    print(f"{fk.column_name} -> {fk.referenced_table}.{fk.referenced_column}")
```

### 4. Complete Database Inspection

Get comprehensive database schema information:

```python
# Inspect entire database
schema = discovery.inspect_database(db_name="CRMICALPS")

# Returns DatabaseSchema object with:
# - All tables
# - All columns per table
# - All relationships
# - SQL type to Python type mappings

print(f"Database: {schema.name}")
print(f"Total tables: {len(schema.tables)}")
for table_name, table_info in schema.tables.items():
    print(f"\nTable: {table_name}")
    print(f"Columns: {len(table_info.columns)}")
    print(f"Foreign Keys: {len(table_info.foreign_keys)}")
```

### 5. SQL Type to Python Type Mapping

Automatically map SQL Server types to Python types:

```python
# Map SQL types to Python types
python_type = discovery.sql_type_to_python_type("nvarchar")  # -> str
python_type = discovery.sql_type_to_python_type("int")       # -> int
python_type = discovery.sql_type_to_python_type("datetime")  # -> datetime
python_type = discovery.sql_type_to_python_type("bit")       # -> bool
```

## Workflow: Adding a New Entity

### Step 1: Discover Schema

```python
discovery = SchemaDiscovery(connection_string)

# Discover the new entity table
address_columns = discovery.discover_columns("Address")
address_fks = discovery.discover_relationships("Address")
```

### Step 2: Identify Properties of Interest

Filter columns based on business requirements (typically from properties.py):

```python
# Define properties of interest
address_properties = [
    "Addr_AddressId",
    "Addr_CompanyId",  # FK to Company
    "Addr_Street1",
    "Addr_City",
    "Addr_State",
    "Addr_PostCode"
]

# Filter discovered columns
relevant_columns = [
    col for col in address_columns
    if col.name in address_properties
]
```

### Step 3: Generate Query with JOINs

Use discovered FK relationships to construct JOINs:

```python
# Build query based on discovered schema
query_builder = discovery.build_query(
    base_table="Address",
    columns=address_properties,
    include_joins=True  # Automatically adds JOINs based on FKs
)

# Generated query includes JOINs to Company table
print(query_builder.to_sql())
```

### Step 4: Generate Dataclass

Use the dataclass-generator skill with discovered schema:

```python
from dataclass_generator import generate_dataclass_from_schema

# Generate dataclass code
dataclass_code = generate_dataclass_from_schema(
    class_name="Address",
    columns=relevant_columns,
    include_relationships=True
)

# Save or execute the generated dataclass
print(dataclass_code)
```

## Key Functions Reference

### `discover_tables(database_name: str) -> List[TableMetadata]`

Discovers all tables in the specified database.

**Parameters:**
- `database_name`: Name of the database to inspect

**Returns:** List of `TableMetadata` objects containing:
- `catalog`: Database catalog name
- `schema`: Schema name (usually 'dbo')
- `name`: Table name
- `type`: Table type (BASE TABLE, VIEW, etc.)

**Example:**
```python
tables = discovery.discover_tables("CRMICALPS")
case_tables = [t for t in tables if "case" in t.name.lower()]
```

---

### `discover_columns(table_name: str) -> List[ColumnMetadata]`

Discovers all columns for a specific table.

**Parameters:**
- `table_name`: Name of the table to inspect

**Returns:** List of `ColumnMetadata` objects containing:
- `name`: Column name
- `data_type`: SQL data type
- `python_type`: Equivalent Python type
- `is_nullable`: Whether column accepts NULL
- `max_length`: Maximum character length (for string types)
- `ordinal_position`: Column order in table

**Example:**
```python
columns = discovery.discover_columns("Company")
nullable_cols = [c.name for c in columns if c.is_nullable]
```

---

### `discover_relationships(table_name: str) -> List[ForeignKeyRelationship]`

Discovers foreign key relationships for a table.

**Parameters:**
- `table_name`: Name of the table to inspect

**Returns:** List of `ForeignKeyRelationship` objects containing:
- `column_name`: Foreign key column name
- `referenced_table`: Target table name
- `referenced_column`: Target column name
- `constraint_name`: FK constraint name
- `cardinality`: Relationship cardinality (many:1, etc.)

**Example:**
```python
fks = discovery.discover_relationships("Cases")
company_fk = next(fk for fk in fks if fk.referenced_table == "Company")
```

---

### `inspect_database(db_name: str) -> DatabaseSchema`

Performs complete database inspection.

**Parameters:**
- `db_name`: Name of the database to inspect

**Returns:** `DatabaseSchema` object containing:
- `name`: Database name
- `tables`: Dictionary of all tables with their metadata
- `relationships`: All foreign key relationships
- `type_mappings`: SQL to Python type mappings

**Example:**
```python
schema = discovery.inspect_database("CRMICALPS")
for table_name in schema.tables:
    print(f"Table: {table_name}")
```

---

### `sql_type_to_python_type(sql_type: str) -> type`

Maps SQL Server data types to Python types.

**Parameters:**
- `sql_type`: SQL Server data type name

**Returns:** Python type object

**Mapping Table:**
| SQL Type | Python Type |
|----------|-------------|
| int, bigint, smallint | int |
| decimal, numeric, money | Decimal |
| float, real | float |
| bit | bool |
| varchar, nvarchar, text | str |
| datetime, datetime2, date | datetime |
| uniqueidentifier | str (UUID) |

**Example:**
```python
python_type = discovery.sql_type_to_python_type("nvarchar")
# Returns: <class 'str'>
```

## Integration with Other Skills

### With dataclass-generator

```python
# 1. Discover schema
columns = discovery.discover_columns("Cases")

# 2. Generate dataclass
from dataclass_generator import generate_dataclass
dataclass_code = generate_dataclass(
    class_name="Case",
    columns=columns
)
```

### With entity extractors

```python
# 1. Discover schema for validation
schema = discovery.inspect_database("CRMICALPS")

# 2. Validate extractor query
from company_extractor import CompanyExtractor
extractor = CompanyExtractor(schema=schema)
extractor.validate_query()  # Ensures query columns exist in schema
```

### With sql-connection-manager

```python
# Share connection for schema discovery
from sql_connection_manager import ConnectionManager

conn_manager = ConnectionManager(connection_string)
discovery = SchemaDiscovery(connection_manager=conn_manager)

# Use same connection pool
tables = discovery.discover_tables("CRMICALPS")
```

## Best Practices

1. **Cache Schema Metadata**: Schema discovery can be slow; cache results for the session
2. **Validate Before Extraction**: Always discover schema before building extraction queries
3. **Handle Schema Changes**: Re-run discovery if database schema changes
4. **Use Type Mappings**: Leverage SQL-to-Python type mappings for dataclass generation
5. **Document Relationships**: Store FK relationships for JOIN generation
6. **Filter Early**: Use properties files to filter columns of interest early
7. **Error Handling**: Wrap discovery calls in try-except for connection failures

## Common Use Cases

### Use Case 1: Add New Entity (Address)

```python
# Step 1: Discover
address_cols = discovery.discover_columns("Address")
address_fks = discovery.discover_relationships("Address")

# Step 2: Filter properties
properties = ["Addr_AddressId", "Addr_CompanyId", "Addr_Street1"]
filtered_cols = [c for c in address_cols if c.name in properties]

# Step 3: Generate query
query = f"""
SELECT
    {', '.join([c.name for c in filtered_cols])},
    comp.Comp_Name AS Company_Name
FROM Address addr
LEFT JOIN Company comp ON addr.Addr_CompanyId = comp.Comp_CompanyId
"""

# Step 4: Generate dataclass using dataclass-generator skill
```

### Use Case 2: Validate Existing Queries

```python
# Discover schema
schema = discovery.inspect_database("CRMICALPS")

# Parse existing query
existing_query = "SELECT Case_CaseId, Case_InvalidColumn FROM Cases"

# Validate columns exist
query_parser = QueryValidator(schema)
validation_result = query_parser.validate(existing_query)

if not validation_result.is_valid:
    print(f"Invalid columns: {validation_result.invalid_columns}")
```

### Use Case 3: Generate Documentation

```python
# Generate markdown documentation for all entities
schema = discovery.inspect_database("CRMICALPS")

for table_name, table_info in schema.tables.items():
    print(f"## {table_name}")
    print(f"| Column | Type | Nullable |")
    print(f"|--------|------|----------|")
    for col in table_info.columns:
        print(f"| {col.name} | {col.data_type} | {col.is_nullable} |")
```

## Error Handling

```python
from scripts.schema_discovery import SchemaDiscovery, SchemaDiscoveryError

try:
    discovery = SchemaDiscovery(connection_string)
    tables = discovery.discover_tables("CRMICALPS")
except SchemaDiscoveryError as e:
    print(f"Schema discovery failed: {e}")
    # Handle connection errors, invalid database names, etc.
```

## Resources

See the `scripts/` directory for:
- `schema_discovery.py` - Main SchemaDiscovery class implementation
- `query_builder.py` - SQL query builder using discovered schema
- `type_mappings.py` - SQL to Python type mapping utilities

See the `references/` directory for:
- `sql_server_types.md` - Complete SQL Server data type reference
- `information_schema_guide.md` - Guide to information_schema views
- `best_practices.md` - Schema discovery best practices
