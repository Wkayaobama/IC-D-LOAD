---
name: dataclass-generator
description: Generate Python dataclasses dynamically from SQL queries and database schemas. Use this skill when you need to create type-safe Python classes representing SQL query results, including denormalized fields from JOINs.
---

# Dataclass Generator

## Overview

This skill generates Python dataclasses dynamically from SQL query results and database schema metadata. It creates type-safe Python classes that represent SQL query output, including denormalized fields from JOINs. Essential for maintaining consistency between SQL queries and Python object models in the extraction pipeline.

## When to Use This Skill

Use this skill when you need to:
- **Generate dataclasses from SQL queries** (including JOINs)
- **Create type-safe Python objects** representing database entities
- **Add new entities** to the pipeline with auto-generated classes
- **Ensure consistency** between SQL schema and Python types
- **Handle denormalized data** from complex queries with multiple joins
- **Convert SQL types to Python types** automatically
- **Generate dataclass code** that can be saved to files

## Core Principle

**The dataclass structure is determined by the SQL query result columns, NOT just the base table schema.**

If your query has JOINs, the dataclass will include denormalized fields from joined tables.

### Example:

**SQL Query:**
```sql
SELECT
    c.Case_CaseId,
    c.Case_Status,
    comp.Comp_Name AS Company_Name,  -- ← From JOIN
    p.Pers_FirstName AS Person_FirstName  -- ← From JOIN
FROM Cases c
LEFT JOIN Company comp ON c.Case_PrimaryCompanyId = comp.Comp_CompanyId
LEFT JOIN Person p ON c.Case_PrimaryPersonId = p.Pers_PersonId
```

**Generated Dataclass:**
```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class Case:
    case_id: int  # From Cases table
    status: Optional[str]  # From Cases table
    company_name: Optional[str]  # From JOIN with Company
    person_first_name: Optional[str]  # From JOIN with Person
```

## Core Capabilities

### 1. Generate from SQL Query

Generate a dataclass directly from a SQL query string:

```python
from scripts.dataclass_generator import DataclassGenerator

generator = DataclassGenerator()

query = """
SELECT
    Addr_AddressId,
    Addr_CompanyId,
    Addr_Street1,
    comp.Comp_Name AS Company_Name
FROM Address
LEFT JOIN Company comp ON Addr_CompanyId = comp.Comp_CompanyId
"""

# Generate dataclass code
dataclass_code = generator.generate_from_query(
    query=query,
    class_name="Address",
    schema_discovery=discovery  # From sql-schema-discovery skill
)

print(dataclass_code)
# Outputs complete Python dataclass code
```

### 2. Generate from ColumnMetadata

Generate from discovered schema metadata:

```python
from scripts.schema_discovery import SchemaDiscovery

# Discover columns
discovery = SchemaDiscovery(connection_string)
columns = discovery.discover_columns("Company")

# Generate dataclass
dataclass_code = generator.generate_from_columns(
    class_name="Company",
    columns=columns
)

print(dataclass_code)
```

### 3. Generate with Custom Properties

Filter columns based on properties of interest:

```python
# Define properties of interest
properties = [
    "Pers_PersonId",
    "Pers_FirstName",
    "Pers_LastName",
    "Pers_Salutation",
    "Pers_Gender",
    "Pers_Title"
]

# Discover all columns
all_columns = discovery.discover_columns("Person")

# Filter to properties of interest
filtered_columns = [
    col for col in all_columns
    if col.name in properties
]

# Generate dataclass with only relevant properties
dataclass_code = generator.generate_from_columns(
    class_name="Contact",
    columns=filtered_columns
)
```

### 4. Generate with Relationships

Include relationship metadata in generated dataclass:

```python
# Discover relationships
relationships = discovery.discover_relationships("Cases")

# Generate dataclass with relationship documentation
dataclass_code = generator.generate_from_columns(
    class_name="Case",
    columns=columns,
    relationships=relationships,
    include_docstring=True
)

# Output includes relationship documentation in docstring
```

### 5. SQL Type to Python Type Conversion

Automatic type conversion with proper imports:

```python
# The generator automatically handles:
# - int, bigint → int
# - varchar, nvarchar → str
# - datetime, datetime2 → datetime
# - bit → bool
# - decimal, money → Decimal
# - NULL columns → Optional[T]

# And adds necessary imports:
# from typing import Optional
# from datetime import datetime
# from decimal import Decimal
```

## Workflow: Complete Entity Generation

### Step 1: Define SQL Query with JOINs

```python
case_query = """
SELECT
    c.Case_CaseId,
    c.Case_PrimaryCompanyId,
    comp.Comp_Name AS Company_Name,
    comp.Comp_WebSite AS Company_WebSite,
    c.Case_PrimaryPersonId,
    p.Pers_FirstName AS Person_FirstName,
    p.Pers_LastName AS Person_LastName,
    v.Emai_EmailAddress AS Person_EmailAddress,
    c.Case_Status,
    c.Case_Stage,
    c.Case_Priority,
    c.Case_Opened,
    c.Case_Closed
FROM Cases c
LEFT JOIN Company comp ON c.Case_PrimaryCompanyId = comp.Comp_CompanyId
LEFT JOIN Person p ON c.Case_PrimaryPersonId = p.Pers_PersonId
LEFT JOIN vEmailCompanyAndPerson v ON c.Case_PrimaryPersonId = v.Pers_PersonId
"""
```

### Step 2: Generate Dataclass from Query

```python
generator = DataclassGenerator()

# Generate with schema discovery for type inference
dataclass_code = generator.generate_from_query(
    query=case_query,
    class_name="Case",
    schema_discovery=discovery,
    include_docstring=True,
    include_validation=False  # Minimal validation via type hints
)
```

### Step 3: Generated Output

```python
# Generated code:
from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class Case:
    """
    Case entity with denormalized Company and Person fields.

    Relationships:
    - Company (many:1 via Case_PrimaryCompanyId)
    - Person (many:1 via Case_PrimaryPersonId)
    """
    # Core Case fields
    case_id: int
    primary_company_id: Optional[int]
    primary_person_id: Optional[int]
    status: Optional[str]
    stage: Optional[str]
    priority: Optional[str]
    opened: Optional[datetime]
    closed: Optional[datetime]

    # Denormalized Company fields (from JOIN)
    company_name: Optional[str]
    company_website: Optional[str]

    # Denormalized Person fields (from JOIN)
    person_first_name: Optional[str]
    person_last_name: Optional[str]
    person_email: Optional[str]
```

### Step 4: Save to File

```python
# Save generated dataclass
output_path = "models/case.py"
generator.save_to_file(dataclass_code, output_path)

# Or execute dynamically
exec(dataclass_code)
# Now Case class is available in current scope
```

## Key Functions Reference

### `generate_from_query(query: str, class_name: str, ...) -> str`

Generate dataclass from SQL query string.

**Parameters:**
- `query`: SQL query string (may include JOINs)
- `class_name`: Name for the generated dataclass
- `schema_discovery`: SchemaDiscovery instance for type inference
- `include_docstring`: Add docstring with relationships (default: True)
- `include_validation`: Add __post_init__ validation (default: False)

**Returns:** Python code as string

**Example:**
```python
code = generator.generate_from_query(
    query="SELECT Comp_CompanyId, Comp_Name FROM Company",
    class_name="Company",
    schema_discovery=discovery
)
```

---

### `generate_from_columns(class_name: str, columns: List[ColumnMetadata], ...) -> str`

Generate dataclass from ColumnMetadata list.

**Parameters:**
- `class_name`: Name for the generated dataclass
- `columns`: List of ColumnMetadata objects
- `relationships`: Optional list of ForeignKeyRelationship objects
- `include_docstring`: Add docstring (default: True)

**Returns:** Python code as string

**Example:**
```python
columns = discovery.discover_columns("Person")
code = generator.generate_from_columns("Contact", columns)
```

---

### `parse_query_columns(query: str) -> List[str]`

Extract column names from SQL query (including aliases).

**Parameters:**
- `query`: SQL query string

**Returns:** List of column names (with aliases resolved)

**Example:**
```python
query = "SELECT Comp_Name AS Company_Name FROM Company"
columns = generator.parse_query_columns(query)
# Returns: ["Company_Name"]
```

---

### `sql_to_python_field_name(sql_column_name: str) -> str`

Convert SQL column name to Python-friendly field name.

**Parameters:**
- `sql_column_name`: SQL column name (e.g., "Case_CaseId")

**Returns:** Python field name (e.g., "case_id")

**Conversion Rules:**
- Removes table prefix (e.g., "Case_" → "")
- Converts to snake_case
- Handles aliases (e.g., "Company_Name" → "company_name")

**Example:**
```python
field_name = generator.sql_to_python_field_name("Case_PrimaryCompanyId")
# Returns: "primary_company_id"
```

---

### `save_to_file(code: str, file_path: str)`

Save generated dataclass code to a Python file.

**Parameters:**
- `code`: Generated Python code
- `file_path`: Output file path

**Example:**
```python
generator.save_to_file(dataclass_code, "models/case.py")
```

## Integration with Other Skills

### With sql-schema-discovery

```python
# 1. Discover schema
discovery = SchemaDiscovery(connection_string)
columns = discovery.discover_columns("Company")

# 2. Generate dataclass
generator = DataclassGenerator()
code = generator.generate_from_columns("Company", columns)
```

### With entity extractors

```python
# 1. Generate dataclass
code = generator.generate_from_query(query, "Case", discovery)

# 2. Save to models directory
generator.save_to_file(code, "models/case.py")

# 3. Import in extractor
from models.case import Case
from case_extractor import CaseExtractor

extractor = CaseExtractor(dataclass_type=Case)
```

### With dataframe-dataclass-converter

```python
# 1. Generate dataclass
exec(dataclass_code)  # Creates Case class

# 2. Use with converter
from dataframe_dataclass_converter import DataFrameConverter

converter = DataFrameConverter()
df = pd.DataFrame(query_results)
cases = converter.dataframe_to_dataclasses(df, Case)
```

## Column Naming Conventions

### SQL to Python Conversion

| SQL Column Name | Python Field Name |
|----------------|-------------------|
| Case_CaseId | case_id |
| Case_PrimaryCompanyId | primary_company_id |
| Comp_Name AS Company_Name | company_name |
| Pers_FirstName | first_name |
| Case_Status | status |

**Rules:**
1. Remove table prefix (e.g., "Case_", "Comp_", "Pers_")
2. Convert to snake_case
3. Handle SQL aliases directly
4. Preserve clarity (e.g., "primary_company_id" not just "company_id")

## Type Mapping Reference

| SQL Type | Python Type | Import Required |
|----------|-------------|-----------------|
| int, bigint, smallint | int | No |
| varchar, nvarchar, text | str | No |
| datetime, datetime2, date | datetime | `from datetime import datetime` |
| bit | bool | No |
| decimal, numeric, money | Decimal | `from decimal import Decimal` |
| float, real | float | No |
| uniqueidentifier | str | No |

**Nullable Columns:** Wrapped in `Optional[T]` (requires `from typing import Optional`)

## Advanced Features

### Custom Field Types

Override default type mappings:

```python
custom_mappings = {
    "Case_Status": "Literal['Open', 'Closed', 'Pending']",
    "Case_Priority": "Literal['High', 'Medium', 'Low']"
}

code = generator.generate_from_columns(
    class_name="Case",
    columns=columns,
    custom_type_mappings=custom_mappings
)
```

### Add Validation Methods

Include __post_init__ for validation:

```python
code = generator.generate_from_columns(
    class_name="Case",
    columns=columns,
    include_validation=True
)

# Generates:
# def __post_init__(self):
#     if self.case_id is not None and self.case_id < 0:
#         raise ValueError("case_id must be positive")
```

### Batch Generation

Generate multiple dataclasses at once:

```python
entities = ["Company", "Person", "Cases", "Address"]

for entity in entities:
    columns = discovery.discover_columns(entity)
    code = generator.generate_from_columns(entity, columns)
    generator.save_to_file(code, f"models/{entity.lower()}.py")
```

## Best Practices

1. **Use Schema Discovery**: Always use sql-schema-discovery for accurate type mapping
2. **Include Docstrings**: Document relationships and cardinality
3. **Filter Properties**: Use properties files to filter relevant columns
4. **Save Generated Code**: Store generated dataclasses in `models/` directory
5. **Version Control**: Commit generated dataclasses to track schema changes
6. **Naming Consistency**: Use consistent naming conventions (snake_case for Python)
7. **Minimal Validation**: Use type hints for validation; avoid complex validation logic

## Common Use Cases

### Use Case 1: New Entity (Address)

```python
# Discover schema
columns = discovery.discover_columns("Address")

# Filter properties
properties = ["Addr_AddressId", "Addr_CompanyId", "Addr_Street1", "Addr_City"]
filtered = [c for c in columns if c.name in properties]

# Generate dataclass
code = generator.generate_from_columns("Address", filtered)
generator.save_to_file(code, "models/address.py")
```

### Use Case 2: Complex Query with Multiple JOINs

```python
complex_query = """
SELECT
    o.Oppo_OpportunityId,
    o.Oppo_Status,
    comp.Comp_Name AS Company_Name,
    p.Pers_FirstName AS Contact_FirstName,
    u.User_FirstName AS Owner_FirstName
FROM Opportunity o
LEFT JOIN Company comp ON o.Oppo_PrimaryCompanyId = comp.Comp_CompanyId
LEFT JOIN Person p ON o.Oppo_PrimaryPersonId = p.Pers_PersonId
LEFT JOIN Users u ON o.Oppo_AssignedUserId = u.User_UserId
"""

code = generator.generate_from_query(complex_query, "Opportunity", discovery)
```

### Use Case 3: Update Existing Dataclass

```python
# Regenerate dataclass after schema changes
new_columns = discovery.discover_columns("Cases")
updated_code = generator.generate_from_columns("Case", new_columns)

# Compare with existing
with open("models/case.py", "r") as f:
    existing_code = f.read()

if updated_code != existing_code:
    print("Schema has changed! Review differences:")
    # Show diff or overwrite
    generator.save_to_file(updated_code, "models/case.py")
```

## Error Handling

```python
from scripts.dataclass_generator import DataclassGeneratorError

try:
    code = generator.generate_from_query(query, "Case", discovery)
except DataclassGeneratorError as e:
    print(f"Generation failed: {e}")
    # Handle query parsing errors, invalid column names, etc.
```

## Resources

See the `scripts/` directory for:
- `dataclass_generator.py` - Main DataclassGenerator class
- `column_parser.py` - SQL query column parser
- `type_mapper.py` - SQL to Python type mapping utilities

See the `references/` directory for:
- `dataclass_patterns.md` - Common dataclass patterns and best practices
- `naming_conventions.md` - SQL to Python naming conversion rules
- `type_mapping_reference.md` - Complete type mapping documentation
