#!/usr/bin/env python3
"""
Python Dataclass Generator Module

Generates Python dataclasses dynamically from SQL queries and database schemas.
Creates type-safe Python classes representing SQL query results, including
denormalized fields from JOINs.
"""

import re
from dataclasses import dataclass
from typing import List, Dict, Optional, Any
from datetime import datetime
from decimal import Decimal


class DataclassGeneratorError(Exception):
    """Exception raised for dataclass generation errors"""
    pass


class DataclassGenerator:
    """
    Generate Python dataclasses from SQL queries and schema metadata.

    Usage:
        generator = DataclassGenerator()
        code = generator.generate_from_query(query, "Case", discovery)
        generator.save_to_file(code, "models/case.py")
    """

    # SQL to Python type mapping
    TYPE_IMPORTS = {
        datetime: "from datetime import datetime",
        Decimal: "from decimal import Decimal",
    }

    def __init__(self):
        self.generated_classes = {}

    def generate_from_query(
        self,
        query: str,
        class_name: str,
        schema_discovery: Any,
        include_docstring: bool = True,
        include_validation: bool = False
    ) -> str:
        """
        Generate dataclass from SQL query string.

        Args:
            query: SQL query string (may include JOINs)
            class_name: Name for the generated dataclass
            schema_discovery: SchemaDiscovery instance for type inference
            include_docstring: Add docstring with relationships
            include_validation: Add __post_init__ validation

        Returns:
            Python code as string

        Example:
            >>> code = generator.generate_from_query(
            ...     query="SELECT Comp_CompanyId, Comp_Name FROM Company",
            ...     class_name="Company",
            ...     schema_discovery=discovery
            ... )
        """
        # Parse column names from query (including aliases)
        column_names = self.parse_query_columns(query)

        # Infer types from schema discovery
        # For complex queries with JOINs, we need to analyze each column
        columns_metadata = []

        for col_name in column_names:
            # Try to infer type from schema
            python_type = self._infer_type_from_query(
                col_name, query, schema_discovery
            )

            # Create synthetic ColumnMetadata
            from scripts.schema_discovery import ColumnMetadata
            col_meta = ColumnMetadata(
                name=col_name,
                data_type="inferred",
                python_type=python_type,
                is_nullable=True  # Conservative assumption for JOINs
            )
            columns_metadata.append(col_meta)

        # Generate dataclass from columns
        return self.generate_from_columns(
            class_name=class_name,
            columns=columns_metadata,
            include_docstring=include_docstring,
            include_validation=include_validation
        )

    def generate_from_columns(
        self,
        class_name: str,
        columns: List[Any],  # List[ColumnMetadata]
        relationships: Optional[List[Any]] = None,
        include_docstring: bool = True,
        include_validation: bool = False,
        custom_type_mappings: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Generate dataclass from ColumnMetadata list.

        Args:
            class_name: Name for the generated dataclass
            columns: List of ColumnMetadata objects
            relationships: Optional list of ForeignKeyRelationship objects
            include_docstring: Add docstring
            include_validation: Add __post_init__ validation
            custom_type_mappings: Override default type mappings

        Returns:
            Python code as string
        """
        # Build imports
        imports = self._build_imports(columns)

        # Build class definition
        class_def = f"@dataclass\nclass {class_name}:\n"

        # Add docstring
        if include_docstring:
            docstring = self._build_docstring(class_name, relationships)
            class_def += f'    """{docstring}"""\n'

        # Add fields
        fields = self._build_fields(columns, custom_type_mappings)
        class_def += fields

        # Add validation method if requested
        if include_validation:
            validation = self._build_validation(columns)
            class_def += f"\n{validation}"

        # Combine imports and class
        code = imports + "\n\n" + class_def

        # Store generated class
        self.generated_classes[class_name] = code

        return code

    def parse_query_columns(self, query: str) -> List[str]:
        """
        Extract column names from SQL query (including aliases).

        Args:
            query: SQL query string

        Returns:
            List of column names (with aliases resolved)

        Example:
            >>> query = "SELECT Comp_Name AS Company_Name FROM Company"
            >>> columns = generator.parse_query_columns(query)
            >>> # Returns: ["Company_Name"]
        """
        # Remove comments and normalize whitespace
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
        query = ' '.join(query.split())

        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            raise DataclassGeneratorError("Could not parse SELECT clause from query")

        select_clause = select_match.group(1)

        # Split by comma (respecting nested parentheses)
        columns = []
        depth = 0
        current_col = ""

        for char in select_clause:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif char == ',' and depth == 0:
                columns.append(current_col.strip())
                current_col = ""
                continue
            current_col += char

        if current_col.strip():
            columns.append(current_col.strip())

        # Extract column names (handle aliases)
        column_names = []
        for col in columns:
            # Check for AS alias
            as_match = re.search(r'\s+AS\s+(\w+)', col, re.IGNORECASE)
            if as_match:
                column_names.append(as_match.group(1))
            else:
                # Extract last part (table.column -> column)
                parts = col.split('.')
                column_name = parts[-1].strip()
                # Remove any remaining brackets
                column_name = re.sub(r'[\[\]]', '', column_name)
                column_names.append(column_name)

        return column_names

    def sql_to_python_field_name(self, sql_column_name: str) -> str:
        """
        Convert SQL column name to Python-friendly field name.

        Args:
            sql_column_name: SQL column name (e.g., "Case_CaseId")

        Returns:
            Python field name (e.g., "case_id")

        Conversion Rules:
        - Removes table prefix (e.g., "Case_" → "")
        - Converts to snake_case
        - Handles aliases (e.g., "Company_Name" → "company_name")
        """
        # Remove brackets
        name = re.sub(r'[\[\]]', '', sql_column_name)

        # Handle common prefixes (Case_, Comp_, Pers_, etc.)
        # Remove table prefix if it's a duplicate
        # E.g., "Case_CaseId" -> "CaseId" -> "case_id"
        parts = name.split('_')
        if len(parts) >= 2:
            # Check if first part is a table prefix
            prefix = parts[0]
            rest = '_'.join(parts[1:])

            # If rest starts with same prefix, remove it
            if rest.startswith(prefix):
                name = rest
            else:
                # Keep full name but convert to snake_case
                name = name

        # Convert to snake_case
        # Insert underscore before capital letters
        name = re.sub('([a-z])([A-Z])', r'\1_\2', name)

        # Convert to lowercase
        name = name.lower()

        return name

    def save_to_file(self, code: str, file_path: str):
        """
        Save generated dataclass code to a Python file.

        Args:
            code: Generated Python code
            file_path: Output file path
        """
        with open(file_path, 'w') as f:
            f.write(code)

    def _build_imports(self, columns: List[Any]) -> str:
        """Build import statements based on column types"""
        imports = ["from dataclasses import dataclass"]

        # Check if Optional is needed
        has_nullable = any(col.is_nullable for col in columns)
        if has_nullable:
            imports.append("from typing import Optional")

        # Check for special types
        types_needed = set()
        for col in columns:
            if col.python_type in self.TYPE_IMPORTS:
                types_needed.add(col.python_type)

        # Add type-specific imports
        for type_class in types_needed:
            imports.append(self.TYPE_IMPORTS[type_class])

        return '\n'.join(imports)

    def _build_docstring(self, class_name: str, relationships: Optional[List[Any]]) -> str:
        """Build docstring with relationship information"""
        docstring = f"{class_name} entity"

        if relationships:
            docstring += " with relationships:\n\n    Relationships:\n"
            for rel in relationships:
                docstring += f"    - {rel.referenced_table} ({rel.cardinality} via {rel.column_name})\n"

        return docstring

    def _build_fields(self, columns: List[Any], custom_mappings: Optional[Dict[str, str]]) -> str:
        """Build field definitions"""
        fields = []

        for col in columns:
            # Convert column name to Python field name
            field_name = self.sql_to_python_field_name(col.name)

            # Determine type
            if custom_mappings and col.name in custom_mappings:
                type_str = custom_mappings[col.name]
            else:
                type_name = col.python_type.__name__
                if col.is_nullable:
                    type_str = f"Optional[{type_name}]"
                else:
                    type_str = type_name

            # Build field line
            field_line = f"    {field_name}: {type_str}"

            # Add default value for Optional fields
            if col.is_nullable:
                field_line += " = None"

            fields.append(field_line)

        return '\n'.join(fields) + '\n'

    def _build_validation(self, columns: List[Any]) -> str:
        """Build __post_init__ validation method"""
        validation = "    def __post_init__(self):\n"
        validation += "        # Add custom validation here\n"
        validation += "        pass\n"
        return validation

    def _infer_type_from_query(self, column_name: str, query: str, schema_discovery: Any) -> type:
        """
        Infer Python type for a column based on query context.

        For columns from JOINs, try to find the source table and column.
        """
        # Default type
        default_type = str

        # Try to find the column in the query context
        # This is a simplified version; a full parser would be more robust

        # Check common type patterns in column names
        if any(keyword in column_name.lower() for keyword in ['id', '_id']):
            return int
        elif any(keyword in column_name.lower() for keyword in ['date', 'time', 'opened', 'closed']):
            return datetime
        elif any(keyword in column_name.lower() for keyword in ['amount', 'cost', 'price']):
            return Decimal
        else:
            return default_type


# Example usage
if __name__ == "__main__":
    from scripts.schema_discovery import SchemaDiscovery

    # Example: Generate Case dataclass

    case_query = """
    SELECT
        c.Case_CaseId,
        c.Case_PrimaryCompanyId,
        comp.Comp_Name AS Company_Name,
        comp.Comp_WebSite AS Company_WebSite,
        c.Case_PrimaryPersonId,
        p.Pers_FirstName AS Person_FirstName,
        p.Pers_LastName AS Person_LastName,
        c.Case_Status,
        c.Case_Stage,
        c.Case_Priority,
        c.Case_Opened,
        c.Case_Closed
    FROM Cases c
    LEFT JOIN Company comp ON c.Case_PrimaryCompanyId = comp.Comp_CompanyId
    LEFT JOIN Person p ON c.Case_PrimaryPersonId = p.Pers_PersonId
    """

    connection_string = "your_connection_string"
    discovery = SchemaDiscovery(connection_string)

    generator = DataclassGenerator()

    # Generate dataclass
    dataclass_code = generator.generate_from_query(
        query=case_query,
        class_name="Case",
        schema_discovery=discovery,
        include_docstring=True
    )

    print("=== GENERATED DATACLASS ===")
    print(dataclass_code)

    # Save to file
    # generator.save_to_file(dataclass_code, "models/case.py")
