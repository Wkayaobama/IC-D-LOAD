#!/usr/bin/env python3
"""
DataFrame Dataclass Converter

Bidirectional conversion between pandas DataFrames and Python dataclasses.
"""

from dataclasses import fields, is_dataclass
from typing import List, Type, TypeVar, Any
import pandas as pd
import re

T = TypeVar('T')


class DataFrameConverter:
    """
    Convert between DataFrames and dataclasses.

    Usage:
        converter = DataFrameConverter()
        cases = converter.dataframe_to_dataclasses(df, Case)
        df = converter.dataclasses_to_dataframe(cases)
    """

    def __init__(self, auto_map_names: bool = True):
        self.auto_map_names = auto_map_names

    def dataframe_to_dataclasses(self, df: pd.DataFrame, dataclass_type: Type[T]) -> List[T]:
        """
        Convert DataFrame to list of dataclass instances.

        Args:
            df: pandas DataFrame
            dataclass_type: Dataclass type to convert to

        Returns:
            List of dataclass instances
        """
        if not is_dataclass(dataclass_type):
            raise ValueError(f"{dataclass_type} is not a dataclass")

        instances = []
        for _, row in df.iterrows():
            # Build kwargs for dataclass constructor
            kwargs = {}
            for field in fields(dataclass_type):
                # Try to find matching column
                col_name = self._find_matching_column(field.name, df.columns)
                if col_name is not None:
                    value = row[col_name]
                    # Handle NaN/None
                    if pd.isna(value):
                        value = None
                    kwargs[field.name] = value
                else:
                    # Field not in DataFrame, use default if available
                    kwargs[field.name] = field.default if field.default is not None else None

            instance = dataclass_type(**kwargs)
            instances.append(instance)

        return instances

    def dataclasses_to_dataframe(self, instances: List[Any]) -> pd.DataFrame:
        """
        Convert list of dataclass instances to DataFrame.

        Args:
            instances: List of dataclass instances

        Returns:
            pandas DataFrame
        """
        if not instances:
            return pd.DataFrame()

        if not is_dataclass(instances[0]):
            raise ValueError("Input must be a list of dataclass instances")

        # Convert to list of dicts
        data = []
        for instance in instances:
            row_dict = {}
            for field in fields(instance):
                value = getattr(instance, field.name)
                # Convert field name back to SQL-style if needed
                col_name = self._python_to_sql_name(field.name)
                row_dict[col_name] = value
            data.append(row_dict)

        return pd.DataFrame(data)

    def _find_matching_column(self, field_name: str, columns: pd.Index) -> str:
        """Find matching DataFrame column for dataclass field"""
        # Try exact match first
        if field_name in columns:
            return field_name

        # Try SQL-style name
        sql_name = self._python_to_sql_name(field_name)
        if sql_name in columns:
            return sql_name

        # Try case-insensitive match
        for col in columns:
            if col.lower() == field_name.lower():
                return col

        # Try with underscores removed
        field_clean = field_name.replace('_', '').lower()
        for col in columns:
            col_clean = col.replace('_', '').lower()
            if col_clean == field_clean:
                return col

        return None

    def _python_to_sql_name(self, python_name: str) -> str:
        """Convert Python field name back to SQL column name"""
        # Simple heuristic: case_id -> Case_CaseId
        # This is a best-effort conversion
        parts = python_name.split('_')
        if len(parts) >= 2:
            # Capitalize each part
            return '_'.join([part.capitalize() for part in parts])
        return python_name.capitalize()


if __name__ == "__main__":
    from dataclasses import dataclass
    from datetime import datetime

    @dataclass
    class TestCase:
        case_id: int
        status: str
        opened: datetime

    # Test conversion
    df = pd.DataFrame({
        'Case_CaseId': [1, 2, 3],
        'Case_Status': ['Open', 'Closed', 'Pending'],
        'Case_Opened': [datetime.now(), datetime.now(), datetime.now()]
    })

    converter = DataFrameConverter()
    cases = converter.dataframe_to_dataclasses(df, TestCase)
    print(f"Converted {len(cases)} cases")

    df_back = converter.dataclasses_to_dataframe(cases)
    print(df_back)
