"""
Generic Entity Extractor
=========================

Reusable extractor that works with any EntityConfig to extract data from SQL Server.

This module provides a generic extraction implementation that:
- Works with EntityConfig definitions
- Reuses connection parameters
- Extracts data to pandas DataFrames
- Converts to dataclasses when needed
- Integrates with Memory/SQL/Chain models

Usage:
    from entity_config import get_entity_config
    from generic_extractor import GenericExtractor
    from config import get_connection_string

    # Create extractor
    case_config = get_entity_config('Case')
    extractor = GenericExtractor(case_config, get_connection_string())

    # Extract to DataFrame
    df = extractor.extract_to_dataframe()

    # Extract to dataclasses
    cases = extractor.extract_to_dataclasses(Case)

    # Save to Bronze layer
    extractor.save_to_bronze(df, "Bronze_Cases.csv")
"""

import sys
from pathlib import Path

# Add paths for imports
sys.path.append(str(Path(__file__).parent.parent / 'sql-connection-manager' / 'scripts'))
sys.path.append(str(Path(__file__).parent.parent / 'dataframe-dataclass-converter' / 'scripts'))

from typing import List, Optional, Type, Any
import pandas as pd
from dataclasses import is_dataclass
from loguru import logger

from .entity_config import EntityConfig


class GenericExtractor:
    """
    Generic extractor for any entity based on EntityConfig.

    This extractor:
    - Executes SQL queries built from EntityConfig
    - Returns pandas DataFrames
    - Optionally converts to dataclasses
    - Saves to Bronze layer CSV
    - Reuses connection string across multiple extractions
    """

    def __init__(self, entity_config: EntityConfig, connection_string: str):
        """
        Initialize generic extractor.

        Args:
            entity_config: EntityConfig instance defining the entity
            connection_string: SQL Server connection string (reused)
        """
        self.entity_config = entity_config
        self.connection_string = connection_string
        self._conn_manager = None

    def extract_to_dataframe(
        self,
        filter_clause: Optional[str] = None,
        include_metadata: bool = False,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract entity data to pandas DataFrame.

        Args:
            filter_clause: Additional WHERE clause filter (e.g., "AND Case_Status = 'Open'")
            include_metadata: Include metadata fields in extraction
            limit: Limit number of rows (for testing)

        Returns:
            pandas DataFrame with entity data

        Example:
            >>> df = extractor.extract_to_dataframe(
            ...     filter_clause="AND Case_Status = 'Open'",
            ...     limit=100
            ... )
        """
        import pyodbc

        # Build query
        query = self.entity_config.build_query(include_metadata=include_metadata)

        # Add additional filter if provided
        if filter_clause:
            if "WHERE" in query.upper():
                query += f" {filter_clause}"
            else:
                query += f" WHERE {filter_clause}"

        # Add limit if provided
        if limit:
            # For SQL Server, use TOP clause
            query = query.replace("SELECT", f"SELECT TOP {limit}", 1)

        logger.debug(f"Executing query for {self.entity_config.name}:")
        logger.debug(query)

        # Execute query
        try:
            conn = pyodbc.connect(self.connection_string)
            df = pd.read_sql(query, conn)
            conn.close()

            logger.info(f"Extracted {len(df)} rows for {self.entity_config.name}")
            return df

        except Exception as e:
            logger.error(f"Extraction failed for {self.entity_config.name}: {str(e)}")
            raise

    def extract_to_dataclasses(
        self,
        dataclass_type: Type,
        filter_clause: Optional[str] = None,
        include_metadata: bool = False,
        limit: Optional[int] = None
    ) -> List[Any]:
        """
        Extract entity data to list of dataclass instances.

        Args:
            dataclass_type: Dataclass type to convert to
            filter_clause: Additional WHERE clause filter
            include_metadata: Include metadata fields
            limit: Limit number of rows

        Returns:
            List of dataclass instances

        Example:
            >>> from case_extractor.scripts.case_extractor import Case
            >>> cases = extractor.extract_to_dataclasses(Case, limit=100)
        """
        if not is_dataclass(dataclass_type):
            raise ValueError(f"{dataclass_type} is not a dataclass")

        # Extract to DataFrame
        df = self.extract_to_dataframe(
            filter_clause=filter_clause,
            include_metadata=include_metadata,
            limit=limit
        )

        # Convert to dataclasses
        from dataframe_converter import DataFrameConverter
        converter = DataFrameConverter()
        instances = converter.dataframe_to_dataclasses(df, dataclass_type)

        logger.info(f"Converted {len(instances)} DataFrame rows to {dataclass_type.__name__} instances")
        return instances

    def save_to_bronze(
        self,
        data: Any,
        output_path: Optional[str] = None,
        bronze_layer_path: str = "bronze_layer"
    ) -> str:
        """
        Save data to Bronze layer CSV.

        Args:
            data: Either DataFrame or list of dataclasses
            output_path: Custom output path (overrides default)
            bronze_layer_path: Base directory for Bronze layer

        Returns:
            Path to saved CSV file

        Example:
            >>> df = extractor.extract_to_dataframe()
            >>> path = extractor.save_to_bronze(df)
            >>> # Or with dataclasses:
            >>> cases = extractor.extract_to_dataclasses(Case)
            >>> path = extractor.save_to_bronze(cases)
        """
        # Determine output path
        if output_path is None:
            output_filename = f"Bronze_{self.entity_config.name}.csv"
            output_path = Path(bronze_layer_path) / output_filename
        else:
            output_path = Path(output_path)

        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to DataFrame if needed
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, list) and len(data) > 0 and is_dataclass(data[0]):
            from dataframe_converter import DataFrameConverter
            converter = DataFrameConverter()
            df = converter.dataclasses_to_dataframe(data)
        else:
            raise ValueError("Data must be DataFrame or list of dataclasses")

        # Clean text columns - remove special characters that can't be properly encoded
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: str(x).replace('\uf02d', '-').replace('\uf0a7', '*') if pd.notna(x) else x)

        # Save to CSV with UTF-8 encoding
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved {len(df)} rows to {output_path}")

        return str(output_path)

    def extract_and_save(
        self,
        filter_clause: Optional[str] = None,
        limit: Optional[int] = None,
        bronze_layer_path: str = "bronze_layer"
    ) -> str:
        """
        Convenience method: Extract and save to Bronze layer in one call.

        Args:
            filter_clause: Additional WHERE clause filter
            limit: Limit number of rows
            bronze_layer_path: Base directory for Bronze layer

        Returns:
            Path to saved CSV file

        Example:
            >>> path = extractor.extract_and_save(limit=1000)
        """
        df = self.extract_to_dataframe(
            filter_clause=filter_clause,
            include_metadata=True,
            limit=limit
        )
        return self.save_to_bronze(df, bronze_layer_path=bronze_layer_path)

    def get_row_count(self) -> int:
        """
        Get total row count for this entity without extracting data.

        Returns:
            Total number of rows
        """
        import pyodbc

        pk = self.entity_config.get_primary_key()
        query = f"""
        SELECT COUNT(*) as count
        FROM [{self.entity_config.database}].[{self.entity_config.schema}].[{self.entity_config.base_table}]
        WHERE [{pk}] IS NOT NULL
        """

        conn = pyodbc.connect(self.connection_string)
        result = pd.read_sql(query, conn)
        conn.close()
        return int(result['count'].iloc[0])

    def preview(self, limit: int = 5) -> pd.DataFrame:
        """
        Preview first N rows of entity data.

        Args:
            limit: Number of rows to preview (default 5)

        Returns:
            DataFrame with preview data
        """
        return self.extract_to_dataframe(limit=limit)


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    from entity_config import get_entity_config
    import sys
    sys.path.append('..')
    from config import get_connection_string

    # Example 1: Extract Cases
    print("\n" + "=" * 70)
    print("Example 1: Extract Cases to DataFrame")
    print("=" * 70 + "\n")

    try:
        case_config = get_entity_config('Case')
        connection_string = get_connection_string()

        extractor = GenericExtractor(case_config, connection_string)

        # Preview
        print("Previewing first 5 cases...")
        preview_df = extractor.preview(limit=5)
        print(preview_df.head())

        # Get row count
        count = extractor.get_row_count()
        print(f"\nTotal cases: {count}")

    except Exception as e:
        print(f"Error: {str(e)}")

    # Example 2: Extract and save multiple entities
    print("\n" + "=" * 70)
    print("Example 2: Extract Multiple Entities")
    print("=" * 70 + "\n")

    try:
        connection_string = get_connection_string()

        for entity_name in ['Company', 'Person', 'Case']:
            print(f"\nExtracting {entity_name}...")
            config = get_entity_config(entity_name)
            extractor = GenericExtractor(config, connection_string)

            # Extract and save
            path = extractor.extract_and_save(limit=100)
            print(f"  ✓ Saved to {path}")

    except Exception as e:
        print(f"Error: {str(e)}")
