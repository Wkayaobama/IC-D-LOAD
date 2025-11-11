#!/usr/bin/env python3
"""
HubSpot Generic Extractor
=========================

Generic extractor for HubSpot entities from PostgreSQL.
Mirrors the Bronze layer pattern but for HubSpot data.

Usage:
    from hubspot_generic_extractor import HubSpotExtractor
    from hubspot_entity_config import get_hubspot_entity_config
    from postgres_connection_manager import PostgreSQLManager

    # Create extractor
    config = get_hubspot_entity_config('contacts')
    pg = PostgreSQLManager()
    extractor = HubSpotExtractor(config, pg)

    # Extract to DataFrame
    df = extractor.extract_to_dataframe(limit=1000)

    # Save to CSV
    csv_path = extractor.save_to_csv(df, './output/contacts.csv')
"""

import os
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime
from loguru import logger

from hubspot_entity_config import HubSpotEntityConfig
from postgres_connection_manager import PostgreSQLManager


class HubSpotExtractor:
    """
    Generic extractor for HubSpot entities from PostgreSQL.

    Features:
    - Query generation from entity config
    - DataFrame extraction
    - CSV export with timestamps
    - Progress monitoring
    - Data validation
    """

    def __init__(self, entity_config: HubSpotEntityConfig, pg_manager: PostgreSQLManager):
        """
        Initialize extractor.

        Args:
            entity_config: HubSpot entity configuration
            pg_manager: PostgreSQL connection manager
        """
        self.config = entity_config
        self.pg = pg_manager
        self.entity_name = entity_config.name

        logger.info(f"Initialized HubSpotExtractor for {self.entity_name}")

    def extract_to_dataframe(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Extract entity data to pandas DataFrame.

        Args:
            limit: Optional row limit for extraction

        Returns:
            DataFrame with entity data

        Raises:
            Exception: If extraction fails
        """
        logger.info(f"Extracting {self.entity_name} from PostgreSQL...")

        # Build query
        query = self.config.build_query(limit=limit)

        logger.debug(f"Generated query:\n{query}")

        # Execute query
        try:
            df = self.pg.execute_query_df(query)

            if df is not None and not df.empty:
                logger.success(
                    f"✓ Extracted {len(df)} {self.entity_name} records "
                    f"with {len(df.columns)} columns"
                )
                return df
            else:
                logger.warning(f"No data found for {self.entity_name}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to extract {self.entity_name}: {e}")
            raise

    def save_to_csv(
        self,
        df: pd.DataFrame,
        output_path: str,
        include_timestamp: bool = True
    ) -> str:
        """
        Save DataFrame to CSV file.

        Args:
            df: DataFrame to save
            output_path: Output file path
            include_timestamp: Add timestamp to filename

        Returns:
            Path to saved CSV file
        """
        if df.empty:
            logger.warning(f"DataFrame is empty, skipping CSV export for {self.entity_name}")
            return ""

        # Add timestamp to filename if requested
        if include_timestamp:
            path_obj = Path(output_path)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{path_obj.stem}_{timestamp}{path_obj.suffix}"
            output_path = str(path_obj.parent / filename)

        # Create directory if needed
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

        # Save to CSV
        try:
            df.to_csv(output_path, index=False)
            file_size = os.path.getsize(output_path)
            logger.success(
                f"✓ Saved {len(df)} records to {output_path} "
                f"({file_size / 1024:.1f} KB)"
            )
            return output_path

        except Exception as e:
            logger.error(f"Failed to save CSV for {self.entity_name}: {e}")
            raise

    def preview(self, limit: int = 5) -> pd.DataFrame:
        """
        Preview first N records.

        Args:
            limit: Number of records to preview

        Returns:
            DataFrame with preview data
        """
        logger.info(f"Previewing first {limit} {self.entity_name} records...")
        return self.extract_to_dataframe(limit=limit)

    def get_row_count(self) -> int:
        """
        Get total row count for entity.

        Returns:
            Total number of rows
        """
        query = f"""
        SELECT COUNT(*) as count
        FROM {self.config.get_qualified_table_name()}
        """

        if self.config.where_clause:
            query += f"\nWHERE {self.config.where_clause}"

        query += ";"

        result = self.pg.execute_query(query)
        count = result[0]['count'] if result else 0

        logger.info(f"{self.entity_name}: {count} total records")
        return count

    def validate_columns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate extracted DataFrame columns match configuration.

        Args:
            df: DataFrame to validate

        Returns:
            Validation results dictionary
        """
        expected_cols = set(self.config.properties)
        actual_cols = set(df.columns)

        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols

        validation = {
            'is_valid': len(missing_cols) == 0,
            'expected_count': len(expected_cols),
            'actual_count': len(actual_cols),
            'missing_columns': list(missing_cols),
            'extra_columns': list(extra_cols),
            'row_count': len(df)
        }

        if validation['is_valid']:
            logger.success(f"✓ Column validation passed for {self.entity_name}")
        else:
            logger.warning(
                f"Column mismatch for {self.entity_name}: "
                f"missing={missing_cols}, extra={extra_cols}"
            )

        return validation

    def get_sample_records(self, df: pd.DataFrame, n: int = 3) -> List[Dict]:
        """
        Get sample records as list of dictionaries.

        Args:
            df: DataFrame to sample from
            n: Number of samples

        Returns:
            List of record dictionaries
        """
        if df.empty:
            return []

        sample_size = min(n, len(df))
        sample_df = df.head(sample_size)

        return sample_df.to_dict('records')

    def extract_and_save(
        self,
        output_dir: str = './output/hubspot',
        limit: Optional[int] = None,
        validate: bool = True
    ) -> Dict[str, Any]:
        """
        Complete extraction workflow: extract, validate, save.

        Args:
            output_dir: Output directory for CSV
            limit: Optional row limit
            validate: Whether to validate columns

        Returns:
            Results dictionary with status and paths
        """
        logger.info(f"Starting extraction workflow for {self.entity_name}")

        results = {
            'entity': self.entity_name,
            'status': 'started',
            'records_extracted': 0,
            'csv_path': None,
            'validation': None,
            'error': None
        }

        try:
            # Extract data
            df = self.extract_to_dataframe(limit=limit)
            results['records_extracted'] = len(df)

            if df.empty:
                results['status'] = 'no_data'
                logger.warning(f"No data extracted for {self.entity_name}")
                return results

            # Validate columns if requested
            if validate:
                validation = self.validate_columns(df)
                results['validation'] = validation

                if not validation['is_valid']:
                    logger.warning(f"Validation issues found for {self.entity_name}")

            # Save to CSV
            csv_filename = f"{self.entity_name}.csv"
            csv_path = os.path.join(output_dir, csv_filename)

            saved_path = self.save_to_csv(df, csv_path, include_timestamp=True)
            results['csv_path'] = saved_path
            results['status'] = 'success'

            logger.success(f"✓ Extraction workflow complete for {self.entity_name}")

        except Exception as e:
            results['status'] = 'error'
            results['error'] = str(e)
            logger.error(f"Extraction workflow failed for {self.entity_name}: {e}")

        return results

    def get_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics for extracted data.

        Args:
            df: DataFrame to summarize

        Returns:
            Summary dictionary
        """
        if df.empty:
            return {'empty': True}

        summary = {
            'entity': self.entity_name,
            'total_records': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'null_counts': df.isnull().sum().to_dict(),
            'sample_record': df.head(1).to_dict('records')[0] if len(df) > 0 else None
        }

        # Add legacy ID statistics
        if self.config.legacy_id_field and self.config.legacy_id_field in df.columns:
            legacy_col = self.config.legacy_id_field
            summary['legacy_id_stats'] = {
                'total': len(df),
                'non_null': df[legacy_col].notna().sum(),
                'null': df[legacy_col].isna().sum(),
                'unique': df[legacy_col].nunique()
            }

        return summary


# ============================================================================
# BATCH EXTRACTOR
# ============================================================================

class HubSpotBatchExtractor:
    """
    Batch extractor for multiple HubSpot entities.

    Extracts multiple entities sequentially with progress monitoring.
    """

    def __init__(self, pg_manager: PostgreSQLManager):
        """
        Initialize batch extractor.

        Args:
            pg_manager: PostgreSQL connection manager
        """
        self.pg = pg_manager
        self.results = []

    def extract_all(
        self,
        entity_configs: List[HubSpotEntityConfig],
        output_dir: str = './output/hubspot',
        limit: Optional[int] = None,
        validate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Extract all entities sequentially.

        Args:
            entity_configs: List of entity configurations
            output_dir: Output directory
            limit: Optional row limit per entity
            validate: Whether to validate columns

        Returns:
            List of result dictionaries
        """
        logger.info(f"Starting batch extraction of {len(entity_configs)} entities")

        self.results = []

        for i, config in enumerate(entity_configs, 1):
            logger.info(f"\n{'='*70}")
            logger.info(f"Extracting entity {i}/{len(entity_configs)}: {config.name}")
            logger.info(f"{'='*70}")

            extractor = HubSpotExtractor(config, self.pg)
            result = extractor.extract_and_save(
                output_dir=output_dir,
                limit=limit,
                validate=validate
            )

            self.results.append(result)

        logger.info(f"\n{'='*70}")
        logger.info("BATCH EXTRACTION COMPLETE")
        logger.info(f"{'='*70}")

        self._print_summary()

        return self.results

    def _print_summary(self):
        """Print summary of batch extraction."""
        total_records = sum(r['records_extracted'] for r in self.results)
        successful = sum(1 for r in self.results if r['status'] == 'success')
        failed = sum(1 for r in self.results if r['status'] == 'error')

        print("\n" + "="*70)
        print("EXTRACTION SUMMARY")
        print("="*70)
        print(f"Total Entities: {len(self.results)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Total Records: {total_records}")
        print("\nDetails:")

        for result in self.results:
            status_icon = "✓" if result['status'] == 'success' else "✗"
            print(f"  {status_icon} {result['entity']}: {result['records_extracted']} records")
            if result['csv_path']:
                print(f"    → {result['csv_path']}")
            if result['error']:
                print(f"    Error: {result['error']}")

        print("="*70 + "\n")


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    from hubspot_entity_config import HUBSPOT_ENTITY_CONFIGS

    # Initialize PostgreSQL manager
    pg = PostgreSQLManager()

    # Example 1: Extract single entity
    print("\n" + "="*70)
    print("Example 1: Extract Contacts")
    print("="*70)

    from hubspot_entity_config import get_hubspot_entity_config

    contacts_config = get_hubspot_entity_config('contacts')
    contacts_extractor = HubSpotExtractor(contacts_config, pg)

    # Preview first 5 records
    preview_df = contacts_extractor.preview(limit=5)
    print(f"\nPreview:\n{preview_df.head()}")

    # Get row count
    total_count = contacts_extractor.get_row_count()

    # Extract and save
    contacts_result = contacts_extractor.extract_and_save(
        output_dir='./output/hubspot',
        limit=100  # Limit for testing
    )

    print(f"\nResult: {contacts_result}")

    # Example 2: Batch extract all entities
    print("\n" + "="*70)
    print("Example 2: Batch Extract All Entities")
    print("="*70)

    batch_extractor = HubSpotBatchExtractor(pg)

    all_configs = list(HUBSPOT_ENTITY_CONFIGS.values())

    batch_results = batch_extractor.extract_all(
        entity_configs=all_configs,
        output_dir='./output/hubspot',
        limit=100,  # Limit for testing
        validate=True
    )

    pg.close()
