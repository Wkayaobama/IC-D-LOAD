"""
Complete Pipeline Example
==========================

This example demonstrates the full modular extraction pipeline:

1. Entity configuration (properties.py + entity_config.py)
2. Generic extraction (generic_extractor.py)
3. Async task execution (extraction_task.py)
4. Model integration (MemoryModel/SqlModel/ChainModel)
5. Multi-entity extraction with shared connection

Run this example:
    python example_pipeline.py
"""

import asyncio
import sys
from pathlib import Path

# Setup paths
sys.path.append(str(Path(__file__).parent))

from loguru import logger

# Import pipeline components
from pipeline_async import (
    extract_entity,
    extract_multiple_entities,
    get_entity_config,
    list_entities
)
from pipeline_async.model.memory import MemoryModel
from pipeline_async.model.chain import ChainModel
from config import get_connection_string


async def example_1_single_entity():
    """
    Example 1: Extract a single entity (Case) - DataFrame only (no dataclasses)
    """
    logger.info("\n" + "=" * 70)
    logger.info("Example 1: Single Entity Extraction (DataFrame)")
    logger.info("=" * 70 + "\n")

    from pipeline_async import GenericExtractor, get_entity_config

    # Setup
    connection_string = get_connection_string()
    case_config = get_entity_config('Case')

    # Create extractor
    extractor = GenericExtractor(case_config, connection_string)

    # Extract to DataFrame (no dataclass conversion needed)
    logger.info("Extracting Cases to DataFrame...")
    df = extractor.extract_to_dataframe(limit=10)

    logger.info(f"\n✓ Extraction completed:")
    logger.info(f"  - Entity: Case")
    logger.info(f"  - Extracted: {len(df)} rows")
    logger.info(f"  - Columns: {len(df.columns)}")

    # Save to Bronze
    path = extractor.save_to_bronze(df)
    logger.info(f"  - Bronze file: {path}")

    # Show preview
    logger.info(f"\nFirst row preview:")
    if len(df) > 0:
        logger.info(f"  {dict(list(df.iloc[0].items())[:3])}...")


async def example_2_multiple_entities():
    """
    Example 2: Extract multiple entities with shared connection - FULL DATASET (DataFrame only)
    """
    logger.info("\n" + "=" * 70)
    logger.info("Example 2: FULL Multi-Entity Extraction (Sequential, DataFrame)")
    logger.info("=" * 70 + "\n")

    from pipeline_async import GenericExtractor, get_entity_config

    connection_string = get_connection_string()
    # Extract ALL core entities with their linkages - NO LIMITS
    entity_names = ['Company', 'Person', 'Address', 'Case', 'Communication']

    results = {}

    # Extract each entity sequentially
    for entity_name in entity_names:
        logger.info(f"\nExtracting {entity_name} (full dataset)...")
        config = get_entity_config(entity_name)
        extractor = GenericExtractor(config, connection_string)

        # Get row count first
        try:
            count = extractor.get_row_count()
            logger.info(f"  Available rows: {count}")
        except Exception as e:
            logger.warning(f"  Could not get row count: {e}")

        # Extract to DataFrame - NO LIMIT
        df = extractor.extract_to_dataframe(limit=None)

        # Save to Bronze
        path = extractor.save_to_bronze(df)

        results[entity_name] = {
            'rows': len(df),
            'path': path
        }

        logger.info(f"  [OK] {entity_name}: {len(df)} rows -> {path}")

    # Show aggregated results
    total_rows = sum(r['rows'] for r in results.values())
    logger.info(f"\n[OK] All extractions completed:")
    logger.info(f"  - Entities: {', '.join(entity_names)}")
    logger.info(f"  - Total extracted: {total_rows} rows")

    # Show breakdown
    logger.info("\nBreakdown:")
    for entity_name, result in results.items():
        logger.info(f"  - {entity_name}: {result['rows']:,} rows")


async def example_3_chain_model():
    """
    Example 3: Extract Communication entity (DataFrame only)
    """
    logger.info("\n" + "=" * 70)
    logger.info("Example 3: Communication Entity Extraction")
    logger.info("=" * 70 + "\n")

    from pipeline_async import GenericExtractor, get_entity_config

    connection_string = get_connection_string()
    config = get_entity_config('Communication')

    extractor = GenericExtractor(config, connection_string)

    # Extract
    logger.info("Extracting Communication records...")
    df = extractor.extract_to_dataframe(limit=15)

    # Save
    path = extractor.save_to_bronze(df)

    logger.info(f"\n✓ Communication extraction completed:")
    logger.info(f"  - Extracted: {len(df)} rows")
    logger.info(f"  - Bronze file: {path}")


async def example_4_filtered_extraction():
    """
    Example 4: Extract with filter clause (DataFrame only)
    """
    logger.info("\n" + "=" * 70)
    logger.info("Example 4: Filtered Extraction")
    logger.info("=" * 70 + "\n")

    from pipeline_async import GenericExtractor, get_entity_config

    connection_string = get_connection_string()
    config = get_entity_config('Case')

    extractor = GenericExtractor(config, connection_string)

    # Extract only open cases (example filter - adjust based on your data)
    logger.info("Extracting open Cases only...")
    try:
        df = extractor.extract_to_dataframe(
            filter_clause="AND Case_Status = 'Open'",
            limit=50
        )

        logger.info(f"\n✓ Filtered extraction completed:")
        logger.info(f"  - Filter: Open cases only")
        logger.info(f"  - Extracted: {len(df)} rows")

        if len(df) > 0:
            path = extractor.save_to_bronze(df, output_path="bronze_layer/Bronze_Cases_Open.csv")
            logger.info(f"  - Bronze file: {path}")
    except Exception as e:
        logger.warning(f"  Filter example skipped (adjust filter for your data): {e}")


async def example_5_all_entities():
    """
    Example 5: Extract ALL configured entities
    """
    logger.info("\n" + "=" * 70)
    logger.info("Example 5: Extract ALL Entities")
    logger.info("=" * 70 + "\n")

    # Get all entity names
    all_entities = list_entities()
    logger.info(f"Available entities: {', '.join(all_entities)}")

    # Setup
    model = MemoryModel()
    connection_string = get_connection_string()

    # Extract all entities with limits
    limits = {entity: 5 for entity in all_entities}  # 5 rows each for testing

    task = await extract_multiple_entities(
        entity_names=all_entities,
        connection_string=connection_string,
        model=model,
        limits=limits,
        save_bronze=True,
        parallel=False  # Sequential for FK dependencies
    )

    # Show results
    stats = task.get_stats()
    logger.info(f"\n✓ Extracted all {len(stats['entities'])} entities:")
    logger.info(f"  - Total rows: {stats['total_extracted']}")
    logger.info(f"  - Total added to model: {stats['total_added']}")

    # Show breakdown
    logger.info("\nBreakdown by entity:")
    for entity, entity_stats in stats['individual_stats'].items():
        logger.info(f"  - {entity}: {entity_stats['extracted']} rows")


async def example_6_direct_extractor():
    """
    Example 6: Using GenericExtractor directly (without Task wrapper)
    """
    logger.info("\n" + "=" * 70)
    logger.info("Example 6: Direct GenericExtractor Usage")
    logger.info("=" * 70 + "\n")

    from pipeline_async.generic_extractor import GenericExtractor

    # Setup
    connection_string = get_connection_string()
    case_config = get_entity_config('Case')

    # Create extractor
    extractor = GenericExtractor(case_config, connection_string)

    # Preview data
    logger.info("Previewing first 3 rows...")
    preview = extractor.preview(limit=3)
    logger.info(f"\nColumns: {list(preview.columns)}")
    logger.info(f"\nFirst row:\n{preview.iloc[0].to_dict()}")

    # Get row count
    count = extractor.get_row_count()
    logger.info(f"\nTotal rows available: {count}")

    # Extract to DataFrame
    df = extractor.extract_to_dataframe(limit=10)
    logger.info(f"\nExtracted {len(df)} rows to DataFrame")

    # Save to Bronze
    path = extractor.save_to_bronze(df)
    logger.info(f"Saved to: {path}")


def print_configuration_info():
    """Print information about configured entities"""
    logger.info("\n" + "=" * 70)
    logger.info("Configured Entities")
    logger.info("=" * 70 + "\n")

    for entity_name in list_entities():
        config = get_entity_config(entity_name)
        all_props = config.get_all_properties()
        logger.info(f"{entity_name}:")
        logger.info(f"  - Table: {config.base_table}")
        logger.info(f"  - Properties: {len(all_props)}")
        logger.info(f"  - JOINs: {len(config.joins)}")
        logger.info(f"  - Primary Key: {config.get_primary_key()}\n")


async def main():
    """Run basic examples (DataFrame-only, no dataclasses needed)"""
    logger.info("\n" + "=" * 70)
    logger.info("IC_Load Modular Extraction Pipeline - Basic Examples")
    logger.info("=" * 70)

    # Print configuration
    print_configuration_info()

    # Run examples (DataFrame-only, no dataclass conversion)

    # Example 1: Single entity
    await example_1_single_entity()

    # Example 2: Multiple entities
    await example_2_multiple_entities()

    # Example 6: Direct extractor usage
    await example_6_direct_extractor()

    # Examples commented out (require dataclass definitions or more setup):
    # await example_3_chain_model()
    # await example_4_filtered_extraction()
    # await example_5_all_entities()

    logger.info("\n" + "=" * 70)
    logger.info("Basic examples completed successfully!")
    logger.info("=" * 70 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("\n\nExecution interrupted by user")
    except Exception as e:
        logger.error(f"\n\nError: {str(e)}")
        import traceback
        traceback.print_exc()
