#!/usr/bin/env python3
"""
HubSpot Data Extraction Script
==============================

Extract HubSpot data from PostgreSQL and save to CSV files for monitoring.

This script extracts data from the HubSpot schema in PostgreSQL and saves
it to CSV files for validation before running reconciliation.

Usage:
    # Extract all entities (limited to 1000 records each)
    python3 extract_hubspot_data.py

    # Extract specific entity
    python3 extract_hubspot_data.py --entity contacts

    # Extract without limit (full data)
    python3 extract_hubspot_data.py --no-limit

    # Extract to custom directory
    python3 extract_hubspot_data.py --output ./my_output

Features:
- Sequential extraction of all HubSpot entities
- CSV export with timestamps
- Column validation against property mappings
- Progress monitoring
- Summary statistics
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from loguru import logger

from postgres_connection_manager import PostgreSQLManager
from hubspot_entity_config import (
    HUBSPOT_ENTITY_CONFIGS,
    get_hubspot_entity_config,
    list_hubspot_entities
)
from hubspot_generic_extractor import HubSpotExtractor, HubSpotBatchExtractor


def setup_logging(verbose: bool = False):
    """Configure logging."""
    logger.remove()  # Remove default handler

    log_level = "DEBUG" if verbose else "INFO"

    # Console handler
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_level
    )

    # File handler
    log_dir = Path("./logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"hubspot_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="DEBUG"
    )

    logger.info(f"Logging to: {log_file}")


def extract_single_entity(
    entity_name: str,
    output_dir: str,
    limit: int = None,
    validate: bool = True
):
    """
    Extract a single HubSpot entity.

    Args:
        entity_name: Entity name (contacts, companies, deals, engagements)
        output_dir: Output directory for CSV
        limit: Optional row limit
        validate: Whether to validate columns
    """
    logger.info(f"Extracting single entity: {entity_name}")

    # Get entity config
    try:
        config = get_hubspot_entity_config(entity_name)
    except ValueError as e:
        logger.error(str(e))
        logger.info(f"Available entities: {list_hubspot_entities()}")
        sys.exit(1)

    # Initialize PostgreSQL connection
    pg = PostgreSQLManager()

    if not pg.test_connection():
        logger.error("PostgreSQL connection failed")
        sys.exit(1)

    # Create extractor
    extractor = HubSpotExtractor(config, pg)

    # Get row count
    try:
        total_count = extractor.get_row_count()
        logger.info(f"Total {entity_name} records in database: {total_count}")
    except Exception as e:
        logger.warning(f"Could not get row count: {e}")

    # Extract and save
    result = extractor.extract_and_save(
        output_dir=output_dir,
        limit=limit,
        validate=validate
    )

    # Print results
    print("\n" + "="*70)
    print("EXTRACTION RESULTS")
    print("="*70)
    print(f"Entity: {result['entity']}")
    print(f"Status: {result['status']}")
    print(f"Records Extracted: {result['records_extracted']}")

    if result['csv_path']:
        print(f"CSV File: {result['csv_path']}")

    if result['validation']:
        val = result['validation']
        print(f"\nValidation:")
        print(f"  Valid: {val['is_valid']}")
        print(f"  Expected Columns: {val['expected_count']}")
        print(f"  Actual Columns: {val['actual_count']}")

        if val['missing_columns']:
            print(f"  Missing: {val['missing_columns']}")
        if val['extra_columns']:
            print(f"  Extra: {val['extra_columns']}")

    if result['error']:
        print(f"\nError: {result['error']}")

    print("="*70 + "\n")

    pg.close()

    return result


def extract_all_entities(
    output_dir: str,
    limit: int = None,
    validate: bool = True
):
    """
    Extract all HubSpot entities.

    Args:
        output_dir: Output directory for CSVs
        limit: Optional row limit per entity
        validate: Whether to validate columns
    """
    logger.info("Extracting all HubSpot entities")

    # Initialize PostgreSQL connection
    pg = PostgreSQLManager()

    if not pg.test_connection():
        logger.error("PostgreSQL connection failed")
        sys.exit(1)

    # Get all entity configs
    all_configs = list(HUBSPOT_ENTITY_CONFIGS.values())

    logger.info(f"Found {len(all_configs)} entities to extract:")
    for config in all_configs:
        logger.info(f"  - {config.name}: {len(config.properties)} properties")

    # Create batch extractor
    batch_extractor = HubSpotBatchExtractor(pg)

    # Extract all entities
    results = batch_extractor.extract_all(
        entity_configs=all_configs,
        output_dir=output_dir,
        limit=limit,
        validate=validate
    )

    pg.close()

    return results


def preview_entity(entity_name: str, limit: int = 10):
    """
    Preview entity data without saving.

    Args:
        entity_name: Entity name
        limit: Number of records to preview
    """
    logger.info(f"Previewing {entity_name} (first {limit} records)")

    # Get entity config
    try:
        config = get_hubspot_entity_config(entity_name)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # Initialize PostgreSQL connection
    pg = PostgreSQLManager()

    if not pg.test_connection():
        logger.error("PostgreSQL connection failed")
        sys.exit(1)

    # Create extractor
    extractor = HubSpotExtractor(config, pg)

    # Preview data
    try:
        df = extractor.preview(limit=limit)

        print("\n" + "="*70)
        print(f"PREVIEW: {entity_name.upper()}")
        print("="*70)
        print(f"\nShape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"\nColumns: {list(df.columns)}")
        print(f"\nFirst {min(limit, len(df))} records:\n")
        print(df.to_string(max_rows=limit))
        print("\n" + "="*70 + "\n")

        # Get summary
        summary = extractor.get_summary(df)
        print("\nSummary Statistics:")
        print(f"  Total Records: {summary['total_records']}")
        print(f"  Total Columns: {summary['total_columns']}")
        print(f"  Memory Usage: {summary['memory_usage_mb']:.2f} MB")

        if 'legacy_id_stats' in summary:
            stats = summary['legacy_id_stats']
            print(f"\nLegacy ID ({config.legacy_id_field}):")
            print(f"  Non-null: {stats['non_null']} ({stats['non_null']/stats['total']*100:.1f}%)")
            print(f"  Null: {stats['null']}")
            print(f"  Unique: {stats['unique']}")

    except Exception as e:
        logger.error(f"Preview failed: {e}")
        sys.exit(1)

    pg.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract HubSpot data from PostgreSQL to CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract all entities (default: 1000 records per entity)
  python3 extract_hubspot_data.py

  # Extract specific entity
  python3 extract_hubspot_data.py --entity contacts

  # Extract full data (no limit)
  python3 extract_hubspot_data.py --no-limit

  # Extract to custom directory
  python3 extract_hubspot_data.py --output ./my_exports

  # Preview data without saving
  python3 extract_hubspot_data.py --preview contacts --limit 10

  # Verbose logging
  python3 extract_hubspot_data.py --verbose
        """
    )

    parser.add_argument(
        '--entity',
        type=str,
        help='Extract specific entity (contacts, companies, deals, engagements)'
    )

    parser.add_argument(
        '--output',
        type=str,
        default='./output/hubspot',
        help='Output directory for CSV files (default: ./output/hubspot)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=1000,
        help='Row limit per entity (default: 1000, use --no-limit for all)'
    )

    parser.add_argument(
        '--no-limit',
        action='store_true',
        help='Extract all records (no limit)'
    )

    parser.add_argument(
        '--no-validate',
        action='store_true',
        help='Skip column validation'
    )

    parser.add_argument(
        '--preview',
        type=str,
        metavar='ENTITY',
        help='Preview entity data without saving to CSV'
    )

    parser.add_argument(
        '--list',
        action='store_true',
        help='List available entities and exit'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(verbose=args.verbose)

    # List entities
    if args.list:
        print("\nAvailable HubSpot Entities:")
        print("="*70)
        for entity_name in list_hubspot_entities():
            config = get_hubspot_entity_config(entity_name)
            print(f"  - {entity_name}: {len(config.properties)} properties")
            print(f"    Table: {config.get_qualified_table_name()}")
            print(f"    Legacy ID: {config.legacy_id_field}")
            print()
        print("="*70)
        sys.exit(0)

    # Preview mode
    if args.preview:
        preview_entity(args.preview, limit=args.limit)
        sys.exit(0)

    # Determine limit
    limit = None if args.no_limit else args.limit

    if limit:
        logger.info(f"Using row limit: {limit} per entity")
    else:
        logger.info("Extracting all records (no limit)")

    # Validate mode
    validate = not args.no_validate

    # Create output directory
    Path(args.output).mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {args.output}")

    # Extract data
    try:
        if args.entity:
            # Extract single entity
            result = extract_single_entity(
                entity_name=args.entity,
                output_dir=args.output,
                limit=limit,
                validate=validate
            )

            # Exit with error if extraction failed
            if result['status'] == 'error':
                sys.exit(1)

        else:
            # Extract all entities
            results = extract_all_entities(
                output_dir=args.output,
                limit=limit,
                validate=validate
            )

            # Exit with error if any extraction failed
            if any(r['status'] == 'error' for r in results):
                sys.exit(1)

        logger.success("✓ Extraction complete!")

    except KeyboardInterrupt:
        logger.warning("\nExtraction interrupted by user")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
