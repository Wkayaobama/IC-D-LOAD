"""
Extract Companies, Persons, and Opportunities
==============================================

This script extracts:
1. Company (with phone numbers)
2. Person (with phone numbers)
3. Opportunity (with computed weighted forecast values)

All extractions include denormalized relationship data.
"""

import sys
from pathlib import Path
from loguru import logger

# Setup paths
sys.path.append(str(Path(__file__).parent))

from pipeline_async.generic_extractor import GenericExtractor
from pipeline_async.entity_config import get_entity_config
from config import get_connection_string


def preview_row_counts(connection_string: str):
    """Preview row counts for each entity before extraction"""
    logger.info("\n" + "=" * 70)
    logger.info("STEP 1: Preview Row Counts")
    logger.info("=" * 70 + "\n")
    
    entities = ['Company', 'Person', 'Opportunity']
    counts = {}
    
    for entity_name in entities:
        try:
            config = get_entity_config(entity_name)
            extractor = GenericExtractor(config, connection_string)
            count = extractor.get_row_count()
            counts[entity_name] = count
            logger.info(f"📊 {entity_name}: {count:,} rows available")
        except Exception as e:
            logger.error(f"❌ Error getting count for {entity_name}: {e}")
            counts[entity_name] = 0
    
    total = sum(counts.values())
    logger.info(f"\n📊 Total rows to extract: {total:,}")
    
    return counts


def show_entity_details(connection_string: str):
    """Show entity configuration details"""
    logger.info("\n" + "=" * 70)
    logger.info("STEP 2: Entity Configuration Details")
    logger.info("=" * 70 + "\n")
    
    entities = ['Company', 'Person', 'Opportunity']
    
    for entity_name in entities:
        config = get_entity_config(entity_name)
        base_props = config.get_base_properties()
        denorm_props = config.get_denormalized_properties()
        computed_props = config.get_computed_properties()
        
        logger.info(f"\n{entity_name}:")
        logger.info(f"  Base Properties: {len(base_props)}")
        
        # Show phone number fields for Company and Person
        if entity_name == 'Company':
            phone_fields = [p for p in base_props if 'Phone' in p]
            logger.info(f"  Phone Fields: {', '.join(phone_fields)}")
        elif entity_name == 'Person':
            phone_fields = [p for p in base_props if 'Phone' in p or 'Mobile' in p]
            logger.info(f"  Phone/Mobile Fields: {', '.join(phone_fields)}")
        
        logger.info(f"  Denormalized Properties: {len(denorm_props)}")
        logger.info(f"  Computed Properties: {len(computed_props)}")
        logger.info(f"  Total Fields: {len(base_props) + len(denorm_props) + len(computed_props)}")


def extract_all_entities(connection_string: str):
    """Extract all three entities with full datasets"""
    logger.info("\n" + "=" * 70)
    logger.info("STEP 3: Full Extraction (No Limits)")
    logger.info("=" * 70 + "\n")
    
    entities = ['Company', 'Person', 'Opportunity']
    results = {}
    
    for entity_name in entities:
        logger.info(f"\n{'='*70}")
        logger.info(f"Extracting {entity_name}...")
        logger.info(f"{'='*70}\n")
        
        try:
            # Get configuration
            config = get_entity_config(entity_name)
            extractor = GenericExtractor(config, connection_string)
            
            # Extract full dataset (no limit)
            logger.info(f"⏳ Extracting all {entity_name} records...")
            df = extractor.extract_to_dataframe(limit=None)
            
            # Show columns with phone numbers
            if entity_name in ['Company', 'Person']:
                phone_cols = [col for col in df.columns if 'Phone' in col or 'Mobile' in col]
                if phone_cols:
                    logger.info(f"✅ Phone columns included: {', '.join(phone_cols)}")
            
            # Save to Bronze
            path = extractor.save_to_bronze(df)
            
            results[entity_name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'path': path
            }
            
            logger.info(f"\n✅ {entity_name} extraction completed:")
            logger.info(f"   - Rows: {len(df):,}")
            logger.info(f"   - Columns: {len(df.columns)}")
            logger.info(f"   - File: {path}")
            
            # Show sample data
            if len(df) > 0:
                logger.info(f"\n📋 Sample record (first row, first 5 fields):")
                sample = dict(list(df.iloc[0].items())[:5])
                for key, value in sample.items():
                    logger.info(f"   {key}: {value}")
            
        except Exception as e:
            logger.error(f"❌ Error extracting {entity_name}: {e}")
            import traceback
            traceback.print_exc()
            results[entity_name] = {
                'rows': 0,
                'columns': 0,
                'path': None,
                'error': str(e)
            }
    
    return results


def show_summary(results: dict):
    """Show extraction summary"""
    logger.info("\n" + "=" * 70)
    logger.info("EXTRACTION SUMMARY")
    logger.info("=" * 70 + "\n")
    
    total_rows = sum(r['rows'] for r in results.values())
    
    logger.info(f"Total Rows Extracted: {total_rows:,}\n")
    
    for entity_name, result in results.items():
        if result.get('error'):
            logger.error(f"❌ {entity_name}: FAILED - {result['error']}")
        else:
            logger.info(f"✅ {entity_name}:")
            logger.info(f"   - Rows: {result['rows']:,}")
            logger.info(f"   - Columns: {result['columns']}")
            logger.info(f"   - File: {result['path']}")


def main():
    """Main extraction workflow"""
    logger.info("\n" + "=" * 70)
    logger.info("Company, Person & Opportunity Extraction")
    logger.info("With Phone Numbers and Denormalized Data")
    logger.info("=" * 70 + "\n")
    
    try:
        # Get connection
        connection_string = get_connection_string()
        logger.info(f"✅ Database connection configured")
        
        # Step 1: Preview row counts
        counts = preview_row_counts(connection_string)
        
        # Step 2: Show entity details
        show_entity_details(connection_string)
        
        # Step 3: Extract all entities
        results = extract_all_entities(connection_string)
        
        # Step 4: Show summary
        show_summary(results)
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ Extraction completed successfully!")
        logger.info("=" * 70 + "\n")
        
    except Exception as e:
        logger.error(f"\n❌ Extraction failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️ Execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)



