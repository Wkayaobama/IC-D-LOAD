#!/usr/bin/env python3
"""
Test script for UTF-8 CSV Cleaning Pipeline
============================================

This script demonstrates how to use the UTF-8 cleaning pipeline
programmatically and provides test cases.

Usage:
    python3 test_utf8_pipeline.py
"""

import pandas as pd
import tempfile
from pathlib import Path
from csv_utf8_cleaner import UTF8Cleaner, CSVSchemaDiscovery, SQLGenerator

def create_test_csv():
    """Create a test CSV with UTF-8 characters."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": [
            "Company's Name",          # Smart quote
            "Test – Company",           # En dash
            "Another "Company"",        # Smart double quotes
            "Café & Résumé",           # Accented characters
            "Product™ by Company®"     # Trademark symbols
        ],
        "email": [
            "contact@company.com",
            "info–company.com",         # En dash (error)
            "sales@company…com",        # Ellipsis (error)
            "support@café.com",         # Accented character
            "admin@company.com"
        ],
        "description": [
            "This is a normal description",
            "Features: • Fast • Easy • Reliable",  # Bullets
            "Temperature: 25° celsius",  # Degree sign
            "Result: 10 × 5 = 50",       # Multiplication sign
            "Copyright © 2024 Company"   # Copyright symbol
        ],
        "amount": [100.50, 200.75, 300.00, 400.25, 500.50]
    }

    df = pd.DataFrame(data)
    return df


def test_schema_discovery():
    """Test schema discovery."""
    print("\n" + "="*80)
    print("TEST 1: Schema Discovery")
    print("="*80)

    df = create_test_csv()
    schema_df = CSVSchemaDiscovery.discover_schema(df)

    print("\nDiscovered Schema:")
    print(schema_df.to_string(index=False))
    print(f"\n✓ Schema discovery successful: {len(schema_df)} columns")


def test_utf8_cleaning():
    """Test UTF-8 character cleaning."""
    print("\n" + "="*80)
    print("TEST 2: UTF-8 Character Cleaning")
    print("="*80)

    df = create_test_csv()

    print("\nOriginal Data (name column):")
    print(df["name"].to_string())

    # Clean data
    cleaner = UTF8Cleaner()
    df_clean = cleaner.clean_dataframe(df, target_columns=["name", "email", "description"])

    print("\nCleaned Data (name column):")
    print(df_clean["name"].to_string())

    # Print statistics
    print("\n")
    cleaner.print_statistics()

    print("✓ UTF-8 cleaning successful")


def test_utf8_cleaning_preserve_accents():
    """Test UTF-8 cleaning with preserved accents."""
    print("\n" + "="*80)
    print("TEST 3: UTF-8 Cleaning (Preserve Accents)")
    print("="*80)

    df = create_test_csv()

    print("\nOriginal Data (name column):")
    print(df["name"].to_string())

    # Clean data with preserved accents
    cleaner = UTF8Cleaner(preserve_accents=True)
    df_clean = cleaner.clean_dataframe(df, target_columns=["name"])

    print("\nCleaned Data (name column, accents preserved):")
    print(df_clean["name"].to_string())

    print("\n✓ UTF-8 cleaning with accent preservation successful")


def test_sql_generation():
    """Test SQL generation."""
    print("\n" + "="*80)
    print("TEST 4: SQL Generation")
    print("="*80)

    df = create_test_csv()
    schema_df = CSVSchemaDiscovery.discover_schema(df)

    sql_gen = SQLGenerator(table_name="test_companies", schema="staging")

    # Generate staging table
    print("\nStaging Table SQL:")
    print("-" * 80)
    staging_sql = sql_gen.generate_create_table(schema_df, staging=True)
    print(staging_sql)

    # Generate production table
    print("\nProduction Table SQL:")
    print("-" * 80)
    prod_sql = sql_gen.generate_create_table(schema_df, staging=False)
    print(prod_sql)

    # Generate insert statements (first 2 rows)
    print("\nSample Insert Statements:")
    print("-" * 80)
    insert_sql = sql_gen.generate_insert(df.head(2), staging=True)
    print(insert_sql[:500] + "...\n")

    print("✓ SQL generation successful")


def test_full_pipeline():
    """Test full pipeline."""
    print("\n" + "="*80)
    print("TEST 5: Full Pipeline")
    print("="*80)

    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        temp_csv = f.name
        df = create_test_csv()
        df.to_csv(f, index=False)

    print(f"\nCreated temporary CSV: {temp_csv}")

    # Read CSV
    df_input = pd.read_csv(temp_csv)
    print(f"Read {len(df_input)} rows")

    # Discover schema
    schema_df = CSVSchemaDiscovery.discover_schema(df_input)
    print(f"Discovered schema: {len(schema_df)} columns")

    # Clean UTF-8
    cleaner = UTF8Cleaner()
    df_clean = cleaner.clean_dataframe(df_input, target_columns=["name", "email", "description"])
    print(f"Cleaned data: {cleaner.stats['_total_'].get('replacements', 0)} replacements")

    # Generate SQL
    sql_gen = SQLGenerator(table_name="test_companies", schema="staging")

    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        temp_sql = f.name

        f.write("-- Full Pipeline Test\n\n")
        f.write("-- Create staging table\n")
        f.write(sql_gen.generate_create_table(schema_df, staging=True))
        f.write("\n\n")
        f.write("-- Insert data\n")
        f.write(sql_gen.generate_insert(df_clean, staging=True))

    print(f"Generated SQL: {temp_sql}")

    # Display file sizes
    csv_size = Path(temp_csv).stat().st_size
    sql_size = Path(temp_sql).stat().st_size
    print(f"\nFile sizes:")
    print(f"  CSV: {csv_size} bytes")
    print(f"  SQL: {sql_size} bytes")

    # Get statistics
    stats_df = cleaner.get_statistics_summary()
    if not stats_df.empty:
        print(f"\nStatistics:")
        print(stats_df.to_string(index=False))

    # Cleanup
    Path(temp_csv).unlink()
    Path(temp_sql).unlink()

    print("\n✓ Full pipeline successful")


def test_custom_replacements():
    """Test custom replacement patterns."""
    print("\n" + "="*80)
    print("TEST 6: Custom Replacement Patterns")
    print("="*80)

    # Custom data
    data = {
        "text": [
            "Value → Result",           # Custom arrow
            "Item #1 | Item #2",        # Custom pipe
            "Price: 100€",              # Euro sign
        ]
    }
    df = pd.DataFrame(data)

    print("\nOriginal Data:")
    print(df["text"].to_string())

    # Custom replacements
    custom_replacements = {
        r'→': '->',      # Arrow to ASCII
        r'\|': '/',      # Pipe to slash
        r'€': 'EUR',     # Euro to EUR
    }

    cleaner = UTF8Cleaner(custom_replacements=custom_replacements)
    df_clean = cleaner.clean_dataframe(df, target_columns=["text"])

    print("\nCleaned Data:")
    print(df_clean["text"].to_string())

    print("\n✓ Custom replacements successful")


def main():
    """Run all tests."""
    print("="*80)
    print("UTF-8 CSV CLEANING PIPELINE - TEST SUITE")
    print("="*80)

    try:
        test_schema_discovery()
        test_utf8_cleaning()
        test_utf8_cleaning_preserve_accents()
        test_sql_generation()
        test_full_pipeline()
        test_custom_replacements()

        print("\n" + "="*80)
        print("ALL TESTS PASSED ✓")
        print("="*80)

    except Exception as e:
        print("\n" + "="*80)
        print(f"TEST FAILED: {e}")
        print("="*80)
        raise


if __name__ == "__main__":
    main()
