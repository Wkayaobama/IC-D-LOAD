#!/usr/bin/env python3
"""
Verify staging tables were created successfully.
"""

from postgres_connection_manager import PostgreSQLManager
from loguru import logger

def verify_staging_tables():
    """
    Check if staging schema and tables exist in PostgreSQL.
    """

    pg = PostgreSQLManager()

    print("\n" + "="*70)
    print("VERIFYING STAGING TABLES")
    print("="*70)

    # 1. Check if staging schema exists
    print("\n1. Checking if 'staging' schema exists...")
    schema_check = pg.execute_query("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name = 'staging';
    """)

    if schema_check:
        print("   ✓ 'staging' schema EXISTS")
    else:
        print("   ✗ 'staging' schema NOT FOUND")
        pg.close()
        return

    # 2. List all tables in staging schema
    print("\n2. Listing tables in 'staging' schema...")
    tables_query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'staging'
        ORDER BY table_name;
    """

    tables = pg.execute_query(tables_query)

    if tables:
        print(f"   Found {len(tables)} tables:")
        for table in tables:
            print(f"     ✓ staging.{table['table_name']}")
    else:
        print("   ✗ No tables found in staging schema")

    # 3. Check row counts
    print("\n3. Checking row counts in staging tables...")

    expected_tables = [
        'companies_reconciliation',
        'contacts_reconciliation',
        'deals_reconciliation',
        'communications_reconciliation',
        'reconciliation_log'
    ]

    for table in expected_tables:
        try:
            count_query = f"SELECT COUNT(*) as count FROM staging.{table};"
            result = pg.execute_query(count_query)
            count = result[0]['count'] if result else 0
            print(f"   ✓ staging.{table}: {count} rows")
        except Exception as e:
            print(f"   ✗ staging.{table}: ERROR - {e}")

    # 4. Show table structure
    print("\n4. Sample table structure (companies_reconciliation)...")
    columns_query = """
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'staging'
          AND table_name = 'companies_reconciliation'
        ORDER BY ordinal_position;
    """

    columns = pg.execute_query(columns_query)

    if columns:
        print(f"   Found {len(columns)} columns:")
        for col in columns[:10]:  # Show first 10 columns
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"     - {col['column_name']}: {col['data_type']} ({nullable})")
        if len(columns) > 10:
            print(f"     ... and {len(columns) - 10} more columns")

    # 5. Show indexes
    print("\n5. Checking indexes...")
    indexes_query = """
        SELECT
            indexname,
            tablename
        FROM pg_indexes
        WHERE schemaname = 'staging'
        ORDER BY tablename, indexname;
    """

    indexes = pg.execute_query(indexes_query)

    if indexes:
        print(f"   Found {len(indexes)} indexes:")
        for idx in indexes[:5]:
            print(f"     ✓ {idx['indexname']} on staging.{idx['tablename']}")
        if len(indexes) > 5:
            print(f"     ... and {len(indexes) - 5} more indexes")

    # 6. Connection string info
    print("\n6. Connection Information:")
    print(f"   Host: {pg.host}")
    print(f"   Port: {pg.port}")
    print(f"   Database: {pg.database}")
    print(f"   User: {pg.user}")

    print("\n" + "="*70)
    print("✓ VERIFICATION COMPLETE")
    print("="*70)

    print("\nTo view tables in your PostgreSQL client:")
    print("  1. Refresh schema list")
    print("  2. Expand 'staging' schema (not 'public')")
    print("  3. Look for tables:")
    print("     - companies_reconciliation")
    print("     - contacts_reconciliation")
    print("     - deals_reconciliation")
    print("     - communications_reconciliation")
    print("     - reconciliation_log")

    print("\nSQL to view all staging tables:")
    print("  SELECT * FROM information_schema.tables WHERE table_schema = 'staging';")

    print("\nSQL to count rows:")
    print("  SELECT COUNT(*) FROM staging.companies_reconciliation;")

    pg.close()

if __name__ == "__main__":
    verify_staging_tables()
