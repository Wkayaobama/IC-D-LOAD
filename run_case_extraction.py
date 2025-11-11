#!/usr/bin/env python3
"""
Simple Case Extraction Script

Run this to test the complete pipeline with minimal setup.
Supports SQL Server, LocalDB, and SQL Server Express.
"""

import sys
import os

# Add paths
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(current_dir, 'sql-connection-manager', 'scripts'))
sys.path.insert(0, os.path.join(current_dir, 'sql-schema-discovery', 'scripts'))

print("=" * 70)
print("IC_Load - Case Extraction Pipeline")
print("=" * 70)

# Step 1: Get connection info
print("\n[1/5] Configuration")
print("-" * 70)

# Try to import config, otherwise ask for input
try:
    from config import get_connection_string, SQL_SERVER, SQL_DATABASE
    connection_string = get_connection_string()
    print(f"✅ Using config.py")
    print(f"   Server: {SQL_SERVER}")
    print(f"   Database: {SQL_DATABASE}")
except ImportError:
    print("⚠️  config.py not found. Using manual input...")
    print("\nTo avoid this prompt in the future:")
    print("  1. Copy config_template.py to config.py")
    print("  2. Update with your server details\n")

    print("Common server names:")
    print("  1. LocalDB: (localdb)\\MSSQLLocalDB")
    print("  2. SQL Express: localhost\\SQLEXPRESS or .\\SQLEXPRESS")
    print("  3. Full SQL Server: localhost or server_name")
    print()

    SQL_SERVER = input("Enter SQL Server name [(localdb)\\MSSQLLocalDB]: ").strip() or "(localdb)\\MSSQLLocalDB"
    SQL_DATABASE = input("Enter Database name [CRMICALPS]: ").strip() or "CRMICALPS"

    use_trusted = input("Use Windows Authentication? (Y/n): ").strip().lower() != 'n'

    # For LocalDB, use a different driver if available
    is_localdb = "localdb" in SQL_SERVER.lower()

    # Try different drivers in order of preference
    drivers_to_try = [
        "ODBC Driver 17 for SQL Server",  # Modern, recommended
        "ODBC Driver 13 for SQL Server",
        "ODBC Driver 11 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"  # Fallback, older driver
    ]

    # For LocalDB, prefer newer drivers
    if is_localdb:
        print(f"\n✓ Detected LocalDB connection")

    connection_string = None

# Step 2: Test connection with multiple drivers
print("\n[2/5] Testing Database Connection")
print("-" * 70)

import pyodbc

# Get list of available drivers
available_drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]
print(f"Available ODBC drivers: {', '.join(available_drivers) if available_drivers else 'None found'}")

if not available_drivers:
    print("\n❌ No SQL Server ODBC drivers found!")
    print("\nPlease install ODBC Driver for SQL Server:")
    print("  Download: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
    sys.exit(1)

# Try each driver until one works
connection_string = None
successful_driver = None

for driver in available_drivers:
    try:
        print(f"\nTrying driver: {driver}")

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"Trusted_Connection={'yes' if use_trusted else 'no'};"
        )

        # For LocalDB, add connection timeout
        if "localdb" in SQL_SERVER.lower():
            conn_str += "Connection Timeout=30;"

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION, DB_NAME()")
        version, db_name = cursor.fetchone()
        print(f"✅ Connected successfully with {driver}!")
        print(f"   Database: {db_name}")
        print(f"   SQL Server: {version.split(chr(10))[0][:80]}...")

        connection_string = conn_str
        successful_driver = driver
        cursor.close()
        conn.close()
        break  # Success! Stop trying other drivers

    except pyodbc.Error as e:
        error_msg = str(e)
        if "does not exist" in error_msg or "access denied" in error_msg:
            print(f"   ✗ Failed: {error_msg[:100]}...")
        else:
            print(f"   ✗ Failed: {error_msg[:100]}...")
        continue

if not connection_string:
    print("\n❌ Could not connect with any available driver!")
    print("\nTroubleshooting for LocalDB:")
    print("  1. Verify LocalDB is installed:")
    print("     Run: sqllocaldb info")
    print("  2. Check if MSSQLLocalDB instance exists:")
    print("     Run: sqllocaldb info MSSQLLocalDB")
    print("  3. Start the instance if stopped:")
    print("     Run: sqllocaldb start MSSQLLocalDB")
    print("  4. Verify database exists:")
    print(f"     Run: sqlcmd -S {SQL_SERVER} -Q \"SELECT name FROM sys.databases\"")
    print("\nTroubleshooting for SQL Server/Express:")
    print("  - Verify SQL Server is running (Services → SQL Server)")
    print("  - Check SQL Server Configuration Manager")
    print("  - Enable TCP/IP protocol if needed")
    print("  - Ensure Windows Authentication is enabled")
    sys.exit(1)

# Step 3: Discover schema
print("\n[3/5] Discovering Cases Table Schema")
print("-" * 70)

try:
    from schema_discovery import SchemaDiscovery

    discovery = SchemaDiscovery(connection_string=connection_string)

    # First, check if Cases table exists
    tables = discovery.discover_tables(SQL_DATABASE)
    table_names = [t.name for t in tables]

    print(f"✅ Found {len(tables)} tables in database")

    # Check for Cases table (case-insensitive)
    cases_table = None
    for name in table_names:
        if name.lower() == 'cases':
            cases_table = name
            break

    if not cases_table:
        # Check for vCases view
        for name in table_names:
            if 'cases' in name.lower():
                cases_table = name
                break

    if not cases_table:
        print("\n❌ Cases table not found!")
        print(f"\nAvailable tables containing 'case' (case-insensitive):")
        case_like_tables = [t for t in table_names if 'case' in t.lower()]
        if case_like_tables:
            for t in case_like_tables:
                print(f"   - {t}")
        else:
            print("   None found")

        print(f"\nAll tables in database:")
        for i, t in enumerate(table_names[:20], 1):
            print(f"   {i}. {t}")
        if len(table_names) > 20:
            print(f"   ... and {len(table_names) - 20} more")

        # Ask user for table name
        print()
        custom_table = input("Enter the Cases table name (or press Enter to exit): ").strip()
        if custom_table:
            cases_table = custom_table
        else:
            sys.exit(1)

    print(f"\n✓ Using table: {cases_table}")

    columns = discovery.discover_columns(cases_table)
    print(f"✅ Discovered {len(columns)} columns")

    if len(columns) == 0:
        print(f"❌ Table '{cases_table}' has no columns or is inaccessible")
        sys.exit(1)

    # Show sample columns
    print("\nSample columns:")
    for col in columns[:10]:
        nullable = "NULL" if col.is_nullable else "NOT NULL"
        print(f"   - {col.name}: {col.data_type} ({nullable})")
    if len(columns) > 10:
        print(f"   ... and {len(columns) - 10} more columns")

except Exception as e:
    print(f"❌ Schema discovery failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 4: Extract data
print("\n[4/5] Extracting Case Data")
print("-" * 70)

try:
    import pandas as pd

    # Find ID column (case-insensitive)
    id_column = None
    for col in columns:
        if 'caseid' in col.name.lower():
            id_column = col.name
            break

    if not id_column:
        id_column = columns[0].name  # Use first column as fallback
        print(f"⚠️  Could not find CaseId column, using: {id_column}")

    # Simple query for testing (limit to 100 records)
    # Build column list from discovered columns (limit to avoid query too long)
    important_columns = [c.name for c in columns[:20]]  # First 20 columns
    column_list = ', '.join([f"[{col}]" for col in important_columns])

    query = f"""
    SELECT TOP 100
        {column_list}
    FROM [{cases_table}]
    WHERE [{id_column}] IS NOT NULL
    ORDER BY [{id_column}] DESC
    """

    print(f"Executing query (TOP 100 records for testing)...")
    print(f"   Table: {cases_table}")
    print(f"   Columns: {len(important_columns)}")

    conn = pyodbc.connect(connection_string)
    df = pd.read_sql(query, conn)
    conn.close()

    print(f"✅ Extracted {len(df)} records")

    if len(df) > 0:
        print(f"\nData shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print("\nFirst 3 records (first 5 columns):")
        print(df.iloc[:3, :5].to_string(index=False))
    else:
        print("⚠️  No records found in table")
        print("   The table may be empty or all records have NULL ID")
        sys.exit(1)

except Exception as e:
    print(f"❌ Data extraction failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 5: Export to CSV
print("\n[5/5] Exporting to Bronze Layer")
print("-" * 70)

try:
    from datetime import datetime

    # Create output directory
    os.makedirs("bronze_layer", exist_ok=True)

    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"bronze_layer/Bronze_Cases_{timestamp}.csv"

    # Add metadata
    df['_extracted_at'] = datetime.now()
    df['_source'] = f'{SQL_DATABASE}.{cases_table}'
    df['_driver'] = successful_driver
    df['_record_count'] = len(df)

    # Export
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    file_size = os.path.getsize(output_file)
    print(f"✅ Exported to: {output_file}")
    print(f"   Records: {len(df)}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Size: {file_size / 1024:.2f} KB")

    print(f"\nCSV columns exported:")
    for i, col in enumerate(df.columns[:15], 1):
        print(f"   {i}. {col}")
    if len(df.columns) > 15:
        print(f"   ... and {len(df.columns) - 15} more columns")

except Exception as e:
    print(f"❌ Export failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Success!
print("\n" + "=" * 70)
print("✅ Pipeline Complete!")
print("=" * 70)
print(f"""
Results:
  📁 Bronze CSV: {output_file}
  📊 Records: {len(df)}
  📋 Columns: {len(df.columns)}
  💾 Size: {file_size / 1024:.2f} KB
  🔌 Driver: {successful_driver}

CSV Structure:
  - Data columns: {len(df.columns) - 4}
  - Metadata columns: 4 (_extracted_at, _source, _driver, _record_count)

Next Steps:
  1. Review the CSV file: {output_file}
  2. Verify data quality and completeness
  3. Use duckdb-transformer to create Silver layer
  4. Apply business logic with pipeline-stage-mapper
  5. Run the full test: python test_case_extraction.py

To avoid connection prompts next time:
  1. Copy config_template.py to config.py
  2. Update with your settings:
     SQL_SERVER = "{SQL_SERVER}"
     SQL_DATABASE = "{SQL_DATABASE}"
""")

print("\n🎉 All skills working correctly!")
