#!/usr/bin/env python3
"""
Test Case Extraction Pipeline

Tests the complete pipeline:
1. Database connection
2. Schema discovery
3. Dataclass generation
4. Case extraction
5. Bronze layer CSV export
"""

import sys
import os
from datetime import datetime

# Add skill directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'sql-connection-manager', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'sql-schema-discovery', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'dataclass-generator', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'dataframe-dataclass-converter', 'scripts'))

# Configuration
SERVER = "your_server_name"  # TODO: Replace with actual server
DATABASE = "CRMICALPS"
TRUSTED_CONNECTION = True

# Build connection string
CONNECTION_STRING = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"Trusted_Connection={'yes' if TRUSTED_CONNECTION else 'no'};"
)

print("=" * 70)
print("IC_Load Case Extraction Pipeline Test")
print("=" * 70)

# ============================================
# Step 1: Test Database Connection
# ============================================
print("\n[Step 1] Testing database connection...")

try:
    from connection_manager import ConnectionManager

    manager = ConnectionManager(
        server=SERVER,
        database=DATABASE,
        trusted_connection=TRUSTED_CONNECTION
    )

    with manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        print(f"✅ Connection successful! Test query result: {result.test}")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("\n⚠️  Please update the SERVER variable in this script with your SQL Server name")
    print("   You can find it by running: SELECT @@SERVERNAME")
    sys.exit(1)

# ============================================
# Step 2: Discover Cases Table Schema
# ============================================
print("\n[Step 2] Discovering Cases table schema...")

try:
    from schema_discovery import SchemaDiscovery

    discovery = SchemaDiscovery(connection_string=CONNECTION_STRING)

    # Discover columns
    columns = discovery.discover_columns("Cases")
    print(f"✅ Found {len(columns)} columns in Cases table")

    # Show first 10 columns
    print("\nFirst 10 columns:")
    for col in columns[:10]:
        nullable = "NULL" if col.is_nullable else "NOT NULL"
        print(f"   - {col.name}: {col.data_type} ({col.python_type.__name__}) {nullable}")

    # Discover relationships
    relationships = discovery.discover_relationships("Cases")
    print(f"\n✅ Found {len(relationships)} foreign key relationships")
    for rel in relationships[:5]:
        print(f"   - {rel.column_name} → {rel.referenced_table}.{rel.referenced_column}")

except Exception as e:
    print(f"❌ Schema discovery failed: {e}")
    sys.exit(1)

# ============================================
# Step 3: Generate Case Dataclass
# ============================================
print("\n[Step 3] Generating Case dataclass...")

try:
    from dataclass_generator import DataclassGenerator

    # SQL query for Cases with denormalized Company and Person
    case_query = """
    SELECT
        c.[Case_CaseId],
        c.[Case_PrimaryCompanyId],
        comp.[Comp_Name] AS Company_Name,
        comp.[Comp_WebSite] AS Company_WebSite,
        c.[Case_PrimaryPersonId],
        p.[Pers_FirstName] AS Person_FirstName,
        p.[Pers_LastName] AS Person_LastName,
        v.[Emai_EmailAddress] AS Person_EmailAddress,
        c.[Case_AssignedUserId],
        c.[Case_ChannelId],
        c.[Case_Description],
        c.[Case_Status],
        c.[Case_Stage],
        c.[Case_Priority],
        c.[Case_Opened],
        c.[Case_Closed],
        c.[Case_CreatedDate],
        c.[Case_UpdatedDate]
    FROM [CRMICALPS].[dbo].[Cases] c
    LEFT JOIN [CRMICALPS].[dbo].[Company] comp
        ON c.[Case_PrimaryCompanyId] = comp.[Comp_CompanyId]
    LEFT JOIN [CRMICALPS].[dbo].[Person] p
        ON c.[Case_PrimaryPersonId] = p.[Pers_PersonId]
    LEFT JOIN [CRMICALPS].[dbo].[vEmailCompanyAndPerson] v
        ON c.[Case_PrimaryPersonId] = v.[Pers_PersonId]
    """

    generator = DataclassGenerator()

    # Generate dataclass code
    dataclass_code = generator.generate_from_query(
        query=case_query,
        class_name="Case",
        schema_discovery=discovery,
        include_docstring=True
    )

    print("✅ Dataclass generated successfully")
    print("\nGenerated dataclass preview:")
    print(dataclass_code[:500] + "...")

    # Save to file
    os.makedirs("models", exist_ok=True)
    generator.save_to_file(dataclass_code, "models/case.py")
    print("\n✅ Saved to models/case.py")

    # Execute dataclass code to make Case available
    exec(dataclass_code, globals())
    print("✅ Case dataclass loaded into memory")

except Exception as e:
    print(f"❌ Dataclass generation failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================
# Step 4: Extract Case Data
# ============================================
print("\n[Step 4] Extracting Case data from database...")

try:
    import pandas as pd

    # Execute query and get DataFrame
    with manager.get_connection() as conn:
        # Add WHERE clause to limit results for testing
        test_query = case_query + "\nWHERE c.[Case_CaseId] IS NOT NULL\nORDER BY c.[Case_CreatedDate] DESC"

        print("   Executing query...")
        df = pd.read_sql(test_query, conn)
        print(f"✅ Extracted {len(df)} cases")

        if len(df) > 0:
            print(f"\nSample data (first row):")
            first_row = df.iloc[0]
            print(f"   Case ID: {first_row.get('Case_CaseId', 'N/A')}")
            print(f"   Status: {first_row.get('Case_Status', 'N/A')}")
            print(f"   Company: {first_row.get('Company_Name', 'N/A')}")
            print(f"   Opened: {first_row.get('Case_Opened', 'N/A')}")

except Exception as e:
    print(f"❌ Data extraction failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================
# Step 5: Convert to Dataclass Instances
# ============================================
print("\n[Step 5] Converting DataFrame to Case dataclass instances...")

try:
    from dataframe_converter import DataFrameConverter

    converter = DataFrameConverter(auto_map_names=True)

    # Convert DataFrame to list of Case instances
    cases = converter.dataframe_to_dataclasses(df, Case)
    print(f"✅ Converted {len(cases)} DataFrame rows to Case instances")

    # Show first case
    if len(cases) > 0:
        first_case = cases[0]
        print(f"\nFirst Case instance:")
        print(f"   case_id: {first_case.case_id}")
        print(f"   status: {first_case.status}")
        print(f"   company_name: {first_case.company_name}")
        print(f"   person_first_name: {first_case.person_first_name}")

except Exception as e:
    print(f"❌ Dataclass conversion failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================
# Step 6: Export to Bronze Layer CSV
# ============================================
print("\n[Step 6] Exporting to Bronze layer CSV...")

try:
    # Create output directory
    os.makedirs("bronze_layer", exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"bronze_layer/Bronze_Cases_{timestamp}.csv"

    # Convert back to DataFrame for CSV export
    df_export = converter.dataclasses_to_dataframe(cases)

    # Add metadata columns
    df_export['_extracted_at'] = datetime.now()
    df_export['_extraction_source'] = 'CRMICALPS.dbo.Cases'
    df_export['_record_count'] = len(df_export)

    # Export to CSV
    df_export.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"✅ Exported to {output_path}")
    print(f"\nCSV Contents:")
    print(f"   - Total records: {len(df_export)}")
    print(f"   - Total columns: {len(df_export.columns)}")
    print(f"   - File size: {os.path.getsize(output_path) / 1024:.2f} KB")
    print(f"\nColumns exported:")
    for i, col in enumerate(df_export.columns[:10]):
        print(f"   {i+1}. {col}")
    if len(df_export.columns) > 10:
        print(f"   ... and {len(df_export.columns) - 10} more columns")

except Exception as e:
    print(f"❌ CSV export failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================
# Summary
# ============================================
print("\n" + "=" * 70)
print("✅ Pipeline Execution Complete!")
print("=" * 70)
print(f"""
Summary:
  ✅ Database connection established
  ✅ Schema discovered ({len(columns)} columns, {len(relationships)} FKs)
  ✅ Case dataclass generated (models/case.py)
  ✅ Data extracted ({len(cases)} records)
  ✅ Converted to type-safe dataclass instances
  ✅ Exported to Bronze layer CSV ({output_path})

Next Steps:
  1. Review the generated dataclass: models/case.py
  2. Check the Bronze CSV: {output_path}
  3. Use duckdb-transformer for Silver layer transformations
  4. Apply business logic with pipeline-stage-mapper
  5. Calculate computed columns with computed-columns-calculator
""")

print("\n🎉 All skills working correctly!")
