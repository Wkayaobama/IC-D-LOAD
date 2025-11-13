"""
Discover full database schema including ALL columns for Company, Person, and Opportunity
"""
import pyodbc
import pandas as pd
from config import get_connection_string

def discover_all_columns(table_name):
    """Discover ALL columns in a table"""
    conn_str = get_connection_string()
    conn = pyodbc.connect(conn_str)

    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Discover all entity tables
tables = ['Company', 'Person', 'Opportunity']

print("=" * 80)
print("FULL DATABASE SCHEMA DISCOVERY")
print("=" * 80)

for table in tables:
    print(f"\n{table}:")
    print("-" * 80)
    try:
        columns = discover_all_columns(table)
        print(f"Total columns: {len(columns)}")
        print("\nALL columns:")
        for idx, row in columns.iterrows():
            print(f"  {row['COLUMN_NAME']:45s} {row['DATA_TYPE']:15s} {'NULL' if row['IS_NULLABLE'] == 'YES' else 'NOT NULL'}")
        
        # Show phone/email/fax columns
        print(f"\nPhone/Email/Fax columns in {table}:")
        phone_cols = columns[columns['COLUMN_NAME'].str.contains('Phone|Email|Fax|Mobile', case=False, na=False)]
        if len(phone_cols) > 0:
            for idx, row in phone_cols.iterrows():
                print(f"  ✓ {row['COLUMN_NAME']}")
        else:
            print("  None found")
            
    except Exception as e:
        print(f"  Error: {e}")

print("\n" + "=" * 80)



