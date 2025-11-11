"""
Discover actual database schema for all entities
"""
import pyodbc
import pandas as pd
from config import get_connection_string

def discover_columns(table_name):
    """Discover actual columns in a table"""
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
tables = ['Company', 'Person', 'Communication', 'Address', 'vCases']

print("=" * 80)
print("ACTUAL DATABASE SCHEMA DISCOVERY")
print("=" * 80)

for table in tables:
    print(f"\n{table}:")
    print("-" * 80)
    try:
        columns = discover_columns(table)
        print(f"Total columns: {len(columns)}")
        print("\nFirst 20 columns:")
        for idx, row in columns.head(20).iterrows():
            print(f"  {row['COLUMN_NAME']:40s} {row['DATA_TYPE']:15s} {'NULL' if row['IS_NULLABLE'] == 'YES' else 'NOT NULL'}")

        if len(columns) > 20:
            print(f"  ... and {len(columns) - 20} more columns")
    except Exception as e:
        print(f"  Error: {e}")

print("\n" + "=" * 80)
