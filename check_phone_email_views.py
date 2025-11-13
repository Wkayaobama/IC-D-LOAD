"""
Check Phone and Email view structures
"""
import pyodbc
import pandas as pd
from config import get_connection_string

conn = pyodbc.connect(get_connection_string())

views = ['vPersonPhone', 'vPersonEmail', 'vCompanyPhone', 'vCompanyEmail']

print("=" * 80)
print("PHONE AND EMAIL VIEW STRUCTURES")
print("=" * 80)

for view_name in views:
    print(f"\n{view_name}:")
    print("-" * 80)
    try:
        # Get columns
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{view_name}'
        ORDER BY ORDINAL_POSITION
        """
        columns = pd.read_sql(query, conn)
        print(f"Columns ({len(columns)}):")
        for idx, row in columns.iterrows():
            print(f"  {row['COLUMN_NAME']:40s} {row['DATA_TYPE']}")
        
        # Get sample data
        sample = pd.read_sql(f"SELECT TOP 1 * FROM {view_name}", conn)
        print(f"\nSample row:")
        for col in sample.columns:
            val = sample[col].iloc[0] if len(sample) > 0 else None
            print(f"  {col}: {val}")
            
    except Exception as e:
        print(f"  Error: {e}")

conn.close()

print("\n" + "=" * 80)



