#!/usr/bin/env python3
"""
DuckDB Transformer

High-performance data transformation using DuckDB.
"""

import duckdb
import pandas as pd
from typing import Optional


class DuckDBProcessor:
    """
    Process data transformations using DuckDB.

    Usage:
        processor = DuckDBProcessor()
        processor.load_csv("Bronze_Cases.csv", "cases")
        result = processor.execute("SELECT * FROM cases WHERE status = 'Open'")
        df = result.df()
    """

    def __init__(self, database: str = ":memory:"):
        self.conn = duckdb.connect(database)

    def load_csv(self, file_path: str, table_name: str):
        """Load CSV file into DuckDB table"""
        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{file_path}')
        """)

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """Load pandas DataFrame into DuckDB table"""
        self.conn.register(table_name, df)

    def execute(self, query: str):
        """Execute SQL query"""
        return self.conn.execute(query)

    def to_dataframe(self, query: str) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        return self.conn.execute(query).df()

    def close(self):
        """Close connection"""
        self.conn.close()


if __name__ == "__main__":
    processor = DuckDBProcessor()

    # Example: Load and transform
    processor.load_csv("Bronze_Cases.csv", "cases")

    # Transform
    df = processor.to_dataframe("""
        SELECT
            status,
            COUNT(*) as count
        FROM cases
        GROUP BY status
    """)

    print(df)
