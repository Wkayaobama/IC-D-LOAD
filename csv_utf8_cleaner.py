#!/usr/bin/env python3
"""
CSV/TSV UTF-8 Character Cleaner and SQL Generator
=================================================

This script processes CSV/TSV files to:
1. Discover schema from CSV files
2. Detect and replace problematic UTF-8 characters in specified columns
3. Generate SQL statements for staging and production tables
4. Generate summary statistics of replacements

Usage:
    python csv_utf8_cleaner.py --input data.csv --column email --output sql/data.sql
    python csv_utf8_cleaner.py --input data.tsv --column name --table my_table --separator "\t"
    python csv_utf8_cleaner.py --input-dir ./csv_files --column description --batch
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import pandas as pd
import numpy as np
from loguru import logger

# ============================================================================
# UTF-8 CHARACTER REPLACEMENT PATTERNS
# ============================================================================

# Common problematic UTF-8 characters and their replacements
UTF8_REPLACEMENTS = {
    # Quotes
    r'['']': "'",           # Smart single quotes
    r'[""]': '"',           # Smart double quotes
    r'«|»': '"',            # French quotes
    r'‹|›': "'",            # Single angle quotes

    # Dashes and hyphens
    r'–': '-',              # En dash
    r'—': '-',              # Em dash
    r'−': '-',              # Minus sign
    r'‐': '-',              # Hyphen

    # Ellipsis
    r'…': '...',            # Horizontal ellipsis

    # Spaces
    r'\u00A0': ' ',         # Non-breaking space
    r'\u2000|\u2001|\u2002|\u2003|\u2004|\u2005|\u2006': ' ',  # Various spaces
    r'\u2007|\u2008|\u2009|\u200A|\u200B': ' ',  # More spaces

    # Special characters
    r'©': '(c)',            # Copyright
    r'®': '(R)',            # Registered trademark
    r'™': '(TM)',           # Trademark
    r'•': '*',              # Bullet
    r'°': 'deg',            # Degree sign
    r'×': 'x',              # Multiplication sign
    r'÷': '/',              # Division sign

    # Accented characters (optional - can be extended)
    r'é|è|ê|ë': 'e',
    r'á|à|â|ä': 'a',
    r'í|ì|î|ï': 'i',
    r'ó|ò|ô|ö': 'o',
    r'ú|ù|û|ü': 'u',
    r'ñ': 'n',
    r'ç': 'c',

    # Control characters
    r'[\x00-\x08\x0B-\x0C\x0E-\x1F]': '',  # Remove control characters
}


class UTF8Cleaner:
    """
    Cleans UTF-8 characters from CSV/TSV files and generates SQL.
    """

    def __init__(
        self,
        custom_replacements: Optional[Dict[str, str]] = None,
        preserve_accents: bool = False
    ):
        """
        Initialize UTF-8 cleaner.

        Args:
            custom_replacements: Additional regex patterns to replace
            preserve_accents: If True, don't replace accented characters
        """
        self.replacements = UTF8_REPLACEMENTS.copy()

        if preserve_accents:
            # Remove accent replacements
            keys_to_remove = [k for k in self.replacements.keys() if any(
                c in k for c in ['é', 'è', 'ê', 'á', 'à', 'í', 'ó', 'ú', 'ñ', 'ç']
            )]
            for key in keys_to_remove:
                del self.replacements[key]

        if custom_replacements:
            self.replacements.update(custom_replacements)

        # Compile regex patterns
        self.compiled_patterns = [
            (re.compile(pattern), replacement)
            for pattern, replacement in self.replacements.items()
        ]

        # Statistics
        self.stats = defaultdict(lambda: defaultdict(int))

    def clean_text(self, text: str, column_name: str = "unknown") -> str:
        """
        Clean UTF-8 characters from text using regex patterns.

        Args:
            text: Text to clean
            column_name: Column name for statistics

        Returns:
            Cleaned text
        """
        if pd.isna(text) or not isinstance(text, str):
            return text

        original_text = text

        for pattern, replacement in self.compiled_patterns:
            matches = pattern.findall(text)
            if matches:
                # Track statistics
                for match in matches:
                    self.stats[column_name][match] += 1
                    self.stats["_total_"]["replacements"] += 1

                text = pattern.sub(replacement, text)

        if original_text != text:
            self.stats[column_name]["_rows_modified_"] += 1

        return text

    def clean_dataframe(
        self,
        df: pd.DataFrame,
        target_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Clean UTF-8 characters from specified columns in DataFrame.

        Args:
            df: Input DataFrame
            target_columns: List of column names to clean (None = all string columns)

        Returns:
            Cleaned DataFrame
        """
        df_clean = df.copy()

        # Determine which columns to clean
        if target_columns is None:
            # Clean all object (string) columns
            target_columns = df_clean.select_dtypes(include=['object']).columns.tolist()
        else:
            # Validate column names
            missing_cols = set(target_columns) - set(df_clean.columns)
            if missing_cols:
                raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

        logger.info(f"Cleaning {len(target_columns)} columns: {target_columns}")

        # Clean each target column
        for col in target_columns:
            logger.info(f"Processing column: {col}")
            df_clean[col] = df_clean[col].apply(
                lambda x: self.clean_text(x, column_name=col)
            )

        return df_clean

    def get_statistics_summary(self) -> pd.DataFrame:
        """
        Get summary statistics of UTF-8 character replacements.

        Returns:
            DataFrame with replacement statistics
        """
        rows = []

        for column, replacements in self.stats.items():
            if column == "_total_":
                continue

            for char, count in replacements.items():
                if char == "_rows_modified_":
                    continue

                rows.append({
                    "column": column,
                    "character": repr(char),
                    "count": count,
                    "rows_modified": replacements.get("_rows_modified_", 0)
                })

        if rows:
            df_stats = pd.DataFrame(rows)
            df_stats = df_stats.sort_values(["column", "count"], ascending=[True, False])
            return df_stats
        else:
            return pd.DataFrame(columns=["column", "character", "count", "rows_modified"])

    def print_statistics(self):
        """Print statistics summary to console."""
        total_replacements = self.stats["_total_"].get("replacements", 0)

        logger.info("=" * 80)
        logger.info("UTF-8 CHARACTER REPLACEMENT STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Total replacements: {total_replacements}")
        logger.info("")

        if total_replacements > 0:
            df_stats = self.get_statistics_summary()

            # Group by column
            for column in df_stats["column"].unique():
                col_stats = df_stats[df_stats["column"] == column]
                rows_modified = col_stats["rows_modified"].iloc[0]

                logger.info(f"Column: {column}")
                logger.info(f"  Rows modified: {rows_modified}")
                logger.info(f"  Character replacements:")

                for _, row in col_stats.iterrows():
                    logger.info(f"    {row['character']:30s} → {row['count']:6d} occurrences")
                logger.info("")

        logger.info("=" * 80)


class CSVSchemaDiscovery:
    """
    Discover schema from CSV/TSV files.
    """

    @staticmethod
    def discover_schema(df: pd.DataFrame) -> pd.DataFrame:
        """
        Discover schema from DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with schema information
        """
        schema_rows = []

        for col in df.columns:
            dtype = df[col].dtype
            null_count = df[col].isna().sum()
            unique_count = df[col].nunique()

            # Infer SQL type
            if dtype == 'object':
                max_len = df[col].astype(str).str.len().max()
                sql_type = f"VARCHAR({max(max_len, 255)})"
            elif dtype == 'int64':
                sql_type = "INTEGER"
            elif dtype == 'float64':
                sql_type = "DECIMAL(15,2)"
            elif dtype == 'bool':
                sql_type = "BOOLEAN"
            elif dtype == 'datetime64[ns]':
                sql_type = "TIMESTAMP"
            else:
                sql_type = "TEXT"

            schema_rows.append({
                "column_name": col,
                "pandas_dtype": str(dtype),
                "sql_type": sql_type,
                "null_count": null_count,
                "unique_count": unique_count,
                "nullable": "YES" if null_count > 0 else "NO"
            })

        return pd.DataFrame(schema_rows)


class SQLGenerator:
    """
    Generate SQL statements for staging and production tables.
    """

    def __init__(self, table_name: str, schema: str = "staging"):
        """
        Initialize SQL generator.

        Args:
            table_name: Name of the table
            schema: Schema name (default: staging)
        """
        self.table_name = table_name
        self.schema = schema

    def generate_create_table(self, schema_df: pd.DataFrame, staging: bool = True) -> str:
        """
        Generate CREATE TABLE statement.

        Args:
            schema_df: DataFrame with schema information
            staging: If True, create staging table with extra columns

        Returns:
            CREATE TABLE SQL statement
        """
        schema_suffix = "_staging" if staging else ""
        table_full_name = f"{self.schema}.{self.table_name}{schema_suffix}"

        columns = []

        # Add ID column for staging
        if staging:
            columns.append("    id SERIAL PRIMARY KEY")

        # Add data columns
        for _, row in schema_df.iterrows():
            col_name = row["column_name"]
            sql_type = row["sql_type"]
            nullable = "NULL" if row["nullable"] == "YES" else "NOT NULL"

            columns.append(f"    {col_name} {sql_type} {nullable}")

        # Add metadata columns for staging
        if staging:
            columns.extend([
                "    created_at TIMESTAMP DEFAULT NOW()",
                "    updated_at TIMESTAMP DEFAULT NOW()",
                "    status VARCHAR(50) DEFAULT 'pending'"
            ])

        sql = f"CREATE TABLE IF NOT EXISTS {table_full_name} (\n"
        sql += ",\n".join(columns)
        sql += "\n);"

        return sql

    def generate_insert(self, df: pd.DataFrame, staging: bool = True) -> str:
        """
        Generate INSERT statements.

        Args:
            df: DataFrame with data
            staging: If True, generate for staging table

        Returns:
            INSERT SQL statements
        """
        schema_suffix = "_staging" if staging else ""
        table_full_name = f"{self.schema}.{self.table_name}{schema_suffix}"

        # Replace NaN with empty string
        df_clean = df.replace(np.nan, "", regex=True)
        # Escape single quotes
        df_clean = df_clean.replace("'", "''", regex=True)

        columns = ", ".join(df_clean.columns)

        insert_statements = []

        for _, row in df_clean.iterrows():
            values = ", ".join(f"'{str(val)}'" for val in row.values)
            sql = f"INSERT INTO {table_full_name} ({columns}) VALUES ({values});"
            insert_statements.append(sql)

        return "\n".join(insert_statements)

    def generate_staging_to_prod(self, schema_df: pd.DataFrame) -> str:
        """
        Generate SQL to move data from staging to production.

        Args:
            schema_df: DataFrame with schema information

        Returns:
            INSERT INTO ... SELECT SQL statement
        """
        staging_table = f"{self.schema}.{self.table_name}_staging"
        prod_table = f"{self.schema}.{self.table_name}"

        columns = ", ".join(schema_df["column_name"].tolist())

        sql = f"""
-- Move data from staging to production
INSERT INTO {prod_table} ({columns})
SELECT {columns}
FROM {staging_table}
WHERE status = 'validated';

-- Mark staged records as processed
UPDATE {staging_table}
SET status = 'processed', updated_at = NOW()
WHERE status = 'validated';
"""
        return sql


def parse_csv_to_df(path: str, separator: str = ",") -> pd.DataFrame:
    """
    Parse CSV/TSV file to DataFrame.

    Args:
        path: Path to CSV/TSV file
        separator: Field separator (default: comma)

    Returns:
        DataFrame
    """
    logger.info(f"Reading file: {path}")
    df = pd.read_csv(path, header=0, sep=separator, dtype=object)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def process_single_file(
    input_path: str,
    target_columns: Optional[List[str]],
    table_name: str,
    output_path: Optional[str],
    separator: str = ",",
    preserve_accents: bool = False,
    schema: str = "staging"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process a single CSV/TSV file.

    Args:
        input_path: Path to input file
        target_columns: Columns to clean (None = all string columns)
        table_name: SQL table name
        output_path: Path to output SQL file
        separator: Field separator
        preserve_accents: Preserve accented characters
        schema: Database schema name

    Returns:
        Tuple of (cleaned DataFrame, statistics DataFrame)
    """
    # Read CSV
    df = parse_csv_to_df(input_path, separator=separator)

    # Discover schema
    logger.info("Discovering schema...")
    schema_df = CSVSchemaDiscovery.discover_schema(df)
    logger.info(f"Schema:\n{schema_df.to_string()}")

    # Clean UTF-8 characters
    logger.info("Cleaning UTF-8 characters...")
    cleaner = UTF8Cleaner(preserve_accents=preserve_accents)
    df_clean = cleaner.clean_dataframe(df, target_columns=target_columns)

    # Print statistics
    cleaner.print_statistics()
    stats_df = cleaner.get_statistics_summary()

    # Generate SQL
    if output_path:
        logger.info(f"Generating SQL statements...")
        sql_gen = SQLGenerator(table_name=table_name, schema=schema)

        with open(output_path, "w", encoding="utf-8") as f:
            # Create staging table
            f.write("-- Create staging table\n")
            f.write(sql_gen.generate_create_table(schema_df, staging=True))
            f.write("\n\n")

            # Create production table
            f.write("-- Create production table\n")
            f.write(sql_gen.generate_create_table(schema_df, staging=False))
            f.write("\n\n")

            # Insert data into staging
            f.write("-- Insert data into staging table\n")
            f.write(sql_gen.generate_insert(df_clean, staging=True))
            f.write("\n\n")

            # Staging to production transfer
            f.write("-- Transfer from staging to production\n")
            f.write(sql_gen.generate_staging_to_prod(schema_df))
            f.write("\n")

        logger.info(f"SQL written to: {output_path}")

    return df_clean, stats_df


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Clean UTF-8 characters from CSV/TSV and generate SQL"
    )

    # Input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input", "-i",
        help="Input CSV/TSV file path"
    )
    input_group.add_argument(
        "--input-dir",
        help="Directory containing CSV/TSV files (batch mode)"
    )

    # Column targeting
    parser.add_argument(
        "--column", "-c",
        action="append",
        help="Column name to clean (can specify multiple times). If not specified, cleans all string columns."
    )

    # Output options
    parser.add_argument(
        "--output", "-o",
        help="Output SQL file path (default: <input_name>.sql)"
    )

    parser.add_argument(
        "--output-dir",
        help="Output directory for SQL files (batch mode)"
    )

    # Table options
    parser.add_argument(
        "--table", "-t",
        help="SQL table name (default: derived from filename)"
    )

    parser.add_argument(
        "--schema", "-s",
        default="staging",
        help="Database schema name (default: staging)"
    )

    # Separator
    parser.add_argument(
        "--separator",
        default=",",
        help='Field separator (default: ","  for CSV, use "\\t" for TSV)'
    )

    # Options
    parser.add_argument(
        "--preserve-accents",
        action="store_true",
        help="Preserve accented characters (é, à, etc.)"
    )

    parser.add_argument(
        "--stats-output",
        help="Output path for statistics CSV file"
    )

    args = parser.parse_args()

    # Handle separator escape sequences
    separator = args.separator.replace("\\t", "\t").replace("\\n", "\n")

    # Single file mode
    if args.input:
        input_path = Path(args.input)

        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            sys.exit(1)

        # Determine table name
        table_name = args.table or input_path.stem

        # Determine output path
        output_path = args.output or f"sql/{table_name}.sql"
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        # Process file
        df_clean, stats_df = process_single_file(
            input_path=str(input_path),
            target_columns=args.column,
            table_name=table_name,
            output_path=output_path,
            separator=separator,
            preserve_accents=args.preserve_accents,
            schema=args.schema
        )

        # Save statistics
        if args.stats_output:
            stats_df.to_csv(args.stats_output, index=False)
            logger.info(f"Statistics saved to: {args.stats_output}")

        logger.info("✓ Processing complete!")

    # Batch mode
    elif args.input_dir:
        input_dir = Path(args.input_dir)
        output_dir = Path(args.output_dir or "sql")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Find all CSV/TSV files
        csv_files = list(input_dir.glob("*.csv")) + list(input_dir.glob("*.tsv"))

        if not csv_files:
            logger.error(f"No CSV/TSV files found in: {input_dir}")
            sys.exit(1)

        logger.info(f"Found {len(csv_files)} files to process")

        all_stats = []

        for csv_file in csv_files:
            logger.info(f"\n{'='*80}")
            logger.info(f"Processing: {csv_file.name}")
            logger.info(f"{'='*80}")

            table_name = csv_file.stem
            output_path = output_dir / f"{table_name}.sql"

            # Detect separator
            file_separator = "\t" if csv_file.suffix == ".tsv" else separator

            try:
                df_clean, stats_df = process_single_file(
                    input_path=str(csv_file),
                    target_columns=args.column,
                    table_name=table_name,
                    output_path=str(output_path),
                    separator=file_separator,
                    preserve_accents=args.preserve_accents,
                    schema=args.schema
                )

                stats_df["file"] = csv_file.name
                all_stats.append(stats_df)

            except Exception as e:
                logger.error(f"Error processing {csv_file.name}: {e}")
                continue

        # Save combined statistics
        if all_stats and args.stats_output:
            combined_stats = pd.concat(all_stats, ignore_index=True)
            combined_stats.to_csv(args.stats_output, index=False)
            logger.info(f"\nCombined statistics saved to: {args.stats_output}")

        logger.info(f"\n✓ Batch processing complete! Processed {len(csv_files)} files")


if __name__ == "__main__":
    main()
