#!/usr/bin/env python3
"""
Communication Aggregation Pipeline
===================================

Translates Power Query nested join + aggregation logic to PostgreSQL + Python.

Power Query Pattern Translated:
1. Deduplicate communications (keep earliest by CreateDate)
2. Parse semicolon/comma-separated values
3. Aggregate contact/company IDs into lists
4. Extract and aggregate emails
5. Count contacts and companies per communication

Meta-Cognitive Principles:
- Map/Reduce pattern for row-by-row transformations
- Vectorized operations where possible
- PostgreSQL for set operations
- Python for complex parsing logic

Usage:
    from communication_aggregator import CommunicationAggregator

    agg = CommunicationAggregator()
    result_df = agg.process_communications(
        staging_table='communications_staging'
    )
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple, Set
from loguru import logger
from postgres_connection_manager import PostgreSQLManager
import re


class CommunicationAggregator:
    """
    Aggregates and processes communication records.

    Translates Power Query transformations:
    - List.Split → string.split + list comprehension
    - List.Transform → map operations
    - List.Distinct → set operations
    - Text.Combine → str.join
    """

    def __init__(self, pg_manager: Optional[PostgreSQLManager] = None):
        """Initialize aggregator."""
        self.pg = pg_manager or PostgreSQLManager()
        logger.info("CommunicationAggregator initialized")

    # =========================================================================
    # PARSING FUNCTIONS - Map/Reduce Pattern
    # =========================================================================

    @staticmethod
    def parse_separated_values(
        value: str,
        separator: str = ';',
        strip: bool = True
    ) -> List[str]:
        """
        Parse separated string into list.

        Power Query equivalent:
            Text.Split([Field], ";")

        Meta-Cognitive: Vectorize this operation with pandas .apply()
        """
        if pd.isna(value) or value == '':
            return []

        parts = str(value).split(separator)

        if strip:
            parts = [p.strip() for p in parts]

        # Remove empty strings
        parts = [p for p in parts if p]

        return parts

    @staticmethod
    def parse_id_list(value: str, separator: str = ',') -> List[int]:
        """
        Parse comma-separated IDs into list of integers.

        Power Query equivalent:
            List.Transform(
                Text.Split(Text.From([IDs]), ","),
                each try Number.From(Text.Trim(_)) otherwise null
            )
        """
        if pd.isna(value) or value == '':
            return []

        parts = str(value).split(separator)
        ids = []

        for part in parts:
            try:
                id_val = int(part.strip())
                ids.append(id_val)
            except (ValueError, AttributeError):
                # Skip invalid IDs
                continue

        return ids

    @staticmethod
    def parse_emails(value: str, separator: str = ';') -> List[str]:
        """
        Parse email addresses from string.

        Handles semicolon-separated emails.
        Can be extended to validate email format.
        """
        if pd.isna(value) or value == '':
            return []

        emails = str(value).split(separator)
        emails = [e.strip() for e in emails if e.strip()]

        return emails

    @staticmethod
    def deduplicate_list(items: List) -> List:
        """
        Remove duplicates while preserving order.

        Power Query equivalent:
            List.Distinct(list)
        """
        seen = set()
        result = []
        for item in items:
            if item not in seen:
                seen.add(item)
                result.append(item)
        return result

    # =========================================================================
    # STEP 1: DEDUPLICATION - Keep Earliest Communication
    # =========================================================================

    def deduplicate_communications(
        self,
        df: pd.DataFrame,
        id_column: str = 'Communication_Record ID',
        date_column: str = 'Communication_CreateDate'
    ) -> pd.DataFrame:
        """
        Deduplicate communications, keeping the earliest record.

        Power Query equivalent:
            #"Sorted Communications" = Table.Sort(#"Changed Type", {
                {"Communication_Record ID", Order.Ascending},
                {"Communication_CreateDate", Order.Ascending}
            }),
            #"Deduplicated" = Table.Distinct(#"Sorted Communications",
                {"Communication_Record ID"}
            )

        Meta-Cognitive: pandas.drop_duplicates with keep='first'
        """
        logger.info(f"Deduplicating communications by {id_column}")
        logger.info(f"  Before: {len(df)} records")

        # Convert date column to datetime
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

        # Sort by ID and date (ascending)
        df_sorted = df.sort_values(
            by=[id_column, date_column],
            ascending=[True, True]
        )

        # Keep first occurrence (earliest)
        df_dedup = df_sorted.drop_duplicates(
            subset=[id_column],
            keep='first'
        ).reset_index(drop=True)

        logger.info(f"  After: {len(df_dedup)} records")
        logger.info(f"  Removed: {len(df) - len(df_dedup)} duplicates")

        return df_dedup

    # =========================================================================
    # STEP 2: PARSE CONTACTS - Extract Contact Names and IDs
    # =========================================================================

    def parse_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse contact information into lists.

        Power Query equivalent:
            #"Added Contact List" = Table.AddColumn(..., "Contact_Names_List",
                each Text.Split([Associated Contact], ";")
            )
        """
        logger.info("Parsing contact information")

        df = df.copy()

        # Parse contact names (semicolon-separated)
        df['Contact_Names_List'] = df['Associated Contact'].apply(
            lambda x: self.parse_separated_values(x, separator=';')
        )

        # Parse contact IDs (comma-separated)
        df['ContactID_List'] = df['Associated Contact IDs'].apply(
            lambda x: self.parse_id_list(x, separator=',')
        )

        logger.info(f"  Parsed contact names and IDs")

        return df

    # =========================================================================
    # STEP 3: PARSE COMPANIES - Extract Company Names and IDs
    # =========================================================================

    def parse_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse company information from multiple sources.

        Power Query equivalent:
            #"Added Company List" = Table.AddColumn(..., "Company_Names_List",
                each let
                    allSources = List.Combine({
                        Text.Split([Company_Com], ";"),
                        {[Associated Company]}
                    }),
                    cleaned = List.Distinct(List.RemoveNulls(allSources))
                in cleaned
            )
        """
        logger.info("Parsing company information")

        df = df.copy()

        def combine_company_names(row):
            """Combine company names from multiple sources."""
            names = []

            # From Company_Com (semicolon-separated)
            names.extend(self.parse_separated_values(row['Company_Com'], separator=';'))

            # From Associated Company (single value)
            if pd.notna(row['Associated Company']) and row['Associated Company'].strip():
                names.append(row['Associated Company'].strip())

            # Remove duplicates
            return self.deduplicate_list(names)

        df['Company_Names_List'] = df.apply(combine_company_names, axis=1)

        def combine_company_ids(row):
            """Combine company IDs from multiple sources."""
            ids = []

            # From Company_Com IDs
            ids.extend(self.parse_id_list(row['Company_Com IDs'], separator=','))

            # From Associated Company IDs
            ids.extend(self.parse_id_list(row['Associated Company IDs'], separator=','))

            # From icAlps_company ID (single value)
            if pd.notna(row['icAlps_company ID']):
                try:
                    ids.append(int(row['icAlps_company ID']))
                except (ValueError, TypeError):
                    pass

            # Remove duplicates
            return self.deduplicate_list(ids)

        df['CompanyID_List'] = df.apply(combine_company_ids, axis=1)

        logger.info(f"  Parsed company names and IDs")

        return df

    # =========================================================================
    # STEP 4: PARSE EMAILS - Extract From/To Emails
    # =========================================================================

    def parse_email_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse email addresses from Communication_From and Communication_To.

        Power Query equivalent:
            #"Added From Emails" = Table.AddColumn(..., "From_Emails_List",
                each Text.Split([Communication_From], ";")
            )
        """
        logger.info("Parsing email fields")

        df = df.copy()

        # Parse From emails
        df['From_Emails_List'] = df['Communication_From'].apply(
            lambda x: self.parse_emails(x, separator=';')
        )

        # Parse To emails
        df['To_Emails_List'] = df['Communication_To'].apply(
            lambda x: self.parse_emails(x, separator=';')
        )

        logger.info(f"  Parsed from/to emails")

        return df

    # =========================================================================
    # STEP 5: AGGREGATE - Create Text Fields and Counts
    # =========================================================================

    def create_aggregated_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create aggregated text fields and counts.

        Power Query equivalent:
            #"Added Contact Names Text" = Table.AddColumn(...,
                "Contact_Names_Aggregated",
                each Text.Combine([Contact_Names_List], "; ")
            )
        """
        logger.info("Creating aggregated fields")

        df = df.copy()

        # Contact aggregations
        df['Contact_Names_Aggregated'] = df['Contact_Names_List'].apply(
            lambda x: '; '.join(x) if x else None
        )

        df['Contact_IDs_Aggregated'] = df['ContactID_List'].apply(
            lambda x: ', '.join(map(str, x)) if x else None
        )

        df['Contact_Count'] = df['Contact_Names_List'].apply(len)

        # Company aggregations
        df['Company_Names_Aggregated'] = df['Company_Names_List'].apply(
            lambda x: '; '.join(x) if x else None
        )

        df['Company_IDs_Aggregated'] = df['CompanyID_List'].apply(
            lambda x: ', '.join(map(str, x)) if x else None
        )

        df['Company_Count'] = df['Company_Names_List'].apply(len)

        # Email aggregations
        df['From_Emails_Aggregated'] = df['From_Emails_List'].apply(
            lambda x: '; '.join(x) if x else None
        )

        df['To_Emails_Aggregated'] = df['To_Emails_List'].apply(
            lambda x: '; '.join(x) if x else None
        )

        logger.info(f"  Created aggregated fields")

        return df

    # =========================================================================
    # STEP 6: CLEANUP - Remove Intermediate Columns
    # =========================================================================

    def cleanup_intermediate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove intermediate list columns.

        Power Query equivalent:
            #"Removed List Columns" = Table.RemoveColumns(...)
        """
        logger.info("Cleaning up intermediate columns")

        columns_to_remove = [
            'Contact_Names_List',
            'ContactID_List',
            'Company_Names_List',
            'CompanyID_List',
            'From_Emails_List',
            'To_Emails_List'
        ]

        # Only remove columns that exist
        columns_to_remove = [c for c in columns_to_remove if c in df.columns]

        df = df.drop(columns=columns_to_remove)

        logger.info(f"  Removed {len(columns_to_remove)} intermediate columns")

        return df

    # =========================================================================
    # MAIN PIPELINE - Compose All Steps
    # =========================================================================

    def process_communications(
        self,
        staging_table: str = 'communications_staging',
        schema: str = 'staging',
        output_table: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Complete communication processing pipeline.

        Meta-Cognitive: Functional composition
        Step 1 → Step 2 → Step 3 → Step 4 → Step 5 → Step 6

        Args:
            staging_table: Input table name
            schema: Database schema
            output_table: Output table name (if None, returns DataFrame only)

        Returns:
            Processed DataFrame
        """
        logger.info("="*80)
        logger.info("COMMUNICATION AGGREGATION PIPELINE")
        logger.info("="*80)

        # Load data from staging
        query = f"SELECT * FROM {schema}.{staging_table}"
        df = self.pg.execute_query_df(query)

        logger.info(f"Loaded {len(df)} records from {schema}.{staging_table}")

        # Apply transformations sequentially
        df = self.deduplicate_communications(df)
        df = self.parse_contacts(df)
        df = self.parse_companies(df)
        df = self.parse_email_fields(df)
        df = self.create_aggregated_fields(df)
        df = self.cleanup_intermediate_columns(df)

        logger.info(f"Processing complete: {len(df)} records")

        # Write to output table if specified
        if output_table:
            logger.info(f"Writing results to {schema}.{output_table}")

            with self.pg.get_connection() as conn:
                df.to_sql(
                    output_table,
                    conn,
                    schema=schema,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )

            logger.info(f"✓ Results written to {schema}.{output_table}")

        logger.info("="*80)

        return df


# =============================================================================
# POSTGRESQL-BASED AGGREGATION (Alternative Approach)
# =============================================================================

class CommunicationAggregatorSQL:
    """
    Alternative: Do aggregations in PostgreSQL using CTEs and JSONB.

    Meta-Cognitive: Push computation to database
    - Faster for large datasets
    - Leverages PostgreSQL's text processing functions
    - Uses JSONB arrays instead of Python lists
    """

    def __init__(self, pg_manager: Optional[PostgreSQLManager] = None):
        """Initialize SQL-based aggregator."""
        self.pg = pg_manager or PostgreSQLManager()

    def process_communications_sql(
        self,
        staging_table: str = 'communications_staging',
        schema: str = 'staging',
        output_table: str = 'communications_processed'
    ):
        """
        Process communications using pure SQL.

        Uses PostgreSQL features:
        - string_to_array() for parsing
        - array_agg() for aggregation
        - DISTINCT for deduplication
        - CTEs for readability
        """
        logger.info("Processing communications with SQL")

        sql = f"""
        WITH deduplicated AS (
            -- Step 1: Deduplicate (keep earliest)
            SELECT DISTINCT ON ("Communication_Record ID")
                *
            FROM {schema}.{staging_table}
            ORDER BY "Communication_Record ID", "Communication_CreateDate"
        ),
        parsed AS (
            -- Step 2-4: Parse lists
            SELECT
                *,
                -- Parse contact names
                string_to_array("Associated Contact", ';') AS contact_names_array,
                -- Parse contact IDs
                string_to_array("Associated Contact IDs", ',')::integer[] AS contact_ids_array,
                -- Parse company names
                array_cat(
                    string_to_array("Company_Com", ';'),
                    ARRAY["Associated Company"]
                ) AS company_names_array,
                -- Parse company IDs
                array_cat(
                    string_to_array("Company_Com IDs", ',')::integer[],
                    array_cat(
                        string_to_array("Associated Company IDs", ',')::integer[],
                        ARRAY["icAlps_company ID"]::integer[]
                    )
                ) AS company_ids_array,
                -- Parse emails
                string_to_array("Communication_From", ';') AS from_emails_array,
                string_to_array("Communication_To", ';') AS to_emails_array
            FROM deduplicated
        ),
        aggregated AS (
            -- Step 5: Aggregate and count
            SELECT
                *,
                -- Contact aggregations
                array_to_string(contact_names_array, '; ') AS "Contact_Names_Aggregated",
                array_to_string(contact_ids_array, ', ') AS "Contact_IDs_Aggregated",
                array_length(contact_names_array, 1) AS "Contact_Count",
                -- Company aggregations
                array_to_string(company_names_array, '; ') AS "Company_Names_Aggregated",
                array_to_string(company_ids_array, ', ') AS "Company_IDs_Aggregated",
                array_length(company_names_array, 1) AS "Company_Count",
                -- Email aggregations
                array_to_string(from_emails_array, '; ') AS "From_Emails_Aggregated",
                array_to_string(to_emails_array, '; ') AS "To_Emails_Aggregated"
            FROM parsed
        )
        -- Step 6: Final selection (remove intermediate arrays)
        SELECT
            "Communication_Record ID",
            "Record ID",
            "Communication Subject",
            "communication type",
            "Communication_CreateDate",
            "Communication Notes",
            "Communication_From",
            "From_Emails_Aggregated",
            "Communication_To",
            "To_Emails_Aggregated",
            "Communication_PersonID",
            "icAlps_company ID",
            "Associated Contact",
            "Contact_Names_Aggregated",
            "Associated Contact IDs",
            "Contact_IDs_Aggregated",
            "Contact_Count",
            "Company_Com",
            "Associated Company",
            "Company_Names_Aggregated",
            "Company_Com IDs",
            "Associated Company IDs",
            "Company_IDs_Aggregated",
            "Company_Count"
        INTO {schema}.{output_table}
        FROM aggregated;
        """

        self.pg.execute_query(sql, fetch=False)

        logger.info(f"✓ Communications processed and saved to {schema}.{output_table}")


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Example 1: Python-based processing
    agg = CommunicationAggregator()
    result_df = agg.process_communications(
        staging_table='communications_staging',
        output_table='communications_processed'
    )

    print(f"\nProcessed {len(result_df)} communications")
    print("\nSample records:")
    print(result_df[['Communication_Record ID', 'Contact_Count', 'Company_Count']].head())

    # Example 2: SQL-based processing (faster for large datasets)
    # agg_sql = CommunicationAggregatorSQL()
    # agg_sql.process_communications_sql(
    #     staging_table='communications_staging',
    #     output_table='communications_processed'
    # )
