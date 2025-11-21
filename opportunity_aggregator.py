#!/usr/bin/env python3
"""
Opportunity/Deal Aggregation Pipeline
======================================

Translates Power Query deal aggregation logic to PostgreSQL + Python.

Power Query Pattern from your code:
1. Nested join with HubspotPerson (contacts)
2. Nested join with HubspotCompany (companies)
3. Group by icalps_deal_id
4. Aggregate Contact_RecordID_List from nested HubspotPerson tables
5. Aggregate Company_RecordID_List from nested HubspotCompany tables
6. Aggregate Company_Name_List from text fields
7. Count contacts and companies per deal

Meta-Cognitive:
- Uses PostgreSQL array functions (string_to_array, array_agg, DISTINCT)
- Handles semicolon-separated company names
- Nested table expansion pattern from Power Query
- List aggregation with deduplication

Usage:
    from opportunity_aggregator import OpportunityAggregator

    agg = OpportunityAggregator()
    result_df = agg.process_opportunities(
        staging_table='deals_staging'
    )
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple, Set
from loguru import logger
from postgres_connection_manager import PostgreSQLManager


class OpportunityAggregator:
    """
    Aggregates and processes opportunity/deal records.

    Translates Power Query pattern:
    - NestedJoin with HubspotPerson → LEFT JOIN + array_agg
    - NestedJoin with HubspotCompany → LEFT JOIN + array_agg
    - Table.Group with list aggregation → GROUP BY + string_agg
    """

    def __init__(self, pg_manager: Optional[PostgreSQLManager] = None):
        """Initialize aggregator."""
        self.pg = pg_manager or PostgreSQLManager()
        logger.info("OpportunityAggregator initialized")

    # =========================================================================
    # PYTHON-BASED AGGREGATION (For moderate datasets)
    # =========================================================================

    @staticmethod
    def parse_company_names(primary_company: str, other_companies: str) -> List[str]:
        """
        Parse and combine company names from multiple sources.

        Power Query equivalent:
            {"Company_Name_List", each
                let
                    allCompanies = List.Combine({
                        [#"Associated Company (Primary)"],
                        List.Transform(
                            Text.Split([#"Other Associated company"], ";"),
                            Text.Trim
                        )
                    }),
                    cleanedCompanies = List.Distinct(
                        List.RemoveNulls(
                            List.Select(allCompanies, each _ <> "")
                        )
                    )
                in
                    if List.IsEmpty(cleanedCompanies) then null
                    else Text.Combine(cleanedCompanies, "; ")
            }
        """
        companies = []

        # Add primary company
        if pd.notna(primary_company) and str(primary_company).strip():
            companies.append(str(primary_company).strip())

        # Add other companies (semicolon-separated)
        if pd.notna(other_companies) and str(other_companies).strip():
            other = [c.strip() for c in str(other_companies).split(';') if c.strip()]
            companies.extend(other)

        # Deduplicate while preserving order
        seen = set()
        unique_companies = []
        for company in companies:
            if company not in seen:
                seen.add(company)
                unique_companies.append(company)

        return unique_companies

    def aggregate_deals_python(
        self,
        deals_df: pd.DataFrame,
        contacts_df: Optional[pd.DataFrame] = None,
        companies_df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Aggregate deals using pandas (Python approach).

        Args:
            deals_df: Deal/opportunity records
            contacts_df: Contact records (for joining)
            companies_df: Company records (for joining)

        Returns:
            Aggregated DataFrame with contact and company lists
        """
        logger.info("Aggregating deals using Python/pandas")

        df = deals_df.copy()

        # Parse company names
        logger.info("Parsing company names from text fields")
        df['Company_Names_List'] = df.apply(
            lambda row: self.parse_company_names(
                row.get('Associated Company (Primary)', ''),
                row.get('Other Associated company', '')
            ),
            axis=1
        )

        # Aggregate company names
        df['Company_Name_List'] = df['Company_Names_List'].apply(
            lambda x: '; '.join(x) if x else None
        )

        df['Company_Count'] = df['Company_Names_List'].apply(len)

        # Join with contacts if provided
        if contacts_df is not None:
            logger.info("Joining with contacts table")
            # Group contacts by deal
            contact_agg = contacts_df.groupby('icalps_deal_id').agg({
                'icalps_contact_id': lambda x: list(x.dropna().unique()),
                'Record ID': lambda x: list(x.dropna().unique())
            }).reset_index()

            contact_agg.columns = ['icalps_deal_id', 'contact_ids_list', 'contact_record_ids_list']

            # Merge with deals
            df = df.merge(contact_agg, on='icalps_deal_id', how='left')

            # Format as text
            df['Contact_RecordID_List'] = df['contact_record_ids_list'].apply(
                lambda x: ', '.join(map(str, x)) if isinstance(x, list) and x else None
            )

            df['Contact_Count'] = df['contact_ids_list'].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
        else:
            df['Contact_RecordID_List'] = None
            df['Contact_Count'] = 0

        # Join with companies if provided
        if companies_df is not None:
            logger.info("Joining with companies table")
            # Group companies by deal
            company_agg = companies_df.groupby('icalps_deal_id').agg({
                'icalps_company_id': lambda x: list(x.dropna().unique()),
                'Record ID': lambda x: list(x.dropna().unique())
            }).reset_index()

            company_agg.columns = ['icalps_deal_id', 'company_ids_list', 'company_record_ids_list']

            # Merge with deals
            df = df.merge(company_agg, on='icalps_deal_id', how='left')

            # Format as text
            df['Company_RecordID_List'] = df['company_record_ids_list'].apply(
                lambda x: ', '.join(map(str, x)) if isinstance(x, list) and x else None
            )

            df['Company_IcAlpsID_List'] = df['company_ids_list'].apply(
                lambda x: ', '.join(map(str, x)) if isinstance(x, list) and x else None
            )
        else:
            df['Company_RecordID_List'] = None
            df['Company_IcAlpsID_List'] = None

        # Clean up intermediate columns
        columns_to_drop = [
            'Company_Names_List',
            'contact_ids_list',
            'contact_record_ids_list',
            'company_ids_list',
            'company_record_ids_list'
        ]
        df = df.drop(columns=[c for c in columns_to_drop if c in df.columns], errors='ignore')

        logger.info(f"Aggregated {len(df)} deals")

        return df

    # =========================================================================
    # SQL-BASED AGGREGATION (Faster for large datasets)
    # =========================================================================

    def aggregate_deals_sql(
        self,
        deals_table: str = 'deals_staging',
        contacts_table: str = 'contacts_staging',
        companies_table: str = 'companies_staging',
        output_table: str = 'deals_aggregated',
        schema: str = 'staging'
    ):
        """
        Aggregate deals using pure PostgreSQL (faster for large datasets).

        Leverages PostgreSQL features:
        - string_to_array() for parsing semicolon-separated values
        - array_agg() for aggregating joined records
        - array_cat() for combining arrays
        - string_agg(DISTINCT) for deduplication
        - CTEs for readability

        This directly translates your Power Query pattern:
        1. Nested join with contacts
        2. Nested join with companies
        3. Group by deal_id
        4. Aggregate lists from nested tables
        """
        logger.info("Aggregating deals using SQL")

        sql = f"""
        WITH deals_with_parsed_companies AS (
            -- Parse company names from text fields (semicolon-separated)
            SELECT
                d.*,
                -- Parse "Associated Company (Primary)" and "Other Associated company"
                array_cat(
                    -- Primary company (single value)
                    CASE
                        WHEN "Associated Company (Primary)" IS NOT NULL
                             AND "Associated Company (Primary)" <> ''
                        THEN ARRAY["Associated Company (Primary)"]
                        ELSE ARRAY[]::text[]
                    END,
                    -- Other companies (semicolon-separated)
                    CASE
                        WHEN "Other Associated company" IS NOT NULL
                             AND "Other Associated company" <> ''
                        THEN string_to_array("Other Associated company", ';')
                        ELSE ARRAY[]::text[]
                    END
                ) AS company_names_array
            FROM {schema}.{deals_table} d
        ),

        deals_with_contacts AS (
            -- Join with contacts (equivalent to NestedJoin in Power Query)
            SELECT
                d.icalps_deal_id,
                d.*,
                c."Record ID" AS contact_record_id,
                c.icalps_contact_id
            FROM deals_with_parsed_companies d
            LEFT JOIN {schema}.{contacts_table} c
                ON d."sageCRM.Oppo_PrimaryPersonId" = c.icalps_contact_id
        ),

        deals_with_companies AS (
            -- Join with companies (equivalent to second NestedJoin)
            SELECT
                dc.*,
                comp."Record ID" AS company_record_id,
                comp.icalps_company_id
            FROM deals_with_contacts dc
            LEFT JOIN {schema}.{companies_table} comp
                ON dc.icalps_company_id = comp.icalps_company_id
        ),

        aggregated_deals AS (
            -- Group and aggregate (equivalent to Table.Group in Power Query)
            SELECT
                icalps_deal_id,

                -- Keep first occurrence of deal data
                (array_agg("Associated Company (Primary)"))[1] AS "Associated Company (Primary)",
                (array_agg("Other Associated company"))[1] AS "Other Associated company",
                (array_agg("sageCRM.Oppo_PrimaryPersonId"))[1] AS "sageCRM.Oppo_PrimaryPersonId",
                (array_agg("sageCRM.Oppo_Certainty"))[1] AS "sageCRM.Oppo_Certainty",
                (array_agg("sageCRM.Oppo_Description"))[1] AS "sageCRM.Oppo_Description",
                (array_agg("sageCRM.Oppo_Stage"))[1] AS "sageCRM.Oppo_Stage",
                (array_agg("sageCRM.Oppo_Status"))[1] AS "sageCRM.Oppo_Status",
                (array_agg("sageCRM.Oppo_CreatedDate"))[1] AS "sageCRM.Oppo_CreatedDate",
                (array_agg("sageCRM.Oppo_UpdatedDate"))[1] AS "sageCRM.Oppo_UpdatedDate",
                (array_agg(icalps_company_id))[1] AS icalps_company_id,

                -- Count contacts
                COUNT(DISTINCT contact_record_id) AS "Contact_Count",

                -- Aggregate Contact Record IDs (from nested HubspotPerson table)
                -- Equivalent to Power Query's List.Combine + List.Transform + Text.Combine
                string_agg(
                    DISTINCT contact_record_id::text,
                    ', '
                    ORDER BY contact_record_id::text
                ) AS "Contact_RecordID_List",

                -- Count companies
                COUNT(DISTINCT company_record_id) AS "Company_Count",

                -- Aggregate Company Record IDs (from nested HubspotCompany table)
                string_agg(
                    DISTINCT company_record_id::text,
                    ', '
                    ORDER BY company_record_id::text
                ) AS "Company_RecordID_List",

                -- Aggregate Company Names (from parsed text fields)
                -- Trim and deduplicate
                array_to_string(
                    ARRAY(
                        SELECT DISTINCT trim(unnest(
                            (array_agg(company_names_array))[1]
                        ))
                        WHERE trim(unnest(
                            (array_agg(company_names_array))[1]
                        )) <> ''
                        ORDER BY 1
                    ),
                    '; '
                ) AS "Company_Name_List",

                -- Keep icalps_company_id list (for reference)
                string_agg(
                    DISTINCT icalps_company_id::text,
                    ', '
                    ORDER BY icalps_company_id::text
                ) AS "Company_IcAlpsID_List"

            FROM deals_with_companies
            GROUP BY icalps_deal_id
        )

        -- Final selection with reordered columns
        SELECT
            icalps_deal_id,
            "Associated Company (Primary)",
            "Other Associated company",
            "Company_Name_List",
            "Company_RecordID_List",
            "Company_IcAlpsID_List",
            "Company_Count",
            "Contact_RecordID_List",
            "Contact_Count",
            "sageCRM.Oppo_PrimaryPersonId",
            "sageCRM.Oppo_Certainty",
            "sageCRM.Oppo_Description",
            "sageCRM.Oppo_Stage",
            "sageCRM.Oppo_Status",
            "sageCRM.Oppo_CreatedDate",
            "sageCRM.Oppo_UpdatedDate",
            icalps_company_id
        INTO {schema}.{output_table}
        FROM aggregated_deals;
        """

        self.pg.execute_query(sql, fetch=False)

        logger.info(f"✓ Deals aggregated and saved to {schema}.{output_table}")

        # Get count
        count_query = f"SELECT COUNT(*) as count FROM {schema}.{output_table}"
        result = self.pg.execute_query(count_query)
        count = result[0]['count'] if result else 0

        logger.info(f"  Total deals: {count}")

    # =========================================================================
    # MAIN PIPELINE
    # =========================================================================

    def process_opportunities(
        self,
        staging_table: str = 'deals_staging',
        contacts_table: str = 'contacts_staging',
        companies_table: str = 'companies_staging',
        output_table: Optional[str] = None,
        schema: str = 'staging',
        use_sql: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        Complete opportunity aggregation pipeline.

        Args:
            staging_table: Input deals/opportunities table
            contacts_table: Contacts table (for joining)
            companies_table: Companies table (for joining)
            output_table: Output table name (if None, returns DataFrame only)
            schema: Database schema
            use_sql: If True, use SQL-based aggregation (faster for large data)

        Returns:
            Aggregated DataFrame (only if use_sql=False)
        """
        logger.info("="*80)
        logger.info("OPPORTUNITY AGGREGATION PIPELINE")
        logger.info("="*80)

        if use_sql:
            # SQL-based aggregation (recommended for production)
            output = output_table or f"{staging_table}_aggregated"

            self.aggregate_deals_sql(
                deals_table=staging_table,
                contacts_table=contacts_table,
                companies_table=companies_table,
                output_table=output,
                schema=schema
            )

            logger.info("="*80)
            return None

        else:
            # Python-based aggregation (for smaller datasets or custom logic)
            logger.info(f"Loading data from {schema}.{staging_table}")

            deals_query = f"SELECT * FROM {schema}.{staging_table}"
            deals_df = self.pg.execute_query_df(deals_query)

            contacts_query = f"SELECT * FROM {schema}.{contacts_table}"
            contacts_df = self.pg.execute_query_df(contacts_query)

            companies_query = f"SELECT * FROM {schema}.{companies_table}"
            companies_df = self.pg.execute_query_df(companies_query)

            logger.info(f"Loaded {len(deals_df)} deals, {len(contacts_df)} contacts, {len(companies_df)} companies")

            # Aggregate
            result_df = self.aggregate_deals_python(
                deals_df,
                contacts_df,
                companies_df
            )

            # Write to output table if specified
            if output_table:
                logger.info(f"Writing results to {schema}.{output_table}")

                with self.pg.get_connection() as conn:
                    result_df.to_sql(
                        output_table,
                        conn,
                        schema=schema,
                        if_exists='replace',
                        index=False,
                        method='multi'
                    )

                logger.info(f"✓ Results written to {schema}.{output_table}")

            logger.info("="*80)
            return result_df


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Example 1: SQL-based aggregation (recommended for production)
    agg = OpportunityAggregator()

    agg.process_opportunities(
        staging_table='deals_staging',
        contacts_table='contacts_staging',
        companies_table='companies_staging',
        output_table='deals_aggregated',
        use_sql=True  # Fast, leverages PostgreSQL
    )

    # Example 2: Python-based aggregation (for custom logic)
    # result_df = agg.process_opportunities(
    #     staging_table='deals_staging',
    #     contacts_table='contacts_staging',
    #     companies_table='companies_staging',
    #     use_sql=False  # Returns DataFrame
    # )
    #
    # print(f"\nProcessed {len(result_df)} opportunities")
    # print("\nSample records:")
    # print(result_df[['icalps_deal_id', 'Contact_Count', 'Company_Count']].head())
