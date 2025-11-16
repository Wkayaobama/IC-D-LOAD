"""
Reconciliation Query Builder
============================

Generates SQL queries to reconcile legacy CRM data with HubSpot CRM data.

Uses foreign keys to:
1. Join legacy data (from Bronze CSV or SQL Server) with HubSpot data (PostgreSQL)
2. Match records using legacy IDs (Comp_CompanyId → icalps_company_id)
3. Retrieve HubSpot record IDs for updates
4. Store reconciled data in staging tables

Query Types:
- Match queries: Find HubSpot records matching legacy IDs
- Missing queries: Find legacy records not in HubSpot
- Conflict queries: Find records with data mismatches
- Update queries: Generate property update statements

Usage:
    from reconciliation_query_builder import ReconciliationQueryBuilder

    builder = ReconciliationQueryBuilder()

    # Build match query for companies
    query = builder.build_match_query('companies')

    # Build insert query for staging table
    insert_query = builder.build_staging_insert_query('companies', matched_data)
"""

from property_mapping_config import get_object_mapping, get_hubspot_properties
from typing import Dict, List, Optional, Tuple
from loguru import logger
import json


class ReconciliationQueryBuilder:
    """
    Builds SQL queries for reconciling legacy CRM with HubSpot CRM.

    Generates queries for:
    - Matching legacy records to HubSpot records
    - Finding unmatched records
    - Detecting conflicts
    - Inserting into staging tables
    - Updating HubSpot via API
    """

    def __init__(self, staging_schema: str = "staging"):
        """
        Initialize query builder.

        Args:
            staging_schema: Name of staging schema
        """
        self.staging_schema = staging_schema

    def build_match_query(
        self,
        object_type: str,
        limit: Optional[int] = None,
        use_bronze_csv: bool = False,
        csv_table_name: Optional[str] = None
    ) -> str:
        """
        Build query to match legacy records with HubSpot records.

        Joins on:
        - Legacy ID field (e.g., Comp_CompanyId)
        - HubSpot icalps_* field (e.g., icalps_company_id)

        Args:
            object_type: HubSpot object type (companies, contacts, deals)
            limit: Optional row limit
            use_bronze_csv: If True, join with Bronze CSV data instead of SQL Server
            csv_table_name: Name of CSV table in DuckDB/PostgreSQL

        Returns:
            SQL query string
        """
        mapping = get_object_mapping(object_type)

        # Get HubSpot properties to select
        hubspot_props = get_hubspot_properties(object_type)
        hubspot_select = ", ".join([f"hs.{prop}" for prop in hubspot_props])

        # Get legacy properties based on property mappings
        legacy_props = [pm.legacy_property for pm in mapping.property_mappings]
        legacy_select = ", ".join([f"leg.{prop}" for prop in legacy_props if prop])

        # Build query
        if use_bronze_csv and csv_table_name:
            # Join with Bronze CSV data (already loaded into PostgreSQL or DuckDB)
            query = f"""
            SELECT
                leg.{mapping.legacy_id_field} as legacy_id,
                hs.{mapping.hubspot_id_field} as hubspot_id,
                {legacy_select},
                {hubspot_select}
            FROM {csv_table_name} leg
            INNER JOIN hubspot.{object_type} hs
                ON leg.{mapping.legacy_id_field}::VARCHAR = hs.{mapping.hubspot_legacy_id_field}::VARCHAR
            WHERE hs.{mapping.hubspot_legacy_id_field} IS NOT NULL
            """
        else:
            # Direct query from HubSpot (for records already marked with legacy IDs)
            query = f"""
            SELECT
                hs.{mapping.hubspot_legacy_id_field} as legacy_id,
                hs.{mapping.hubspot_id_field} as hubspot_id,
                {hubspot_select}
            FROM hubspot.{object_type} hs
            WHERE hs.{mapping.hubspot_legacy_id_field} IS NOT NULL
            """

        if limit:
            query += f"\nLIMIT {limit}"

        return query

    def build_unmatched_legacy_query(
        self,
        object_type: str,
        csv_table_name: str,
        limit: Optional[int] = None
    ) -> str:
        """
        Build query to find legacy records NOT in HubSpot.

        Uses LEFT JOIN to find records where HubSpot ID is NULL.

        Args:
            object_type: HubSpot object type
            csv_table_name: Name of Bronze CSV table
            limit: Optional row limit

        Returns:
            SQL query string
        """
        mapping = get_object_mapping(object_type)

        # Get legacy properties
        legacy_props = [pm.legacy_property for pm in mapping.property_mappings]
        legacy_select = ", ".join([f"leg.{prop}" for prop in legacy_props if prop])

        query = f"""
        SELECT
            leg.{mapping.legacy_id_field} as legacy_id,
            {legacy_select}
        FROM {csv_table_name} leg
        LEFT JOIN hubspot.{object_type} hs
            ON leg.{mapping.legacy_id_field}::VARCHAR = hs.{mapping.hubspot_legacy_id_field}::VARCHAR
        WHERE hs.{mapping.hubspot_id_field} IS NULL
        """

        if limit:
            query += f"\nLIMIT {limit}"

        return query

    def build_staging_insert_query(
        self,
        object_type: str,
        legacy_id: int,
        hubspot_id: Optional[int],
        legacy_properties: Dict[str, any],
        properties_to_update: Dict[str, any],
        reconciliation_status: str = 'matched',
        match_confidence: Optional[float] = None,
        notes: Optional[str] = None
    ) -> Tuple[str, Tuple]:
        """
        Build INSERT query for staging reconciliation table.

        Args:
            object_type: HubSpot object type
            legacy_id: Legacy record ID
            hubspot_id: HubSpot record ID (None if unmatched)
            legacy_properties: Legacy properties as dict
            properties_to_update: Properties to update in HubSpot as dict
            reconciliation_status: Status (matched, new, conflict)
            match_confidence: Match confidence score (0-100)
            notes: Optional notes

        Returns:
            Tuple of (query, params)
        """
        mapping = get_object_mapping(object_type)

        # Determine staging table name
        if object_type == 'companies':
            staging_table = f"{self.staging_schema}.companies_reconciliation"
            legacy_col = "legacy_company_id"
            hubspot_col = "hubspot_company_id"
            # Extract name and domain
            legacy_name = legacy_properties.get('Comp_Name')
            hubspot_name = properties_to_update.get('name')
            extra_cols = "legacy_name, hubspot_name, legacy_domain, hubspot_domain,"
            extra_vals = "%s, %s, %s, %s,"
            extra_params = (
                legacy_name,
                hubspot_name,
                legacy_properties.get('Comp_WebSite'),
                properties_to_update.get('domain')
            )

        elif object_type == 'contacts':
            staging_table = f"{self.staging_schema}.contacts_reconciliation"
            legacy_col = "legacy_contact_id"
            hubspot_col = "hubspot_contact_id"
            # Extract email and name
            extra_cols = "legacy_email, hubspot_email, legacy_firstname, legacy_lastname, hubspot_firstname, hubspot_lastname, legacy_company_id, hubspot_company_id,"
            extra_vals = "%s, %s, %s, %s, %s, %s, %s, %s,"
            extra_params = (
                legacy_properties.get('Pers_EmailAddress'),
                properties_to_update.get('email'),
                legacy_properties.get('Pers_FirstName'),
                legacy_properties.get('Pers_LastName'),
                properties_to_update.get('firstname'),
                properties_to_update.get('lastname'),
                legacy_properties.get('Pers_CompanyId'),
                properties_to_update.get('associatedcompanyid')
            )

        elif object_type == 'deals':
            staging_table = f"{self.staging_schema}.deals_reconciliation"
            legacy_col = "legacy_deal_id"
            hubspot_col = "hubspot_deal_id"
            # Extract deal details
            extra_cols = "legacy_dealname, hubspot_dealname, legacy_amount, hubspot_amount, legacy_company_id, hubspot_company_id, legacy_contact_id, hubspot_contact_id,"
            extra_vals = "%s, %s, %s, %s, %s, %s, %s, %s,"
            extra_params = (
                legacy_properties.get('Oppo_Description'),
                properties_to_update.get('dealname'),
                legacy_properties.get('Oppo_Forecast'),
                properties_to_update.get('amount'),
                legacy_properties.get('Oppo_PrimaryCompanyId'),
                properties_to_update.get('associatedcompanyid'),
                legacy_properties.get('Oppo_PrimaryPersonId'),
                properties_to_update.get('primary_contact_id')
            )

        elif object_type in ['communications', 'engagements']:
            staging_table = f"{self.staging_schema}.communications_reconciliation"
            legacy_col = "legacy_communication_id"
            hubspot_col = "hubspot_engagement_id"
            # Extract communication details
            extra_cols = "legacy_type, legacy_subject, legacy_datetime, legacy_company_id, hubspot_company_id, legacy_contact_id, hubspot_contact_id,"
            extra_vals = "%s, %s, %s, %s, %s, %s, %s,"
            extra_params = (
                legacy_properties.get('Comm_Type'),
                legacy_properties.get('Comm_Subject'),
                legacy_properties.get('Comm_DateTime'),
                legacy_properties.get('Comp_CompanyId'),
                properties_to_update.get('associated_company_id'),
                legacy_properties.get('Pers_PersonId'),
                properties_to_update.get('associated_contact_id')
            )

        else:
            raise ValueError(f"Unknown object type: {object_type}")

        # Convert dicts to JSON strings
        legacy_props_json = json.dumps(legacy_properties)
        props_to_update_json = json.dumps(properties_to_update)

        # Build INSERT query with UPSERT (ON CONFLICT UPDATE)
        query = f"""
        INSERT INTO {staging_table} (
            {legacy_col},
            {hubspot_col},
            {extra_cols}
            legacy_properties,
            properties_to_update,
            reconciliation_status,
            match_confidence,
            notes,
            last_updated
        )
        VALUES (
            %s, %s, {extra_vals} %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT ({legacy_col})
        DO UPDATE SET
            {hubspot_col} = EXCLUDED.{hubspot_col},
            properties_to_update = EXCLUDED.properties_to_update,
            reconciliation_status = EXCLUDED.reconciliation_status,
            match_confidence = EXCLUDED.match_confidence,
            notes = EXCLUDED.notes,
            last_updated = NOW();
        """

        params = (
            legacy_id,
            hubspot_id,
            *extra_params,
            legacy_props_json,
            props_to_update_json,
            reconciliation_status,
            match_confidence,
            notes
        )

        return query, params

    def build_property_update_json(
        self,
        object_type: str,
        legacy_properties: Dict[str, any]
    ) -> Dict[str, any]:
        """
        Build property update JSON for HubSpot API from legacy properties.

        Args:
            object_type: HubSpot object type
            legacy_properties: Legacy property values

        Returns:
            Dictionary of HubSpot property updates
        """
        mapping = get_object_mapping(object_type)
        updates = {}

        for prop_mapping in mapping.property_mappings:
            legacy_value = legacy_properties.get(prop_mapping.legacy_property)

            # Skip if no value
            if legacy_value is None or legacy_value == '':
                continue

            # Apply transformation if specified
            if prop_mapping.transformation:
                # TODO: Implement transformations
                pass

            updates[prop_mapping.hubspot_property] = legacy_value

        return updates

    def build_reconciliation_log_insert(
        self,
        operation: str,
        entity_type: str,
        legacy_id: Optional[int],
        hubspot_id: Optional[int],
        status: str,
        error_message: Optional[str] = None,
        execution_time_ms: Optional[int] = None
    ) -> Tuple[str, Tuple]:
        """
        Build INSERT query for reconciliation log.

        Args:
            operation: Operation type (match, update, create)
            entity_type: Entity type (company, contact, deal)
            legacy_id: Legacy record ID
            hubspot_id: HubSpot record ID
            status: Success/failure
            error_message: Error details if failed
            execution_time_ms: Execution time in milliseconds

        Returns:
            Tuple of (query, params)
        """
        query = f"""
        INSERT INTO {self.staging_schema}.reconciliation_log (
            operation,
            entity_type,
            legacy_id,
            hubspot_id,
            status,
            error_message,
            execution_time_ms
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        params = (
            operation,
            entity_type,
            legacy_id,
            hubspot_id,
            status,
            error_message,
            execution_time_ms
        )

        return query, params


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    builder = ReconciliationQueryBuilder()

    # Example 1: Build match query for companies
    print("=" * 80)
    print("MATCH QUERY - Companies (with Bronze CSV)")
    print("=" * 80)
    query = builder.build_match_query('companies', limit=10, use_bronze_csv=True, csv_table_name='bronze_companies')
    print(query)

    # Example 2: Build unmatched query
    print("\n" + "=" * 80)
    print("UNMATCHED QUERY - Contacts")
    print("=" * 80)
    query = builder.build_unmatched_legacy_query('contacts', csv_table_name='bronze_persons', limit=10)
    print(query)

    # Example 3: Build staging insert
    print("\n" + "=" * 80)
    print("STAGING INSERT - Company")
    print("=" * 80)
    legacy_props = {
        'Comp_Name': 'ACME Corp',
        'Comp_WebSite': 'acme.com',
        'Comp_CompanyId': 123
    }
    hubspot_props = {
        'name': 'ACME Corp',
        'domain': 'acme.com'
    }
    query, params = builder.build_staging_insert_query(
        'companies',
        legacy_id=123,
        hubspot_id=456789,
        legacy_properties=legacy_props,
        properties_to_update=hubspot_props,
        reconciliation_status='matched',
        match_confidence=100.0
    )
    print(query)
    print(f"\nParams: {params}")
