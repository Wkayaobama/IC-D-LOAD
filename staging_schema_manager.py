"""
Staging Schema Manager for HubSpot CRM Reconciliation
=====================================================

This module creates and manages staging tables for reconciling legacy CRM data
with HubSpot CRM data stored in PostgreSQL.

Staging tables store the reconciliation results before pushing to production:
- Legacy CRM record IDs
- HubSpot record IDs
- Property mappings
- Update status

Usage:
    from staging_schema_manager import StagingSchemaManager

    staging_mgr = StagingSchemaManager()
    staging_mgr.create_all_staging_tables()
"""

from postgres_connection_manager import PostgreSQLManager
from loguru import logger
from typing import List, Dict, Optional
import pandas as pd


class StagingSchemaManager:
    """
    Manages staging schema and tables for CRM reconciliation.

    Creates staging tables for:
    - Companies (legacy → HubSpot companies)
    - Contacts (legacy → HubSpot contacts)
    - Deals (legacy → HubSpot deals)
    - Communications (legacy → HubSpot engagements/notes)
    """

    def __init__(self, pg_manager: Optional[PostgreSQLManager] = None):
        """
        Initialize staging schema manager.

        Args:
            pg_manager: PostgreSQL manager instance (creates new if None)
        """
        self.pg = pg_manager or PostgreSQLManager()
        self.staging_schema = "staging"

    def create_staging_schema(self) -> None:
        """
        Create staging schema if it doesn't exist.
        """
        self.pg.create_schema(self.staging_schema, if_not_exists=True)
        logger.info(f"✓ Staging schema '{self.staging_schema}' ready")

    def create_staging_companies_table(self) -> None:
        """
        Create staging table for company reconciliation.

        Schema:
        - legacy_company_id: Company ID from legacy CRM (IC'ALPS)
        - hubspot_company_id: Company record ID in HubSpot
        - legacy_name: Company name from legacy CRM
        - hubspot_name: Company name in HubSpot
        - properties_to_update: JSON of properties to update
        - reconciliation_status: Status (matched, new, conflict)
        - last_updated: Timestamp of last update
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.staging_schema}.companies_reconciliation (
            id SERIAL PRIMARY KEY,
            legacy_company_id INTEGER NOT NULL,
            hubspot_company_id BIGINT,
            legacy_name VARCHAR(500),
            hubspot_name VARCHAR(500),
            legacy_domain VARCHAR(500),
            hubspot_domain VARCHAR(500),
            legacy_properties JSONB,
            properties_to_update JSONB,
            reconciliation_status VARCHAR(50) DEFAULT 'pending',
            match_confidence DECIMAL(5,2),
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            last_updated TIMESTAMP DEFAULT NOW(),
            UNIQUE(legacy_company_id)
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_staging_companies_legacy_id
            ON {self.staging_schema}.companies_reconciliation(legacy_company_id);
        CREATE INDEX IF NOT EXISTS idx_staging_companies_hubspot_id
            ON {self.staging_schema}.companies_reconciliation(hubspot_company_id);
        CREATE INDEX IF NOT EXISTS idx_staging_companies_status
            ON {self.staging_schema}.companies_reconciliation(reconciliation_status);
        """

        self.pg.execute_query(query, fetch=False)
        logger.info(f"✓ Created staging table: {self.staging_schema}.companies_reconciliation")

    def create_staging_contacts_table(self) -> None:
        """
        Create staging table for contact reconciliation.

        Schema:
        - legacy_contact_id: Contact ID from legacy CRM (IC'ALPS)
        - hubspot_contact_id: Contact record ID in HubSpot
        - legacy_email: Email from legacy CRM
        - hubspot_email: Email in HubSpot
        - properties_to_update: JSON of properties to update
        - reconciliation_status: Status (matched, new, conflict)
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.staging_schema}.contacts_reconciliation (
            id SERIAL PRIMARY KEY,
            legacy_contact_id INTEGER NOT NULL,
            hubspot_contact_id BIGINT,
            legacy_email VARCHAR(500),
            hubspot_email VARCHAR(500),
            legacy_firstname VARCHAR(200),
            legacy_lastname VARCHAR(200),
            hubspot_firstname VARCHAR(200),
            hubspot_lastname VARCHAR(200),
            legacy_company_id INTEGER,
            hubspot_company_id BIGINT,
            legacy_properties JSONB,
            properties_to_update JSONB,
            reconciliation_status VARCHAR(50) DEFAULT 'pending',
            match_confidence DECIMAL(5,2),
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            last_updated TIMESTAMP DEFAULT NOW(),
            UNIQUE(legacy_contact_id)
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_staging_contacts_legacy_id
            ON {self.staging_schema}.contacts_reconciliation(legacy_contact_id);
        CREATE INDEX IF NOT EXISTS idx_staging_contacts_hubspot_id
            ON {self.staging_schema}.contacts_reconciliation(hubspot_contact_id);
        CREATE INDEX IF NOT EXISTS idx_staging_contacts_email
            ON {self.staging_schema}.contacts_reconciliation(legacy_email);
        CREATE INDEX IF NOT EXISTS idx_staging_contacts_status
            ON {self.staging_schema}.contacts_reconciliation(reconciliation_status);
        """

        self.pg.execute_query(query, fetch=False)
        logger.info(f"✓ Created staging table: {self.staging_schema}.contacts_reconciliation")

    def create_staging_deals_table(self) -> None:
        """
        Create staging table for deal/opportunity reconciliation.

        Schema:
        - legacy_deal_id: Opportunity ID from legacy CRM (IC'ALPS)
        - hubspot_deal_id: Deal record ID in HubSpot
        - properties_to_update: JSON of properties to update
        - reconciliation_status: Status (matched, new, conflict)
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.staging_schema}.deals_reconciliation (
            id SERIAL PRIMARY KEY,
            legacy_deal_id INTEGER NOT NULL,
            hubspot_deal_id BIGINT,
            legacy_dealname VARCHAR(500),
            hubspot_dealname VARCHAR(500),
            legacy_amount DECIMAL(15,2),
            hubspot_amount DECIMAL(15,2),
            legacy_company_id INTEGER,
            hubspot_company_id BIGINT,
            legacy_contact_id INTEGER,
            hubspot_contact_id BIGINT,
            legacy_properties JSONB,
            properties_to_update JSONB,
            reconciliation_status VARCHAR(50) DEFAULT 'pending',
            match_confidence DECIMAL(5,2),
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            last_updated TIMESTAMP DEFAULT NOW(),
            UNIQUE(legacy_deal_id)
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_staging_deals_legacy_id
            ON {self.staging_schema}.deals_reconciliation(legacy_deal_id);
        CREATE INDEX IF NOT EXISTS idx_staging_deals_hubspot_id
            ON {self.staging_schema}.deals_reconciliation(hubspot_deal_id);
        CREATE INDEX IF NOT EXISTS idx_staging_deals_status
            ON {self.staging_schema}.deals_reconciliation(reconciliation_status);
        """

        self.pg.execute_query(query, fetch=False)
        logger.info(f"✓ Created staging table: {self.staging_schema}.deals_reconciliation")

    def create_staging_communications_table(self) -> None:
        """
        Create staging table for communication/engagement reconciliation.

        Schema:
        - legacy_communication_id: Communication ID from legacy CRM
        - hubspot_engagement_id: Engagement ID in HubSpot
        - properties_to_update: JSON of properties to update
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.staging_schema}.communications_reconciliation (
            id SERIAL PRIMARY KEY,
            legacy_communication_id INTEGER NOT NULL,
            hubspot_engagement_id BIGINT,
            legacy_type VARCHAR(100),
            legacy_subject TEXT,
            legacy_datetime TIMESTAMP,
            legacy_company_id INTEGER,
            hubspot_company_id BIGINT,
            legacy_contact_id INTEGER,
            hubspot_contact_id BIGINT,
            legacy_properties JSONB,
            properties_to_update JSONB,
            reconciliation_status VARCHAR(50) DEFAULT 'pending',
            match_confidence DECIMAL(5,2),
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            last_updated TIMESTAMP DEFAULT NOW(),
            UNIQUE(legacy_communication_id)
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_staging_comms_legacy_id
            ON {self.staging_schema}.communications_reconciliation(legacy_communication_id);
        CREATE INDEX IF NOT EXISTS idx_staging_comms_hubspot_id
            ON {self.staging_schema}.communications_reconciliation(hubspot_engagement_id);
        CREATE INDEX IF NOT EXISTS idx_staging_comms_status
            ON {self.staging_schema}.communications_reconciliation(reconciliation_status);
        """

        self.pg.execute_query(query, fetch=False)
        logger.info(f"✓ Created staging table: {self.staging_schema}.communications_reconciliation")

    def create_reconciliation_log_table(self) -> None:
        """
        Create table to log reconciliation operations.

        Schema:
        - operation: Type of operation (match, update, create)
        - entity_type: Type of entity (company, contact, deal, communication)
        - legacy_id: Legacy record ID
        - hubspot_id: HubSpot record ID
        - status: Success/failure
        - error_message: Error details if failed
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.staging_schema}.reconciliation_log (
            id SERIAL PRIMARY KEY,
            operation VARCHAR(100) NOT NULL,
            entity_type VARCHAR(50) NOT NULL,
            legacy_id INTEGER,
            hubspot_id BIGINT,
            status VARCHAR(50),
            error_message TEXT,
            execution_time_ms INTEGER,
            timestamp TIMESTAMP DEFAULT NOW()
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_recon_log_entity
            ON {self.staging_schema}.reconciliation_log(entity_type);
        CREATE INDEX IF NOT EXISTS idx_recon_log_status
            ON {self.staging_schema}.reconciliation_log(status);
        CREATE INDEX IF NOT EXISTS idx_recon_log_timestamp
            ON {self.staging_schema}.reconciliation_log(timestamp);
        """

        self.pg.execute_query(query, fetch=False)
        logger.info(f"✓ Created staging table: {self.staging_schema}.reconciliation_log")

    def create_all_staging_tables(self) -> None:
        """
        Create all staging tables at once.
        """
        logger.info("Creating all staging tables...")

        self.create_staging_schema()
        self.create_staging_companies_table()
        self.create_staging_contacts_table()
        self.create_staging_deals_table()
        self.create_staging_communications_table()
        self.create_reconciliation_log_table()

        logger.info("✓ All staging tables created successfully!")

    def drop_all_staging_tables(self, confirm: bool = False) -> None:
        """
        Drop all staging tables (use with caution!).

        Args:
            confirm: Must be True to actually drop tables
        """
        if not confirm:
            logger.warning("⚠ Drop operation requires confirm=True")
            return

        tables = [
            "companies_reconciliation",
            "contacts_reconciliation",
            "deals_reconciliation",
            "communications_reconciliation",
            "reconciliation_log"
        ]

        for table in tables:
            self.pg.drop_table(table, schema=self.staging_schema, cascade=True)

        logger.info("✓ All staging tables dropped")

    def get_staging_table_stats(self) -> Dict[str, int]:
        """
        Get row counts for all staging tables.

        Returns:
            Dictionary mapping table name to row count
        """
        tables = [
            "companies_reconciliation",
            "contacts_reconciliation",
            "deals_reconciliation",
            "communications_reconciliation"
        ]

        stats = {}
        for table in tables:
            query = f"SELECT COUNT(*) as count FROM {self.staging_schema}.{table};"
            result = self.pg.execute_query(query)
            stats[table] = result[0]['count'] if result else 0

        return stats

    def clear_staging_table(self, table_name: str) -> None:
        """
        Clear all data from a staging table.

        Args:
            table_name: Name of staging table (without schema prefix)
        """
        query = f"TRUNCATE TABLE {self.staging_schema}.{table_name} RESTART IDENTITY CASCADE;"
        self.pg.execute_query(query, fetch=False)
        logger.info(f"✓ Cleared staging table: {self.staging_schema}.{table_name}")


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    print("Initializing staging schema manager...")

    staging_mgr = StagingSchemaManager()

    # Create all staging tables
    print("\nCreating staging tables...")
    staging_mgr.create_all_staging_tables()

    # Get stats
    print("\nStaging table statistics:")
    stats = staging_mgr.get_staging_table_stats()
    for table, count in stats.items():
        print(f"  {table}: {count} rows")

    print("\n✓ Staging schema ready!")
