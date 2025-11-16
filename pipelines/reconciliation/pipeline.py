"""
CRM Reconciliation Pipeline
===========================

Main orchestrator for reconciling legacy CRM data with HubSpot CRM.

Workflow:
1. Connect to PostgreSQL (HubSpot synced database)
2. Create staging schema and tables
3. Load Bronze layer CSV data
4. Execute reconciliation queries (JOIN legacy ↔ HubSpot)
5. Store reconciliation results in staging tables
6. Generate property update payloads
7. Log reconciliation operations

Usage:
    from crm_reconciliation_pipeline import CRMReconciliationPipeline

    pipeline = CRMReconciliationPipeline()

    # Reconcile all objects
    pipeline.reconcile_all()

    # Reconcile specific object type
    pipeline.reconcile_companies(limit=100)
"""

from postgres_connection_manager import PostgreSQLManager
from staging_schema_manager import StagingSchemaManager
from reconciliation_query_builder import ReconciliationQueryBuilder
from property_mapping_config import get_object_mapping, get_hubspot_properties
from workflow_api_client import WorkflowAPIClient
from loguru import logger
import pandas as pd
from typing import Dict, List, Optional
import time
import json


class CRMReconciliationPipeline:
    """
    Main pipeline for CRM reconciliation.

    Coordinates:
    - PostgreSQL connection
    - Staging table management
    - Query execution
    - Data reconciliation
    - Logging
    """

    def __init__(
        self,
        pg_manager: Optional[PostgreSQLManager] = None,
        staging_manager: Optional[StagingSchemaManager] = None
    ):
        """
        Initialize reconciliation pipeline.

        Args:
            pg_manager: PostgreSQL manager (creates new if None)
            staging_manager: Staging manager (creates new if None)
        """
        self.pg = pg_manager or PostgreSQLManager()
        self.staging = staging_manager or StagingSchemaManager(self.pg)
        self.query_builder = ReconciliationQueryBuilder()
        self.workflow_client = WorkflowAPIClient()

        logger.info("✓ CRM Reconciliation Pipeline initialized")

    def setup_staging_environment(self) -> None:
        """
        Set up staging schema and tables.
        """
        logger.info("Setting up staging environment...")
        self.staging.create_all_staging_tables()
        logger.info("✓ Staging environment ready")

    def load_bronze_csv_to_postgres(
        self,
        csv_path: str,
        table_name: str,
        schema: str = "public"
    ) -> None:
        """
        Load Bronze CSV file into PostgreSQL for reconciliation.

        Args:
            csv_path: Path to Bronze CSV file
            table_name: Target table name
            schema: Target schema (default: public)
        """
        logger.info(f"Loading Bronze CSV: {csv_path} → {schema}.{table_name}")

        # Read CSV
        df = pd.read_csv(csv_path)
        logger.info(f"  Read {len(df)} rows from CSV")

        # Create table and load data
        # Note: This is a simplified approach. In production, use proper table creation.
        with self.pg.get_connection() as conn:
            df.to_sql(
                table_name,
                conn,
                schema=schema,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )

        logger.info(f"✓ Loaded {len(df)} rows to {schema}.{table_name}")

    def reconcile_companies(
        self,
        bronze_csv_path: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict:
        """
        Reconcile legacy companies with HubSpot companies.

        Args:
            bronze_csv_path: Path to Bronze_Company.csv (optional)
            limit: Limit number of records to reconcile

        Returns:
            Reconciliation statistics
        """
        logger.info("=" * 80)
        logger.info("RECONCILING COMPANIES")
        logger.info("=" * 80)

        start_time = time.time()
        stats = {
            'object_type': 'companies',
            'total': 0,
            'matched': 0,
            'unmatched': 0,
            'conflicts': 0,
            'errors': 0
        }

        try:
            # Load Bronze CSV if provided
            if bronze_csv_path:
                self.load_bronze_csv_to_postgres(
                    bronze_csv_path,
                    table_name='bronze_companies'
                )

            # Build and execute match query
            mapping = get_object_mapping('companies')
            query = self.query_builder.build_match_query(
                'companies',
                limit=limit,
                use_bronze_csv=bool(bronze_csv_path),
                csv_table_name='bronze_companies' if bronze_csv_path else None
            )

            logger.info("Executing match query...")
            df = self.pg.execute_query_df(query)
            stats['total'] = len(df)
            logger.info(f"  Found {len(df)} records")

            # Process each matched record
            for idx, row in df.iterrows():
                try:
                    legacy_id = int(row['legacy_id'])
                    hubspot_id = int(row['hubspot_id']) if row['hubspot_id'] else None

                    # Extract legacy properties
                    legacy_props = {
                        col: row[col] for col in df.columns
                        if col.startswith(('Comp_', 'Addr_'))
                    }

                    # Build property updates
                    props_to_update = self.query_builder.build_property_update_json(
                        'companies',
                        legacy_props
                    )

                    # Insert into staging table
                    insert_query, params = self.query_builder.build_staging_insert_query(
                        'companies',
                        legacy_id=legacy_id,
                        hubspot_id=hubspot_id,
                        legacy_properties=legacy_props,
                        properties_to_update=props_to_update,
                        reconciliation_status='matched' if hubspot_id else 'new',
                        match_confidence=100.0 if hubspot_id else None
                    )

                    self.pg.execute_query(insert_query, params=params, fetch=False)

                    if hubspot_id:
                        stats['matched'] += 1
                    else:
                        stats['unmatched'] += 1

                except Exception as e:
                    logger.error(f"✗ Error processing company {row.get('legacy_id')}: {e}")
                    stats['errors'] += 1

            # Log operation
            execution_time = int((time.time() - start_time) * 1000)
            log_query, log_params = self.query_builder.build_reconciliation_log_insert(
                operation='reconcile',
                entity_type='company',
                legacy_id=None,
                hubspot_id=None,
                status='success',
                execution_time_ms=execution_time
            )
            self.pg.execute_query(log_query, params=log_params, fetch=False)

            logger.info("✓ Company reconciliation complete")
            logger.info(f"  Total: {stats['total']}")
            logger.info(f"  Matched: {stats['matched']}")
            logger.info(f"  Unmatched: {stats['unmatched']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info(f"  Time: {execution_time}ms")

        except Exception as e:
            logger.error(f"✗ Company reconciliation failed: {e}")
            stats['status'] = 'failed'
            stats['error'] = str(e)

        return stats

    def reconcile_contacts(
        self,
        bronze_csv_path: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict:
        """
        Reconcile legacy contacts with HubSpot contacts.

        Args:
            bronze_csv_path: Path to Bronze_Person.csv (optional)
            limit: Limit number of records to reconcile

        Returns:
            Reconciliation statistics
        """
        logger.info("=" * 80)
        logger.info("RECONCILING CONTACTS")
        logger.info("=" * 80)

        start_time = time.time()
        stats = {
            'object_type': 'contacts',
            'total': 0,
            'matched': 0,
            'unmatched': 0,
            'errors': 0
        }

        try:
            # Load Bronze CSV if provided
            if bronze_csv_path:
                self.load_bronze_csv_to_postgres(
                    bronze_csv_path,
                    table_name='bronze_persons'
                )

            # Build and execute match query
            query = self.query_builder.build_match_query(
                'contacts',
                limit=limit,
                use_bronze_csv=bool(bronze_csv_path),
                csv_table_name='bronze_persons' if bronze_csv_path else None
            )

            logger.info("Executing match query...")
            df = self.pg.execute_query_df(query)
            stats['total'] = len(df)
            logger.info(f"  Found {len(df)} records")

            # Process each matched record
            for idx, row in df.iterrows():
                try:
                    legacy_id = int(row['legacy_id'])
                    hubspot_id = int(row['hubspot_id']) if row['hubspot_id'] else None

                    # Extract legacy properties
                    legacy_props = {
                        col: row[col] for col in df.columns
                        if col.startswith(('Pers_', 'Comp_', 'Addr_'))
                    }

                    # Build property updates
                    props_to_update = self.query_builder.build_property_update_json(
                        'contacts',
                        legacy_props
                    )

                    # Insert into staging table
                    insert_query, params = self.query_builder.build_staging_insert_query(
                        'contacts',
                        legacy_id=legacy_id,
                        hubspot_id=hubspot_id,
                        legacy_properties=legacy_props,
                        properties_to_update=props_to_update,
                        reconciliation_status='matched' if hubspot_id else 'new',
                        match_confidence=100.0 if hubspot_id else None
                    )

                    self.pg.execute_query(insert_query, params=params, fetch=False)

                    if hubspot_id:
                        stats['matched'] += 1
                    else:
                        stats['unmatched'] += 1

                except Exception as e:
                    logger.error(f"✗ Error processing contact {row.get('legacy_id')}: {e}")
                    stats['errors'] += 1

            # Log operation
            execution_time = int((time.time() - start_time) * 1000)
            log_query, log_params = self.query_builder.build_reconciliation_log_insert(
                operation='reconcile',
                entity_type='contact',
                legacy_id=None,
                hubspot_id=None,
                status='success',
                execution_time_ms=execution_time
            )
            self.pg.execute_query(log_query, params=log_params, fetch=False)

            logger.info("✓ Contact reconciliation complete")
            logger.info(f"  Total: {stats['total']}")
            logger.info(f"  Matched: {stats['matched']}")
            logger.info(f"  Unmatched: {stats['unmatched']}")
            logger.info(f"  Errors: {stats['errors']}")

        except Exception as e:
            logger.error(f"✗ Contact reconciliation failed: {e}")
            stats['status'] = 'failed'
            stats['error'] = str(e)

        return stats

    def reconcile_deals(
        self,
        bronze_csv_path: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict:
        """
        Reconcile legacy opportunities with HubSpot deals.

        Args:
            bronze_csv_path: Path to Bronze_Opportunity.csv (optional)
            limit: Limit number of records to reconcile

        Returns:
            Reconciliation statistics
        """
        logger.info("=" * 80)
        logger.info("RECONCILING DEALS")
        logger.info("=" * 80)

        # Similar implementation to reconcile_companies
        # ... (implementation details omitted for brevity)

        stats = {
            'object_type': 'deals',
            'total': 0,
            'matched': 0,
            'unmatched': 0,
            'errors': 0
        }

        logger.info("✓ Deal reconciliation complete (placeholder)")
        return stats

    def reconcile_communications(
        self,
        bronze_csv_path: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict:
        """
        Reconcile legacy communications with HubSpot engagements.

        Args:
            bronze_csv_path: Path to Bronze_Communication.csv (optional)
            limit: Limit number of records to reconcile

        Returns:
            Reconciliation statistics
        """
        logger.info("=" * 80)
        logger.info("RECONCILING COMMUNICATIONS")
        logger.info("=" * 80)

        # Similar implementation to reconcile_companies
        # ... (implementation details omitted for brevity)

        stats = {
            'object_type': 'communications',
            'total': 0,
            'matched': 0,
            'unmatched': 0,
            'errors': 0
        }

        logger.info("✓ Communication reconciliation complete (placeholder)")
        return stats

    def reconcile_all(
        self,
        bronze_layer_path: str = "bronze_layer",
        limit_per_object: Optional[int] = None
    ) -> Dict:
        """
        Reconcile all objects (companies, contacts, deals, communications).

        Args:
            bronze_layer_path: Path to Bronze layer directory
            limit_per_object: Limit records per object type

        Returns:
            Combined reconciliation statistics
        """
        logger.info("=" * 80)
        logger.info("STARTING FULL CRM RECONCILIATION")
        logger.info("=" * 80)

        all_stats = {}

        # Reconcile in dependency order
        objects = [
            ('companies', 'Bronze_Company.csv'),
            ('contacts', 'Bronze_Person.csv'),
            ('deals', 'Bronze_Opportunity.csv'),
            ('communications', 'Bronze_Communication.csv')
        ]

        for object_type, csv_filename in objects:
            csv_path = f"{bronze_layer_path}/{csv_filename}"

            if object_type == 'companies':
                stats = self.reconcile_companies(csv_path, limit=limit_per_object)
            elif object_type == 'contacts':
                stats = self.reconcile_contacts(csv_path, limit=limit_per_object)
            elif object_type == 'deals':
                stats = self.reconcile_deals(csv_path, limit=limit_per_object)
            elif object_type == 'communications':
                stats = self.reconcile_communications(csv_path, limit=limit_per_object)

            all_stats[object_type] = stats

        # Print summary
        logger.info("=" * 80)
        logger.info("RECONCILIATION SUMMARY")
        logger.info("=" * 80)

        for object_type, stats in all_stats.items():
            logger.info(f"\n{object_type.upper()}:")
            logger.info(f"  Total: {stats.get('total', 0)}")
            logger.info(f"  Matched: {stats.get('matched', 0)}")
            logger.info(f"  Unmatched: {stats.get('unmatched', 0)}")
            logger.info(f"  Errors: {stats.get('errors', 0)}")

        return all_stats

    def get_reconciliation_stats(self) -> Dict:
        """
        Get statistics from staging tables.

        Returns:
            Dictionary of statistics per object type
        """
        return self.staging.get_staging_table_stats()


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    logger.info("Initializing CRM Reconciliation Pipeline...")

    pipeline = CRMReconciliationPipeline()

    # Step 1: Setup staging environment
    pipeline.setup_staging_environment()

    # Step 2: Test PostgreSQL connection
    if pipeline.pg.test_connection():
        logger.info("✓ PostgreSQL connection successful")

    # Step 3: Get current stats
    stats = pipeline.get_reconciliation_stats()
    logger.info("\nCurrent staging table statistics:")
    for table, count in stats.items():
        logger.info(f"  {table}: {count} rows")

    # Step 4: Reconcile (commented out - requires Bronze CSV files)
    # pipeline.reconcile_all(bronze_layer_path="bronze_layer", limit_per_object=100)

    logger.info("\n✓ Pipeline ready!")
