#!/usr/bin/env python3
"""
Phase 3: Master Table Loading Script
=====================================

This script loads the communications CSV into the master staging table
with activity type classification.

Prerequisites:
- Phase 2 complete (staging tables created)
- Communications CSV prepared with 22 columns

Usage:
    python load_communications_master.py communications_master.csv

CSV Column Requirements (22 columns):
- Comm_CommunicationId (INTEGER, PRIMARY KEY)
- Comm_OpportunityId (INTEGER, nullable)
- Comm_CaseId (INTEGER, nullable)
- Comm_Type (VARCHAR, required)
- Comm_Action (VARCHAR, nullable)
- Comm_Status (VARCHAR, nullable)
- Comm_Priority (VARCHAR, nullable)
- Comm_DateTime (TIMESTAMP, required)
- Comm_ToDateTime (TIMESTAMP, nullable)
- Comm_Note (TEXT, nullable)
- Comm_Subject (TEXT, nullable)
- Comm_Email_Clean (VARCHAR, nullable)
- Comm_CreatedDate (TIMESTAMP, required)
- Comm_UpdatedDate (TIMESTAMP, nullable)
- Comm_OriginalDateTime (TIMESTAMP, nullable)
- Comm_OriginalToDateTime (TIMESTAMP, nullable)
- Person_Id (INTEGER, nullable)
- Person_FirstName (VARCHAR, nullable)
- Person_LastName (VARCHAR, nullable)
- Person_EmailAddress (VARCHAR, nullable)
- Company_Id (INTEGER, nullable)
- Company_Name (VARCHAR, nullable)
"""

import sys
import pandas as pd
from pathlib import Path
from loguru import logger
from postgres_connection_manager import PostgreSQLManager
import time
from typing import Dict, List

# Configure logging
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO"
)
logger.add(
    "phase3_master_table_loading.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    level="DEBUG",
    rotation="10 MB"
)


class Phase3MasterTableLoader:
    """
    Loads communications data into master staging table.
    """

    REQUIRED_COLUMNS = [
        'Comm_CommunicationId',
        'Comm_Type',
        'Comm_DateTime',
        'Comm_CreatedDate'
    ]

    EXPECTED_COLUMNS = [
        'Comm_CommunicationId', 'Comm_OpportunityId', 'Comm_CaseId',
        'Comm_Type', 'Comm_Action', 'Comm_Status', 'Comm_Priority',
        'Comm_DateTime', 'Comm_ToDateTime', 'Comm_Note', 'Comm_Subject',
        'Comm_Email_Clean', 'Comm_CreatedDate', 'Comm_UpdatedDate',
        'Comm_OriginalDateTime', 'Comm_OriginalToDateTime',
        'Person_Id', 'Person_FirstName', 'Person_LastName', 'Person_EmailAddress',
        'Company_Id', 'Company_Name'
    ]

    def __init__(self, csv_path: str):
        """
        Initialize loader with CSV file path.

        Args:
            csv_path: Path to communications CSV file
        """
        logger.info("="*80)
        logger.info("Phase 3: Master Table Loading")
        logger.info("="*80)

        self.csv_path = Path(csv_path)
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        logger.info(f"CSV file: {self.csv_path}")

        try:
            self.pg = PostgreSQLManager()
            logger.info("✓ PostgreSQL connection established")
        except Exception as e:
            logger.error(f"✗ Failed to connect to PostgreSQL: {e}")
            raise

    def validate_csv_columns(self, df: pd.DataFrame) -> bool:
        """
        Validate CSV has required columns.

        Args:
            df: DataFrame loaded from CSV

        Returns:
            True if valid, False otherwise
        """
        logger.info("Validating CSV columns...")

        # Check required columns exist
        missing_required = set(self.REQUIRED_COLUMNS) - set(df.columns)
        if missing_required:
            logger.error(f"✗ Missing required columns: {missing_required}")
            return False

        logger.info(f"✓ All {len(self.REQUIRED_COLUMNS)} required columns present")

        # Check if all expected columns exist
        missing_expected = set(self.EXPECTED_COLUMNS) - set(df.columns)
        if missing_expected:
            logger.warning(f"⚠ Missing optional columns: {missing_expected}")

        # Report extra columns
        extra_columns = set(df.columns) - set(self.EXPECTED_COLUMNS)
        if extra_columns:
            logger.warning(f"⚠ Extra columns (will be ignored): {extra_columns}")

        logger.info(f"✓ CSV has {len(df.columns)} columns")
        return True

    def load_csv(self) -> pd.DataFrame:
        """
        Load CSV into DataFrame with validation.

        Returns:
            DataFrame with communications data
        """
        logger.info(f"Loading CSV: {self.csv_path}")
        start_time = time.time()

        try:
            # Load CSV
            df = pd.read_csv(self.csv_path)

            elapsed = time.time() - start_time
            logger.info(f"✓ Loaded {len(df):,} rows in {elapsed:.2f}s")

            # Validate columns
            if not self.validate_csv_columns(df):
                raise ValueError("CSV validation failed - missing required columns")

            # Check for duplicates on Comm_CommunicationId
            duplicates = df[df.duplicated(subset=['Comm_CommunicationId'], keep=False)]
            if not duplicates.empty:
                logger.error(f"✗ Found {len(duplicates)} duplicate Comm_CommunicationId values")
                logger.error(f"  Duplicate IDs: {duplicates['Comm_CommunicationId'].tolist()[:10]}")
                raise ValueError("CSV contains duplicate Comm_CommunicationId values")

            logger.info("✓ No duplicate Comm_CommunicationId values found")

            # Check for NULL in required fields
            for col in self.REQUIRED_COLUMNS:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    logger.error(f"✗ {col} has {null_count} NULL values (required field)")
                    raise ValueError(f"Required field {col} contains NULL values")

            logger.info("✓ All required fields populated")

            return df

        except pd.errors.EmptyDataError:
            logger.error("✗ CSV file is empty")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"✗ Failed to parse CSV: {e}")
            raise
        except Exception as e:
            logger.error(f"✗ Failed to load CSV: {e}")
            raise

    def classify_activity_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived_activity_type column using classification logic.

        Args:
            df: DataFrame with communications data

        Returns:
            DataFrame with derived_activity_type column added
        """
        logger.info("Classifying activity types...")

        # Apply classification logic (matching SQL function logic)
        def classify(row):
            comm_type = str(row.get('Comm_Type', '')).lower().strip()
            comm_priority = str(row.get('Comm_Priority', '')).lower().strip()
            comm_caseid = row.get('Comm_CaseId')

            # Explicit type mappings
            type_map = {
                # Calls
                'call': 'calls', 'phone': 'calls', 'voip': 'calls',
                'zoom': 'calls', 'telephone': 'calls', 'phonecall': 'calls',
                # Emails
                'email': 'emails', 'e-mail': 'emails',
                # Meetings
                'meeting': 'meetings', 'appointment': 'meetings', 'visit': 'meetings',
                'demo': 'meetings', 'presentation': 'meetings',
                # Notes
                'note': 'notes', 'comment': 'notes', 'annotation': 'notes', 'memo': 'notes',
                # Tasks
                'task': 'tasks', 'todo': 'tasks', 'to-do': 'tasks',
                'follow-up': 'tasks', 'followup': 'tasks', 'action': 'tasks',
                # Communications
                'sms': 'communications', 'text': 'communications', 'linkedin': 'communications',
                'whatsapp': 'communications', 'wa': 'communications', 'message': 'communications',
                # Postal Mail
                'postal': 'postal_mail', 'letter': 'postal_mail'
            }

            # Check explicit mapping
            if comm_type in type_map:
                return type_map[comm_type]

            # Case-to-Task/Note conversion
            if comm_type == 'case' and pd.notna(comm_caseid):
                if comm_priority in ['high', 'urgent', 'critical']:
                    return 'tasks'
                else:
                    return 'notes'

            # Default fallback
            return 'notes'

        # Apply classification
        df['derived_activity_type'] = df.apply(classify, axis=1)

        # Report distribution
        logger.info("Activity type distribution:")
        distribution = df['derived_activity_type'].value_counts()
        for activity_type, count in distribution.items():
            pct = 100.0 * count / len(df)
            logger.info(f"  {activity_type}: {count:,} ({pct:.2f}%)")

        logger.info(f"✓ Classified {len(df):,} records")

        return df

    def prepare_dataframe_for_insert(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for database insert.

        Args:
            df: DataFrame with communications data

        Returns:
            DataFrame ready for insert
        """
        logger.info("Preparing DataFrame for database insert...")

        # Rename columns to match staging table schema (lowercase with underscores)
        column_mapping = {
            'Comm_CommunicationId': 'comm_communicationid',
            'Comm_OpportunityId': 'comm_opportunityid',
            'Comm_CaseId': 'comm_caseid',
            'Comm_Type': 'comm_type',
            'Comm_Action': 'comm_action',
            'Comm_Status': 'comm_status',
            'Comm_Priority': 'comm_priority',
            'Comm_DateTime': 'comm_datetime',
            'Comm_ToDateTime': 'comm_todatetime',
            'Comm_Note': 'comm_note',
            'Comm_Subject': 'comm_subject',
            'Comm_Email_Clean': 'comm_email_clean',
            'Comm_CreatedDate': 'comm_createddate',
            'Comm_UpdatedDate': 'comm_updateddate',
            'Comm_OriginalDateTime': 'comm_originaldatetime',
            'Comm_OriginalToDateTime': 'comm_originaltodatetime',
            'Person_Id': 'person_id',
            'Person_FirstName': 'person_firstname',
            'Person_LastName': 'person_lastname',
            'Person_EmailAddress': 'person_emailaddress',
            'Company_Id': 'company_id',
            'Company_Name': 'company_name',
            'derived_activity_type': 'derived_activity_type'
        }

        df_prepared = df.rename(columns=column_mapping)

        # Select only columns that exist in both CSV and mapping
        available_columns = [col for col in column_mapping.values() if col in df_prepared.columns]
        df_prepared = df_prepared[available_columns]

        # Convert timestamps to datetime
        timestamp_columns = [
            'comm_datetime', 'comm_todatetime', 'comm_createddate',
            'comm_updateddate', 'comm_originaldatetime', 'comm_originaltodatetime'
        ]
        for col in timestamp_columns:
            if col in df_prepared.columns:
                df_prepared[col] = pd.to_datetime(df_prepared[col], errors='coerce')

        logger.info(f"✓ DataFrame prepared with {len(available_columns)} columns")

        return df_prepared

    def insert_to_database(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        Insert DataFrame into staging table in batches.

        Args:
            df: Prepared DataFrame
            batch_size: Number of rows per batch

        Returns:
            Number of rows inserted
        """
        logger.info(f"Inserting {len(df):,} rows into staging.ic_communication_master...")
        logger.info(f"Using batch size: {batch_size}")

        total_inserted = 0
        num_batches = (len(df) + batch_size - 1) // batch_size

        start_time = time.time()

        for batch_num in range(num_batches):
            batch_start = batch_num * batch_size
            batch_end = min((batch_num + 1) * batch_size, len(df))
            batch_df = df.iloc[batch_start:batch_end]

            try:
                # Use pandas to_sql with conflict handling
                batch_df.to_sql(
                    name='ic_communication_master',
                    con=self.pg.connection_pool.getconn(),
                    schema='staging',
                    if_exists='append',
                    index=False,
                    method='multi'
                )

                total_inserted += len(batch_df)

                # Progress logging
                progress_pct = 100.0 * batch_end / len(df)
                logger.info(f"  Batch {batch_num + 1}/{num_batches}: Inserted {batch_end:,}/{len(df):,} rows ({progress_pct:.1f}%)")

            except Exception as e:
                logger.error(f"✗ Failed to insert batch {batch_num + 1}: {e}")
                raise

        elapsed = time.time() - start_time
        rows_per_sec = total_inserted / elapsed if elapsed > 0 else 0

        logger.success(f"✓ Inserted {total_inserted:,} rows in {elapsed:.2f}s ({rows_per_sec:.0f} rows/sec)")

        return total_inserted

    def validate_inserted_data(self, expected_count: int) -> bool:
        """
        Validate data was inserted correctly.

        Args:
            expected_count: Expected number of rows

        Returns:
            True if validation passes, False otherwise
        """
        logger.info("Validating inserted data...")

        # Check row count
        count_query = "SELECT COUNT(*) as count FROM staging.ic_communication_master;"
        try:
            result = self.pg.execute_query(count_query)
            actual_count = result[0]['count']

            if actual_count != expected_count:
                logger.error(f"✗ Row count mismatch: expected {expected_count}, got {actual_count}")
                return False

            logger.info(f"✓ Row count matches: {actual_count:,}")

        except Exception as e:
            logger.error(f"✗ Failed to validate row count: {e}")
            return False

        # Check activity type distribution
        distribution_query = """
        SELECT
            derived_activity_type,
            COUNT(*) as count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
        FROM staging.ic_communication_master
        GROUP BY derived_activity_type
        ORDER BY count DESC;
        """

        try:
            results = self.pg.execute_query(distribution_query)
            logger.info("Activity type distribution in database:")
            for row in results:
                logger.info(f"  {row['derived_activity_type']}: {row['count']:,} ({row['pct']}%)")

            logger.success("✓ Data validation complete")
            return True

        except Exception as e:
            logger.error(f"✗ Failed to validate distribution: {e}")
            return False

    def execute_phase3(self) -> bool:
        """
        Execute complete Phase 3: Master Table Loading.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Step 1: Load CSV
            logger.info("")
            logger.info("Step 1: Loading CSV...")
            df = self.load_csv()

            # Step 2: Classify activity types
            logger.info("")
            logger.info("Step 2: Classifying activity types...")
            df = self.classify_activity_types(df)

            # Step 3: Prepare for insert
            logger.info("")
            logger.info("Step 3: Preparing data for insert...")
            df_prepared = self.prepare_dataframe_for_insert(df)

            # Step 4: Insert to database
            logger.info("")
            logger.info("Step 4: Inserting to database...")
            inserted_count = self.insert_to_database(df_prepared)

            # Step 5: Validate
            logger.info("")
            logger.info("Step 5: Validating inserted data...")
            validation_passed = self.validate_inserted_data(inserted_count)

            logger.info("")

            if validation_passed:
                logger.success("="*80)
                logger.success("✓ PHASE 3: MASTER TABLE LOADING - COMPLETE")
                logger.success("="*80)
                logger.info("")
                logger.info("Next Steps:")
                logger.info("  1. Execute Phase 4: FK Resolution")
                logger.info("  2. Run: python resolve_foreign_keys.py")
                logger.info("")
                return True
            else:
                logger.error("="*80)
                logger.error("✗ PHASE 3: MASTER TABLE LOADING - VALIDATION FAILED")
                logger.error("="*80)
                return False

        except Exception as e:
            logger.error(f"\n✗ Phase 3 failed: {e}")
            logger.exception("Full traceback:")
            return False


def main():
    """
    Main execution function.
    """
    if len(sys.argv) != 2:
        logger.error("Usage: python load_communications_master.py <csv_file>")
        logger.error("")
        logger.error("Example:")
        logger.error("  python load_communications_master.py communications_master.csv")
        sys.exit(1)

    csv_path = sys.argv[1]

    try:
        loader = Phase3MasterTableLoader(csv_path)
        success = loader.execute_phase3()

        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.warning("\n⚠ Phase 3 execution interrupted by user")
        sys.exit(130)

    except Exception as e:
        logger.error(f"\n✗ Phase 3 execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
