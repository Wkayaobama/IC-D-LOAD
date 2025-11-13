#!/usr/bin/env python3
"""
Phase 2: Database Setup Execution Script
=========================================

This script executes the DDL and view creation scripts to set up the
HubSpot activities staging infrastructure.

Tasks:
1. Execute DDL script (tables, indexes, constraints, functions)
2. Execute views script (transformation views, validation views)
3. Verify table creation
4. Test classification function
5. Generate Phase 2 validation report

Usage:
    python execute_phase2_database_setup.py
"""

import sys
from pathlib import Path
from loguru import logger
from postgres_connection_manager import PostgreSQLManager
import time

# Configure logging
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO"
)
logger.add(
    "phase2_database_setup.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    level="DEBUG",
    rotation="10 MB"
)


class Phase2DatabaseSetup:
    """
    Executes Phase 2: Database Setup for HubSpot activities staging.
    """

    def __init__(self):
        """Initialize with PostgreSQL connection."""
        logger.info("="*80)
        logger.info("Phase 2: Database Setup - HubSpot Activities Staging")
        logger.info("="*80)

        try:
            self.pg = PostgreSQLManager()
            logger.info("✓ PostgreSQL connection established")
        except Exception as e:
            logger.error(f"✗ Failed to connect to PostgreSQL: {e}")
            raise

    def read_sql_file(self, filename: str) -> str:
        """
        Read SQL file content.

        Args:
            filename: Path to SQL file

        Returns:
            SQL file content as string
        """
        filepath = Path(filename)
        if not filepath.exists():
            raise FileNotFoundError(f"SQL file not found: {filename}")

        logger.info(f"Reading SQL file: {filename}")
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        logger.info(f"✓ Read {len(content)} characters from {filename}")
        return content

    def execute_sql_script(self, sql_content: str, script_name: str) -> bool:
        """
        Execute SQL script with error handling.

        Args:
            sql_content: SQL script content
            script_name: Name of script (for logging)

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Executing SQL script: {script_name}")
        start_time = time.time()

        try:
            # Execute the entire script
            self.pg.execute_query(sql_content, fetch=False)

            elapsed = time.time() - start_time
            logger.success(f"✓ {script_name} executed successfully in {elapsed:.2f}s")
            return True

        except Exception as e:
            logger.error(f"✗ Failed to execute {script_name}: {e}")
            logger.error(f"  Error type: {type(e).__name__}")

            # Try to provide more context
            if hasattr(e, 'pgerror'):
                logger.error(f"  PostgreSQL error: {e.pgerror}")

            return False

    def verify_tables_created(self) -> bool:
        """
        Verify that all staging tables were created.

        Returns:
            True if all tables exist, False otherwise
        """
        logger.info("Verifying table creation...")

        expected_tables = [
            'ic_communication_master',
            'ic_calls',
            'ic_emails',
            'ic_meetings',
            'ic_notes',
            'ic_tasks',
            'ic_communications',
            'ic_postal_mail'
        ]

        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging'
          AND table_name LIKE 'ic_%'
        ORDER BY table_name;
        """

        try:
            results = self.pg.execute_query(query)
            created_tables = [row['table_name'] for row in results]

            logger.info(f"Found {len(created_tables)} tables in staging schema:")
            for table in created_tables:
                logger.info(f"  ✓ {table}")

            # Check if all expected tables exist
            missing_tables = set(expected_tables) - set(created_tables)
            if missing_tables:
                logger.error(f"✗ Missing tables: {missing_tables}")
                return False

            logger.success(f"✓ All {len(expected_tables)} expected tables created successfully")
            return True

        except Exception as e:
            logger.error(f"✗ Failed to verify tables: {e}")
            return False

    def verify_indexes_created(self) -> bool:
        """
        Verify that indexes were created.

        Returns:
            True if indexes exist, False otherwise
        """
        logger.info("Verifying index creation...")

        query = """
        SELECT
            tablename,
            COUNT(*) as index_count
        FROM pg_indexes
        WHERE schemaname = 'staging'
          AND tablename LIKE 'ic_%'
        GROUP BY tablename
        ORDER BY tablename;
        """

        try:
            results = self.pg.execute_query(query)

            if not results:
                logger.warning("⚠ No indexes found")
                return False

            total_indexes = 0
            for row in results:
                total_indexes += row['index_count']
                logger.info(f"  ✓ {row['tablename']}: {row['index_count']} indexes")

            logger.success(f"✓ Total {total_indexes} indexes created")

            # Expect at least 40 indexes based on DDL
            if total_indexes < 40:
                logger.warning(f"⚠ Expected at least 40 indexes, found {total_indexes}")
                return False

            return True

        except Exception as e:
            logger.error(f"✗ Failed to verify indexes: {e}")
            return False

    def verify_function_created(self) -> bool:
        """
        Verify that the classification function was created.

        Returns:
            True if function exists, False otherwise
        """
        logger.info("Verifying function creation...")

        query = """
        SELECT
            routine_name,
            routine_type
        FROM information_schema.routines
        WHERE routine_schema = 'staging'
          AND routine_name = 'classify_activity_type';
        """

        try:
            results = self.pg.execute_query(query)

            if not results:
                logger.error("✗ Function 'classify_activity_type' not found")
                return False

            logger.success("✓ Function 'classify_activity_type' created successfully")
            return True

        except Exception as e:
            logger.error(f"✗ Failed to verify function: {e}")
            return False

    def test_classification_function(self) -> bool:
        """
        Test the classification function with sample inputs.

        Returns:
            True if all tests pass, False otherwise
        """
        logger.info("Testing classification function...")

        test_cases = [
            # (comm_type, comm_priority, comm_caseid, expected_result)
            ('Call', None, None, 'calls'),
            ('Email', None, None, 'emails'),
            ('Meeting', None, None, 'meetings'),
            ('Note', None, None, 'notes'),
            ('Task', None, None, 'tasks'),
            ('SMS', None, None, 'communications'),
            ('Postal', None, None, 'postal_mail'),
            ('Case', 'High', 123, 'tasks'),  # Case-to-Task (high priority)
            ('Case', 'Low', 456, 'notes'),   # Case-to-Note (low priority)
            ('Unknown', None, None, 'notes'), # Default fallback
            (None, None, None, 'notes'),     # NULL fallback
        ]

        all_passed = True

        for comm_type, comm_priority, comm_caseid, expected in test_cases:
            # Handle NULL values for SQL
            type_sql = f"'{comm_type}'" if comm_type else 'NULL'
            priority_sql = f"'{comm_priority}'" if comm_priority else 'NULL'
            caseid_sql = str(comm_caseid) if comm_caseid else 'NULL'

            query = f"""
            SELECT staging.classify_activity_type(
                {type_sql},
                {priority_sql},
                {caseid_sql}
            ) as result;
            """

            try:
                results = self.pg.execute_query(query)
                actual = results[0]['result']

                if actual == expected:
                    logger.info(f"  ✓ classify_activity_type({type_sql}, {priority_sql}, {caseid_sql}) = '{actual}' [PASS]")
                else:
                    logger.error(f"  ✗ classify_activity_type({type_sql}, {priority_sql}, {caseid_sql}) = '{actual}', expected '{expected}' [FAIL]")
                    all_passed = False

            except Exception as e:
                logger.error(f"  ✗ Test failed with error: {e}")
                all_passed = False

        if all_passed:
            logger.success(f"✓ All {len(test_cases)} classification tests passed")
        else:
            logger.error("✗ Some classification tests failed")

        return all_passed

    def verify_views_created(self) -> bool:
        """
        Verify that transformation and validation views were created.

        Returns:
            True if all views exist, False otherwise
        """
        logger.info("Verifying view creation...")

        expected_views = [
            'v_ic_activities_summary',
            'v_ic_derivative_coverage',
            'v_ic_calls_staging',
            'v_ic_emails_staging',
            'v_ic_meetings_staging',
            'v_ic_notes_staging',
            'v_ic_tasks_staging',
            'v_ic_communications_staging',
            'v_ic_postal_mail_staging'
        ]

        query = """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'staging'
          AND table_name LIKE '%ic_%'
        ORDER BY table_name;
        """

        try:
            results = self.pg.execute_query(query)
            created_views = [row['table_name'] for row in results]

            logger.info(f"Found {len(created_views)} views in staging schema:")
            for view in created_views:
                logger.info(f"  ✓ {view}")

            # Check if all expected views exist
            missing_views = set(expected_views) - set(created_views)
            if missing_views:
                logger.error(f"✗ Missing views: {missing_views}")
                return False

            logger.success(f"✓ All {len(expected_views)} expected views created successfully")
            return True

        except Exception as e:
            logger.error(f"✗ Failed to verify views: {e}")
            return False

    def test_views_queryable(self) -> bool:
        """
        Test that all views are queryable (even if empty).

        Returns:
            True if all views can be queried, False otherwise
        """
        logger.info("Testing view queryability...")

        views_to_test = [
            'v_ic_activities_summary',
            'v_ic_derivative_coverage',
            'v_ic_calls_staging',
            'v_ic_emails_staging',
            'v_ic_meetings_staging',
            'v_ic_notes_staging',
            'v_ic_tasks_staging',
            'v_ic_communications_staging',
            'v_ic_postal_mail_staging'
        ]

        all_passed = True

        for view in views_to_test:
            query = f"SELECT * FROM staging.{view} LIMIT 1;"

            try:
                results = self.pg.execute_query(query)
                # It's OK if results are empty (no data yet), just shouldn't error
                logger.info(f"  ✓ {view}: Queryable [PASS]")

            except Exception as e:
                logger.error(f"  ✗ {view}: Failed to query - {e} [FAIL]")
                all_passed = False

        if all_passed:
            logger.success(f"✓ All {len(views_to_test)} views are queryable")
        else:
            logger.error("✗ Some views failed to query")

        return all_passed

    def generate_phase2_report(self) -> dict:
        """
        Generate Phase 2 completion report.

        Returns:
            Dictionary with phase status and metrics
        """
        logger.info("="*80)
        logger.info("Phase 2: Validation Report")
        logger.info("="*80)

        report = {
            'phase': 'Phase 2: Database Setup',
            'status': 'UNKNOWN',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'checks': {}
        }

        # Run all verification checks
        checks = [
            ('tables_created', self.verify_tables_created()),
            ('indexes_created', self.verify_indexes_created()),
            ('function_created', self.verify_function_created()),
            ('function_tests_passed', self.test_classification_function()),
            ('views_created', self.verify_views_created()),
            ('views_queryable', self.test_views_queryable())
        ]

        for check_name, check_result in checks:
            report['checks'][check_name] = check_result

        # Determine overall status
        all_passed = all(result for _, result in checks)
        report['status'] = 'PASS' if all_passed else 'FAIL'

        # Print summary
        logger.info("")
        logger.info("Phase 2 Validation Summary:")
        logger.info("-" * 80)
        for check_name, check_result in checks:
            status_icon = "✓" if check_result else "✗"
            status_text = "PASS" if check_result else "FAIL"
            logger.info(f"  {status_icon} {check_name}: {status_text}")

        logger.info("-" * 80)
        if all_passed:
            logger.success(f"✓ Phase 2: Database Setup - COMPLETE")
        else:
            logger.error(f"✗ Phase 2: Database Setup - FAILED")

        return report

    def execute_phase2(self) -> bool:
        """
        Execute complete Phase 2: Database Setup.

        Returns:
            True if phase completed successfully, False otherwise
        """
        logger.info("")
        logger.info("Starting Phase 2 execution...")
        logger.info("")

        # Step 1: Execute DDL script
        logger.info("Step 1: Executing DDL script...")
        ddl_sql = self.read_sql_file('hubspot_activities_staging_ddl.sql')
        if not self.execute_sql_script(ddl_sql, 'DDL Script'):
            logger.error("✗ Phase 2 aborted: DDL script execution failed")
            return False

        logger.info("")

        # Step 2: Execute views script
        logger.info("Step 2: Executing views script...")
        views_sql = self.read_sql_file('hubspot_activities_staging_views.sql')
        if not self.execute_sql_script(views_sql, 'Views Script'):
            logger.error("✗ Phase 2 aborted: Views script execution failed")
            return False

        logger.info("")

        # Step 3: Validate phase completion
        logger.info("Step 3: Validating phase completion...")
        report = self.generate_phase2_report()

        logger.info("")

        if report['status'] == 'PASS':
            logger.success("="*80)
            logger.success("✓ PHASE 2: DATABASE SETUP - COMPLETE")
            logger.success("="*80)
            logger.info("")
            logger.info("Next Steps:")
            logger.info("  1. Prepare communications CSV (22 columns)")
            logger.info("  2. Execute Phase 3: Master Table Loading")
            logger.info("  3. Run: python load_communications_master.py")
            logger.info("")
            return True
        else:
            logger.error("="*80)
            logger.error("✗ PHASE 2: DATABASE SETUP - FAILED")
            logger.error("="*80)
            logger.error("")
            logger.error("Review errors above and fix issues before proceeding.")
            logger.error("")
            return False


def main():
    """
    Main execution function.
    """
    try:
        phase2 = Phase2DatabaseSetup()
        success = phase2.execute_phase2()

        # Exit with appropriate code
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.warning("\n⚠ Phase 2 execution interrupted by user")
        sys.exit(130)

    except Exception as e:
        logger.error(f"\n✗ Phase 2 execution failed with exception: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
