"""
PostgreSQL Connection Manager for HubSpot CRM Reconciliation
============================================================

This module provides connection pooling and query execution for the PostgreSQL
database that syncs with HubSpot CRM data.

Connection Details:
- Host: 2219-revops.pgm5k8mhg52j6k63k3dd54em0v.postgres.stacksync.com
- Port: 5432
- Database: postgres
- User: postgres
- Password: 4LjdtD27gKxc5bptfFZp

Usage:
    from postgres_connection_manager import PostgreSQLManager

    pg_manager = PostgreSQLManager()

    # Execute query
    results = pg_manager.execute_query("SELECT * FROM hubspot.contacts LIMIT 10")

    # Execute with parameters
    results = pg_manager.execute_query(
        "SELECT * FROM hubspot.contacts WHERE icalps_contact_id = %s",
        params=(1234,)
    )
"""

import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
import pandas as pd
from typing import List, Dict, Optional, Any, Tuple
from contextlib import contextmanager
from loguru import logger
import time


class PostgreSQLManager:
    """
    Manages PostgreSQL connections and query execution for HubSpot CRM data.

    Features:
    - Connection pooling
    - Automatic retry on connection failure
    - Query execution with parameterization
    - DataFrame conversion
    - Transaction management
    """

    def __init__(
        self,
        host: str = "2219-revops.pgm5k8mhg52j6k63k3dd54em0v.postgres.stacksync.com",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "4LjdtD27gKxc5bptfFZp",
        min_conn: int = 1,
        max_conn: int = 10
    ):
        """
        Initialize PostgreSQL connection manager.

        Args:
            host: PostgreSQL server host
            port: PostgreSQL server port
            database: Database name
            user: Username
            password: Password
            min_conn: Minimum connections in pool
            max_conn: Maximum connections in pool
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        # Create connection pool
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=min_conn,
                maxconn=max_conn,
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connect_timeout=30
            )
            logger.info(f"✓ PostgreSQL connection pool created: {host}:{port}/{database}")
        except Exception as e:
            logger.error(f"✗ Failed to create connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection from the pool.

        Usage:
            with pg_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM table")
        """
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        """
        Context manager for getting a cursor.

        Args:
            dict_cursor: If True, returns RealDictCursor (rows as dicts)

        Usage:
            with pg_manager.get_cursor() as cursor:
                cursor.execute("SELECT * FROM table")
                results = cursor.fetchall()
        """
        with self.get_connection() as conn:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"✗ Query failed, rolled back: {e}")
                raise
            finally:
                cursor.close()

    def execute_query(
        self,
        query: str,
        params: Optional[Tuple] = None,
        fetch: bool = True,
        retry: int = 3
    ) -> Optional[List[Dict]]:
        """
        Execute a SQL query with optional parameters.

        Args:
            query: SQL query string
            params: Query parameters (tuple)
            fetch: If True, fetch and return results
            retry: Number of retry attempts on failure

        Returns:
            List of rows as dictionaries (if fetch=True), else None

        Example:
            results = pg_manager.execute_query(
                "SELECT * FROM hubspot.contacts WHERE icalps_contact_id = %s",
                params=(1234,)
            )
        """
        attempt = 0
        last_error = None

        while attempt < retry:
            try:
                with self.get_cursor() as cursor:
                    cursor.execute(query, params)

                    if fetch:
                        results = cursor.fetchall()
                        logger.info(f"✓ Query executed successfully, fetched {len(results)} rows")
                        return results
                    else:
                        logger.info(f"✓ Query executed successfully (no fetch)")
                        return None

            except Exception as e:
                attempt += 1
                last_error = e
                logger.warning(f"✗ Query attempt {attempt}/{retry} failed: {e}")

                if attempt < retry:
                    time.sleep(2 ** attempt)  # Exponential backoff

        logger.error(f"✗ Query failed after {retry} attempts: {last_error}")
        raise last_error

    def execute_query_df(
        self,
        query: str,
        params: Optional[Tuple] = None
    ) -> pd.DataFrame:
        """
        Execute query and return results as pandas DataFrame.

        Args:
            query: SQL query string
            params: Query parameters (tuple)

        Returns:
            pandas DataFrame with query results

        Example:
            df = pg_manager.execute_query_df(
                "SELECT * FROM hubspot.contacts WHERE icalps_contact_id IS NOT NULL"
            )
        """
        results = self.execute_query(query, params, fetch=True)

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        logger.info(f"✓ Converted {len(df)} rows to DataFrame with {len(df.columns)} columns")
        return df

    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table
            schema: Schema name (default: public)

        Returns:
            True if table exists, False otherwise
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            );
        """
        result = self.execute_query(query, params=(schema, table_name))
        return result[0]['exists'] if result else False

    def get_table_columns(self, table_name: str, schema: str = "public") -> List[str]:
        """
        Get list of column names for a table.

        Args:
            table_name: Name of the table
            schema: Schema name (default: public)

        Returns:
            List of column names
        """
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            ORDER BY ordinal_position;
        """
        results = self.execute_query(query, params=(schema, table_name))
        return [row['column_name'] for row in results] if results else []

    def create_schema(self, schema_name: str, if_not_exists: bool = True) -> None:
        """
        Create a schema in the database.

        Args:
            schema_name: Name of schema to create
            if_not_exists: If True, only create if doesn't exist
        """
        query = f"CREATE SCHEMA {'IF NOT EXISTS' if if_not_exists else ''} {schema_name};"
        self.execute_query(query, fetch=False)
        logger.info(f"✓ Schema '{schema_name}' created")

    def drop_table(self, table_name: str, schema: str = "public", cascade: bool = False) -> None:
        """
        Drop a table from the database.

        Args:
            table_name: Name of table to drop
            schema: Schema name (default: public)
            cascade: If True, drop dependent objects
        """
        cascade_str = "CASCADE" if cascade else ""
        query = f"DROP TABLE IF EXISTS {schema}.{table_name} {cascade_str};"
        self.execute_query(query, fetch=False)
        logger.info(f"✓ Table '{schema}.{table_name}' dropped")

    def get_hubspot_tables(self) -> List[str]:
        """
        Get list of HubSpot tables in the database.

        Returns:
            List of table names in hubspot schema
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'hubspot'
            ORDER BY table_name;
        """
        results = self.execute_query(query)
        return [row['table_name'] for row in results] if results else []

    def test_connection(self) -> bool:
        """
        Test the PostgreSQL connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            result = self.execute_query("SELECT version();")
            logger.info(f"✓ PostgreSQL connection test successful")
            logger.info(f"  Version: {result[0]['version'][:50]}...")
            return True
        except Exception as e:
            logger.error(f"✗ PostgreSQL connection test failed: {e}")
            return False

    def close(self):
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("✓ PostgreSQL connection pool closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_default_connection() -> PostgreSQLManager:
    """
    Get a PostgreSQL manager with default connection settings.

    Returns:
        PostgreSQLManager instance
    """
    return PostgreSQLManager()


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Test connection
    print("Testing PostgreSQL connection...")

    with PostgreSQLManager() as pg:
        # Test basic connection
        if pg.test_connection():
            print("\n✓ Connection successful!\n")

        # List HubSpot tables
        print("HubSpot tables:")
        tables = pg.get_hubspot_tables()
        for table in tables:
            print(f"  - hubspot.{table}")

        # Example query: Get contacts with icalps IDs
        print("\nQuerying contacts with icalps_contact_id...")
        query = """
            SELECT hs_object_id, firstname, lastname, email, icalps_contact_id
            FROM hubspot.contacts
            WHERE icalps_contact_id IS NOT NULL
            LIMIT 5;
        """
        df = pg.execute_query_df(query)
        print(df.to_string(index=False))
