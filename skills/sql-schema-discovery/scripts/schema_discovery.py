#!/usr/bin/env python3
"""
SQL Server Schema Discovery Module

Provides automated database schema introspection for SQL Server databases.
Queries information_schema system views to discover tables, columns, data types,
constraints, and relationships.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Any
from datetime import datetime
from decimal import Decimal
import pyodbc


@dataclass
class TableMetadata:
    """Metadata for a database table"""
    catalog: str
    schema: str
    name: str
    type: str  # BASE TABLE, VIEW, etc.


@dataclass
class ColumnMetadata:
    """Metadata for a table column"""
    name: str
    data_type: str
    python_type: type
    is_nullable: bool
    max_length: Optional[int] = None
    ordinal_position: int = 0
    default_value: Optional[str] = None


@dataclass
class ForeignKeyRelationship:
    """Foreign key relationship metadata"""
    column_name: str
    referenced_table: str
    referenced_column: str
    constraint_name: str
    cardinality: str = "many:1"  # Default assumption


@dataclass
class DatabaseSchema:
    """Complete database schema information"""
    name: str
    tables: Dict[str, 'TableInfo']
    type_mappings: Dict[str, type]


@dataclass
class TableInfo:
    """Complete information about a table"""
    name: str
    columns: List[ColumnMetadata]
    foreign_keys: List[ForeignKeyRelationship]
    primary_keys: List[str]


class SchemaDiscoveryError(Exception):
    """Exception raised for schema discovery errors"""
    pass


class SchemaDiscovery:
    """
    Main class for SQL Server schema discovery.

    Usage:
        discovery = SchemaDiscovery(connection_string="your_connection_string")
        tables = discovery.discover_tables("CRMICALPS")
        columns = discovery.discover_columns("Company")
    """

    # SQL Server to Python type mappings
    TYPE_MAPPINGS = {
        'int': int,
        'bigint': int,
        'smallint': int,
        'tinyint': int,
        'decimal': Decimal,
        'numeric': Decimal,
        'money': Decimal,
        'smallmoney': Decimal,
        'float': float,
        'real': float,
        'bit': bool,
        'varchar': str,
        'nvarchar': str,
        'char': str,
        'nchar': str,
        'text': str,
        'ntext': str,
        'datetime': datetime,
        'datetime2': datetime,
        'date': datetime,
        'time': datetime,
        'smalldatetime': datetime,
        'uniqueidentifier': str,  # UUID as string
        'xml': str,
        'binary': bytes,
        'varbinary': bytes,
        'image': bytes,
    }

    def __init__(self, connection_string: Optional[str] = None,
                 connection_manager: Optional[Any] = None):
        """
        Initialize SchemaDiscovery.

        Args:
            connection_string: ODBC connection string for SQL Server
            connection_manager: Optional ConnectionManager instance (for connection pooling)
        """
        self.connection_string = connection_string
        self.connection_manager = connection_manager
        self._schema_cache = {}

    def _get_connection(self):
        """Get database connection (from manager or create new)"""
        if self.connection_manager:
            return self.connection_manager.get_connection()
        elif self.connection_string:
            return pyodbc.connect(self.connection_string)
        else:
            raise SchemaDiscoveryError("No connection string or connection manager provided")

    def discover_tables(self, database_name: str) -> List[TableMetadata]:
        """
        Discover all tables in the specified database.

        Args:
            database_name: Name of the database to inspect

        Returns:
            List of TableMetadata objects

        Example:
            >>> discovery = SchemaDiscovery(connection_string)
            >>> tables = discovery.discover_tables("CRMICALPS")
            >>> for table in tables:
            ...     print(f"{table.schema}.{table.name}")
        """
        query = """
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_catalog = ?
            AND table_schema = 'dbo'
        ORDER BY table_name
        """

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, database_name)
                results = cursor.fetchall()

                tables = [
                    TableMetadata(
                        catalog=row.table_catalog,
                        schema=row.table_schema,
                        name=row.table_name,
                        type=row.table_type
                    )
                    for row in results
                ]

                return tables

        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to discover tables: {str(e)}")

    def discover_columns(self, table_name: str) -> List[ColumnMetadata]:
        """
        Discover all columns for a specific table.

        Args:
            table_name: Name of the table to inspect

        Returns:
            List of ColumnMetadata objects

        Example:
            >>> columns = discovery.discover_columns("Company")
            >>> for col in columns:
            ...     print(f"{col.name}: {col.data_type}")
        """
        query = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            character_maximum_length,
            ordinal_position,
            column_default
        FROM information_schema.columns
        WHERE table_name = ?
        ORDER BY ordinal_position
        """

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, table_name)
                results = cursor.fetchall()

                columns = [
                    ColumnMetadata(
                        name=row.column_name,
                        data_type=row.data_type,
                        python_type=self.sql_type_to_python_type(row.data_type),
                        is_nullable=(row.is_nullable == 'YES'),
                        max_length=row.character_maximum_length,
                        ordinal_position=row.ordinal_position,
                        default_value=row.column_default
                    )
                    for row in results
                ]

                return columns

        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to discover columns for {table_name}: {str(e)}")

    def discover_relationships(self, table_name: str) -> List[ForeignKeyRelationship]:
        """
        Discover foreign key relationships for a table.

        Args:
            table_name: Name of the table to inspect

        Returns:
            List of ForeignKeyRelationship objects

        Example:
            >>> fks = discovery.discover_relationships("Cases")
            >>> for fk in fks:
            ...     print(f"{fk.column_name} -> {fk.referenced_table}.{fk.referenced_column}")
        """
        query = """
        SELECT
            kcu.column_name,
            ccu.table_name AS referenced_table,
            ccu.column_name AS referenced_column,
            rc.constraint_name
        FROM information_schema.referential_constraints rc
        INNER JOIN information_schema.key_column_usage kcu
            ON rc.constraint_name = kcu.constraint_name
        INNER JOIN information_schema.constraint_column_usage ccu
            ON rc.unique_constraint_name = ccu.constraint_name
        WHERE kcu.table_name = ?
        """

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, table_name)
                results = cursor.fetchall()

                relationships = [
                    ForeignKeyRelationship(
                        column_name=row.column_name,
                        referenced_table=row.referenced_table,
                        referenced_column=row.referenced_column,
                        constraint_name=row.constraint_name,
                        cardinality="many:1"  # Standard FK assumption
                    )
                    for row in results
                ]

                return relationships

        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to discover relationships for {table_name}: {str(e)}")

    def discover_primary_keys(self, table_name: str) -> List[str]:
        """
        Discover primary key columns for a table.

        Args:
            table_name: Name of the table to inspect

        Returns:
            List of primary key column names
        """
        query = """
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE objectproperty(object_id(constraint_name), 'IsPrimaryKey') = 1
            AND table_name = ?
        """

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, table_name)
                results = cursor.fetchall()
                return [row.column_name for row in results]

        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to discover primary keys for {table_name}: {str(e)}")

    def inspect_database(self, db_name: str) -> DatabaseSchema:
        """
        Perform complete database inspection.

        Args:
            db_name: Name of the database to inspect

        Returns:
            DatabaseSchema object with complete schema information

        Example:
            >>> schema = discovery.inspect_database("CRMICALPS")
            >>> print(f"Database: {schema.name}")
            >>> print(f"Tables: {len(schema.tables)}")
            >>> for table_name, table_info in schema.tables.items():
            ...     print(f"  {table_name}: {len(table_info.columns)} columns")
        """
        # Check cache first
        if db_name in self._schema_cache:
            return self._schema_cache[db_name]

        try:
            # Discover all tables
            tables = self.discover_tables(db_name)

            # Build complete schema
            table_info_dict = {}

            for table in tables:
                table_name = table.name

                # Discover columns
                columns = self.discover_columns(table_name)

                # Discover relationships
                foreign_keys = self.discover_relationships(table_name)

                # Discover primary keys
                primary_keys = self.discover_primary_keys(table_name)

                # Store table info
                table_info_dict[table_name] = TableInfo(
                    name=table_name,
                    columns=columns,
                    foreign_keys=foreign_keys,
                    primary_keys=primary_keys
                )

            # Create DatabaseSchema object
            schema = DatabaseSchema(
                name=db_name,
                tables=table_info_dict,
                type_mappings=self.TYPE_MAPPINGS
            )

            # Cache the schema
            self._schema_cache[db_name] = schema

            return schema

        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to inspect database {db_name}: {str(e)}")

    def sql_type_to_python_type(self, sql_type: str) -> type:
        """
        Map SQL Server data type to Python type.

        Args:
            sql_type: SQL Server data type name

        Returns:
            Python type object

        Example:
            >>> discovery.sql_type_to_python_type("nvarchar")
            <class 'str'>
            >>> discovery.sql_type_to_python_type("int")
            <class 'int'>
        """
        sql_type_lower = sql_type.lower()
        return self.TYPE_MAPPINGS.get(sql_type_lower, str)  # Default to str if unknown

    def build_query(self, base_table: str, columns: List[str],
                    include_joins: bool = True) -> 'QueryBuilder':
        """
        Build a SQL query based on discovered schema.

        Args:
            base_table: Base table name
            columns: List of column names to select
            include_joins: Whether to automatically include JOINs based on FKs

        Returns:
            QueryBuilder object

        Example:
            >>> query = discovery.build_query(
            ...     base_table="Address",
            ...     columns=["Addr_AddressId", "Addr_CompanyId", "Addr_Street1"],
            ...     include_joins=True
            ... )
            >>> print(query.to_sql())
        """
        from .query_builder import QueryBuilder
        return QueryBuilder(self, base_table, columns, include_joins)

    def clear_cache(self):
        """Clear the schema cache"""
        self._schema_cache.clear()


# Example usage
if __name__ == "__main__":
    # Example connection string
    connection_string = (
        "DRIVER={SQL Server};"
        "SERVER=your_server;"
        "DATABASE=CRMICALPS;"
        "Trusted_Connection=yes;"
    )

    # Initialize discovery
    discovery = SchemaDiscovery(connection_string)

    # Discover tables
    print("=== TABLES ===")
    tables = discovery.discover_tables("CRMICALPS")
    for table in tables[:5]:  # First 5 tables
        print(f"{table.schema}.{table.name} ({table.type})")

    # Discover columns for Company table
    print("\n=== COMPANY COLUMNS ===")
    columns = discovery.discover_columns("Company")
    for col in columns[:10]:  # First 10 columns
        nullable = "NULL" if col.is_nullable else "NOT NULL"
        print(f"{col.name}: {col.data_type} ({col.python_type.__name__}) {nullable}")

    # Discover relationships for Cases table
    print("\n=== CASES RELATIONSHIPS ===")
    relationships = discovery.discover_relationships("Cases")
    for fk in relationships:
        print(f"{fk.column_name} -> {fk.referenced_table}.{fk.referenced_column}")

    # Complete database inspection
    print("\n=== DATABASE INSPECTION ===")
    schema = discovery.inspect_database("CRMICALPS")
    print(f"Database: {schema.name}")
    print(f"Total tables: {len(schema.tables)}")
