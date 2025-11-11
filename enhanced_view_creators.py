"""
Enhanced View Creation Methods with Complete Properties
========================================================

This module provides enhanced versions of the SQL view creation methods,
integrating complete property lists from ENTITY_PROPERTIES.md with the
existing view structure.

Each method now includes:
- Base properties from source tables
- Denormalized properties from JOINs
- Computed properties (where applicable)
- Metadata fields
- Complete SELECT statements with all columns

Usage:
    from enhanced_view_creators import create_companies_view, create_persons_view

    # Create view with complete properties
    create_companies_view(cursor)
    create_persons_view(cursor)
"""

import pyodbc
from typing import Optional


def create_companies_view(cursor: pyodbc.Cursor) -> None:
    """
    Create or replace the Processed_Companies view with complete properties.

    Properties included:
    - Base Company Properties (15 fields)
    - Denormalized Address Properties (7 fields)
    - Metadata (2 fields)

    Total: 24 properties
    """
    view_sql = """
    IF OBJECT_ID('Processed_Companies', 'V') IS NOT NULL
        DROP VIEW Processed_Companies;
    """
    cursor.execute(view_sql)

    view_sql = """
    CREATE VIEW Processed_Companies AS
    SELECT DISTINCT
        -- Base Company Properties
        c.Comp_CompanyId,
        c.Comp_Name,
        c.Comp_Website,
        c.Comp_Type,
        c.Comp_Status,
        c.Comp_Source,
        c.Comp_Territory,
        c.Comp_Sector,
        c.Comp_Revenue,
        c.Comp_Employees,
        c.Comp_EmailAddress,
        c.Comp_PhoneCountryCode,
        c.Comp_PhoneCityCode,
        c.Comp_PhoneNumber,
        c.Comp_CreatedDate,
        c.Comp_UpdatedDate,

        -- Denormalized Address Properties
        a.Addr_AddressId,
        a.Addr_Street1,
        a.Addr_Street2,
        a.Addr_City,
        a.Addr_State,
        a.Addr_PostCode,
        a.Addr_Country,

        -- Metadata
        c.bronze_extracted_at,
        c.bronze_source_file
    FROM Bronze_companies c
    LEFT JOIN Bronze_addresses a ON c.Comp_CompanyId = a.Addr_CompanyId
    WHERE c.Comp_CompanyId IS NOT NULL
    """
    cursor.execute(view_sql)
    cursor.commit()
    print("✓ Created Processed_Companies view with 24 properties")


def create_persons_view(cursor: pyodbc.Cursor) -> None:
    """
    Create or replace the Processed_Persons view with complete properties.

    Properties included:
    - Base Person Properties (16 fields)
    - Denormalized Company Properties (3 fields)
    - Denormalized Address Properties (7 fields)
    - Metadata (2 fields)

    Total: 28 properties
    """
    view_sql = """
    IF OBJECT_ID('Processed_Persons', 'V') IS NOT NULL
        DROP VIEW Processed_Persons;
    """
    cursor.execute(view_sql)

    view_sql = """
    CREATE VIEW Processed_Persons AS
    SELECT DISTINCT
        -- Base Person Properties
        p.Pers_PersonId,
        p.Pers_Salutation,
        p.Pers_FirstName,
        p.Pers_MiddleName,
        p.Pers_LastName,
        p.Pers_EmailAddress,
        p.Pers_PhoneCountryCode,
        p.Pers_PhoneCityCode,
        p.Pers_PhoneNumber,
        p.Pers_Territory,
        p.Pers_Type,
        p.Pers_Status,
        p.Pers_Department,
        p.Pers_JobTitle,
        p.Pers_CreatedDate,
        p.Pers_UpdatedDate,

        -- Denormalized Company Properties
        c.Comp_CompanyId,
        c.Comp_Name AS Company_Name,
        c.Comp_Website AS Company_Website,

        -- Denormalized Address Properties
        a.Addr_AddressId,
        a.Addr_Street1,
        a.Addr_Street2,
        a.Addr_City,
        a.Addr_State,
        a.Addr_PostCode,
        a.Addr_Country,

        -- Metadata
        p.bronze_extracted_at,
        p.bronze_source_file
    FROM Bronze_persons p
    LEFT JOIN Bronze_companies c ON p.Pers_CompanyId = c.Comp_CompanyId
    LEFT JOIN Bronze_addresses a ON p.Pers_PersonId = a.Addr_PersonId
    WHERE p.Pers_PersonId IS NOT NULL
    """
    cursor.execute(view_sql)
    cursor.commit()
    print("✓ Created Processed_Persons view with 28 properties")


def create_opportunities_view(cursor: pyodbc.Cursor) -> None:
    """
    Create or replace the Processed_Opportunities view with complete properties.

    Properties included:
    - Base Opportunity Properties (17 fields)
    - Denormalized Company Properties (3 fields)
    - Denormalized Person Properties (3 fields)
    - Computed Properties (2 fields: Weighted_Forecast, Net_Amount)
    - Metadata (2 fields)

    Total: 27 properties
    """
    view_sql = """
    IF OBJECT_ID('Processed_Opportunities', 'V') IS NOT NULL
        DROP VIEW Processed_Opportunities;
    """
    cursor.execute(view_sql)

    view_sql = """
    CREATE VIEW Processed_Opportunities AS
    SELECT DISTINCT
        -- Base Opportunity Properties
        o.Oppo_OpportunityId,
        o.Oppo_Description,
        o.Oppo_Status,
        o.Oppo_Stage,
        o.Oppo_Certainty,
        o.Oppo_Forecast,
        o.Oppo_CloseDate,
        o.Oppo_Source,
        o.Oppo_Type,
        o.Oppo_Territory,
        o.Oppo_ProductLine,
        o.Oppo_EstimatedRevenue,
        o.Oppo_DiscountPercent,
        o.Oppo_Notes,
        o.Oppo_CreatedDate,
        o.Oppo_UpdatedDate,
        o.Oppo_LastActivityDate,

        -- Denormalized Company Properties
        c.Comp_CompanyId,
        c.Comp_Name AS Company_Name,
        c.Comp_Website AS Company_Website,

        -- Denormalized Person Properties
        p.Pers_PersonId,
        p.Pers_FirstName AS Person_FirstName,
        p.Pers_LastName AS Person_LastName,

        -- Computed Properties
        (o.Oppo_Forecast * o.Oppo_Certainty / 100.0) AS Weighted_Forecast,
        (o.Oppo_EstimatedRevenue * (1 - o.Oppo_DiscountPercent / 100.0)) AS Net_Amount,

        -- Metadata
        o.bronze_extracted_at,
        o.bronze_source_file
    FROM Bronze_opportunities o
    LEFT JOIN Bronze_companies c ON o.Oppo_PrimaryCompanyId = c.Comp_CompanyId
    LEFT JOIN Bronze_persons p ON o.Oppo_PrimaryPersonId = p.Pers_PersonId
    WHERE o.Oppo_OpportunityId IS NOT NULL
    """
    cursor.execute(view_sql)
    cursor.commit()
    print("✓ Created Processed_Opportunities view with 27 properties (including 2 computed)")


def create_communications_view(cursor: pyodbc.Cursor) -> None:
    """
    Create or replace the Processed_Communications view with complete properties.

    Properties included:
    - Base Communication Properties (11 fields)
    - Denormalized Company Properties (2 fields)
    - Denormalized Person Properties (2 fields)
    - Denormalized Opportunity Properties (2 fields)
    - Metadata (2 fields)

    Total: 19 properties
    """
    view_sql = """
    IF OBJECT_ID('Processed_Communications', 'V') IS NOT NULL
        DROP VIEW Processed_Communications;
    """
    cursor.execute(view_sql)

    view_sql = """
    CREATE VIEW Processed_Communications AS
    SELECT DISTINCT
        -- Base Communication Properties
        comm.Comm_CommunicationId,
        comm.Comm_Type,
        comm.Comm_Action,
        comm.Comm_Status,
        comm.Comm_Subject,
        comm.Comm_Note,
        comm.Comm_OriginalDateTime,
        comm.Comm_DateTime,
        comm.Comm_Priority,
        comm.Comm_CreatedDate,
        comm.Comm_UpdatedDate,

        -- Denormalized Company Properties
        c.Comp_CompanyId,
        c.Comp_Name AS Company_Name,

        -- Denormalized Person Properties
        p.Pers_PersonId,
        CONCAT(p.Pers_FirstName, ' ', p.Pers_LastName) AS Person_FullName,

        -- Denormalized Opportunity Properties
        o.Oppo_OpportunityId,
        o.Oppo_Description AS Opportunity_Description,

        -- Metadata
        comm.bronze_extracted_at,
        comm.bronze_source_file
    FROM Bronze_communications comm
    LEFT JOIN Bronze_companies c ON comm.Comm_CompanyId = c.Comp_CompanyId
    LEFT JOIN Bronze_persons p ON comm.Comm_PersonId = p.Pers_PersonId
    LEFT JOIN Bronze_opportunities o ON comm.Comm_OpportunityId = o.Oppo_OpportunityId
    WHERE comm.Comm_CommunicationId IS NOT NULL
    """
    cursor.execute(view_sql)
    cursor.commit()
    print("✓ Created Processed_Communications view with 19 properties")


def create_cases_view(cursor: pyodbc.Cursor) -> None:
    """
    Create or replace the Processed_Cases view with complete properties.

    Properties included:
    - Base Case Properties (21 fields)
    - Denormalized Company Properties (3 fields)
    - Denormalized Person Properties (3 fields)
    - Metadata (2 fields)

    Total: 29 properties
    """
    view_sql = """
    IF OBJECT_ID('Processed_Cases', 'V') IS NOT NULL
        DROP VIEW Processed_Cases;
    """
    cursor.execute(view_sql)

    view_sql = """
    CREATE VIEW Processed_Cases AS
    SELECT DISTINCT
        -- Base Case Properties
        ca.Case_CaseId,
        ca.Case_Reference,
        ca.Case_Status,
        ca.Case_Type,
        ca.Case_Priority,
        ca.Case_Severity,
        ca.Case_Origin,
        ca.Case_Subject,
        ca.Case_Description,
        ca.Case_Resolution,
        ca.Case_OpenedDate,
        ca.Case_ClosedDate,
        ca.Case_ResponseTime,
        ca.Case_ResolutionTime,
        ca.Case_SLAStatus,
        ca.Case_Product,
        ca.Case_Category,
        ca.Case_Subcategory,
        ca.Case_AssignedTo,
        ca.Case_CreatedDate,
        ca.Case_UpdatedDate,

        -- Denormalized Company Properties
        c.Comp_CompanyId,
        c.Comp_Name AS Company_Name,
        c.Comp_Website AS Company_Website,

        -- Denormalized Person Properties
        p.Pers_PersonId,
        p.Pers_FirstName AS Person_FirstName,
        p.Pers_LastName AS Person_LastName,

        -- Metadata
        ca.bronze_extracted_at,
        ca.bronze_source_file
    FROM Bronze_cases ca
    LEFT JOIN Bronze_companies c ON ca.Case_PrimaryCompanyId = c.Comp_CompanyId
    LEFT JOIN Bronze_persons p ON ca.Case_PrimaryPersonId = p.Pers_PersonId
    WHERE ca.Case_CaseId IS NOT NULL
    """
    cursor.execute(view_sql)
    cursor.commit()
    print("✓ Created Processed_Cases view with 29 properties")


def create_social_networks_view(cursor: pyodbc.Cursor) -> None:
    """
    Create or replace the Processed_SocialNetworks view with complete properties.

    Properties included:
    - Base Social Network Properties (9 fields)
    - Denormalized Person Properties (3 fields)
    - Metadata (2 fields)

    Total: 14 properties
    """
    view_sql = """
    IF OBJECT_ID('Processed_SocialNetworks', 'V') IS NOT NULL
        DROP VIEW Processed_SocialNetworks;
    """
    cursor.execute(view_sql)

    view_sql = """
    CREATE VIEW Processed_SocialNetworks AS
    SELECT DISTINCT
        -- Base Social Network Properties
        sn.SoN_SocialNetworkId,
        sn.SoN_Platform,
        sn.SoN_ProfileURL,
        sn.SoN_Username,
        sn.SoN_FollowerCount,
        sn.SoN_ConnectionCount,
        sn.SoN_Verified,
        sn.SoN_CreatedDate,
        sn.SoN_UpdatedDate,

        -- Denormalized Person Properties
        p.Pers_PersonId,
        p.Pers_FirstName AS Person_FirstName,
        p.Pers_LastName AS Person_LastName,

        -- Metadata
        sn.bronze_extracted_at,
        sn.bronze_source_file
    FROM Bronze_social_networks sn
    LEFT JOIN Bronze_persons p ON sn.SoN_PersonId = p.Pers_PersonId
    WHERE sn.SoN_SocialNetworkId IS NOT NULL
    """
    cursor.execute(view_sql)
    cursor.commit()
    print("✓ Created Processed_SocialNetworks view with 14 properties")


def create_addresses_view(cursor: pyodbc.Cursor) -> None:
    """
    Create or replace the Processed_Addresses view with complete properties.

    Properties included:
    - Base Address Properties (10 fields)
    - Denormalized Company Properties (2 fields, if linked)
    - Denormalized Person Properties (2 fields, if linked)
    - Metadata (2 fields)

    Total: 16 properties
    """
    view_sql = """
    IF OBJECT_ID('Processed_Addresses', 'V') IS NOT NULL
        DROP VIEW Processed_Addresses;
    """
    cursor.execute(view_sql)

    view_sql = """
    CREATE VIEW Processed_Addresses AS
    SELECT DISTINCT
        -- Base Address Properties
        a.Addr_AddressId,
        a.Addr_Type,
        a.Addr_Street1,
        a.Addr_Street2,
        a.Addr_City,
        a.Addr_State,
        a.Addr_PostCode,
        a.Addr_Country,
        a.Addr_CreatedDate,
        a.Addr_UpdatedDate,

        -- Denormalized Company Properties (if linked to company)
        c.Comp_CompanyId,
        c.Comp_Name AS Company_Name,

        -- Denormalized Person Properties (if linked to person)
        p.Pers_PersonId,
        CONCAT(p.Pers_FirstName, ' ', p.Pers_LastName) AS Person_FullName,

        -- Metadata
        a.bronze_extracted_at,
        a.bronze_source_file
    FROM Bronze_addresses a
    LEFT JOIN Bronze_companies c ON a.Addr_CompanyId = c.Comp_CompanyId
    LEFT JOIN Bronze_persons p ON a.Addr_PersonId = p.Pers_PersonId
    WHERE a.Addr_AddressId IS NOT NULL
    """
    cursor.execute(view_sql)
    cursor.commit()
    print("✓ Created Processed_Addresses view with 16 properties")


def create_all_views(cursor: pyodbc.Cursor) -> None:
    """
    Create all enhanced views in sequence.

    This is the main entry point to set up all views with complete properties.

    Views created:
    1. Processed_Companies (24 properties)
    2. Processed_Persons (28 properties)
    3. Processed_Opportunities (27 properties)
    4. Processed_Communications (19 properties)
    5. Processed_Cases (29 properties)
    6. Processed_SocialNetworks (14 properties)
    7. Processed_Addresses (16 properties)

    Usage:
        from sql_connection_manager import ConnectionManager
        from enhanced_view_creators import create_all_views

        conn_mgr = ConnectionManager(connection_string)
        with conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            create_all_views(cursor)
    """
    print("\n" + "=" * 60)
    print("Creating Enhanced Views with Complete Properties")
    print("=" * 60 + "\n")

    create_companies_view(cursor)
    create_persons_view(cursor)
    create_opportunities_view(cursor)
    create_communications_view(cursor)
    create_cases_view(cursor)
    create_social_networks_view(cursor)
    create_addresses_view(cursor)

    print("\n" + "=" * 60)
    print("✓ All 7 views created successfully")
    print("=" * 60)
    print("\nTotal properties across all views: 157")
    print("  - Companies: 24")
    print("  - Persons: 28")
    print("  - Opportunities: 27 (including 2 computed)")
    print("  - Communications: 19")
    print("  - Cases: 29")
    print("  - Social Networks: 14")
    print("  - Addresses: 16")


if __name__ == "__main__":
    """
    Test script to create all views.

    Before running:
    1. Ensure Bronze layer tables exist
    2. Update config.py with correct connection details
    3. Install required packages: pip install pyodbc
    """
    from config import get_connection_string
    import pyodbc

    try:
        connection_string = get_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        create_all_views(cursor)

        cursor.close()
        conn.close()

        print("\n✓ Database connection closed")

    except Exception as e:
        print(f"\n❌ Error creating views: {str(e)}")
        print("\nTroubleshooting:")
        print("1. Check that Bronze layer tables exist")
        print("2. Verify config.py connection settings")
        print("3. Ensure pyodbc is installed")
        print("4. Check database permissions")
