"""
Entity Configuration System
============================

Declarative entity definitions linking properties from properties.py to SQL queries.

This module provides a clean separation between:
- Property definitions (what to extract) - from properties.py
- SQL query structure (how to extract) - defined here
- Connection parameters (where to extract from) - from config.py
- Data models (type-safe structures) - dataclasses

Usage:
    from entity_config import ENTITY_CONFIGS, EntityConfig

    # Get configuration for Case entity
    case_config = ENTITY_CONFIGS['Case']

    # Access properties
    all_props = case_config.get_all_properties()
    base_props = case_config.get_base_properties()

    # Generate SQL query
    query = case_config.build_query()
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable
from properties import (
    COMPANY_PROPERTIES,
    PERSON_PROPERTIES,
    OPPORTUNITY_PROPERTIES,
    COMMUNICATION_PROPERTIES,
    CASE_PROPERTIES,
    SOCIAL_NETWORK_PROPERTIES,
    ADDRESS_PROPERTIES
)


@dataclass
class EntityConfig:
    """
    Configuration for a single entity extraction.

    Attributes:
        name: Entity name (e.g., 'Case', 'Company')
        properties: Property dictionary from properties.py
        base_table: Primary table/view to query from
        database: Database name (default CRMICALPS)
        schema: Schema name (default dbo)
        joins: List of JOIN clauses for denormalization
        where_clause: Optional WHERE filter
        dataclass_factory: Function to create dataclass from dict
    """
    name: str
    properties: Dict[str, List[str]]
    base_table: str
    database: str = "CRMICALPS"
    schema: str = "dbo"
    joins: List[str] = field(default_factory=list)
    where_clause: Optional[str] = None
    dataclass_factory: Optional[Callable] = None

    def get_all_properties(self) -> List[str]:
        """Get all property names (base + denormalized + computed + metadata)"""
        all_props = list(self.properties.get('base', []))
        all_props.extend(self.properties.get('denormalized', []))
        all_props.extend(self.properties.get('computed', []))
        all_props.extend(self.properties.get('metadata', []))
        return all_props

    def get_base_properties(self) -> List[str]:
        """Get only base properties"""
        return list(self.properties.get('base', []))

    def get_denormalized_properties(self) -> List[str]:
        """Get only denormalized properties"""
        return list(self.properties.get('denormalized', []))

    def get_computed_properties(self) -> List[str]:
        """Get only computed properties"""
        return list(self.properties.get('computed', []))

    def build_query(self, include_metadata: bool = False) -> str:
        """
        Build complete SQL SELECT query from entity configuration.

        Args:
            include_metadata: Include metadata fields (bronze_extracted_at, etc.)

        Returns:
            Complete SQL query string
        """
        # Build SELECT clause
        select_fields = []

        # Add base properties (from base table)
        for prop in self.get_base_properties():
            select_fields.append(f"base.[{prop}]")

        # Add denormalized properties (aliased from JOINs)
        for prop in self.get_denormalized_properties():
            # These should already contain full specification like "comp.[Comp_Name] AS Company_Name"
            select_fields.append(prop)

        # Add computed properties (these come from the query itself)
        for prop in self.get_computed_properties():
            select_fields.append(prop)

        # Add metadata if requested
        if include_metadata:
            for prop in self.properties.get('metadata', []):
                select_fields.append(f"base.[{prop}]")

        # Build FROM clause
        from_clause = f"FROM [{self.database}].[{self.schema}].[{self.base_table}] base"

        # Build query
        query_parts = [
            "SELECT",
            ",\n        ".join(select_fields),
            from_clause
        ]

        # Add JOINs
        if self.joins:
            query_parts.extend(self.joins)

        # Add WHERE clause
        if self.where_clause:
            query_parts.append(f"WHERE {self.where_clause}")

        return "\n    ".join(query_parts)

    def get_primary_key(self) -> str:
        """Get primary key field name (assumes first base property)"""
        base_props = self.get_base_properties()
        return base_props[0] if base_props else None


# ============================================================================
# ENTITY CONFIGURATIONS
# ============================================================================

# Case Entity Configuration - Using actual available columns from vCases view
# Denormalized fields should map to "alias.Column AS Result_Name"
CASE_SIMPLE_PROPERTIES = {
    'base': [
        'Case_CaseId',
        'Case_PrimaryCompanyId',
        'Case_PrimaryPersonId',
        'Case_AssignedUserId',
        'Case_Description',
        'Case_Status',
        'Case_Stage',
        'Case_Priority',
        'Case_Opened',
        'Case_Closed'
    ],
    'denormalized': [
        'comp.[Comp_Name] AS Company_Name',
        'comp.[Comp_WebSite] AS Company_WebSite',
        'p.[Pers_FirstName] AS Person_FirstName',
        'p.[Pers_LastName] AS Person_LastName',
        'v.[Emai_EmailAddress] AS Person_EmailAddress'
    ],
    'metadata': []
}

CASE_CONFIG = EntityConfig(
    name="Case",
    properties=CASE_SIMPLE_PROPERTIES,  # Use simple properties that match actual schema
    base_table="vCases",  # Using view from SQL Server
    joins=[
        "LEFT JOIN [CRMICALPS].[dbo].[Company] comp ON base.[Case_PrimaryCompanyId] = comp.[Comp_CompanyId]",
        "LEFT JOIN [CRMICALPS].[dbo].[Person] p ON base.[Case_PrimaryPersonId] = p.[Pers_PersonId]",
        "LEFT JOIN [CRMICALPS].[dbo].[vEmailCompanyAndPerson] v ON base.[Case_PrimaryPersonId] = v.[Pers_PersonId]"
    ],
    where_clause=None
)

# Company Entity Configuration - Using actual available columns
COMPANY_SIMPLE_PROPERTIES = {
    'base': [
        'Comp_CompanyId',
        'Comp_PrimaryPersonId',
        'Comp_PrimaryAddressId',
        'Comp_Name',
        'Comp_Type',
        'Comp_Status',
        'Comp_Source',
        'Comp_Territory',
        'Comp_Revenue',
        'Comp_Employees',
        'Comp_Sector',
        'Comp_WebSite',
        'Comp_CreatedDate',
        'Comp_UpdatedDate'
    ],
    'denormalized': [
        'a.[Addr_AddressId] AS Address_Id',
        'a.[Addr_Address1] AS Address_Street1',
        'a.[Addr_City] AS Address_City',
        'a.[Addr_State] AS Address_State',
        'a.[Addr_Country] AS Address_Country',
        'a.[Addr_PostCode] AS Address_PostCode'
    ],
    'metadata': []
}

COMPANY_CONFIG = EntityConfig(
    name="Company",
    properties=COMPANY_SIMPLE_PROPERTIES,
    base_table="Company",
    joins=[
        "LEFT JOIN [CRMICALPS].[dbo].[Address] a ON base.[Comp_PrimaryAddressId] = a.[Addr_AddressId]"
    ],
    where_clause="base.[Comp_CompanyId] IS NOT NULL"
)

# Person Entity Configuration - Using actual available columns
PERSON_SIMPLE_PROPERTIES = {
    'base': [
        'Pers_PersonId',
        'Pers_CompanyId',
        'Pers_PrimaryAddressId',
        'Pers_Salutation',
        'Pers_FirstName',
        'Pers_LastName',
        'Pers_MiddleName',
        'Pers_Gender',
        'Pers_Title',
        'Pers_Department',
        'Pers_Status',
        'Pers_Source',
        'Pers_Territory',
        'Pers_WebSite',
        'Pers_CreatedDate',
        'Pers_UpdatedDate'
    ],
    'denormalized': [
        'c.[Comp_Name] AS Company_Name',
        'c.[Comp_WebSite] AS Company_WebSite',
        'a.[Addr_AddressId] AS Address_Id',
        'a.[Addr_Address1] AS Address_Street1',
        'a.[Addr_City] AS Address_City',
        'a.[Addr_Country] AS Address_Country'
    ],
    'metadata': []
}

PERSON_CONFIG = EntityConfig(
    name="Person",
    properties=PERSON_SIMPLE_PROPERTIES,
    base_table="Person",
    joins=[
        "LEFT JOIN [CRMICALPS].[dbo].[Company] c ON base.[Pers_CompanyId] = c.[Comp_CompanyId]",
        "LEFT JOIN [CRMICALPS].[dbo].[Address] a ON base.[Pers_PrimaryAddressId] = a.[Addr_AddressId]"
    ],
    where_clause="base.[Pers_PersonId] IS NOT NULL"
)

# Opportunity Entity Configuration
OPPORTUNITY_CONFIG = EntityConfig(
    name="Opportunity",
    properties=OPPORTUNITY_PROPERTIES,
    base_table="Opportunity",
    joins=[
        "LEFT JOIN [CRMICALPS].[dbo].[Company] c ON base.[Oppo_PrimaryCompanyId] = c.[Comp_CompanyId]",
        "LEFT JOIN [CRMICALPS].[dbo].[Person] p ON base.[Oppo_PrimaryPersonId] = p.[Pers_PersonId]"
    ],
    where_clause="base.[Oppo_OpportunityId] IS NOT NULL"
)

# Communication Entity Configuration - Using vCalendarCommunication view
# This view already includes denormalized Person and Company data
COMMUNICATION_SIMPLE_PROPERTIES = {
    'base': [
        'Comm_CommunicationId',
        'Comm_OpportunityId',
        'Comm_CaseId',
        'Comm_Type',
        'Comm_Action',
        'Comm_Status',
        'Comm_Priority',
        'Comm_DateTime',
        'Comm_ToDateTime',
        'Comm_Note',
        'Comm_Subject',
        'Comm_Email',
        'Comm_CreatedDate',
        'Comm_UpdatedDate',
        'Comm_OriginalDateTime',
        'Comm_OriginalToDateTime'
    ],
    'denormalized': [
        # Person data already in view
        'base.[Pers_PersonId] AS Person_Id',
        'base.[Pers_FirstName] AS Person_FirstName',
        'base.[Pers_LastName] AS Person_LastName',
        'base.[Pers_EmailAddress] AS Person_EmailAddress',
        # Company data already in view
        'base.[Comp_CompanyId] AS Company_Id',
        'base.[Comp_Name] AS Company_Name',
        # Case data already in view
        'base.[Case_Description] AS Case_Description',
        'base.[Case_PrimaryCompanyId] AS Case_CompanyId',
        'base.[Case_PrimaryPersonId] AS Case_PersonId'
    ],
    'metadata': []
}

COMMUNICATION_CONFIG = EntityConfig(
    name="Communication",
    properties=COMMUNICATION_SIMPLE_PROPERTIES,
    base_table="vCalendarCommunication",  # Using view with denormalized data
    joins=[],  # No joins needed - view already has everything
    where_clause="base.[Comm_CommunicationId] IS NOT NULL"
)

# Social Network Entity Configuration
SOCIAL_NETWORK_CONFIG = EntityConfig(
    name="SocialNetwork",
    properties=SOCIAL_NETWORK_PROPERTIES,
    base_table="SocialNetwork",
    joins=[
        "LEFT JOIN [CRMICALPS].[dbo].[Person] p ON base.[SoN_PersonId] = p.[Pers_PersonId]"
    ],
    where_clause="base.[SoN_SocialNetworkId] IS NOT NULL"
)

# Address Entity Configuration - Using actual available columns
ADDRESS_SIMPLE_PROPERTIES = {
    'base': [
        'Addr_AddressId',
        'Addr_Address1',
        'Addr_Address2',
        'Addr_Address3',
        'Addr_City',
        'Addr_State',
        'Addr_Country',
        'Addr_PostCode',
        'Addr_CreatedDate',
        'Addr_UpdatedDate'
    ],
    'denormalized': [],  # Address is typically joined TO other entities, not the other way
    'metadata': []
}

ADDRESS_CONFIG = EntityConfig(
    name="Address",
    properties=ADDRESS_SIMPLE_PROPERTIES,
    base_table="Address",
    joins=[],  # No joins - Address is a lookup table
    where_clause="base.[Addr_AddressId] IS NOT NULL"
)


# ============================================================================
# ENTITY REGISTRY
# ============================================================================

ENTITY_CONFIGS: Dict[str, EntityConfig] = {
    'Case': CASE_CONFIG,
    'Company': COMPANY_CONFIG,
    'Person': PERSON_CONFIG,
    'Opportunity': OPPORTUNITY_CONFIG,
    'Communication': COMMUNICATION_CONFIG,
    'SocialNetwork': SOCIAL_NETWORK_CONFIG,
    'Address': ADDRESS_CONFIG
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_entity_config(entity_name: str) -> EntityConfig:
    """
    Get entity configuration by name.

    Args:
        entity_name: Name of entity ('Case', 'Company', etc.)

    Returns:
        EntityConfig instance

    Raises:
        ValueError: If entity not found
    """
    if entity_name not in ENTITY_CONFIGS:
        raise ValueError(f"Unknown entity: {entity_name}. Available: {list(ENTITY_CONFIGS.keys())}")
    return ENTITY_CONFIGS[entity_name]


def list_entities() -> List[str]:
    """Get list of all configured entity names"""
    return list(ENTITY_CONFIGS.keys())


def print_entity_configs():
    """Print summary of all entity configurations"""
    print("\n" + "=" * 70)
    print("Entity Configuration Summary")
    print("=" * 70)

    for name, config in ENTITY_CONFIGS.items():
        all_props = config.get_all_properties()
        base_props = config.get_base_properties()
        denorm_props = config.get_denormalized_properties()
        computed_props = config.get_computed_properties()

        print(f"\n{name}:")
        print(f"  Base Table: [{config.database}].[{config.schema}].[{config.base_table}]")
        print(f"  Properties: {len(all_props)} total")
        print(f"    - Base: {len(base_props)}")
        print(f"    - Denormalized: {len(denorm_props)}")
        print(f"    - Computed: {len(computed_props)}")
        print(f"  JOINs: {len(config.joins)}")
        print(f"  Primary Key: {config.get_primary_key()}")

    print("\n" + "=" * 70 + "\n")


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Print configuration summary
    print_entity_configs()

    # Example: Build query for Case entity
    print("\n" + "=" * 70)
    print("Example: Generated SQL Query for Case Entity")
    print("=" * 70 + "\n")

    case_config = get_entity_config('Case')
    query = case_config.build_query(include_metadata=True)
    print(query)

    print("\n" + "=" * 70)
    print("Example: Company Entity Properties")
    print("=" * 70 + "\n")

    company_config = get_entity_config('Company')
    print(f"All properties ({len(company_config.get_all_properties())}):")
    for prop in company_config.get_all_properties()[:5]:
        print(f"  - {prop}")
    print(f"  ... ({len(company_config.get_all_properties()) - 5} more)")
