"""
Entity Property Definitions for IC_Load Pipeline
=================================================

This module contains complete property definitions for all entities,
organized by entity type. These definitions match the enhanced SQL views
in enhanced_view_creators.py.

Each entity includes:
- Base properties from source table
- Denormalized properties from JOINs
- Computed properties (where applicable)
- Metadata fields

Usage:
    from properties import COMPANY_PROPERTIES, PERSON_PROPERTIES

    # Get all property names for an entity
    company_props = COMPANY_PROPERTIES['base'] + COMPANY_PROPERTIES['denormalized']
"""

# ============================================================================
# COMPANY PROPERTIES (24 total)
# ============================================================================

COMPANY_PROPERTIES = {
    'base': [
        'Comp_CompanyId',           # Primary key
        'Comp_Name',                # Company name
        'Comp_Website',             # Website URL
        'Comp_Type',                # Type (e.g., Customer, Prospect)
        'Comp_Status',              # Status (e.g., Active, Inactive)
        'Comp_Source',              # Lead source
        'Comp_Territory',           # Geographic territory
        'Comp_Sector',              # Industry sector
        'Comp_Revenue',             # Annual revenue
        'Comp_Employees',           # Number of employees
        'Comp_EmailAddress',        # Primary email
        'Comp_PhoneCountryCode',    # Phone country code
        'Comp_PhoneCityCode',       # Phone city/area code
        'Comp_PhoneNumber',         # Phone number
        'Comp_CreatedDate',         # Record creation date
        'Comp_UpdatedDate',         # Last update date
    ],
    'denormalized': [
        # From Address JOIN (many:1 - Company can have multiple addresses)
        'Addr_AddressId',           # Address ID
        'Addr_Street1',             # Street address line 1
        'Addr_Street2',             # Street address line 2
        'Addr_City',                # City
        'Addr_State',               # State/Province
        'Addr_PostCode',            # Postal code
        'Addr_Country',             # Country
    ],
    'metadata': [
        'bronze_extracted_at',      # Extraction timestamp
        'bronze_source_file',       # Source CSV filename
    ]
}

# ============================================================================
# PERSON PROPERTIES (28 total)
# ============================================================================

PERSON_PROPERTIES = {
    'base': [
        'Pers_PersonId',            # Primary key
        'Pers_Salutation',          # Salutation (Mr., Ms., Dr., etc.)
        'Pers_FirstName',           # First name
        'Pers_MiddleName',          # Middle name
        'Pers_LastName',            # Last name
        'Pers_EmailAddress',        # Primary email
        'Pers_PhoneCountryCode',    # Phone country code
        'Pers_PhoneCityCode',       # Phone city/area code
        'Pers_PhoneNumber',         # Phone number
        'Pers_Territory',           # Geographic territory
        'Pers_Type',                # Type (e.g., Contact, Lead)
        'Pers_Status',              # Status (e.g., Active, Inactive)
        'Pers_Department',          # Department
        'Pers_JobTitle',            # Job title
        'Pers_CreatedDate',         # Record creation date
        'Pers_UpdatedDate',         # Last update date
    ],
    'denormalized': [
        # From Company JOIN (many:1 - Person belongs to Company)
        'Comp_CompanyId',           # Company ID
        'Company_Name',             # Company name (alias)
        'Company_Website',          # Company website (alias)

        # From Address JOIN (many:1 - Person can have multiple addresses)
        'Addr_AddressId',           # Address ID
        'Addr_Street1',             # Street address line 1
        'Addr_Street2',             # Street address line 2
        'Addr_City',                # City
        'Addr_State',               # State/Province
        'Addr_PostCode',            # Postal code
        'Addr_Country',             # Country
    ],
    'metadata': [
        'bronze_extracted_at',      # Extraction timestamp
        'bronze_source_file',       # Source CSV filename
    ]
}

# ============================================================================
# OPPORTUNITY PROPERTIES (27 total, including 2 computed)
# ============================================================================

OPPORTUNITY_PROPERTIES = {
    'base': [
        'Oppo_OpportunityId',       # Primary key
        'Oppo_Description',         # Opportunity description
        'Oppo_Status',              # Status (e.g., Open, Closed Won, Closed Lost)
        'Oppo_Stage',               # Pipeline stage
        'Oppo_Certainty',           # Win probability (0-100)
        'Oppo_Forecast',            # Forecasted amount
        'Oppo_CloseDate',           # Expected close date
        'Oppo_Source',              # Lead source
        'Oppo_Type',                # Opportunity type
        'Oppo_Territory',           # Geographic territory
        'Oppo_ProductLine',         # Product line
        'Oppo_EstimatedRevenue',    # Estimated revenue
        'Oppo_DiscountPercent',     # Discount percentage
        'Oppo_Notes',               # Notes
        'Oppo_CreatedDate',         # Record creation date
        'Oppo_UpdatedDate',         # Last update date
        'Oppo_LastActivityDate',    # Last activity date
    ],
    'denormalized': [
        # From Company JOIN (many:1 - Opportunity belongs to Company)
        'Comp_CompanyId',           # Company ID
        'Company_Name',             # Company name (alias)
        'Company_Website',          # Company website (alias)

        # From Person JOIN (many:1 - Opportunity has primary contact)
        'Pers_PersonId',            # Person ID
        'Person_FirstName',         # Person first name (alias)
        'Person_LastName',          # Person last name (alias)
    ],
    'computed': [
        # Computed at query time
        'Weighted_Forecast',        # = Forecast * Certainty / 100
        'Net_Amount',               # = EstimatedRevenue * (1 - DiscountPercent / 100)
    ],
    'metadata': [
        'bronze_extracted_at',      # Extraction timestamp
        'bronze_source_file',       # Source CSV filename
    ]
}

# ============================================================================
# COMMUNICATION PROPERTIES (19 total)
# ============================================================================

COMMUNICATION_PROPERTIES = {
    'base': [
        'Comm_CommunicationId',     # Primary key
        'Comm_Type',                # Type (e.g., Email, Phone, Meeting)
        'Comm_Action',              # Action (e.g., Inbound, Outbound)
        'Comm_Status',              # Status (e.g., Completed, Scheduled)
        'Comm_Subject',             # Subject line
        'Comm_Note',                # Communication notes
        'Comm_OriginalDateTime',    # Original scheduled date/time
        'Comm_DateTime',            # Actual date/time
        'Comm_Priority',            # Priority level
        'Comm_CreatedDate',         # Record creation date
        'Comm_UpdatedDate',         # Last update date
    ],
    'denormalized': [
        # From Company JOIN (many:1 - Communication linked to Company)
        'Comp_CompanyId',           # Company ID
        'Company_Name',             # Company name (alias)

        # From Person JOIN (many:1 - Communication linked to Person)
        'Pers_PersonId',            # Person ID
        'Person_FullName',          # Person full name (computed: FirstName + LastName)

        # From Opportunity JOIN (many:1 - Communication linked to Opportunity)
        'Oppo_OpportunityId',       # Opportunity ID
        'Opportunity_Description',  # Opportunity description (alias)
    ],
    'metadata': [
        'bronze_extracted_at',      # Extraction timestamp
        'bronze_source_file',       # Source CSV filename
    ]
}

# ============================================================================
# CASE PROPERTIES (29 total)
# ============================================================================

CASE_PROPERTIES = {
    'base': [
        'Case_CaseId',              # Primary key
        'Case_Reference',           # Case reference number
        'Case_Status',              # Status (e.g., Open, In Progress, Closed)
        'Case_Type',                # Type (e.g., Bug, Feature Request, Support)
        'Case_Priority',            # Priority (e.g., High, Medium, Low)
        'Case_Severity',            # Severity level
        'Case_Origin',              # Origin (e.g., Email, Phone, Web)
        'Case_Subject',             # Subject/Title
        'Case_Description',         # Description
        'Case_Resolution',          # Resolution notes
        'Case_OpenedDate',          # Date opened
        'Case_ClosedDate',          # Date closed
        'Case_ResponseTime',        # Time to first response (hours)
        'Case_ResolutionTime',      # Time to resolution (hours)
        'Case_SLAStatus',           # SLA status (e.g., Met, Breached)
        'Case_Product',             # Product related to case
        'Case_Category',            # Category
        'Case_Subcategory',         # Subcategory
        'Case_AssignedTo',          # Assigned user/team
        'Case_CreatedDate',         # Record creation date
        'Case_UpdatedDate',         # Last update date
    ],
    'denormalized': [
        # From Company JOIN (many:1 - Case belongs to Company)
        'Comp_CompanyId',           # Company ID
        'Company_Name',             # Company name (alias)
        'Company_Website',          # Company website (alias)

        # From Person JOIN (many:1 - Case has primary contact)
        'Pers_PersonId',            # Person ID
        'Person_FirstName',         # Person first name (alias)
        'Person_LastName',          # Person last name (alias)
    ],
    'metadata': [
        'bronze_extracted_at',      # Extraction timestamp
        'bronze_source_file',       # Source CSV filename
    ]
}

# ============================================================================
# SOCIAL NETWORK PROPERTIES (14 total)
# ============================================================================

SOCIAL_NETWORK_PROPERTIES = {
    'base': [
        'SoN_SocialNetworkId',      # Primary key
        'SoN_Platform',             # Platform (e.g., LinkedIn, Twitter, Facebook)
        'SoN_ProfileURL',           # Profile URL
        'SoN_Username',             # Username/Handle
        'SoN_FollowerCount',        # Number of followers
        'SoN_ConnectionCount',      # Number of connections
        'SoN_Verified',             # Verified status (boolean)
        'SoN_CreatedDate',          # Record creation date
        'SoN_UpdatedDate',          # Last update date
    ],
    'denormalized': [
        # From Person JOIN (many:1 - Social Network belongs to Person)
        'Pers_PersonId',            # Person ID
        'Person_FirstName',         # Person first name (alias)
        'Person_LastName',          # Person last name (alias)
    ],
    'metadata': [
        'bronze_extracted_at',      # Extraction timestamp
        'bronze_source_file',       # Source CSV filename
    ]
}

# ============================================================================
# ADDRESS PROPERTIES (16 total)
# ============================================================================

ADDRESS_PROPERTIES = {
    'base': [
        'Addr_AddressId',           # Primary key
        'Addr_Type',                # Type (e.g., Billing, Shipping, Main)
        'Addr_Street1',             # Street address line 1
        'Addr_Street2',             # Street address line 2
        'Addr_City',                # City
        'Addr_State',               # State/Province
        'Addr_PostCode',            # Postal code
        'Addr_Country',             # Country
        'Addr_CreatedDate',         # Record creation date
        'Addr_UpdatedDate',         # Last update date
    ],
    'denormalized': [
        # From Company JOIN (many:1 - Address can belong to Company)
        'Comp_CompanyId',           # Company ID (nullable)
        'Company_Name',             # Company name (alias, nullable)

        # From Person JOIN (many:1 - Address can belong to Person)
        'Pers_PersonId',            # Person ID (nullable)
        'Person_FullName',          # Person full name (computed: FirstName + LastName, nullable)
    ],
    'metadata': [
        'bronze_extracted_at',      # Extraction timestamp
        'bronze_source_file',       # Source CSV filename
    ]
}

# ============================================================================
# ENTITY PROPERTY COUNTS
# ============================================================================

ENTITY_COUNTS = {
    'Company': 24,
    'Person': 28,
    'Opportunity': 27,  # Including 2 computed
    'Communication': 19,
    'Case': 29,
    'SocialNetwork': 14,
    'Address': 16,
}

TOTAL_PROPERTIES = sum(ENTITY_COUNTS.values())  # 157 total properties

# ============================================================================
# CARDINALITY RULES
# ============================================================================

ENTITY_RELATIONSHIPS = {
    'Company:Address': 'one:many',
    'Company:Person': 'one:many',
    'Company:Opportunity': 'one:many',
    'Company:Case': 'one:many',
    'Company:Communication': 'one:many',

    'Person:Address': 'one:many',
    'Person:Opportunity': 'one:many',
    'Person:Case': 'one:many',
    'Person:Communication': 'one:many',
    'Person:SocialNetwork': 'one:many',

    'Opportunity:Communication': 'one:many',

    'Case:Communication': 'one:many',
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_all_properties(entity_name: str) -> list:
    """
    Get all properties for an entity (base + denormalized + computed + metadata).

    Args:
        entity_name: Name of entity ('Company', 'Person', 'Opportunity', etc.)

    Returns:
        List of all property names for the entity

    Example:
        >>> get_all_properties('Company')
        ['Comp_CompanyId', 'Comp_Name', ..., 'bronze_extracted_at']
    """
    property_map = {
        'Company': COMPANY_PROPERTIES,
        'Person': PERSON_PROPERTIES,
        'Opportunity': OPPORTUNITY_PROPERTIES,
        'Communication': COMMUNICATION_PROPERTIES,
        'Case': CASE_PROPERTIES,
        'SocialNetwork': SOCIAL_NETWORK_PROPERTIES,
        'Address': ADDRESS_PROPERTIES,
    }

    if entity_name not in property_map:
        raise ValueError(f"Unknown entity: {entity_name}")

    props = property_map[entity_name]
    all_props = props.get('base', [])

    if 'denormalized' in props:
        all_props.extend(props['denormalized'])

    if 'computed' in props:
        all_props.extend(props['computed'])

    if 'metadata' in props:
        all_props.extend(props['metadata'])

    return all_props


def get_property_count(entity_name: str) -> int:
    """
    Get total property count for an entity.

    Args:
        entity_name: Name of entity ('Company', 'Person', 'Opportunity', etc.)

    Returns:
        Total number of properties

    Example:
        >>> get_property_count('Company')
        24
    """
    return ENTITY_COUNTS.get(entity_name, 0)


def get_base_properties(entity_name: str) -> list:
    """
    Get only base properties for an entity (excludes denormalized, computed, metadata).

    Args:
        entity_name: Name of entity ('Company', 'Person', 'Opportunity', etc.)

    Returns:
        List of base property names

    Example:
        >>> get_base_properties('Company')
        ['Comp_CompanyId', 'Comp_Name', 'Comp_Website', ...]
    """
    property_map = {
        'Company': COMPANY_PROPERTIES,
        'Person': PERSON_PROPERTIES,
        'Opportunity': OPPORTUNITY_PROPERTIES,
        'Communication': COMMUNICATION_PROPERTIES,
        'Case': CASE_PROPERTIES,
        'SocialNetwork': SOCIAL_NETWORK_PROPERTIES,
        'Address': ADDRESS_PROPERTIES,
    }

    if entity_name not in property_map:
        raise ValueError(f"Unknown entity: {entity_name}")

    return property_map[entity_name].get('base', [])


def print_entity_summary():
    """
    Print a summary of all entities and their property counts.

    Usage:
        >>> from properties import print_entity_summary
        >>> print_entity_summary()
    """
    print("\n" + "=" * 60)
    print("Entity Property Summary")
    print("=" * 60)

    for entity, count in ENTITY_COUNTS.items():
        print(f"  {entity:20s}: {count:3d} properties")

    print("-" * 60)
    print(f"  {'TOTAL':20s}: {TOTAL_PROPERTIES:3d} properties")
    print("=" * 60 + "\n")


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Print summary
    print_entity_summary()

    # Get all Company properties
    company_props = get_all_properties('Company')
    print(f"\nCompany has {len(company_props)} properties:")
    for prop in company_props[:5]:
        print(f"  - {prop}")
    print(f"  ... ({len(company_props) - 5} more)")

    # Get only base Opportunity properties
    oppo_base = get_base_properties('Opportunity')
    print(f"\nOpportunity has {len(oppo_base)} base properties:")
    for prop in oppo_base[:5]:
        print(f"  - {prop}")
    print(f"  ... ({len(oppo_base) - 5} more)")

    # Show relationships
    print("\n" + "=" * 60)
    print("Entity Relationships")
    print("=" * 60)
    for relationship, cardinality in list(ENTITY_RELATIONSHIPS.items())[:5]:
        print(f"  {relationship:30s}: {cardinality}")
    print(f"  ... ({len(ENTITY_RELATIONSHIPS) - 5} more)")
