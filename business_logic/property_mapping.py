"""
Property Mapping Configuration
==============================

Maps legacy CRM properties to HubSpot CRM properties for reconciliation.

This module defines:
- Property mappings for each object type (Company, Contact, Deal, Communication)
- Filter rules for HubSpot property discovery
- Property groups to query
- Required vs optional properties

Usage:
    from property_mapping_config import PROPERTY_MAPPINGS, get_hubspot_properties

    # Get HubSpot properties for contacts
    contact_props = get_hubspot_properties('contacts')

    # Get property mapping
    mapping = PROPERTY_MAPPINGS['contacts']
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class PropertyMapping:
    """
    Defines property mapping between legacy CRM and HubSpot.

    Attributes:
        legacy_property: Property name in legacy CRM
        hubspot_property: Property name in HubSpot
        is_required: If True, property must exist for reconciliation
        transformation: Optional transformation function name
        default_value: Default value if legacy property is None
    """
    legacy_property: str
    hubspot_property: str
    is_required: bool = False
    transformation: Optional[str] = None
    default_value: Optional[str] = None


@dataclass
class ObjectMapping:
    """
    Complete mapping configuration for an object type.

    Attributes:
        object_type: HubSpot object type (contacts, companies, deals)
        legacy_table: Legacy CRM table name
        legacy_id_field: Legacy ID field name
        hubspot_id_field: HubSpot record ID field
        hubspot_legacy_id_field: HubSpot field storing legacy ID (icalps_*)
        property_mappings: List of property mappings
        required_properties: List of required HubSpot properties
        property_group: HubSpot property group to filter by (optional)
    """
    object_type: str
    legacy_table: str
    legacy_id_field: str
    hubspot_id_field: str
    hubspot_legacy_id_field: str
    property_mappings: List[PropertyMapping] = field(default_factory=list)
    required_properties: List[str] = field(default_factory=list)
    property_group: Optional[str] = "IcAlps"


# ============================================================================
# COMPANY MAPPINGS
# ============================================================================

COMPANY_MAPPING = ObjectMapping(
    object_type="companies",
    legacy_table="Company",
    legacy_id_field="Comp_CompanyId",
    hubspot_id_field="hs_object_id",
    hubspot_legacy_id_field="icalps_company_id",
    property_mappings=[
        PropertyMapping("Comp_Name", "name", is_required=True),
        PropertyMapping("Comp_WebSite", "domain"),
        PropertyMapping("Comp_WebSite", "website"),
        PropertyMapping("Comp_PhoneNumber", "phone"),
        PropertyMapping("Comp_Type", "type"),
        PropertyMapping("Comp_Territory", "territory"),
        PropertyMapping("Comp_Sector", "industry"),
        PropertyMapping("Comp_Employees", "numberofemployees"),
        PropertyMapping("Comp_Revenue", "annualrevenue"),
        PropertyMapping("Comp_Source", "hs_lead_source"),
        PropertyMapping("Addr_City", "city"),
        PropertyMapping("Addr_State", "state"),
        PropertyMapping("Addr_Country", "country"),
        PropertyMapping("Addr_PostCode", "zip"),
        PropertyMapping("Addr_Street1", "address"),
        PropertyMapping("Comp_CreatedDate", "createdate"),
    ],
    required_properties=[
        "hs_object_id",
        "name",
        "icalps_company_id",
        "domain",
        "city",
        "country"
    ]
)


# ============================================================================
# CONTACT MAPPINGS
# ============================================================================

CONTACT_MAPPING = ObjectMapping(
    object_type="contacts",
    legacy_table="Person",
    legacy_id_field="Pers_PersonId",
    hubspot_id_field="hs_object_id",
    hubspot_legacy_id_field="icalps_contact_id",
    property_mappings=[
        PropertyMapping("Pers_FirstName", "firstname", is_required=True),
        PropertyMapping("Pers_LastName", "lastname", is_required=True),
        PropertyMapping("Pers_EmailAddress", "email"),
        PropertyMapping("Pers_PhoneNumber", "phone"),
        PropertyMapping("Pers_MobileNumber", "mobilephone"),
        PropertyMapping("Pers_Title", "jobtitle"),
        PropertyMapping("Pers_Department", "department"),
        PropertyMapping("Pers_Salutation", "salutation"),
        PropertyMapping("Pers_CompanyId", "associatedcompanyid"),
        PropertyMapping("Comp_Name", "company"),
        PropertyMapping("Addr_City", "city"),
        PropertyMapping("Addr_State", "state"),
        PropertyMapping("Addr_Country", "country"),
        PropertyMapping("Addr_PostCode", "zip"),
        PropertyMapping("Addr_Street1", "address"),
        PropertyMapping("Pers_CreatedDate", "createdate"),
    ],
    required_properties=[
        "hs_object_id",
        "firstname",
        "lastname",
        "email",
        "icalps_contact_id",
        "company"
    ]
)


# ============================================================================
# DEAL MAPPINGS
# ============================================================================

DEAL_MAPPING = ObjectMapping(
    object_type="deals",
    legacy_table="Opportunity",
    legacy_id_field="Oppo_OpportunityId",
    hubspot_id_field="hs_object_id",
    hubspot_legacy_id_field="icalps_deal_id",
    property_mappings=[
        PropertyMapping("Oppo_Description", "dealname", is_required=True),
        PropertyMapping("Oppo_Forecast", "amount"),
        PropertyMapping("Oppo_Stage", "dealstage"),
        PropertyMapping("Oppo_Status", "deal_status"),
        PropertyMapping("Oppo_Certainty", "deal_certainty"),
        PropertyMapping("Oppo_Type", "dealtype"),
        PropertyMapping("Oppo_Source", "deal_source"),
        PropertyMapping("Oppo_TargetClose", "closedate"),
        PropertyMapping("Oppo_ActualClose", "actual_close_date"),
        PropertyMapping("Oppo_Priority", "deal_priority"),
        PropertyMapping("Oppo_Product", "deal_brand"),
        PropertyMapping("Oppo_Note", "deal_notes"),
        PropertyMapping("Oppo_PrimaryCompanyId", "associatedcompanyid"),
        PropertyMapping("Comp_Name", "company"),
        PropertyMapping("Pers_PersonId", "primary_contact_id"),
        PropertyMapping("Oppo_CreatedDate", "createdate"),
    ],
    required_properties=[
        "hs_object_id",
        "dealname",
        "icalps_deal_id",
        "amount",
        "dealstage",
        "company"
    ]
)


# ============================================================================
# COMMUNICATION MAPPINGS
# ============================================================================

COMMUNICATION_MAPPING = ObjectMapping(
    object_type="engagements",
    legacy_table="Communication",
    legacy_id_field="Comm_CommunicationId",
    hubspot_id_field="hs_object_id",
    hubspot_legacy_id_field="icalps_communication_id",
    property_mappings=[
        PropertyMapping("Comm_Subject", "hs_engagement_subject"),
        PropertyMapping("Comm_Note", "hs_note_body"),
        PropertyMapping("Comm_DateTime", "hs_timestamp"),
        PropertyMapping("Comm_Type", "hs_engagement_type"),
        PropertyMapping("Comm_Action", "hs_call_direction"),
        PropertyMapping("Comm_Status", "hs_engagement_status"),
        PropertyMapping("Comm_OriginalDateTime", "hs_original_datetime"),
        PropertyMapping("Comm_OpportunityId", "associated_deal_id"),
        PropertyMapping("Comm_CaseId", "associated_ticket_id"),
        PropertyMapping("Comp_CompanyId", "associated_company_id"),
        PropertyMapping("Pers_PersonId", "associated_contact_id"),
    ],
    required_properties=[
        "hs_object_id",
        "hs_engagement_subject",
        "hs_timestamp",
        "hs_engagement_type"
    ]
)


# ============================================================================
# MAPPING REGISTRY
# ============================================================================

PROPERTY_MAPPINGS: Dict[str, ObjectMapping] = {
    'companies': COMPANY_MAPPING,
    'contacts': CONTACT_MAPPING,
    'deals': DEAL_MAPPING,
    'communications': COMMUNICATION_MAPPING,
    'engagements': COMMUNICATION_MAPPING  # Alias
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_object_mapping(object_type: str) -> ObjectMapping:
    """
    Get property mapping configuration for an object type.

    Args:
        object_type: HubSpot object type (companies, contacts, deals, engagements)

    Returns:
        ObjectMapping instance

    Raises:
        ValueError: If object type not found
    """
    if object_type not in PROPERTY_MAPPINGS:
        raise ValueError(
            f"Unknown object type: {object_type}. "
            f"Available: {list(PROPERTY_MAPPINGS.keys())}"
        )
    return PROPERTY_MAPPINGS[object_type]


def get_hubspot_properties(object_type: str, required_only: bool = False) -> List[str]:
    """
    Get list of HubSpot properties to query for an object type.

    Args:
        object_type: HubSpot object type
        required_only: If True, return only required properties

    Returns:
        List of HubSpot property names
    """
    mapping = get_object_mapping(object_type)

    if required_only:
        return mapping.required_properties

    # Return all HubSpot properties from mappings + required properties
    hubspot_props = set(mapping.required_properties)
    for prop_mapping in mapping.property_mappings:
        hubspot_props.add(prop_mapping.hubspot_property)

    return sorted(list(hubspot_props))


def get_legacy_to_hubspot_mapping(object_type: str) -> Dict[str, str]:
    """
    Get dictionary mapping legacy property names to HubSpot property names.

    Args:
        object_type: HubSpot object type

    Returns:
        Dictionary {legacy_property: hubspot_property}
    """
    mapping = get_object_mapping(object_type)
    return {
        pm.legacy_property: pm.hubspot_property
        for pm in mapping.property_mappings
    }


def get_hubspot_filter_clause(object_type: str) -> str:
    """
    Generate SQL WHERE clause to filter HubSpot records for reconciliation.

    Filters by:
    1. Records with icalps_* ID not null (already reconciled)
    2. Records in IcAlps property group (if applicable)

    Args:
        object_type: HubSpot object type

    Returns:
        SQL WHERE clause string
    """
    mapping = get_object_mapping(object_type)

    # Filter by icalps_* field not null
    filter_parts = [
        f"{mapping.hubspot_legacy_id_field} IS NOT NULL"
    ]

    # Add property group filter if specified
    if mapping.property_group:
        # Note: Property group filtering depends on HubSpot API structure
        # This is a placeholder - actual implementation may vary
        pass

    return " AND ".join(filter_parts)


def build_hubspot_select_query(object_type: str, limit: Optional[int] = None) -> str:
    """
    Build SELECT query for fetching HubSpot records for reconciliation.

    Args:
        object_type: HubSpot object type
        limit: Optional row limit

    Returns:
        SQL query string
    """
    mapping = get_object_mapping(object_type)
    properties = get_hubspot_properties(object_type)

    # Build SELECT clause
    select_clause = ", ".join(properties)

    # Build query
    query = f"""
    SELECT {select_clause}
    FROM hubspot.{object_type}
    WHERE {get_hubspot_filter_clause(object_type)}
    """

    if limit:
        query += f"\nLIMIT {limit}"

    return query


def print_mapping_summary():
    """
    Print summary of all property mappings.
    """
    print("\n" + "=" * 80)
    print("Property Mapping Summary")
    print("=" * 80)

    for obj_type, mapping in PROPERTY_MAPPINGS.items():
        print(f"\n{obj_type.upper()}:")
        print(f"  Legacy Table: {mapping.legacy_table}")
        print(f"  Legacy ID Field: {mapping.legacy_id_field}")
        print(f"  HubSpot ID Field: {mapping.hubspot_id_field}")
        print(f"  HubSpot Legacy ID Field: {mapping.hubspot_legacy_id_field}")
        print(f"  Property Mappings: {len(mapping.property_mappings)}")
        print(f"  Required Properties: {len(mapping.required_properties)}")
        print(f"  Property Group: {mapping.property_group or 'None'}")

        print(f"\n  Property Mappings:")
        for pm in mapping.property_mappings[:5]:
            required_str = " [REQUIRED]" if pm.is_required else ""
            print(f"    {pm.legacy_property} → {pm.hubspot_property}{required_str}")
        if len(mapping.property_mappings) > 5:
            print(f"    ... ({len(mapping.property_mappings) - 5} more)")

    print("\n" + "=" * 80 + "\n")


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Print mapping summary
    print_mapping_summary()

    # Example: Get HubSpot properties for contacts
    print("HubSpot properties for CONTACTS:")
    contact_props = get_hubspot_properties('contacts')
    for prop in contact_props:
        print(f"  - {prop}")

    # Example: Get legacy→HubSpot mapping for companies
    print("\nLegacy→HubSpot mapping for COMPANIES:")
    company_mapping = get_legacy_to_hubspot_mapping('companies')
    for legacy_prop, hubspot_prop in list(company_mapping.items())[:10]:
        print(f"  {legacy_prop} → {hubspot_prop}")

    # Example: Build HubSpot query
    print("\nHubSpot query for DEALS:")
    deal_query = build_hubspot_select_query('deals', limit=10)
    print(deal_query)
