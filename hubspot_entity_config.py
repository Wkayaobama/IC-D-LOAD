"""
HubSpot Entity Configuration
=============================

Declarative entity definitions for HubSpot CRM objects in PostgreSQL.

Similar to entity_config.py but for HubSpot objects:
- Contacts (hubspot.contacts)
- Companies (hubspot.companies)
- Deals (hubspot.deals)
- Engagements/Communications (hubspot.engagements)

Usage:
    from hubspot_entity_config import HUBSPOT_ENTITY_CONFIGS, get_hubspot_entity_config

    # Get configuration for contacts
    contacts_config = get_hubspot_entity_config('contacts')

    # Access properties
    all_props = contacts_config.get_all_properties()
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from property_mapping_config import (
    COMPANY_MAPPING,
    CONTACT_MAPPING,
    DEAL_MAPPING,
    COMMUNICATION_MAPPING
)


@dataclass
class HubSpotEntityConfig:
    """
    Configuration for HubSpot entity extraction from PostgreSQL.

    Attributes:
        name: Entity name (contacts, companies, deals)
        table_name: PostgreSQL table name (hubspot.contacts)
        schema: Schema name (default: hubspot)
        properties: List of properties to extract
        id_field: Primary key field (hs_object_id)
        legacy_id_field: Legacy ID field (icalps_*)
        where_clause: Optional WHERE filter
        order_by: Optional ORDER BY clause
    """
    name: str
    table_name: str
    schema: str = "hubspot"
    properties: List[str] = field(default_factory=list)
    id_field: str = "hs_object_id"
    legacy_id_field: Optional[str] = None
    where_clause: Optional[str] = None
    order_by: Optional[str] = None

    def get_all_properties(self) -> List[str]:
        """Get all property names."""
        return list(self.properties)

    def get_qualified_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema}.{self.table_name}"

    def build_query(self, limit: Optional[int] = None) -> str:
        """
        Build SELECT query for extraction.

        Args:
            limit: Optional row limit

        Returns:
            SQL query string
        """
        # Build SELECT clause
        if self.properties:
            select_fields = ", ".join(self.properties)
        else:
            select_fields = "*"

        # Build query
        query_parts = [
            f"SELECT {select_fields}",
            f"FROM {self.get_qualified_table_name()}"
        ]

        # Add WHERE clause
        if self.where_clause:
            query_parts.append(f"WHERE {self.where_clause}")

        # Add ORDER BY
        if self.order_by:
            query_parts.append(f"ORDER BY {self.order_by}")

        # Add LIMIT
        if limit:
            query_parts.append(f"LIMIT {limit}")

        return "\n".join(query_parts) + ";"


# ============================================================================
# HUBSPOT ENTITY CONFIGURATIONS
# ============================================================================

# Contacts Configuration
CONTACTS_CONFIG = HubSpotEntityConfig(
    name="contacts",
    table_name="contacts",
    properties=[
        # Core fields
        "hs_object_id",
        "firstname",
        "lastname",
        "email",
        "phone",
        "mobilephone",

        # Job info
        "jobtitle",
        "department",
        "salutation",

        # Company association
        "company",
        "associatedcompanyid",

        # Address
        "address",
        "city",
        "state",
        "country",
        "zip",

        # Legacy ID
        "icalps_contact_id",

        # Metadata
        "createdate",
        "lastmodifieddate",
        "hs_lastmodifieddate",
    ],
    legacy_id_field="icalps_contact_id",
    where_clause="icalps_contact_id IS NOT NULL",
    order_by="hs_object_id"
)

# Companies Configuration
COMPANIES_CONFIG = HubSpotEntityConfig(
    name="companies",
    table_name="companies",
    properties=[
        # Core fields
        "hs_object_id",
        "name",
        "domain",
        "website",
        "phone",

        # Address
        "address",
        "city",
        "state",
        "country",
        "zip",

        # Company info
        "industry",
        "type",
        "numberofemployees",
        "annualrevenue",

        # Lead source
        "hs_lead_source",

        # Territory
        "territory",

        # Legacy ID
        "icalps_company_id",

        # Metadata
        "createdate",
        "hs_lastmodifieddate",
    ],
    legacy_id_field="icalps_company_id",
    where_clause="icalps_company_id IS NOT NULL",
    order_by="hs_object_id"
)

# Deals Configuration
DEALS_CONFIG = HubSpotEntityConfig(
    name="deals",
    table_name="deals",
    properties=[
        # Core fields
        "hs_object_id",
        "dealname",
        "amount",
        "dealstage",
        "pipeline",

        # Dates
        "closedate",
        "createdate",
        "hs_lastmodifieddate",

        # Deal details
        "dealtype",
        "deal_status",
        "deal_certainty",
        "deal_priority",
        "deal_source",
        "deal_brand",
        "deal_notes",

        # Associations
        "company",
        "associatedcompanyid",

        # Legacy ID
        "icalps_deal_id",
    ],
    legacy_id_field="icalps_deal_id",
    where_clause="icalps_deal_id IS NOT NULL",
    order_by="hs_object_id"
)

# Engagements/Communications Configuration
ENGAGEMENTS_CONFIG = HubSpotEntityConfig(
    name="engagements",
    table_name="engagements",
    properties=[
        # Core fields
        "hs_object_id",
        "hs_engagement_type",
        "hs_timestamp",

        # Content
        "hs_engagement_subject",
        "hs_note_body",
        "hs_engagement_status",

        # Type-specific
        "hs_call_direction",
        "hs_call_duration",
        "hs_meeting_title",
        "hs_email_subject",

        # Associations
        "associated_company_id",
        "associated_contact_id",
        "associated_deal_id",

        # Legacy ID
        "icalps_communication_id",

        # Metadata
        "createdate",
        "hs_lastmodifieddate",
    ],
    legacy_id_field="icalps_communication_id",
    where_clause="icalps_communication_id IS NOT NULL",
    order_by="hs_timestamp DESC"
)


# ============================================================================
# ENTITY REGISTRY
# ============================================================================

HUBSPOT_ENTITY_CONFIGS: Dict[str, HubSpotEntityConfig] = {
    'contacts': CONTACTS_CONFIG,
    'companies': COMPANIES_CONFIG,
    'deals': DEALS_CONFIG,
    'engagements': ENGAGEMENTS_CONFIG,
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_hubspot_entity_config(entity_name: str) -> HubSpotEntityConfig:
    """
    Get HubSpot entity configuration by name.

    Args:
        entity_name: Name of entity (contacts, companies, deals, engagements)

    Returns:
        HubSpotEntityConfig instance

    Raises:
        ValueError: If entity not found
    """
    if entity_name not in HUBSPOT_ENTITY_CONFIGS:
        raise ValueError(
            f"Unknown HubSpot entity: {entity_name}. "
            f"Available: {list(HUBSPOT_ENTITY_CONFIGS.keys())}"
        )
    return HUBSPOT_ENTITY_CONFIGS[entity_name]


def list_hubspot_entities() -> List[str]:
    """Get list of all configured HubSpot entity names."""
    return list(HUBSPOT_ENTITY_CONFIGS.keys())


def print_hubspot_entity_configs():
    """Print summary of all HubSpot entity configurations."""
    print("\n" + "=" * 70)
    print("HubSpot Entity Configuration Summary")
    print("=" * 70)

    for name, config in HUBSPOT_ENTITY_CONFIGS.items():
        print(f"\n{name.upper()}:")
        print(f"  Table: {config.get_qualified_table_name()}")
        print(f"  ID Field: {config.id_field}")
        print(f"  Legacy ID Field: {config.legacy_id_field}")
        print(f"  Properties: {len(config.properties)}")
        print(f"  Filter: {config.where_clause or 'None'}")

        print(f"\n  Sample Properties:")
        for prop in config.properties[:5]:
            print(f"    - {prop}")
        if len(config.properties) > 5:
            print(f"    ... ({len(config.properties) - 5} more)")

    print("\n" + "=" * 70 + "\n")


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Print configuration summary
    print_hubspot_entity_configs()

    # Example: Build query for contacts
    print("\n" + "=" * 70)
    print("Example: Generated SQL Query for Contacts")
    print("=" * 70 + "\n")

    contacts_config = get_hubspot_entity_config('contacts')
    query = contacts_config.build_query(limit=10)
    print(query)

    # Example: List all entities
    print("\n" + "=" * 70)
    print("Available HubSpot Entities")
    print("=" * 70 + "\n")

    for entity in list_hubspot_entities():
        config = get_hubspot_entity_config(entity)
        print(f"  - {entity}: {len(config.properties)} properties")
