#!/usr/bin/env python3
"""
Pipeline Configuration System
==============================

Configuration over hardcoding - centralized configuration for all entities.

Meta-Cognitive Principle: Configuration as Data
- Easy to modify without code changes
- Validation at load time
- Type-safe with dataclasses
- Version controlled

Usage:
    from pipeline_config import get_entity_config, EntityType

    config = get_entity_config(EntityType.COMMUNICATIONS)
    print(config.primary_keys)
    print(config.columns)
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from enum import Enum, auto
import yaml
from pathlib import Path


class EntityType(Enum):
    """Supported entity types."""
    COMMUNICATIONS = "communications"
    DEALS = "deals"
    COMPANIES = "companies"
    CONTACTS = "contacts"
    CASES = "cases"


@dataclass
class EntityConfig:
    """
    Configuration for a single entity type.

    Meta-Cognitive: This replaces hardcoded table names and column lists
    scattered throughout R and Power Query code.
    """
    entity_type: EntityType
    display_name: str

    # Database configuration
    staging_table: str
    production_table: str
    schema: str = "staging"

    # Schema definition
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: Dict[str, str] = field(default_factory=dict)  # {column: references_table}
    columns: List[str] = field(default_factory=list)
    critical_fields: List[str] = field(default_factory=list)  # Fields that must not be null

    # Processing configuration
    enable_deduplication: bool = True
    deduplication_key: Optional[str] = None
    deduplication_sort_by: Optional[str] = None
    deduplication_sort_order: str = "asc"  # 'asc' or 'desc'

    # Aggregation configuration
    enable_aggregation: bool = False
    aggregation_fields: Dict[str, str] = field(default_factory=dict)  # {source: target}

    # UTF-8 cleaning configuration
    utf8_clean_columns: List[str] = field(default_factory=list)
    preserve_accents: bool = False

    # Delta loading configuration
    enable_delta_loading: bool = True
    timestamp_column: Optional[str] = None

    # Additional metadata
    metadata: Dict = field(default_factory=dict)


# =============================================================================
# ENTITY CONFIGURATIONS
# =============================================================================

COMMUNICATIONS_CONFIG = EntityConfig(
    entity_type=EntityType.COMMUNICATIONS,
    display_name="Communications",

    # Tables
    staging_table="communications_staging",
    production_table="communications",
    schema="staging",

    # Keys
    primary_keys=["Communication_Record ID"],
    foreign_keys={
        "Communication_PersonID": "contacts",
        "icAlps_company ID": "companies"
    },

    # Schema
    columns=[
        "Record ID",
        "Communication Subject",
        "communication type",
        "icAlps_company ID",
        "Communication_CreateDate",
        "Communication Notes",
        "Communication_From",
        "Communication_To",
        "Associated Contact",
        "Communication_PersonID",
        "Company_Com",
        "Associated Company",
        "Communication_Record ID",
        "Associated Contact IDs",
        "Company_Com IDs",
        "Associated Company IDs"
    ],
    critical_fields=[
        "Communication_Record ID",
        "Communication_CreateDate"
    ],

    # Deduplication
    enable_deduplication=True,
    deduplication_key="Communication_Record ID",
    deduplication_sort_by="Communication_CreateDate",
    deduplication_sort_order="asc",

    # Aggregation
    enable_aggregation=True,
    aggregation_fields={
        "Associated Contact": "Contact_Names_Aggregated",
        "Associated Contact IDs": "Contact_IDs_Aggregated",
        "Company_Com": "Company_Names_Aggregated",
        "Communication_From": "From_Emails_Aggregated",
        "Communication_To": "To_Emails_Aggregated"
    },

    # UTF-8 cleaning
    utf8_clean_columns=[
        "Communication Subject",
        "Communication Notes",
        "Communication_From",
        "Communication_To",
        "Associated Contact",
        "Company_Com"
    ],
    preserve_accents=False,

    # Delta loading
    enable_delta_loading=True,
    timestamp_column="Communication_CreateDate"
)


DEALS_CONFIG = EntityConfig(
    entity_type=EntityType.DEALS,
    display_name="Deals/Opportunities",

    # Tables
    staging_table="deals_staging",
    production_table="deals",
    schema="staging",

    # Keys
    primary_keys=["icalps_deal_id"],
    foreign_keys={
        "icalps_company_id": "companies",
        "sageCRM.Oppo_PrimaryPersonId": "contacts"
    },

    # Schema
    columns=[
        "icalps_deal_id",
        "icalps_company_id",
        "Associated Company (Primary)",
        "Other Associated company",
        "sageCRM.Oppo_PrimaryPersonId",
        "sageCRM.Oppo_Certainty",
        "sageCRM.Oppo_Description",
        "sageCRM.Oppo_Stage",
        "sageCRM.Oppo_Status",
        "sageCRM.Oppo_CreatedDate",
        "sageCRM.Oppo_UpdatedDate"
    ],
    critical_fields=[
        "icalps_deal_id"
    ],

    # Deduplication
    enable_deduplication=True,
    deduplication_key="icalps_deal_id",
    deduplication_sort_by="sageCRM.Oppo_UpdatedDate",
    deduplication_sort_order="desc",

    # Aggregation
    enable_aggregation=True,
    aggregation_fields={
        "Associated Company (Primary)": "Company_Name_List",
        "Other Associated company": "Company_Name_List"
    },

    # UTF-8 cleaning
    utf8_clean_columns=[
        "sageCRM.Oppo_Description",
        "Associated Company (Primary)"
    ],

    # Delta loading
    enable_delta_loading=True,
    timestamp_column="sageCRM.Oppo_UpdatedDate"
)


COMPANIES_CONFIG = EntityConfig(
    entity_type=EntityType.COMPANIES,
    display_name="Companies",

    # Tables
    staging_table="companies_staging",
    production_table="companies",
    schema="staging",

    # Keys
    primary_keys=["icalps_company_id"],
    foreign_keys={},

    # Schema
    columns=[
        "icalps_company_id",
        "Record ID",
        "Company_Name",
        "Company_Domain",
        "Company_CreatedDate",
        "Company_UpdatedDate"
    ],
    critical_fields=[
        "icalps_company_id",
        "Company_Name"
    ],

    # Deduplication
    enable_deduplication=True,
    deduplication_key="icalps_company_id",
    deduplication_sort_by="Company_UpdatedDate",
    deduplication_sort_order="desc",

    # UTF-8 cleaning
    utf8_clean_columns=[
        "Company_Name"
    ],

    # Delta loading
    enable_delta_loading=True,
    timestamp_column="Company_UpdatedDate"
)


CONTACTS_CONFIG = EntityConfig(
    entity_type=EntityType.CONTACTS,
    display_name="Contacts/People",

    # Tables
    staging_table="contacts_staging",
    production_table="contacts",
    schema="staging",

    # Keys
    primary_keys=["icalps_contact_id"],
    foreign_keys={
        "icalps_company_id": "companies"
    },

    # Schema
    columns=[
        "icalps_contact_id",
        "icalps_company_id",
        "Person_FirstName",
        "Person_LastName",
        "Person_Email",
        "Person_CreatedDate",
        "Person_UpdatedDate"
    ],
    critical_fields=[
        "icalps_contact_id"
    ],

    # Deduplication
    enable_deduplication=True,
    deduplication_key="icalps_contact_id",
    deduplication_sort_by="Person_UpdatedDate",
    deduplication_sort_order="desc",

    # UTF-8 cleaning
    utf8_clean_columns=[
        "Person_FirstName",
        "Person_LastName",
        "Person_Email"
    ],

    # Delta loading
    enable_delta_loading=True,
    timestamp_column="Person_UpdatedDate"
)


CASES_CONFIG = EntityConfig(
    entity_type=EntityType.CASES,
    display_name="Cases/Tickets",

    # Tables
    staging_table="cases_staging",
    production_table="cases",
    schema="staging",

    # Keys
    primary_keys=["Case_CaseId"],
    foreign_keys={
        "Case_PrimaryCompanyId": "companies",
        "Case_PrimaryPersonId": "contacts"
    },

    # Schema
    columns=[
        "Case_CaseId",
        "Case_PrimaryCompanyId",
        "Case_PrimaryPersonId",
        "Case_Description",
        "Case_Status",
        "Case_Stage",
        "Case_Priority",
        "Case_Source",
        "Case_Product",
        "Case_ProblemType",
        "Case_SolutionType",
        "Case_ProblemNote",
        "Case_SolutionNote",
        "Case_Opened",
        "Case_Closed",
        "Case_CreatedDate",
        "Case_UpdatedDate"
    ],
    critical_fields=[
        "Case_CaseId"
    ],

    # Deduplication
    enable_deduplication=True,
    deduplication_key="Case_CaseId",
    deduplication_sort_by="Case_UpdatedDate",
    deduplication_sort_order="desc",

    # UTF-8 cleaning
    utf8_clean_columns=[
        "Case_Description",
        "Case_ProblemNote",
        "Case_SolutionNote"
    ],

    # Delta loading
    enable_delta_loading=True,
    timestamp_column="Case_UpdatedDate"
)


# =============================================================================
# CONFIGURATION REGISTRY
# =============================================================================

ENTITY_CONFIGS: Dict[EntityType, EntityConfig] = {
    EntityType.COMMUNICATIONS: COMMUNICATIONS_CONFIG,
    EntityType.DEALS: DEALS_CONFIG,
    EntityType.COMPANIES: COMPANIES_CONFIG,
    EntityType.CONTACTS: CONTACTS_CONFIG,
    EntityType.CASES: CASES_CONFIG
}


# =============================================================================
# ACCESSOR FUNCTIONS
# =============================================================================

def get_entity_config(entity_type: EntityType) -> EntityConfig:
    """
    Get configuration for an entity type.

    Args:
        entity_type: Entity type enum

    Returns:
        EntityConfig for the entity

    Raises:
        KeyError: If entity type not configured
    """
    if entity_type not in ENTITY_CONFIGS:
        raise KeyError(f"No configuration found for entity type: {entity_type}")

    return ENTITY_CONFIGS[entity_type]


def get_all_entity_types() -> List[EntityType]:
    """Get list of all configured entity types."""
    return list(ENTITY_CONFIGS.keys())


def get_entity_display_name(entity_type: EntityType) -> str:
    """Get display name for entity type."""
    return get_entity_config(entity_type).display_name


def validate_config(config: EntityConfig) -> List[str]:
    """
    Validate entity configuration.

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    if not config.primary_keys:
        errors.append("primary_keys cannot be empty")

    if not config.columns:
        errors.append("columns cannot be empty")

    if config.enable_deduplication and not config.deduplication_key:
        errors.append("deduplication_key required when deduplication enabled")

    if config.enable_delta_loading and not config.timestamp_column:
        errors.append("timestamp_column required when delta loading enabled")

    # Check that primary keys are in columns
    for pk in config.primary_keys:
        if pk not in config.columns:
            errors.append(f"Primary key '{pk}' not in columns list")

    # Check that critical fields are in columns
    for field in config.critical_fields:
        if field not in config.columns:
            errors.append(f"Critical field '{field}' not in columns list")

    return errors


def validate_all_configs() -> Dict[EntityType, List[str]]:
    """
    Validate all entity configurations.

    Returns:
        Dict mapping entity type to list of errors
    """
    results = {}

    for entity_type, config in ENTITY_CONFIGS.items():
        errors = validate_config(config)
        if errors:
            results[entity_type] = errors

    return results


# =============================================================================
# YAML EXPORT/IMPORT (Optional)
# =============================================================================

def export_config_to_yaml(entity_type: EntityType, output_path: Path):
    """Export entity configuration to YAML file."""
    config = get_entity_config(entity_type)

    # Convert to dict (manual since dataclass with Enum)
    config_dict = {
        'entity_type': config.entity_type.value,
        'display_name': config.display_name,
        'staging_table': config.staging_table,
        'production_table': config.production_table,
        'schema': config.schema,
        'primary_keys': config.primary_keys,
        'foreign_keys': config.foreign_keys,
        'columns': config.columns,
        'critical_fields': config.critical_fields,
        'enable_deduplication': config.enable_deduplication,
        'deduplication_key': config.deduplication_key,
        'deduplication_sort_by': config.deduplication_sort_by,
        'deduplication_sort_order': config.deduplication_sort_order,
        'enable_aggregation': config.enable_aggregation,
        'aggregation_fields': config.aggregation_fields,
        'utf8_clean_columns': config.utf8_clean_columns,
        'preserve_accents': config.preserve_accents,
        'enable_delta_loading': config.enable_delta_loading,
        'timestamp_column': config.timestamp_column,
        'metadata': config.metadata
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    print(f"Exported config to {output_path}")


def export_all_configs_to_yaml(output_dir: Path = Path('config')):
    """Export all entity configurations to YAML files."""
    for entity_type in get_all_entity_types():
        output_file = output_dir / f"{entity_type.value}_config.yaml"
        export_config_to_yaml(entity_type, output_file)


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    print("Entity Pipeline Configuration")
    print("=" * 80)

    # Validate all configs
    print("\nValidating configurations...")
    validation_results = validate_all_configs()

    if validation_results:
        print("❌ Validation errors found:")
        for entity_type, errors in validation_results.items():
            print(f"\n{entity_type.value}:")
            for error in errors:
                print(f"  - {error}")
    else:
        print("✅ All configurations valid")

    # Display summary
    print("\nConfigured Entities:")
    print("-" * 80)

    for entity_type in get_all_entity_types():
        config = get_entity_config(entity_type)
        print(f"\n{config.display_name} ({entity_type.value})")
        print(f"  Tables: {config.staging_table} → {config.production_table}")
        print(f"  Primary Keys: {config.primary_keys}")
        print(f"  Columns: {len(config.columns)}")
        print(f"  Deduplication: {config.enable_deduplication}")
        print(f"  Delta Loading: {config.enable_delta_loading}")
        print(f"  UTF-8 Cleaning: {len(config.utf8_clean_columns)} columns")

    # Example: Get specific config
    print("\n" + "=" * 80)
    print("Example: Communications Configuration")
    print("=" * 80)

    comm_config = get_entity_config(EntityType.COMMUNICATIONS)
    print(f"Display Name: {comm_config.display_name}")
    print(f"Primary Keys: {comm_config.primary_keys}")
    print(f"Staging Table: {comm_config.staging_table}")
    print(f"Production Table: {comm_config.production_table}")
    print(f"Columns ({len(comm_config.columns)}):")
    for col in comm_config.columns[:5]:
        print(f"  - {col}")
    print(f"  ... and {len(comm_config.columns) - 5} more")

    # Export to YAML (optional)
    # export_all_configs_to_yaml()
