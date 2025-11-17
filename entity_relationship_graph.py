"""
Entity Relationship Graph Visualization
========================================

Visualizes the network of entity relationships between legacy (CRMICALPS)
and new (HubSpot) CRM systems, showing:
- Entity nodes (legacy and new)
- Cardinality relationships within each system
- Cross-system entity mappings

Uses NetworkX for graph construction and visualization.
"""

import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


# ============================================================================
# ENTITY DEFINITIONS
# ============================================================================

class EntityType(Enum):
    """Entity system type"""
    LEGACY = "legacy"
    HUBSPOT = "hubspot"


@dataclass
class Entity:
    """Represents a database entity"""
    name: str
    entity_type: EntityType
    table_name: str
    primary_key: str
    legacy_id_field: Optional[str] = None  # For HubSpot entities

    def get_node_id(self) -> str:
        """Get unique node identifier"""
        return f"{self.entity_type.value}_{self.name}"


@dataclass
class Relationship:
    """Represents a relationship between entities"""
    from_entity: str  # node_id
    to_entity: str    # node_id
    cardinality: str  # e.g., "1:N", "N:1", "1:1", "N:M"
    relationship_type: str  # "association", "mapping", "aggregation"
    foreign_key: Optional[str] = None
    description: Optional[str] = None


# ============================================================================
# LEGACY ENTITIES (CRMICALPS)
# ============================================================================

LEGACY_ENTITIES = [
    Entity("Company", EntityType.LEGACY, "Company", "Comp_CompanyId"),
    Entity("Person", EntityType.LEGACY, "Person", "Pers_PersonId"),
    Entity("Address", EntityType.LEGACY, "Address", "Addr_AddressId"),
    Entity("Case", EntityType.LEGACY, "vCases", "Case_CaseId"),
    Entity("Communication", EntityType.LEGACY, "vCalendarCommunication", "Comm_CommunicationId"),
    Entity("Opportunity", EntityType.LEGACY, "Opportunity", "Oppo_OpportunityId"),
    Entity("SocialNetwork", EntityType.LEGACY, "SocialNetwork", "SoN_SocialNetworkId"),
]


# ============================================================================
# HUBSPOT ENTITIES
# ============================================================================

HUBSPOT_ENTITIES = [
    Entity("contacts", EntityType.HUBSPOT, "hubspot.contacts", "hs_object_id", "icalps_contact_id"),
    Entity("companies", EntityType.HUBSPOT, "hubspot.companies", "hs_object_id", "icalps_company_id"),
    Entity("deals", EntityType.HUBSPOT, "hubspot.deals", "hs_object_id", "icalps_deal_id"),
    Entity("engagements", EntityType.HUBSPOT, "hubspot.engagements", "hs_object_id", "icalps_communication_id"),
]


# ============================================================================
# LEGACY RELATIONSHIPS
# ============================================================================

LEGACY_RELATIONSHIPS = [
    # Company -> Address (1:0..1)
    Relationship(
        from_entity="legacy_Company",
        to_entity="legacy_Address",
        cardinality="1:0..1",
        relationship_type="association",
        foreign_key="Comp_PrimaryAddressId",
        description="Company primary address"
    ),

    # Person -> Company (N:1)
    Relationship(
        from_entity="legacy_Person",
        to_entity="legacy_Company",
        cardinality="N:1",
        relationship_type="association",
        foreign_key="Pers_CompanyId",
        description="Person works at company"
    ),

    # Person -> Address (N:0..1)
    Relationship(
        from_entity="legacy_Person",
        to_entity="legacy_Address",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="Pers_PrimaryAddressId",
        description="Person primary address"
    ),

    # Case -> Company (N:0..1)
    Relationship(
        from_entity="legacy_Case",
        to_entity="legacy_Company",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="Case_PrimaryCompanyId",
        description="Case primary company"
    ),

    # Case -> Person (N:0..1)
    Relationship(
        from_entity="legacy_Case",
        to_entity="legacy_Person",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="Case_PrimaryPersonId",
        description="Case primary contact"
    ),

    # Communication -> Case (N:0..1)
    Relationship(
        from_entity="legacy_Communication",
        to_entity="legacy_Case",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="Comm_CaseId",
        description="Communication about case"
    ),

    # Opportunity -> Company (N:0..1)
    Relationship(
        from_entity="legacy_Opportunity",
        to_entity="legacy_Company",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="Oppo_PrimaryCompanyId",
        description="Opportunity for company"
    ),

    # Opportunity -> Person (N:0..1)
    Relationship(
        from_entity="legacy_Opportunity",
        to_entity="legacy_Person",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="Oppo_PrimaryPersonId",
        description="Opportunity contact person"
    ),

    # SocialNetwork -> Person (N:1)
    Relationship(
        from_entity="legacy_SocialNetwork",
        to_entity="legacy_Person",
        cardinality="N:1",
        relationship_type="association",
        foreign_key="SoN_PersonId",
        description="Social network profile"
    ),
]


# ============================================================================
# HUBSPOT RELATIONSHIPS
# ============================================================================

HUBSPOT_RELATIONSHIPS = [
    # contacts -> companies (N:0..1)
    Relationship(
        from_entity="hubspot_contacts",
        to_entity="hubspot_companies",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="associatedcompanyid",
        description="Contact works at company"
    ),

    # deals -> companies (N:0..1)
    Relationship(
        from_entity="hubspot_deals",
        to_entity="hubspot_companies",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="associatedcompanyid",
        description="Deal with company"
    ),

    # engagements -> companies (N:0..1)
    Relationship(
        from_entity="hubspot_engagements",
        to_entity="hubspot_companies",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="associated_company_id",
        description="Engagement with company"
    ),

    # engagements -> contacts (N:0..1)
    Relationship(
        from_entity="hubspot_engagements",
        to_entity="hubspot_contacts",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="associated_contact_id",
        description="Engagement with contact"
    ),

    # engagements -> deals (N:0..1)
    Relationship(
        from_entity="hubspot_engagements",
        to_entity="hubspot_deals",
        cardinality="N:0..1",
        relationship_type="association",
        foreign_key="associated_deal_id",
        description="Engagement about deal"
    ),
]


# ============================================================================
# CROSS-SYSTEM MAPPINGS
# ============================================================================

MAPPING_RELATIONSHIPS = [
    # Person -> contacts
    Relationship(
        from_entity="legacy_Person",
        to_entity="hubspot_contacts",
        cardinality="1:1",
        relationship_type="mapping",
        foreign_key="icalps_contact_id",
        description="Person mapped to contact"
    ),

    # Company -> companies
    Relationship(
        from_entity="legacy_Company",
        to_entity="hubspot_companies",
        cardinality="1:1",
        relationship_type="mapping",
        foreign_key="icalps_company_id",
        description="Company mapped to company"
    ),

    # Case/Opportunity -> deals
    Relationship(
        from_entity="legacy_Case",
        to_entity="hubspot_deals",
        cardinality="1:1",
        relationship_type="mapping",
        foreign_key="icalps_deal_id",
        description="Case mapped to deal"
    ),

    Relationship(
        from_entity="legacy_Opportunity",
        to_entity="hubspot_deals",
        cardinality="1:1",
        relationship_type="mapping",
        foreign_key="icalps_deal_id",
        description="Opportunity mapped to deal"
    ),

    # Communication -> engagements
    Relationship(
        from_entity="legacy_Communication",
        to_entity="hubspot_engagements",
        cardinality="1:1",
        relationship_type="mapping",
        foreign_key="icalps_communication_id",
        description="Communication mapped to engagement"
    ),
]


# ============================================================================
# GRAPH BUILDER
# ============================================================================

class EntityRelationshipGraph:
    """Builds and visualizes entity relationship graph"""

    def __init__(self):
        self.G = nx.DiGraph()  # Directed graph for relationships
        self.entities: Dict[str, Entity] = {}
        self.relationships: List[Relationship] = []

    def add_entities(self, entities: List[Entity]):
        """Add entities as nodes"""
        for entity in entities:
            node_id = entity.get_node_id()
            self.entities[node_id] = entity
            self.G.add_node(
                node_id,
                label=entity.name,
                entity_type=entity.entity_type.value,
                table_name=entity.table_name,
                primary_key=entity.primary_key
            )

    def add_relationships(self, relationships: List[Relationship]):
        """Add relationships as edges"""
        for rel in relationships:
            self.relationships.append(rel)
            self.G.add_edge(
                rel.from_entity,
                rel.to_entity,
                cardinality=rel.cardinality,
                relationship_type=rel.relationship_type,
                foreign_key=rel.foreign_key or "",
                description=rel.description or ""
            )

    def get_node_color(self, node_id: str) -> str:
        """Get color for node based on entity type"""
        entity = self.entities[node_id]
        if entity.entity_type == EntityType.LEGACY:
            return '#FFB6C1'  # Light pink for legacy
        else:
            return '#87CEEB'  # Sky blue for HubSpot

    def get_edge_color(self, relationship_type: str) -> str:
        """Get color for edge based on relationship type"""
        colors = {
            'association': '#666666',  # Gray for associations
            'mapping': '#FF6347',      # Tomato red for mappings
            'aggregation': '#4169E1'   # Royal blue for aggregations
        }
        return colors.get(relationship_type, '#000000')

    def get_edge_style(self, relationship_type: str) -> str:
        """Get line style for edge based on relationship type"""
        styles = {
            'association': 'solid',
            'mapping': 'dashed',
            'aggregation': 'dotted'
        }
        return styles.get(relationship_type, 'solid')

    def visualize_2d(self, output_file: str = "entity_relationship_graph_2d.png"):
        """Create 2D visualization of the graph"""
        plt.figure(figsize=(24, 18))

        # Create layout - separate legacy and HubSpot entities
        legacy_nodes = [n for n in self.G.nodes() if n.startswith('legacy_')]
        hubspot_nodes = [n for n in self.G.nodes() if n.startswith('hubspot_')]

        # Use hierarchical layout
        pos = {}

        # Position legacy entities on the left
        legacy_y_spacing = 2.0
        for i, node in enumerate(legacy_nodes):
            pos[node] = (0, -i * legacy_y_spacing)

        # Position HubSpot entities on the right
        hubspot_y_spacing = 3.0
        for i, node in enumerate(hubspot_nodes):
            pos[node] = (10, -i * hubspot_y_spacing)

        # Draw nodes
        for node in self.G.nodes():
            color = self.get_node_color(node)
            entity = self.entities[node]

            # Draw node
            nx.draw_networkx_nodes(
                self.G, pos,
                nodelist=[node],
                node_color=color,
                node_size=3000,
                node_shape='s',  # Square
                alpha=0.9,
                edgecolors='black',
                linewidths=2
            )

            # Draw label
            nx.draw_networkx_labels(
                self.G, pos,
                labels={node: entity.name},
                font_size=10,
                font_weight='bold'
            )

        # Draw edges by type
        for rel_type in ['association', 'mapping', 'aggregation']:
            edges = [
                (u, v) for u, v, d in self.G.edges(data=True)
                if d.get('relationship_type') == rel_type
            ]

            if edges:
                edge_labels = {
                    (u, v): self.G[u][v]['cardinality']
                    for u, v in edges
                }

                nx.draw_networkx_edges(
                    self.G, pos,
                    edgelist=edges,
                    edge_color=self.get_edge_color(rel_type),
                    style=self.get_edge_style(rel_type),
                    width=2 if rel_type == 'mapping' else 1.5,
                    alpha=0.8 if rel_type == 'mapping' else 0.6,
                    arrows=True,
                    arrowsize=20,
                    arrowstyle='->',
                    connectionstyle='arc3,rad=0.1'
                )

                # Draw edge labels
                nx.draw_networkx_edge_labels(
                    self.G, pos,
                    edge_labels=edge_labels,
                    font_size=8,
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7)
                )

        # Add legend
        legend_elements = [
            plt.Line2D([0], [0], marker='s', color='w',
                      markerfacecolor='#FFB6C1', markersize=15,
                      label='Legacy Entity (CRMICALPS)', markeredgecolor='black', markeredgewidth=2),
            plt.Line2D([0], [0], marker='s', color='w',
                      markerfacecolor='#87CEEB', markersize=15,
                      label='HubSpot Entity', markeredgecolor='black', markeredgewidth=2),
            plt.Line2D([0], [0], color='#666666', linewidth=2,
                      label='Association (within system)'),
            plt.Line2D([0], [0], color='#FF6347', linewidth=2, linestyle='--',
                      label='Mapping (legacy → HubSpot)'),
        ]

        plt.legend(handles=legend_elements, loc='upper left', fontsize=12)

        # Add title and labels
        plt.title('Entity Relationship Graph: Legacy (CRMICALPS) ↔ HubSpot CRM',
                 fontsize=20, fontweight='bold', pad=20)
        plt.text(0, max(pos.values(), key=lambda x: x[1])[1] + 1,
                'Legacy System', fontsize=14, fontweight='bold', ha='center')
        plt.text(10, max(pos.values(), key=lambda x: x[1])[1] + 1,
                'HubSpot System', fontsize=14, fontweight='bold', ha='center')

        plt.axis('off')
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"✓ 2D visualization saved to: {output_file}")
        plt.close()

    def visualize_3d(self, output_file: str = "entity_relationship_graph_3d.png"):
        """Create 3D visualization of the graph"""
        try:
            from mpl_toolkits.mplot3d import Axes3D
        except ImportError:
            print("⚠ 3D visualization requires mpl_toolkits.mplot3d")
            return

        fig = plt.figure(figsize=(20, 16))
        ax = fig.add_subplot(111, projection='3d')

        # Create 3D layout
        legacy_nodes = [n for n in self.G.nodes() if n.startswith('legacy_')]
        hubspot_nodes = [n for n in self.G.nodes() if n.startswith('hubspot_')]

        pos_3d = {}

        # Position legacy entities in a circular pattern on z=0 plane
        n_legacy = len(legacy_nodes)
        for i, node in enumerate(legacy_nodes):
            angle = 2 * np.pi * i / n_legacy
            radius = 5
            pos_3d[node] = (radius * np.cos(angle), radius * np.sin(angle), 0)

        # Position HubSpot entities in a circular pattern on z=5 plane
        n_hubspot = len(hubspot_nodes)
        for i, node in enumerate(hubspot_nodes):
            angle = 2 * np.pi * i / n_hubspot
            radius = 5
            pos_3d[node] = (radius * np.cos(angle), radius * np.sin(angle), 5)

        # Draw nodes
        for node in self.G.nodes():
            x, y, z = pos_3d[node]
            color = self.get_node_color(node)
            entity = self.entities[node]

            ax.scatter(x, y, z, c=color, s=500, marker='o',
                      edgecolors='black', linewidths=2, alpha=0.9)
            ax.text(x, y, z, entity.name, fontsize=10, fontweight='bold')

        # Draw edges
        for u, v, data in self.G.edges(data=True):
            x_vals = [pos_3d[u][0], pos_3d[v][0]]
            y_vals = [pos_3d[u][1], pos_3d[v][1]]
            z_vals = [pos_3d[u][2], pos_3d[v][2]]

            color = self.get_edge_color(data['relationship_type'])
            linestyle = self.get_edge_style(data['relationship_type'])

            ax.plot(x_vals, y_vals, z_vals,
                   color=color, linestyle=linestyle, linewidth=2, alpha=0.6)

        ax.set_xlabel('X')
        ax.set_ylabel('Y')
        ax.set_zlabel('Z (Legacy=0, HubSpot=5)')
        ax.set_title('3D Entity Relationship Graph: Legacy ↔ HubSpot',
                    fontsize=16, fontweight='bold', pad=20)

        plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"✓ 3D visualization saved to: {output_file}")
        plt.close()

    def print_summary(self):
        """Print graph summary"""
        print("\n" + "=" * 80)
        print("ENTITY RELATIONSHIP GRAPH SUMMARY")
        print("=" * 80)

        print(f"\n📊 Graph Statistics:")
        print(f"  Total Entities: {self.G.number_of_nodes()}")
        print(f"  Total Relationships: {self.G.number_of_edges()}")

        legacy_count = sum(1 for n in self.G.nodes() if n.startswith('legacy_'))
        hubspot_count = sum(1 for n in self.G.nodes() if n.startswith('hubspot_'))

        print(f"\n  Legacy Entities: {legacy_count}")
        print(f"  HubSpot Entities: {hubspot_count}")

        # Count relationship types
        rel_types = {}
        for _, _, data in self.G.edges(data=True):
            rel_type = data.get('relationship_type', 'unknown')
            rel_types[rel_type] = rel_types.get(rel_type, 0) + 1

        print(f"\n📌 Relationships by Type:")
        for rel_type, count in rel_types.items():
            print(f"  {rel_type.capitalize()}: {count}")

        # Entity details
        print(f"\n🏢 Legacy Entities:")
        for node_id in sorted([n for n in self.G.nodes() if n.startswith('legacy_')]):
            entity = self.entities[node_id]
            print(f"  • {entity.name} ({entity.table_name}) - PK: {entity.primary_key}")

        print(f"\n🚀 HubSpot Entities:")
        for node_id in sorted([n for n in self.G.nodes() if n.startswith('hubspot_')]):
            entity = self.entities[node_id]
            legacy_id = f" - Legacy ID: {entity.legacy_id_field}" if entity.legacy_id_field else ""
            print(f"  • {entity.name} ({entity.table_name}) - PK: {entity.primary_key}{legacy_id}")

        # Cardinality summary
        print(f"\n🔗 Cardinality Distribution:")
        cardinalities = {}
        for _, _, data in self.G.edges(data=True):
            card = data.get('cardinality', 'unknown')
            cardinalities[card] = cardinalities.get(card, 0) + 1

        for card, count in sorted(cardinalities.items()):
            print(f"  {card}: {count} relationships")

        print("\n" + "=" * 80 + "\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def build_entity_relationship_graph() -> EntityRelationshipGraph:
    """Build the complete entity relationship graph"""

    print("\n🔨 Building Entity Relationship Graph...")

    # Initialize graph
    graph = EntityRelationshipGraph()

    # Add entities
    print("  📝 Adding legacy entities...")
    graph.add_entities(LEGACY_ENTITIES)

    print("  📝 Adding HubSpot entities...")
    graph.add_entities(HUBSPOT_ENTITIES)

    # Add relationships
    print("  🔗 Adding legacy relationships...")
    graph.add_relationships(LEGACY_RELATIONSHIPS)

    print("  🔗 Adding HubSpot relationships...")
    graph.add_relationships(HUBSPOT_RELATIONSHIPS)

    print("  🔗 Adding cross-system mappings...")
    graph.add_relationships(MAPPING_RELATIONSHIPS)

    print("  ✓ Graph construction complete!")

    return graph


def main():
    """Main execution"""

    # Build graph
    graph = build_entity_relationship_graph()

    # Print summary
    graph.print_summary()

    # Create visualizations
    print("🎨 Creating visualizations...\n")

    graph.visualize_2d("entity_relationship_graph_2d.png")
    graph.visualize_3d("entity_relationship_graph_3d.png")

    # Export graph data
    print("\n💾 Exporting graph data...")

    # Export to GraphML (can be opened in tools like Gephi, Cytoscape)
    nx.write_graphml(graph.G, "entity_relationship_graph.graphml")
    print("  ✓ GraphML export: entity_relationship_graph.graphml")

    # Export to JSON
    import json
    graph_data = nx.node_link_data(graph.G)
    with open("entity_relationship_graph.json", "w") as f:
        json.dump(graph_data, f, indent=2)
    print("  ✓ JSON export: entity_relationship_graph.json")

    print("\n" + "=" * 80)
    print("✨ Entity Relationship Graph Generation Complete!")
    print("=" * 80)
    print("\nGenerated files:")
    print("  1. entity_relationship_graph_2d.png - 2D visualization")
    print("  2. entity_relationship_graph_3d.png - 3D visualization")
    print("  3. entity_relationship_graph.graphml - GraphML format")
    print("  4. entity_relationship_graph.json - JSON format")
    print("\n")


if __name__ == "__main__":
    main()
