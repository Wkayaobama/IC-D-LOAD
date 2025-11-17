# Entity Relationship Graph Documentation

## Overview

This project provides a comprehensive visualization and analysis system for understanding the complex network of relationships between legacy CRM (CRMICALPS) and new CRM (HubSpot) database entities.

The system creates **network graphs** where:
- **Nodes** represent database entities (tables)
- **Edges** represent relationships with specific cardinality rules
- **Colors and styles** distinguish between systems and relationship types

## Key Features

### 1. Dual-System Entity Mapping
- **Legacy System (CRMICALPS)**: 7 entities
  - Company, Person, Address, Case, Communication, Opportunity, SocialNetwork
- **HubSpot System**: 4 entities
  - companies, contacts, deals, engagements

### 2. Relationship Visualization
- **Association edges** (solid gray): Relationships within the same system
- **Mapping edges** (dashed red): Legacy-to-HubSpot entity mappings
- **Cardinality labels**: Show relationship constraints (1:1, N:1, N:0..1, etc.)

### 3. Network Analysis
- **Centrality metrics**: Identify most important/connected entities
- **Path analysis**: Understand how entities connect
- **Mapping coverage**: Track which legacy entities are migrated
- **Complexity scoring**: Measure system interconnectedness

## Architecture

### Core Modules

#### 1. `entity_relationship_graph.py`
Main graph construction and visualization module.

**Key Components:**
- `Entity`: Dataclass representing a database entity
- `Relationship`: Dataclass representing entity relationships with cardinality
- `EntityRelationshipGraph`: Main graph builder and visualizer

**Functions:**
- `build_entity_relationship_graph()`: Constructs the complete graph
- `visualize_2d()`: Creates 2D static visualization
- `visualize_3d()`: Creates 3D static visualization

#### 2. `entity_network_analysis.py`
Advanced network analysis and metrics.

**Key Components:**
- `EntityNetworkAnalyzer`: Analyzes graph structure and relationships

**Analysis Types:**
- **Centrality Analysis**: Which entities are most connected/important
- **Path Analysis**: How entities connect across systems
- **Mapping Coverage**: Which legacy entities have HubSpot mappings
- **Complexity Metrics**: Measure of system complexity

#### 3. `generate_entity_graphs.py`
Master orchestration script.

**Usage:**
```bash
# Generate everything (analysis + visualizations)
python3 generate_entity_graphs.py

# Analysis only
python3 generate_entity_graphs.py --analysis-only

# Visualizations only
python3 generate_entity_graphs.py --viz-only

# Include interactive HTML
python3 generate_entity_graphs.py --interactive
```

## Entity Definitions

### Legacy Entities (CRMICALPS)

| Entity | Table | Primary Key | Description |
|--------|-------|-------------|-------------|
| Company | `Company` | `Comp_CompanyId` | Business organizations |
| Person | `Person` | `Pers_PersonId` | Individual contacts |
| Address | `Address` | `Addr_AddressId` | Physical addresses |
| Case | `vCases` | `Case_CaseId` | Support cases/tickets |
| Communication | `vCalendarCommunication` | `Comm_CommunicationId` | Interactions/activities |
| Opportunity | `Opportunity` | `Oppo_OpportunityId` | Sales opportunities |
| SocialNetwork | `SocialNetwork` | `SoN_SocialNetworkId` | Social media profiles |

### HubSpot Entities

| Entity | Table | Primary Key | Legacy ID Field | Mapped From |
|--------|-------|-------------|----------------|-------------|
| companies | `hubspot.companies` | `hs_object_id` | `icalps_company_id` | Company |
| contacts | `hubspot.contacts` | `hs_object_id` | `icalps_contact_id` | Person |
| deals | `hubspot.deals` | `hs_object_id` | `icalps_deal_id` | Case, Opportunity |
| engagements | `hubspot.engagements` | `hs_object_id` | `icalps_communication_id` | Communication |

## Relationship Cardinality

### Cardinality Notation
- `1:1` - One-to-one (each entity maps to exactly one of the other)
- `1:N` - One-to-many
- `N:1` - Many-to-one
- `N:M` - Many-to-many
- `1:0..1` - One-to-zero-or-one (optional relationship)
- `N:0..1` - Many-to-zero-or-one

### Legacy System Relationships

```
Company (1) ──────> (0..1) Address
   ↑
   │ (N:1)
   │
Person (N) ──────> (0..1) Address
   ↑
   │ (N:0..1)
   │
Case (N) ──────> (0..1) Company
Case (N) ──────> (0..1) Person
   ↑
   │ (N:0..1)
   │
Communication

Opportunity (N) ──────> (0..1) Company
Opportunity (N) ──────> (0..1) Person

SocialNetwork (N) ──────> (1) Person
```

### HubSpot System Relationships

```
contacts (N) ──────> (0..1) companies

deals (N) ──────> (0..1) companies

engagements (N) ──────> (0..1) companies
engagements (N) ──────> (0..1) contacts
engagements (N) ──────> (0..1) deals
```

### Cross-System Mappings

```
Legacy System          HubSpot System
─────────────          ──────────────
Company ──────────> companies (1:1)
Person ──────────> contacts (1:1)
Case ──────────> deals (1:1)
Opportunity ──────────> deals (1:1)
Communication ──────────> engagements (1:1)

[Unmapped]
SocialNetwork
Address
```

## Network Analysis Results

### Centrality Analysis

**Most Connected Entities (Degree Centrality):**
1. **Person** (0.600) - Hub of the legacy system
2. **Company** (0.500) - Core business entity
3. **Case** (0.400) - Links many entities
4. **companies** (0.400) - HubSpot hub
5. **deals** (0.400) - HubSpot central entity

**Key Insight**: `Person` and `Company` are the most critical entities in the legacy system, connecting to multiple other entities.

### Path Analysis

- **Total Paths**: 15 (legacy → HubSpot)
- **Direct Mappings**: 5
- **Indirect Paths**: 10

### Mapping Coverage

- **Coverage Rate**: 71.4%
- **Mapped Entities**: 5 out of 7
- **Unmapped Entities**:
  - `SocialNetwork` (no HubSpot equivalent)
  - `Address` (embedded in other entities)

### Complexity Metrics

- **Complexity Score**: 16.90 (Low)
- **Many-to-Many Relationships**: 0
- **Optional Relationships**: 12
- **Interpretation**: Simple entity structure, easy to understand and maintain

## Generated Outputs

### 1. Visualizations

#### `entity_relationship_graph_2d.png`
- High-resolution 2D graph
- Legacy entities on left (pink boxes)
- HubSpot entities on right (blue boxes)
- Association edges (solid gray lines)
- Mapping edges (dashed red lines)
- Cardinality labels on all edges

#### `entity_relationship_graph_3d.png`
- 3D spatial representation
- Legacy entities on z=0 plane (bottom)
- HubSpot entities on z=5 plane (top)
- Vertical mapping edges show cross-system connections

#### `entity_relationship_graph_interactive.html` (optional)
- Interactive browser-based visualization
- Hover for entity details
- Drag to rearrange
- Zoom and pan
- Requires: `pip install pyvis`

### 2. Analysis Reports

#### `entity_network_analysis_report.txt`
Comprehensive text report including:
- Centrality analysis
- Path analysis
- Mapping coverage
- Complexity metrics

#### `entity_network_metrics.csv`
Spreadsheet with centrality metrics for each entity:
- Entity name and system
- Degree centrality
- In-degree centrality
- Out-degree centrality
- Betweenness centrality
- Total connections

### 3. Data Exports

#### `entity_relationship_graph.graphml`
- GraphML format
- Import into Gephi, Cytoscape, or other graph analysis tools
- Preserves all node and edge attributes

#### `entity_relationship_graph.json`
- JSON format
- Node-link structure
- Easy to parse with custom scripts
- Full attribute preservation

## Installation

### Requirements

```bash
pip install -r entity_graph_requirements.txt
```

Or manually:
```bash
pip install networkx>=3.0 matplotlib>=3.5.0 numpy>=1.21.0
```

For interactive visualization:
```bash
pip install pyvis>=0.3.0
```

### Files Required

- `entity_relationship_graph.py` - Core graph builder
- `entity_network_analysis.py` - Analysis module
- `generate_entity_graphs.py` - Master script
- `entity_graph_requirements.txt` - Dependencies

## Usage Examples

### Basic Generation

```bash
# Generate all outputs
python3 generate_entity_graphs.py
```

### Analysis Only

```bash
# Skip visualization generation
python3 generate_entity_graphs.py --analysis-only
```

### Visualization Only

```bash
# Skip network analysis
python3 generate_entity_graphs.py --viz-only
```

### Interactive Visualization

```bash
# Generate interactive HTML
python3 generate_entity_graphs.py --interactive
```

## Customization

### Adding New Entities

Edit `entity_relationship_graph.py`:

```python
# Add to LEGACY_ENTITIES
LEGACY_ENTITIES.append(
    Entity("NewEntity", EntityType.LEGACY, "NewTable", "New_Id")
)

# Add to HUBSPOT_ENTITIES
HUBSPOT_ENTITIES.append(
    Entity("new_entity", EntityType.HUBSPOT, "hubspot.new", "hs_object_id", "icalps_new_id")
)
```

### Adding New Relationships

Edit `entity_relationship_graph.py`:

```python
# Add to LEGACY_RELATIONSHIPS, HUBSPOT_RELATIONSHIPS, or MAPPING_RELATIONSHIPS
LEGACY_RELATIONSHIPS.append(
    Relationship(
        from_entity="legacy_EntityA",
        to_entity="legacy_EntityB",
        cardinality="N:1",
        relationship_type="association",
        foreign_key="EntityA_EntityBId",
        description="EntityA belongs to EntityB"
    )
)
```

### Customizing Visualization

In `EntityRelationshipGraph.visualize_2d()`:

```python
# Change colors
def get_node_color(self, node_id: str) -> str:
    if entity.entity_type == EntityType.LEGACY:
        return '#YOUR_COLOR'  # Change legacy color
    else:
        return '#YOUR_COLOR'  # Change HubSpot color

# Adjust layout
pos[node] = (x, y)  # Customize positions
```

## Integration with Existing Code

### Using Entity Configurations

```python
from entity_relationship_graph import (
    build_entity_relationship_graph,
    LEGACY_ENTITIES,
    HUBSPOT_ENTITIES
)

# Build graph
graph = build_entity_relationship_graph()

# Access entities
for entity in LEGACY_ENTITIES:
    print(f"{entity.name}: {entity.table_name}")

# Query relationships
for u, v, data in graph.G.edges(data=True):
    print(f"{u} -> {v}: {data['cardinality']}")
```

### Network Analysis

```python
from entity_network_analysis import EntityNetworkAnalyzer

# Create analyzer
analyzer = EntityNetworkAnalyzer(graph)

# Get metrics
metrics = analyzer.compute_centrality_metrics()

# Find central entities
central = analyzer.find_most_central_entities(top_n=5)

# Check mapping coverage
coverage = analyzer.analyze_mapping_coverage()
print(f"Coverage: {coverage['mapping_coverage']:.1f}%")
```

## Understanding the Visualization

### Node Types

**Legacy Entities (Pink Boxes)**
- Represent tables in CRMICALPS database
- Source system for data migration
- Connected by association relationships

**HubSpot Entities (Blue Boxes)**
- Represent tables in HubSpot PostgreSQL
- Target system for data migration
- Connected by association relationships

### Edge Types

**Solid Gray Lines (Association)**
- Relationships within the same system
- Foreign key constraints
- Business logic dependencies

**Dashed Red Lines (Mapping)**
- Cross-system entity mappings
- Migration paths
- Legacy ID field links

### Edge Labels

Show cardinality of relationships:
- `1:1` - Each entity maps to exactly one
- `N:1` - Many entities map to one
- `N:0..1` - Many entities optionally map to one

## Troubleshooting

### Import Errors

```bash
# Install missing packages
pip install networkx matplotlib numpy

# For interactive visualization
pip install pyvis
```

### Visualization Not Generated

Check file permissions:
```bash
ls -la entity_*.png
```

Ensure matplotlib backend is available:
```bash
python3 -c "import matplotlib.pyplot as plt; plt.figure(); plt.savefig('test.png')"
```

### Memory Issues

For very large graphs, reduce resolution:

```python
# In visualize_2d()
plt.figure(figsize=(12, 9))  # Smaller figure size
plt.savefig(output_file, dpi=150)  # Lower DPI
```

## Performance

- **Graph Construction**: < 1 second
- **2D Visualization**: 1-2 seconds
- **3D Visualization**: 2-3 seconds
- **Network Analysis**: < 1 second
- **Total Execution**: 5-10 seconds

## Future Enhancements

### Planned Features
1. **Data Flow Visualization**: Show actual data movement
2. **Migration Status Tracking**: Real-time sync status
3. **Anomaly Detection**: Identify broken relationships
4. **Historical Analysis**: Track schema evolution
5. **Interactive Web Dashboard**: Live exploration

### Integration Opportunities
1. **CI/CD Pipeline**: Auto-generate on schema changes
2. **Documentation**: Auto-embed in docs
3. **Monitoring**: Track relationship health
4. **Migration Tools**: Guide data migration

## References

### NetworkX Resources
- [NetworkX Documentation](https://networkx.org/documentation/stable/)
- [Graph Algorithms](https://networkx.org/documentation/stable/reference/algorithms/index.html)
- [Visualization Guide](https://networkx.org/documentation/stable/reference/drawing.html)

### External Tools
- **Gephi**: Advanced graph visualization ([gephi.org](https://gephi.org))
- **Cytoscape**: Network analysis platform ([cytoscape.org](https://cytoscape.org))
- **Neo4j**: Graph database ([neo4j.com](https://neo4j.com))

## License

This project is part of the IC-D-LOAD data migration system.

## Contact

For questions or issues, please refer to the main project documentation.

---

**Last Updated**: 2025-11-17
**Version**: 1.0.0
