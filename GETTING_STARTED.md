# Getting Started with Entity Relationship Graphs

## Quick Start

### Installation

```bash
# Install required packages
pip install -r entity_graph_requirements.txt

# Or install manually
pip install networkx matplotlib numpy
```

### Generate Your First Graph

```bash
# Generate everything (recommended for first time)
python3 generate_entity_graphs.py

# This creates:
# - entity_relationship_graph_2d.png
# - entity_relationship_graph_3d.png
# - entity_network_analysis_report.txt
# - entity_network_metrics.csv
# - entity_relationship_graph.json
# - entity_relationship_graph.graphml
```

### View Results

```bash
# Open visualizations
open entity_relationship_graph_2d.png
open entity_relationship_graph_3d.png

# Read analysis report
cat entity_network_analysis_report.txt
```

---

## Understanding the Output

### 2D Visualization

The 2D graph shows:
- **Left column (pink boxes)**: Legacy CRMICALPS entities
- **Right column (blue boxes)**: HubSpot entities
- **Solid gray lines**: Associations within the same system
- **Dashed red lines**: Mappings between legacy and HubSpot
- **Edge labels**: Cardinality (1:1, N:1, etc.)

**Example**:
```
Person ─────N:1────> Company        (Legacy system)
   |                    |
   | 1:1 (mapping)      | 1:1 (mapping)
   |                    |
   v                    v
contacts ──N:0..1──> companies      (HubSpot system)
```

### 3D Visualization

The 3D graph shows:
- **Bottom plane (z=0)**: Legacy entities in circular pattern
- **Top plane (z=5)**: HubSpot entities in circular pattern
- **Vertical lines**: Cross-system mappings
- **Horizontal lines**: Within-system associations

**Interpretation**: Vertical "cables" show how legacy data flows up to HubSpot.

### Analysis Report

Key sections:
1. **Centrality Analysis**: Which entities are most important/connected
2. **Path Analysis**: How entities connect across systems
3. **Mapping Coverage**: What percentage of legacy entities are mapped
4. **Complexity Metrics**: How complex is the relationship structure

---

## Common Tasks

### Task 1: Add a New Entity

**Scenario**: You want to add a new entity to the graph.

**Steps**:

1. **Define the entity** in `entity_relationship_graph.py`:

```python
# Add to LEGACY_ENTITIES or HUBSPOT_ENTITIES
LEGACY_ENTITIES.append(
    Entity(
        name="Invoice",                    # Display name
        entity_type=EntityType.LEGACY,     # System type
        table_name="Invoice",              # Database table
        primary_key="Inv_InvoiceId"        # Primary key field
    )
)
```

2. **Regenerate the graph**:

```bash
python3 generate_entity_graphs.py
```

3. **Verify** the new entity appears in the visualization.

### Task 2: Add a New Relationship

**Scenario**: You want to show that Invoices belong to Companies.

**Steps**:

1. **Define the relationship** in `entity_relationship_graph.py`:

```python
# Add to LEGACY_RELATIONSHIPS
LEGACY_RELATIONSHIPS.append(
    Relationship(
        from_entity="legacy_Invoice",           # Source (child)
        to_entity="legacy_Company",             # Target (parent)
        cardinality="N:1",                      # Many invoices to one company
        relationship_type="association",        # Type
        foreign_key="Inv_CompanyId",           # FK field
        description="Invoice belongs to company"
    )
)
```

2. **Regenerate the graph**:

```bash
python3 generate_entity_graphs.py
```

3. **Check the visualization** for the new edge with "N:1" label.

### Task 3: Add a Cross-System Mapping

**Scenario**: You've created a mapping from legacy Invoices to HubSpot custom object.

**Steps**:

1. **Add HubSpot entity**:

```python
HUBSPOT_ENTITIES.append(
    Entity(
        name="invoices",
        entity_type=EntityType.HUBSPOT,
        table_name="hubspot.invoices",
        primary_key="hs_object_id",
        legacy_id_field="icalps_invoice_id"    # Legacy ID link
    )
)
```

2. **Add mapping relationship**:

```python
MAPPING_RELATIONSHIPS.append(
    Relationship(
        from_entity="legacy_Invoice",
        to_entity="hubspot_invoices",
        cardinality="1:1",                      # One-to-one mapping
        relationship_type="mapping",            # Cross-system
        foreign_key="icalps_invoice_id",
        description="Invoice mapped to HubSpot invoice"
    )
)
```

3. **Regenerate** and verify the **dashed red line** appears.

### Task 4: Change Visualization Colors

**Scenario**: You want different colors for entity types.

**Steps**:

1. **Edit color functions** in `entity_relationship_graph.py`:

```python
def get_node_color(self, node_id: str) -> str:
    """Get color for node based on entity type"""
    entity = self.entities[node_id]
    if entity.entity_type == EntityType.LEGACY:
        return '#FFA07A'  # Light salmon (change from pink)
    else:
        return '#98FB98'  # Pale green (change from blue)
```

2. **Regenerate**:

```bash
python3 generate_entity_graphs.py
```

### Task 5: Generate Only Analysis (Skip Visualization)

**Scenario**: You only need the metrics, not the images.

```bash
python3 generate_entity_graphs.py --analysis-only
```

**Output**:
- `entity_network_analysis_report.txt`
- `entity_network_metrics.csv`

### Task 6: Generate Interactive HTML Visualization

**Scenario**: You want an interactive graph you can explore in a browser.

**Steps**:

1. **Install pyvis**:

```bash
pip install pyvis
```

2. **Generate with interactive flag**:

```bash
python3 generate_entity_graphs.py --interactive
```

3. **Open in browser**:

```bash
open entity_relationship_graph_interactive.html
```

**Features**:
- Drag nodes to reposition
- Hover for entity details
- Zoom and pan
- Physics-based layout

### Task 7: Export Graph Data for Custom Processing

**Scenario**: You want to analyze the graph in another tool or script.

**Options**:

**Option 1: JSON (for Python/JavaScript)**
```python
import json
import networkx as nx

# Load graph from JSON
with open('entity_relationship_graph.json', 'r') as f:
    data = json.load(f)

G = nx.node_link_graph(data)

# Query the graph
print(f"Total entities: {G.number_of_nodes()}")
print(f"Total relationships: {G.number_of_edges()}")

# Find all companies
companies = [
    n for n, attrs in G.nodes(data=True)
    if 'Company' in attrs.get('label', '')
]
```

**Option 2: GraphML (for Gephi/Cytoscape)**
```bash
# Import entity_relationship_graph.graphml into:
# - Gephi: Advanced graph visualization
# - Cytoscape: Network analysis platform
```

**Option 3: CSV (for Excel/Spreadsheet)**
```python
import pandas as pd

# Load metrics
df = pd.read_csv('entity_network_metrics.csv')

# Analyze in pandas
print(df.sort_values('Degree Centrality', ascending=False))
```

### Task 8: Customize Layout Spacing

**Scenario**: Nodes are overlapping in your visualization.

**Steps**:

1. **Edit layout parameters** in `entity_relationship_graph.py`:

```python
def visualize_2d(self, output_file: str = "graph_2d.png"):
    # ...

    # Increase vertical spacing
    legacy_y_spacing = 3.0    # Changed from 2.0
    for i, node in enumerate(legacy_nodes):
        pos[node] = (0, -i * legacy_y_spacing)

    # Increase horizontal separation
    hubspot_x = 15    # Changed from 10
    for i, node in enumerate(hubspot_nodes):
        pos[node] = (hubspot_x, -i * 3.0)
```

2. **Regenerate**.

### Task 9: Filter Graph by Entity Type

**Scenario**: You only want to see legacy entities and their relationships.

**Steps**:

1. **Create custom script**:

```python
from entity_relationship_graph import build_entity_relationship_graph
import networkx as nx
import matplotlib.pyplot as plt

# Build full graph
full_graph = build_entity_relationship_graph()

# Filter to legacy nodes only
legacy_nodes = [n for n in full_graph.G.nodes() if n.startswith('legacy_')]

# Create subgraph
legacy_subgraph = full_graph.G.subgraph(legacy_nodes)

# Visualize
pos = nx.spring_layout(legacy_subgraph)
nx.draw(legacy_subgraph, pos, with_labels=True, node_color='#FFB6C1')
plt.savefig('legacy_only.png')
```

### Task 10: Find All Paths Between Two Entities

**Scenario**: You want to know all ways Person connects to deals.

**Steps**:

1. **Use NetworkX path algorithms**:

```python
from entity_relationship_graph import build_entity_relationship_graph
import networkx as nx

graph = build_entity_relationship_graph()
G = graph.G

source = 'legacy_Person'
target = 'hubspot_deals'

# Find all simple paths
all_paths = list(nx.all_simple_paths(G, source, target, cutoff=5))

print(f"Found {len(all_paths)} paths from {source} to {target}:")
for i, path in enumerate(all_paths, 1):
    print(f"\nPath {i}:")
    for j in range(len(path) - 1):
        u, v = path[j], path[j+1]
        cardinality = G[u][v]['cardinality']
        rel_type = G[u][v]['relationship_type']
        print(f"  {u} --[{cardinality}, {rel_type}]--> {v}")
```

**Example Output**:
```
Found 2 paths from legacy_Person to hubspot_deals:

Path 1:
  legacy_Person --[1:1, mapping]--> hubspot_contacts
  hubspot_contacts --[N:0..1, association]--> hubspot_companies
  hubspot_companies --[N:0..1, association]--> hubspot_deals

Path 2:
  legacy_Person --[1:1, mapping]--> hubspot_contacts
  hubspot_contacts --[N:0..1, association]--> hubspot_deals
```

---

## Advanced Usage

### Custom Analysis Script

Create your own analysis:

```python
from entity_relationship_graph import build_entity_relationship_graph
from entity_network_analysis import EntityNetworkAnalyzer

# Build graph
graph = build_entity_relationship_graph()

# Create analyzer
analyzer = EntityNetworkAnalyzer(graph)

# Custom queries
metrics = analyzer.compute_centrality_metrics()

# Find most central legacy entity
legacy_metrics = {
    k: v for k, v in metrics.items()
    if k.startswith('legacy_')
}

most_central = max(
    legacy_metrics.items(),
    key=lambda x: x[1]['degree']
)

print(f"Most central legacy entity: {most_central[0]}")
print(f"Degree centrality: {most_central[1]['degree']:.3f}")

# Find unmapped entities
coverage = analyzer.analyze_mapping_coverage()
print(f"\nUnmapped entities: {coverage['unmapped_entities']}")

# Calculate complexity
complexity = analyzer.analyze_relationship_complexity()
print(f"\nComplexity score: {complexity['complexity_score']:.2f}")
```

### Batch Processing Multiple Configurations

Generate graphs for different entity subsets:

```python
import os
from entity_relationship_graph import EntityRelationshipGraph, Entity, Relationship, EntityType

configs = [
    {
        'name': 'core_entities',
        'entities': ['Company', 'Person', 'companies', 'contacts'],
    },
    {
        'name': 'transaction_entities',
        'entities': ['Case', 'Opportunity', 'deals'],
    },
]

for config in configs:
    # Build custom graph
    graph = EntityRelationshipGraph()

    # Add filtered entities
    # ... (implementation)

    # Generate visualization
    output_file = f"graph_{config['name']}.png"
    graph.visualize_2d(output_file)

    print(f"Generated: {output_file}")
```

### Integration with CI/CD

Add graph generation to your pipeline:

```bash
#!/bin/bash
# .github/workflows/generate-graphs.sh

set -e

echo "Installing dependencies..."
pip install -r entity_graph_requirements.txt

echo "Generating entity graphs..."
python3 generate_entity_graphs.py --analysis-only

echo "Checking for unmapped entities..."
if grep -q "Unmapped entities:" entity_network_analysis_report.txt; then
    echo "⚠️ Warning: Unmapped entities detected"
    grep -A 10 "Unmapped entities:" entity_network_analysis_report.txt
fi

echo "Checking mapping coverage..."
coverage=$(grep "Coverage Rate:" entity_network_analysis_report.txt | awk '{print $3}')
echo "Mapping coverage: $coverage"

if (( $(echo "$coverage < 70.0" | bc -l) )); then
    echo "❌ Error: Mapping coverage below 70%"
    exit 1
fi

echo "✅ Graph analysis complete"
```

---

## Troubleshooting

### Problem: "ModuleNotFoundError: No module named 'networkx'"

**Solution**:
```bash
pip install networkx matplotlib numpy
```

### Problem: Visualization file is empty or corrupted

**Solution**:
```bash
# Check matplotlib backend
python3 -c "import matplotlib; print(matplotlib.get_backend())"

# If no display available, set backend
export MPLBACKEND=Agg
python3 generate_entity_graphs.py
```

### Problem: Nodes are overlapping in visualization

**Solution**: Increase spacing in layout algorithm (see Task 8 above)

### Problem: Graph generation is slow

**Solution**:
```bash
# Skip visualization, only generate analysis
python3 generate_entity_graphs.py --analysis-only

# Or reduce image resolution
# Edit visualize_2d() to use lower DPI:
plt.savefig(output_file, dpi=150)  # Instead of 300
```

### Problem: Cannot import graph JSON in another tool

**Solution**:
```python
# Ensure correct format
import networkx as nx
import json

# Load graph
with open('entity_relationship_graph.json', 'r') as f:
    data = json.load(f)

# Check structure
print("Keys:", data.keys())
print("Nodes:", len(data['nodes']))
print("Links:", len(data['links']))

# Convert to NetworkX
G = nx.node_link_graph(data)
```

### Problem: Want to add custom node attributes

**Solution**:
```python
# In entity_relationship_graph.py, modify add_entities():

def add_entities(self, entities: List[Entity]):
    for entity in entities:
        node_id = entity.get_node_id()
        self.entities[node_id] = entity

        # Add custom attributes
        self.G.add_node(
            node_id,
            label=entity.name,
            entity_type=entity.entity_type.value,
            table_name=entity.table_name,
            primary_key=entity.primary_key,
            # Custom attributes:
            row_count=entity.get_row_count(),  # Add your method
            last_updated=entity.last_updated_date,
        )
```

---

## Examples Gallery

### Example 1: Simple Two-Entity Graph

```python
from entity_relationship_graph import EntityRelationshipGraph, Entity, Relationship, EntityType

# Create minimal graph
graph = EntityRelationshipGraph()

# Add entities
entities = [
    Entity("Person", EntityType.LEGACY, "Person", "Pers_PersonId"),
    Entity("Company", EntityType.LEGACY, "Company", "Comp_CompanyId"),
]
graph.add_entities(entities)

# Add relationship
relationships = [
    Relationship(
        from_entity="legacy_Person",
        to_entity="legacy_Company",
        cardinality="N:1",
        relationship_type="association",
        foreign_key="Pers_CompanyId",
        description="Person works at company"
    )
]
graph.add_relationships(relationships)

# Visualize
graph.visualize_2d("simple_graph.png")
```

### Example 2: Query Graph Metrics

```python
from entity_network_analysis import EntityNetworkAnalyzer
from entity_relationship_graph import build_entity_relationship_graph

# Build graph
graph = build_entity_relationship_graph()
analyzer = EntityNetworkAnalyzer(graph)

# Get centrality metrics
metrics = analyzer.compute_centrality_metrics()

# Print top 3 entities by degree centrality
sorted_entities = sorted(
    metrics.items(),
    key=lambda x: x[1]['degree'],
    reverse=True
)[:3]

print("Top 3 Most Connected Entities:")
for i, (entity, scores) in enumerate(sorted_entities, 1):
    print(f"{i}. {entity}")
    print(f"   Degree Centrality: {scores['degree']:.3f}")
    print(f"   Total Connections: {scores['total_connections']}")
```

### Example 3: Export for Documentation

```python
from entity_relationship_graph import build_entity_relationship_graph
import json

# Build graph
graph = build_entity_relationship_graph()

# Extract entity list for documentation
entities_doc = []
for node_id, entity in graph.entities.items():
    entities_doc.append({
        'System': entity.entity_type.value,
        'Name': entity.name,
        'Table': entity.table_name,
        'Primary Key': entity.primary_key
    })

# Extract relationship list
relationships_doc = []
for u, v, data in graph.G.edges(data=True):
    from_name = graph.entities[u].name
    to_name = graph.entities[v].name
    relationships_doc.append({
        'From': from_name,
        'To': to_name,
        'Cardinality': data['cardinality'],
        'Type': data['relationship_type'],
        'Description': data.get('description', '')
    })

# Save as JSON for documentation generator
with open('entities_for_docs.json', 'w') as f:
    json.dump({
        'entities': entities_doc,
        'relationships': relationships_doc
    }, f, indent=2)

print("Documentation data exported to entities_for_docs.json")
```

---

## Best Practices

### 1. Naming Conventions

**Node IDs**:
- Use `{system}_{entity}` format
- Examples: `legacy_Company`, `hubspot_companies`
- Ensures uniqueness across systems

**Attributes**:
- Use snake_case: `entity_type`, `primary_key`
- Be consistent across all nodes/edges

### 2. Cardinality Notation

**Standard notation**:
- `1:1` - One-to-one
- `1:N` - One-to-many
- `N:1` - Many-to-one
- `N:M` - Many-to-many
- `1:0..1` - One-to-zero-or-one (optional)
- `N:0..1` - Many-to-zero-or-one (optional)

**Direction**:
- Follow foreign key direction
- Child entity → Parent entity

### 3. Relationship Types

**Use standard types**:
- `association` - Within-system relationships
- `mapping` - Cross-system entity mappings
- `aggregation` - Composite relationships (if needed)

### 4. Visual Design

**Colors**:
- Use distinct colors for different systems
- Maintain sufficient contrast
- Consider color-blind friendly palettes

**Layout**:
- Keep systems visually separated
- Use consistent spacing
- Minimize edge crossings

### 5. Documentation

**Always document**:
- Entity purpose and source table
- Relationship cardinality and FK
- Mapping logic (legacy → new)
- Any data transformations

---

## Next Steps

1. **Explore** the generated visualizations
2. **Read** the analysis report to understand your entity structure
3. **Customize** colors, layout, or entities as needed
4. **Export** data for use in other tools (Gephi, documentation, etc.)
5. **Integrate** graph generation into your workflow

## Resources

- **NetworkX Documentation**: https://networkx.org/documentation/stable/
- **Matplotlib Gallery**: https://matplotlib.org/stable/gallery/
- **Graph Theory Primer**: https://en.wikipedia.org/wiki/Graph_theory
- **Gephi (Graph Visualization)**: https://gephi.org/
- **Cytoscape (Network Analysis)**: https://cytoscape.org/

## Questions?

See `SKILL.md` for technical deep-dive on algorithms and data structures.
See `ENTITY_GRAPH_README.md` for complete system documentation.
