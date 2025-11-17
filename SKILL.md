# NetworkX Entity Graph - Technical Deep Dive

## Table of Contents
1. [Graph Data Structures](#graph-data-structures)
2. [Node Architecture](#node-architecture)
3. [Edge Architecture](#edge-architecture)
4. [Layout Algorithms](#layout-algorithms)
5. [Visualization Algorithms](#visualization-algorithms)
6. [Implementation Patterns](#implementation-patterns)

---

## Graph Data Structures

### The Graph Object

In NetworkX, graph data are stored in a **dictionary-like** fashion under a `Graph` object:

```python
import networkx as nx

# Create a directed graph (edges have direction)
G = nx.DiGraph()

# For undirected graphs, use:
# G = nx.Graph()
```

In our implementation, we use a **directed graph** (`DiGraph`) because entity relationships have directionality:
- Foreign keys point from child → parent
- Mappings flow from legacy → HubSpot

### Data Storage Model

NetworkX uses **three-level dictionary nesting**:

```
Graph G
├── G.nodes        # Dictionary: {node_id: {attributes}}
├── G.edges        # Dictionary: {(node1, node2): {attributes}}
└── G.adj          # Adjacency dictionary: {node: {neighbor: edge_data}}
```

**Key Principle**: Any **hashable** object can be a node:
- ✅ Strings: `"legacy_Company"`
- ✅ Tuples: `("legacy", "Company")`
- ✅ Numbers: `1, 2, 3`
- ❌ Lists: `["legacy", "Company"]` (not hashable)
- ❌ Sets: `{"legacy", "Company"}` (not hashable)

---

## Node Architecture

### Node Data Structure

Nodes are stored in `G.nodes`, a dictionary-like container:

```python
# Access node data
G.nodes[node_id]

# Example output:
{
    'label': 'Company',
    'entity_type': 'legacy',
    'table_name': 'Company',
    'primary_key': 'Comp_CompanyId'
}
```

### Adding Nodes: The Pattern

Our implementation uses a structured approach to add entities as nodes:

```python
def add_entities(self, entities: List[Entity]):
    """Add entities as nodes with attributes"""
    for entity in entities:
        # Generate unique node identifier
        node_id = entity.get_node_id()  # e.g., "legacy_Company"

        # Store entity reference for later use
        self.entities[node_id] = entity

        # Add node to graph with attributes
        self.G.add_node(
            node_id,                              # Node identifier
            label=entity.name,                     # Display name
            entity_type=entity.entity_type.value,  # "legacy" or "hubspot"
            table_name=entity.table_name,          # Database table
            primary_key=entity.primary_key         # Primary key field
        )
```

**Why This Pattern?**
1. **Unique IDs**: `get_node_id()` ensures no collisions between systems
   - Legacy: `legacy_Company`
   - HubSpot: `hubspot_companies`

2. **Dual Storage**:
   - `self.entities[node_id]` → Full Entity object (rich data)
   - `G.nodes[node_id]` → Graph attributes (visualization data)

3. **Attribute Dictionary**: NetworkX stores arbitrary key-value pairs per node

### Accessing Node Data

```python
# Method 1: Dictionary access
node_attrs = G.nodes['legacy_Company']
print(node_attrs['label'])  # "Company"

# Method 2: Get specific attribute
label = G.nodes['legacy_Company'].get('label')

# Method 3: Iterate all nodes
for node_id, attrs in G.nodes(data=True):
    print(f"{node_id}: {attrs}")

# Method 4: Get node list
all_nodes = list(G.nodes())
```

### Node Attributes Schema

In our implementation, each node has:

| Attribute | Type | Example | Purpose |
|-----------|------|---------|---------|
| `label` | str | `"Company"` | Display name for visualization |
| `entity_type` | str | `"legacy"` or `"hubspot"` | System classification |
| `table_name` | str | `"Company"` | Database table name |
| `primary_key` | str | `"Comp_CompanyId"` | Primary key field |

---

## Edge Architecture

### Edge Data Structure

Edges are stored in `G.edges`, representing relationships between nodes:

```python
# Access edge data
G.edges[node1, node2]

# Example output:
{
    'cardinality': 'N:1',
    'relationship_type': 'association',
    'foreign_key': 'Pers_CompanyId',
    'description': 'Person works at company'
}
```

### Adding Edges: The Pattern

```python
def add_relationships(self, relationships: List[Relationship]):
    """Add relationships as edges with attributes"""
    for rel in relationships:
        self.relationships.append(rel)

        # Add directed edge
        self.G.add_edge(
            rel.from_entity,              # Source node
            rel.to_entity,                # Target node
            cardinality=rel.cardinality,  # "1:1", "N:1", etc.
            relationship_type=rel.relationship_type,  # "association", "mapping"
            foreign_key=rel.foreign_key or "",
            description=rel.description or ""
        )
```

**Edge Direction**:
- `from_entity` → `to_entity` follows foreign key direction
- Example: `Person` → `Company` (Person.Pers_CompanyId references Company)

### Accessing Edge Data

```python
# Method 1: Direct access
edge_data = G.edges['legacy_Person', 'legacy_Company']
print(edge_data['cardinality'])  # "N:1"

# Method 2: Alternative syntax
edge_data = G['legacy_Person']['legacy_Company']

# Method 3: Iterate all edges
for u, v, data in G.edges(data=True):
    print(f"{u} -> {v}: {data['cardinality']}")

# Method 4: Filter edges by attribute
association_edges = [
    (u, v) for u, v, d in G.edges(data=True)
    if d.get('relationship_type') == 'association'
]
```

### Edge Attributes Schema

| Attribute | Type | Example | Purpose |
|-----------|------|---------|---------|
| `cardinality` | str | `"N:1"` | Relationship multiplicity |
| `relationship_type` | str | `"association"` | Type classification |
| `foreign_key` | str | `"Pers_CompanyId"` | FK field name |
| `description` | str | `"Person works at company"` | Human-readable description |

### Filtering Edges by Type

Common pattern to separate edge types for visualization:

```python
# Group edges by relationship type
for rel_type in ['association', 'mapping', 'aggregation']:
    edges = [
        (u, v) for u, v, d in G.edges(data=True)
        if d.get('relationship_type') == rel_type
    ]

    # Process each group differently
    if edges:
        # Create edge labels from cardinality
        edge_labels = {
            (u, v): G[u][v]['cardinality']
            for u, v in edges
        }

        # Draw with type-specific styling
        draw_edges(edges, edge_labels, rel_type)
```

---

## Layout Algorithms

Layout algorithms determine **node positions** in 2D/3D space before visualization.

### 2D Hierarchical Layout

**Algorithm**: Separate systems into left/right columns

```python
def create_2d_layout(G, legacy_nodes, hubspot_nodes):
    """
    Position nodes in hierarchical side-by-side layout.

    Legacy entities:  Left column  (x=0)
    HubSpot entities: Right column (x=10)
    """
    pos = {}

    # Position legacy entities vertically on left
    legacy_y_spacing = 2.0
    for i, node in enumerate(legacy_nodes):
        pos[node] = (0, -i * legacy_y_spacing)
        # Result: (0, 0), (0, -2), (0, -4), ...

    # Position HubSpot entities vertically on right
    hubspot_y_spacing = 3.0
    for i, node in enumerate(hubspot_nodes):
        pos[node] = (10, -i * hubspot_y_spacing)
        # Result: (10, 0), (10, -3), (10, -6), ...

    return pos
```

**Mathematical Properties**:
- X-coordinate: Categorical (0 for legacy, 10 for HubSpot)
- Y-coordinate: Sequential spacing with negative direction (top-to-bottom)
- Spacing can vary by system to avoid overlap

**Position Dictionary Format**:
```python
pos = {
    'legacy_Company':  (0, 0),
    'legacy_Person':   (0, -2),
    'hubspot_companies': (10, 0),
    'hubspot_contacts':  (10, -3),
}
```

### 3D Circular Layout

**Algorithm**: Position nodes in circular patterns on separate Z-planes

```python
def create_3d_layout(G, legacy_nodes, hubspot_nodes):
    """
    Position nodes in 3D space:
    - Legacy:  Circular pattern on z=0 plane
    - HubSpot: Circular pattern on z=5 plane
    """
    pos_3d = {}

    # Legacy entities on bottom plane (z=0)
    n_legacy = len(legacy_nodes)
    radius = 5

    for i, node in enumerate(legacy_nodes):
        # Calculate angle for even distribution around circle
        angle = 2 * np.pi * i / n_legacy

        # Convert polar to Cartesian coordinates
        x = radius * np.cos(angle)
        y = radius * np.sin(angle)
        z = 0  # Bottom plane

        pos_3d[node] = (x, y, z)

    # HubSpot entities on top plane (z=5)
    n_hubspot = len(hubspot_nodes)

    for i, node in enumerate(hubspot_nodes):
        angle = 2 * np.pi * i / n_hubspot

        x = radius * np.cos(angle)
        y = radius * np.sin(angle)
        z = 5  # Top plane

        pos_3d[node] = (x, y, z)

    return pos_3d
```

**Mathematical Properties**:

1. **Polar to Cartesian Conversion**:
   ```
   angle = 2π * i / n    (evenly distribute around circle)
   x = r * cos(angle)
   y = r * sin(angle)
   ```

2. **Circle Division**:
   - 4 nodes: angles = [0°, 90°, 180°, 270°]
   - 7 nodes: angles = [0°, 51.4°, 102.9°, 154.3°, ...]

3. **Z-plane Separation**:
   - Legacy:  z = 0 (bottom)
   - HubSpot: z = 5 (top)
   - Mapping edges span vertically between planes

**Position Dictionary Format**:
```python
pos_3d = {
    'legacy_Company':     (5.0, 0.0, 0),
    'legacy_Person':      (0.0, 5.0, 0),
    'hubspot_companies':  (5.0, 0.0, 5),
    'hubspot_contacts':   (0.0, 5.0, 5),
}
```

### Alternative Layouts

NetworkX provides many built-in layouts:

```python
# Spring layout (force-directed)
pos = nx.spring_layout(G, dim=3, scale=1.9)

# Kamada-Kawai layout (energy-based)
pos = nx.kamada_kawai_layout(G)

# Circular layout
pos = nx.circular_layout(G)

# Shell layout (concentric circles)
pos = nx.shell_layout(G)
```

For our use case, **custom layouts** work best because we need:
- Clear system separation (legacy vs. HubSpot)
- Predictable positioning for consistency
- Control over spacing and overlap

---

## Visualization Algorithms

### 2D Visualization Algorithm

**Step-by-step process**:

```python
def visualize_2d(self, output_file: str = "graph_2d.png"):
    """Create 2D visualization with NetworkX and Matplotlib"""

    # STEP 1: Initialize figure
    plt.figure(figsize=(24, 18))

    # STEP 2: Create layout (position dictionary)
    legacy_nodes = [n for n in G.nodes() if n.startswith('legacy_')]
    hubspot_nodes = [n for n in G.nodes() if n.startswith('hubspot_')]

    pos = {}
    for i, node in enumerate(legacy_nodes):
        pos[node] = (0, -i * 2.0)
    for i, node in enumerate(hubspot_nodes):
        pos[node] = (10, -i * 3.0)

    # STEP 3: Draw nodes (by entity type for different colors)
    for node in G.nodes():
        color = get_node_color(node)  # Pink for legacy, blue for HubSpot
        entity = self.entities[node]

        # Draw single node
        nx.draw_networkx_nodes(
            G, pos,
            nodelist=[node],       # Single node list
            node_color=color,
            node_size=3000,
            node_shape='s',        # Square
            alpha=0.9,
            edgecolors='black',
            linewidths=2
        )

        # Draw node label
        nx.draw_networkx_labels(
            G, pos,
            labels={node: entity.name},
            font_size=10,
            font_weight='bold'
        )

    # STEP 4: Draw edges (by relationship type for different styles)
    for rel_type in ['association', 'mapping', 'aggregation']:
        # Filter edges of this type
        edges = [
            (u, v) for u, v, d in G.edges(data=True)
            if d.get('relationship_type') == rel_type
        ]

        if edges:
            # Create edge labels (cardinality)
            edge_labels = {
                (u, v): G[u][v]['cardinality']
                for u, v in edges
            }

            # Draw edges with type-specific styling
            nx.draw_networkx_edges(
                G, pos,
                edgelist=edges,
                edge_color=get_edge_color(rel_type),
                style=get_edge_style(rel_type),  # 'solid', 'dashed', 'dotted'
                width=2 if rel_type == 'mapping' else 1.5,
                alpha=0.8 if rel_type == 'mapping' else 0.6,
                arrows=True,
                arrowsize=20,
                arrowstyle='->',
                connectionstyle='arc3,rad=0.1'  # Curved edges
            )

            # Draw edge labels
            nx.draw_networkx_edge_labels(
                G, pos,
                edge_labels=edge_labels,
                font_size=8,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7)
            )

    # STEP 5: Add legend, title, and save
    plt.legend(...)
    plt.title("Entity Relationship Graph")
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
```

**Key Algorithms**:

1. **Color Mapping**:
   ```python
   def get_node_color(self, node_id: str) -> str:
       entity = self.entities[node_id]
       return '#FFB6C1' if entity.entity_type == EntityType.LEGACY else '#87CEEB'
   ```

2. **Edge Styling**:
   ```python
   def get_edge_style(self, relationship_type: str) -> str:
       styles = {
           'association': 'solid',
           'mapping': 'dashed',
           'aggregation': 'dotted'
       }
       return styles.get(relationship_type, 'solid')
   ```

3. **Edge Filtering**:
   ```python
   # List comprehension to filter edges by attribute
   edges = [
       (u, v) for u, v, d in G.edges(data=True)
       if d.get('relationship_type') == rel_type
   ]
   ```

### 3D Visualization Algorithm

**Uses matplotlib's 3D projection**:

```python
def visualize_3d(self, output_file: str = "graph_3d.png"):
    """Create 3D visualization"""
    from mpl_toolkits.mplot3d import Axes3D

    # STEP 1: Create 3D figure
    fig = plt.figure(figsize=(20, 16))
    ax = fig.add_subplot(111, projection='3d')

    # STEP 2: Create 3D layout
    pos_3d = create_3d_circular_layout(G, legacy_nodes, hubspot_nodes)

    # STEP 3: Draw nodes as 3D scatter points
    for node in G.nodes():
        x, y, z = pos_3d[node]  # Unpack 3D coordinates
        color = get_node_color(node)
        entity = self.entities[node]

        # Draw 3D point
        ax.scatter(
            x, y, z,
            c=color,
            s=500,           # Size
            marker='o',      # Circle
            edgecolors='black',
            linewidths=2,
            alpha=0.9
        )

        # Add text label at 3D position
        ax.text(x, y, z, entity.name, fontsize=10, fontweight='bold')

    # STEP 4: Draw edges as 3D lines
    for u, v, data in G.edges(data=True):
        # Get endpoints from position dictionary
        x_vals = [pos_3d[u][0], pos_3d[v][0]]
        y_vals = [pos_3d[u][1], pos_3d[v][1]]
        z_vals = [pos_3d[u][2], pos_3d[v][2]]

        # Get edge styling
        color = get_edge_color(data['relationship_type'])
        linestyle = get_edge_style(data['relationship_type'])

        # Draw 3D line connecting nodes
        ax.plot(
            x_vals, y_vals, z_vals,
            color=color,
            linestyle=linestyle,
            linewidth=2,
            alpha=0.6
        )

    # STEP 5: Configure axes and save
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_zlabel('Z (Legacy=0, HubSpot=5)')
    ax.set_title('3D Entity Relationship Graph')

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
```

**Key 3D Concepts**:

1. **3D Line Drawing**:
   ```python
   # Extract x, y, z coordinates for both endpoints
   x_vals = [pos_3d[u][0], pos_3d[v][0]]  # [start_x, end_x]
   y_vals = [pos_3d[u][1], pos_3d[v][1]]  # [start_y, end_y]
   z_vals = [pos_3d[u][2], pos_3d[v][2]]  # [start_z, end_z]

   # Plot connects the points
   ax.plot(x_vals, y_vals, z_vals)
   ```

2. **3D Text Positioning**:
   ```python
   ax.text(x, y, z, label)  # Text appears at 3D coordinates
   ```

3. **Projection**:
   - 3D coordinates are projected onto 2D screen
   - User can rotate view (in interactive mode)
   - Static image captures one viewing angle

### JSON Export Algorithm

**Export graph as JSON for programmatic access**:

```python
def export_to_json(self, output_file: str = "graph.json"):
    """Export graph to JSON format"""
    import json

    # Convert NetworkX graph to node-link format
    graph_data = nx.node_link_data(G)

    # Structure:
    # {
    #   "directed": true,
    #   "multigraph": false,
    #   "graph": {},
    #   "nodes": [
    #     {"id": "legacy_Company", "label": "Company", ...},
    #     {"id": "legacy_Person", "label": "Person", ...}
    #   ],
    #   "links": [
    #     {"source": "legacy_Person", "target": "legacy_Company", "cardinality": "N:1", ...}
    #   ]
    # }

    # Save to file
    with open(output_file, 'w') as f:
        json.dump(graph_data, f, indent=2)
```

**JSON Structure**:

```json
{
  "directed": true,
  "nodes": [
    {
      "id": "legacy_Company",
      "label": "Company",
      "entity_type": "legacy",
      "table_name": "Company",
      "primary_key": "Comp_CompanyId"
    }
  ],
  "links": [
    {
      "source": "legacy_Person",
      "target": "legacy_Company",
      "cardinality": "N:1",
      "relationship_type": "association",
      "foreign_key": "Pers_CompanyId"
    }
  ]
}
```

**Reading JSON back**:

```python
import json
import networkx as nx

# Load from JSON
with open('graph.json', 'r') as f:
    graph_data = json.load(f)

# Convert to NetworkX graph
G = nx.node_link_graph(graph_data)

# Access node attributes
print(G.nodes['legacy_Company']['label'])  # "Company"

# Access edge attributes
print(G['legacy_Person']['legacy_Company']['cardinality'])  # "N:1"
```

---

## Implementation Patterns

### Pattern 1: Dual Storage (Entity + Graph)

**Problem**: Need both rich entity data and graph attributes

**Solution**: Store entity objects separately from graph

```python
class EntityRelationshipGraph:
    def __init__(self):
        self.G = nx.DiGraph()           # NetworkX graph
        self.entities: Dict[str, Entity] = {}  # Entity objects
        self.relationships: List[Relationship] = []

    def add_entities(self, entities: List[Entity]):
        for entity in entities:
            node_id = entity.get_node_id()

            # Store full entity object
            self.entities[node_id] = entity

            # Store minimal graph attributes
            self.G.add_node(
                node_id,
                label=entity.name,
                entity_type=entity.entity_type.value
            )
```

**Benefits**:
- Graph stays lightweight (only visualization data)
- Entity objects have full methods and properties
- Easy to extend without modifying graph

### Pattern 2: Filtered Edge Rendering

**Problem**: Need different visual styles for different edge types

**Solution**: Filter edges by attribute, render separately

```python
def render_edges_by_type(G, pos):
    """Render edges with type-specific styling"""

    for rel_type in ['association', 'mapping', 'aggregation']:
        # Filter to edges of this type
        edges = [
            (u, v) for u, v, d in G.edges(data=True)
            if d.get('relationship_type') == rel_type
        ]

        if not edges:
            continue

        # Extract labels for these edges
        edge_labels = {
            (u, v): G[u][v]['cardinality']
            for u, v in edges
        }

        # Render with type-specific style
        nx.draw_networkx_edges(
            G, pos,
            edgelist=edges,
            edge_color=COLOR_MAP[rel_type],
            style=STYLE_MAP[rel_type],
            width=WIDTH_MAP[rel_type]
        )

        nx.draw_networkx_edge_labels(G, pos, edge_labels)
```

**Benefits**:
- Clean separation of concerns
- Easy to add new edge types
- Type-specific styling without conditional logic

### Pattern 3: Position Dictionary Abstraction

**Problem**: Layout algorithms are complex and reusable

**Solution**: Separate layout from rendering

```python
def create_layout(G, layout_type='hierarchical'):
    """
    Create position dictionary for nodes.

    Returns:
        dict: {node_id: (x, y)} or {node_id: (x, y, z)}
    """
    if layout_type == 'hierarchical':
        return create_hierarchical_layout(G)
    elif layout_type == 'circular_3d':
        return create_circular_3d_layout(G)
    elif layout_type == 'spring':
        return nx.spring_layout(G)

def render_graph(G, pos):
    """Render graph using pre-computed positions"""
    # Draw nodes
    for node in G.nodes():
        x, y = pos[node]
        plt.scatter(x, y, ...)

    # Draw edges
    for u, v in G.edges():
        x_vals = [pos[u][0], pos[v][0]]
        y_vals = [pos[u][1], pos[v][1]]
        plt.plot(x_vals, y_vals, ...)
```

**Benefits**:
- Layouts are reusable across visualizations
- Easy to experiment with different layouts
- Clean separation: layout (where) vs. rendering (how)

### Pattern 4: Type-Based Styling

**Problem**: Need consistent visual mapping of entity/edge types

**Solution**: Centralize styling logic in mapping functions

```python
class EntityRelationshipGraph:
    def get_node_color(self, node_id: str) -> str:
        """Map entity type to color"""
        entity = self.entities[node_id]
        color_map = {
            EntityType.LEGACY: '#FFB6C1',   # Pink
            EntityType.HUBSPOT: '#87CEEB'   # Blue
        }
        return color_map[entity.entity_type]

    def get_edge_color(self, relationship_type: str) -> str:
        """Map relationship type to color"""
        color_map = {
            'association': '#666666',  # Gray
            'mapping': '#FF6347',      # Red
            'aggregation': '#4169E1'   # Blue
        }
        return color_map.get(relationship_type, '#000000')

    def get_edge_style(self, relationship_type: str) -> str:
        """Map relationship type to line style"""
        style_map = {
            'association': 'solid',
            'mapping': 'dashed',
            'aggregation': 'dotted'
        }
        return style_map.get(relationship_type, 'solid')
```

**Benefits**:
- Single source of truth for styling
- Easy to update visual theme
- Type-safe with enums

### Pattern 5: Modular Graph Construction

**Problem**: Building complex graphs with many entities/relationships

**Solution**: Separate construction into phases

```python
def build_entity_relationship_graph() -> EntityRelationshipGraph:
    """Build graph in phases"""

    # Phase 1: Initialize
    graph = EntityRelationshipGraph()

    # Phase 2: Add all entities
    graph.add_entities(LEGACY_ENTITIES)
    graph.add_entities(HUBSPOT_ENTITIES)

    # Phase 3: Add all relationships
    graph.add_relationships(LEGACY_RELATIONSHIPS)
    graph.add_relationships(HUBSPOT_RELATIONSHIPS)
    graph.add_relationships(MAPPING_RELATIONSHIPS)

    # Phase 4: Validate (optional)
    validate_graph(graph)

    return graph
```

**Benefits**:
- Clear construction sequence
- Easy to debug (phase-by-phase)
- Can insert validation between phases

---

## Complete Example: Building and Visualizing

### Full Workflow

```python
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
from dataclasses import dataclass
from typing import List, Dict

# 1. DEFINE DATA STRUCTURES
@dataclass
class Entity:
    name: str
    entity_type: str  # "legacy" or "hubspot"
    table_name: str
    primary_key: str

    def get_node_id(self) -> str:
        return f"{self.entity_type}_{self.name}"

@dataclass
class Relationship:
    from_entity: str
    to_entity: str
    cardinality: str
    relationship_type: str

# 2. CREATE GRAPH BUILDER
class EntityGraph:
    def __init__(self):
        self.G = nx.DiGraph()
        self.entities: Dict[str, Entity] = {}

    def add_entities(self, entities: List[Entity]):
        for entity in entities:
            node_id = entity.get_node_id()
            self.entities[node_id] = entity
            self.G.add_node(
                node_id,
                label=entity.name,
                entity_type=entity.entity_type,
                table_name=entity.table_name
            )

    def add_relationships(self, relationships: List[Relationship]):
        for rel in relationships:
            self.G.add_edge(
                rel.from_entity,
                rel.to_entity,
                cardinality=rel.cardinality,
                relationship_type=rel.relationship_type
            )

    def create_2d_layout(self):
        legacy_nodes = [n for n in self.G.nodes() if n.startswith('legacy_')]
        hubspot_nodes = [n for n in self.G.nodes() if n.startswith('hubspot_')]

        pos = {}
        for i, node in enumerate(legacy_nodes):
            pos[node] = (0, -i * 2)
        for i, node in enumerate(hubspot_nodes):
            pos[node] = (10, -i * 2)

        return pos

    def visualize(self, output_file='graph.png'):
        pos = self.create_2d_layout()

        plt.figure(figsize=(16, 12))

        # Draw nodes
        for node in self.G.nodes():
            color = '#FFB6C1' if node.startswith('legacy_') else '#87CEEB'
            nx.draw_networkx_nodes(
                self.G, pos, nodelist=[node],
                node_color=color, node_size=2000, node_shape='s'
            )
            nx.draw_networkx_labels(
                self.G, pos,
                labels={node: self.entities[node].name}
            )

        # Draw edges
        nx.draw_networkx_edges(self.G, pos, arrows=True)

        edge_labels = {
            (u, v): self.G[u][v]['cardinality']
            for u, v in self.G.edges()
        }
        nx.draw_networkx_edge_labels(self.G, pos, edge_labels)

        plt.axis('off')
        plt.tight_layout()
        plt.savefig(output_file, dpi=300)
        plt.close()

# 3. BUILD GRAPH
entities = [
    Entity("Company", "legacy", "Company", "Comp_CompanyId"),
    Entity("Person", "legacy", "Person", "Pers_PersonId"),
    Entity("companies", "hubspot", "hubspot.companies", "hs_object_id"),
    Entity("contacts", "hubspot", "hubspot.contacts", "hs_object_id"),
]

relationships = [
    Relationship("legacy_Person", "legacy_Company", "N:1", "association"),
    Relationship("legacy_Company", "hubspot_companies", "1:1", "mapping"),
    Relationship("legacy_Person", "hubspot_contacts", "1:1", "mapping"),
]

graph = EntityGraph()
graph.add_entities(entities)
graph.add_relationships(relationships)

# 4. VISUALIZE
graph.visualize('entity_graph.png')

# 5. EXPORT JSON
import json
graph_data = nx.node_link_data(graph.G)
with open('entity_graph.json', 'w') as f:
    json.dump(graph_data, f, indent=2)
```

---

## Summary

### Key Data Structures
- **Graph Object** (`G`): Container for nodes and edges
- **Nodes** (`G.nodes`): Dictionary of `{node_id: attributes}`
- **Edges** (`G.edges`): Dictionary of `{(u, v): attributes}`

### Key Algorithms
- **2D Layout**: Hierarchical column placement
- **3D Layout**: Circular pattern on separate Z-planes
- **Edge Filtering**: List comprehension with attribute matching
- **Coordinate Mapping**: Polar to Cartesian conversion

### Key Patterns
- **Dual Storage**: Entity objects + graph attributes
- **Filtered Rendering**: Process edge types separately
- **Position Abstraction**: Layout functions return position dictionaries
- **Type-Based Styling**: Centralized color/style mapping
- **Modular Construction**: Phase-by-phase graph building

### Implementation Checklist

- [ ] Define Entity and Relationship dataclasses
- [ ] Create DiGraph instance
- [ ] Implement add_entities() with attributes
- [ ] Implement add_relationships() with attributes
- [ ] Create layout algorithm (2D or 3D)
- [ ] Implement node rendering (color, size, shape)
- [ ] Implement edge rendering (color, style, width)
- [ ] Add labels (nodes and edges)
- [ ] Export to JSON
- [ ] Save visualization to file

---

**Next Steps**: See `GETTING_STARTED.md` for practical examples and tutorials.
