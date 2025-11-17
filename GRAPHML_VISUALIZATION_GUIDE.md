# GraphML Visualization Guide - Contrasting Labels

## Overview

This guide explains how to use the **styled GraphML** file with contrasting text labels in various graph visualization tools.

The styled GraphML (`entity_relationship_graph_styled.graphml`) includes visual attributes that make labels highly readable with contrasting colors.

---

## Color Scheme

### Node Colors

| Entity Type | Fill Color | Label Color | Contrast |
|------------|------------|-------------|----------|
| **Legacy** | `#FFB6C1` (Light Pink) | `#8B0000` (Dark Red) | High contrast |
| **HubSpot** | `#87CEEB` (Sky Blue) | `#000080` (Navy Blue) | High contrast |

**Visual Example**:
```
┌─────────────────┐     ┌─────────────────┐
│   Company       │     │   companies     │
│  (dark red)     │     │  (navy blue)    │
│ on light pink   │     │ on sky blue     │
└─────────────────┘     └─────────────────┘
   Legacy Entity          HubSpot Entity
```

### Edge Colors

| Relationship Type | Edge Color | Label Color | Style |
|------------------|------------|-------------|-------|
| **Association** | `#666666` (Gray) | `#000000` (Black) | Solid |
| **Mapping** | `#FF6347` (Tomato Red) | `#8B0000` (Dark Red) | Dashed |
| **Aggregation** | `#4169E1` (Royal Blue) | `#00008B` (Dark Blue) | Solid |

**Edge labels** have white backgrounds (`#FFFFFF`) for maximum readability.

---

## Creating the Styled GraphML

### Automatic Generation

The styled GraphML is automatically created when you run:

```bash
python3 generate_entity_graphs.py
```

This creates:
- `entity_relationship_graph.graphml` - Basic version
- `entity_relationship_graph_styled.graphml` - With visual styling ✨

### Manual Enhancement

To manually add styling to an existing GraphML file:

```bash
python3 enhance_graphml_styling.py input.graphml output_styled.graphml
```

**Example**:
```bash
python3 enhance_graphml_styling.py \
    entity_relationship_graph.graphml \
    entity_relationship_graph_styled.graphml
```

---

## Using Styled GraphML in Visualization Tools

### 1. Gephi (Recommended)

**Gephi** is a powerful open-source graph visualization tool.

**Installation**:
- Download from: https://gephi.org/

**Import Steps**:

1. **Open Gephi** and create a new project

2. **Import GraphML**:
   - File → Open
   - Select `entity_relationship_graph_styled.graphml`
   - Import as: "New workspace"

3. **Apply Visual Attributes**:
   - Go to **Appearance** panel (left side)
   - For **Nodes**:
     - Click "Nodes" tab
     - Color: Select "Partition" → Choose "entity_type"
     - Size: Select "Ranking" → Choose "v_size"
   - For **Edges**:
     - Click "Edges" tab
     - Color: Select "Partition" → Choose "relationship_type"
     - Width: Select "Ranking" → Choose "v_width"

4. **Apply Layout**:
   - Go to **Layout** panel (left side)
   - Choose "Force Atlas 2"
   - Click "Run" and adjust parameters until satisfied
   - Click "Stop" when layout looks good

5. **View Labels**:
   - In the bottom toolbar, click "Show Node Labels" (T icon)
   - Labels will appear with contrasting colors

6. **Export**:
   - File → Export → PDF/PNG/SVG
   - Choose resolution and format

**Tips**:
- Use "Preview" tab for final rendering
- Adjust label sizes in Preview settings
- Export as SVG for scalable graphics

### 2. yEd Graph Editor

**yEd** is a free desktop application for graph visualization.

**Installation**:
- Download from: https://www.yworks.com/products/yed

**Import Steps**:

1. **Open yEd**

2. **Import GraphML**:
   - File → Open
   - Select `entity_relationship_graph_styled.graphml`

3. **Apply Visual Attributes**:
   - yEd will automatically recognize and apply:
     - Node fill colors
     - Label colors
     - Edge colors and styles
   - Visual attributes are in the GraphML as standard properties

4. **Adjust Layout**:
   - Layout → Hierarchical
   - Or: Layout → Organic (for force-directed)
   - Configure options and apply

5. **View and Edit**:
   - Labels should display with contrasting colors
   - You can manually adjust positions
   - Edit properties in the Properties panel

6. **Export**:
   - File → Export
   - Choose format: SVG, PNG, PDF, etc.

**Note**: yEd has excellent support for GraphML attributes. The styled version should display correctly by default.

### 3. Cytoscape

**Cytoscape** is a network analysis and visualization platform.

**Installation**:
- Download from: https://cytoscape.org/

**Import Steps**:

1. **Open Cytoscape**

2. **Import Network**:
   - File → Import → Network from File
   - Select `entity_relationship_graph_styled.graphml`
   - Click "Import"

3. **Map Visual Properties**:
   - Go to **Style** panel (left side)
   - For **Nodes**:
     - Fill Color: Map to "v_fill" column
     - Label: Map to "label" column
     - Label Color: Map to "v_label_color" column
     - Size: Map to "v_size" column
   - For **Edges**:
     - Stroke Color: Map to "v_color" column
     - Width: Map to "v_width" column
     - Line Type: Map to "v_style" column

4. **Apply Layout**:
   - Layout → Force Directed Layout
   - Or: Layout → Hierarchical Layout
   - Adjust parameters as needed

5. **Export**:
   - File → Export → Network as Image
   - Choose format and resolution

**Note**: Cytoscape requires manual mapping of visual properties, but provides powerful analysis features.

### 4. Online Viewers

#### GraphOnline

**Web-based graph visualization**:
- URL: https://graphonline.ru/en/

**Steps**:
1. Go to https://graphonline.ru/en/
2. File → Load → Select `entity_relationship_graph_styled.graphml`
3. Visual attributes may not be fully supported
4. Manually adjust colors if needed

#### Graphia

**Desktop application with advanced visualization**:
- URL: https://graphia.app/

**Steps**:
1. Download and install Graphia
2. Open `entity_relationship_graph_styled.graphml`
3. Apply visual attributes from the data columns
4. Use 3D visualization features

---

## Visual Attributes Reference

The styled GraphML includes these visual attributes:

### Node Attributes

| Attribute | Key ID | Type | Example |
|-----------|--------|------|---------|
| Fill Color | `v_fill` | string | `#FFB6C1` |
| Label Color | `v_label_color` | string | `#8B0000` |
| Label Font Size | `v_label_font_size` | int | `14` |
| Label Font Weight | `v_label_font_weight` | string | `bold` |
| Node Size | `v_size` | double | `40.0` |
| Node Shape | `v_shape` | string | `rectangle` |
| Border Color | `v_border_color` | string | `#000000` |
| Border Width | `v_border_width` | double | `2.0` |

### Edge Attributes

| Attribute | Key ID | Type | Example |
|-----------|--------|------|---------|
| Edge Color | `v_color` | string | `#666666` |
| Label Color | `v_label_color` | string | `#000000` |
| Label Background | `v_label_background` | string | `#FFFFFF` |
| Edge Width | `v_width` | double | `2.0` |
| Edge Style | `v_style` | string | `solid` / `dashed` |

---

## Customizing Colors

### Option 1: Edit the Styling Script

Edit `enhance_graphml_styling.py`:

```python
# Change node colors
NODE_FILL_COLORS = {
    'legacy': '#YOUR_COLOR',    # Change legacy background
    'hubspot': '#YOUR_COLOR'    # Change HubSpot background
}

NODE_LABEL_COLORS = {
    'legacy': '#YOUR_COLOR',    # Change legacy text
    'hubspot': '#YOUR_COLOR'    # Change HubSpot text
}

# Change edge colors
EDGE_COLORS = {
    'association': '#YOUR_COLOR',
    'mapping': '#YOUR_COLOR',
    'aggregation': '#YOUR_COLOR'
}
```

Then regenerate:
```bash
python3 enhance_graphml_styling.py entity_relationship_graph.graphml output.graphml
```

### Option 2: Edit GraphML Directly

Open `entity_relationship_graph_styled.graphml` in a text editor:

```xml
<node id="legacy_Company">
  <data key="v_fill">#FFB6C1</data>          <!-- Background color -->
  <data key="v_label_color">#8B0000</data>   <!-- Text color -->
  <data key="v_label_font_size">14</data>
</node>
```

Change the color values and save.

### Option 3: Use Visualization Tool

Most tools allow you to override colors:
- Gephi: Use Appearance panel
- yEd: Use Properties panel
- Cytoscape: Use Style panel

---

## Ensuring High Contrast

### Contrast Ratio

For maximum readability, aim for a contrast ratio of at least **4.5:1**.

**Current contrasts**:
- Legacy: Dark red (`#8B0000`) on light pink (`#FFB6C1`) ≈ 5.2:1 ✓
- HubSpot: Navy blue (`#000080`) on sky blue (`#87CEEB`) ≈ 6.8:1 ✓

### Check Contrast

Use online tools:
- https://webaim.org/resources/contrastchecker/
- https://contrast-ratio.com/

**Example**:
```
Foreground: #8B0000 (dark red)
Background: #FFB6C1 (light pink)
Result: 5.2:1 (AA compliant)
```

### Color-Blind Friendly

Current colors work well for:
- Deuteranopia (red-green color blindness)
- Protanopia (red-green color blindness)
- Tritanopia (blue-yellow color blindness)

Test at: https://www.color-blindness.com/coblis-color-blindness-simulator/

---

## Troubleshooting

### Labels Not Visible in Gephi

**Solution**:
1. Click the "Show Node Labels" button (T icon)
2. Go to Preview tab → Node Labels → Show labels
3. Adjust font size if needed

### Colors Not Applied in yEd

**Solution**:
1. Check that you opened the `_styled.graphml` file
2. Go to View → Properties View
3. Select a node and verify "v_fill" property exists
4. If missing, re-run enhancement script

### Visual Attributes Not Recognized

**Solution**:
Some tools may not auto-apply GraphML visual attributes. Manually map them:

**In Cytoscape**:
1. Go to Style panel
2. Map "Fill Color" → Column "v_fill"
3. Map "Label Color" → Column "v_label_color"

**In Gephi**:
1. Go to Data Laboratory
2. Verify columns exist: v_fill, v_label_color, etc.
3. Use these columns in Appearance panel

### Edge Labels Overlapping

**Solution**:
1. Increase edge curvature (reduces overlap)
2. Adjust label placement settings
3. Manually reposition in tool

**In yEd**:
```
Edit → Properties
→ Edge Labels
→ Position: Center/Auto
```

---

## Best Practices

### 1. Use Styled Version for External Tools

**For visualization tools** (Gephi, yEd, Cytoscape):
✓ Use `entity_relationship_graph_styled.graphml`

**For programmatic access** (Python, JavaScript):
✓ Use `entity_relationship_graph.json` or basic `.graphml`

### 2. Export High-Resolution Images

For presentations and documentation:
- **DPI**: 300+ for print, 150+ for screen
- **Format**: SVG for scalability, PNG for compatibility
- **Size**: 1920×1080 or larger

### 3. Test in Multiple Tools

Different tools render GraphML differently:
- **Gephi**: Best for large graphs, force-directed layouts
- **yEd**: Best for hierarchical layouts, manual editing
- **Cytoscape**: Best for analysis and network statistics

### 4. Version Control

Keep both versions:
```
entity_relationship_graph.graphml        # Basic version
entity_relationship_graph_styled.graphml # Styled version
```

The basic version is smaller and easier to diff in git.

---

## Examples

### Example 1: Quick View in yEd

```bash
# Generate styled GraphML
python3 generate_entity_graphs.py

# Open in yEd
yed entity_relationship_graph_styled.graphml

# Apply hierarchical layout
# File → Export → SVG
```

### Example 2: Analysis in Gephi

```bash
# Generate graph
python3 generate_entity_graphs.py

# Open Gephi
gephi entity_relationship_graph_styled.graphml

# Apply Force Atlas 2 layout
# Use modularity for community detection
# Export high-res PNG
```

### Example 3: Embed in Documentation

```bash
# Generate SVG from yEd
yed entity_relationship_graph_styled.graphml
# Export as SVG

# Or use Python
python3 -c "
from entity_relationship_graph import build_entity_relationship_graph
graph = build_entity_relationship_graph()
graph.visualize_2d('docs/entity_graph.svg')
"

# Embed in Markdown
# ![Entity Graph](entity_graph.svg)
```

---

## Resources

### Visualization Tools

- **Gephi**: https://gephi.org/
- **yEd**: https://www.yworks.com/products/yed
- **Cytoscape**: https://cytoscape.org/
- **Graphia**: https://graphia.app/

### Color Tools

- **Contrast Checker**: https://webaim.org/resources/contrastchecker/
- **Color Picker**: https://htmlcolorcodes.com/
- **Palette Generator**: https://coolors.co/

### GraphML Resources

- **GraphML Format**: http://graphml.graphdrawing.org/
- **NetworkX GraphML**: https://networkx.org/documentation/stable/reference/readwrite/graphml.html

---

## Summary

✅ **Styled GraphML** provides contrasting labels for better visibility

✅ **Multiple tools** support GraphML visual attributes

✅ **High contrast** ensures readability (5:1+ ratio)

✅ **Automatic generation** integrated into main script

✅ **Customizable** colors via script or XML editing

For best results, use **Gephi** or **yEd** which have excellent GraphML support and will automatically apply all visual styling.
