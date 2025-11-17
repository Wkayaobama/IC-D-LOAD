#!/usr/bin/env python3
"""
Enhance GraphML with Visual Styling
====================================

Adds contrasting colors and styling attributes to GraphML files for
better visualization in tools like Gephi, yEd, and Cytoscape.

Features:
- Node fill colors (background)
- Contrasting label colors (text)
- Font sizes and weights
- Edge colors and styles
- Label backgrounds for readability

Usage:
    python3 enhance_graphml_styling.py [input.graphml] [output.graphml]
"""

import xml.etree.ElementTree as ET
import argparse
from pathlib import Path


# ============================================================================
# COLOR SCHEMES
# ============================================================================

# Node fill colors by entity type
NODE_FILL_COLORS = {
    'legacy': '#FFB6C1',    # Light pink
    'hubspot': '#87CEEB'    # Sky blue
}

# Contrasting text colors for labels
NODE_LABEL_COLORS = {
    'legacy': '#8B0000',    # Dark red (high contrast on pink)
    'hubspot': '#000080'    # Navy blue (high contrast on sky blue)
}

# Edge colors by relationship type
EDGE_COLORS = {
    'association': '#666666',  # Gray
    'mapping': '#FF6347',      # Tomato red
    'aggregation': '#4169E1'   # Royal blue
}

# Edge label colors (contrasting on white background)
EDGE_LABEL_COLORS = {
    'association': '#000000',  # Black
    'mapping': '#8B0000',      # Dark red
    'aggregation': '#00008B'   # Dark blue
}


# ============================================================================
# GRAPHML ENHANCEMENT
# ============================================================================

class GraphMLStyler:
    """Enhance GraphML with visual styling attributes"""

    def __init__(self, input_file: str):
        """Load GraphML file"""
        self.tree = ET.parse(input_file)
        self.root = self.tree.getroot()

        # GraphML namespace
        self.ns = {'graphml': 'http://graphml.graphdrawing.org/xmlns'}

        # Register namespace for writing
        ET.register_namespace('', 'http://graphml.graphdrawing.org/xmlns')
        ET.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')

    def add_visual_attribute_keys(self):
        """
        Add GraphML keys for visual attributes.

        These keys define what visual properties can be attached to nodes/edges.
        """
        # Find existing keys to avoid duplicates
        existing_keys = {
            key.get('id'): key
            for key in self.root.findall('graphml:key', self.ns)
        }

        # Visual attribute definitions
        visual_keys = [
            # Node visual attributes
            ('v_fill', 'node', 'string', 'Fill Color'),
            ('v_label_color', 'node', 'string', 'Label Color'),
            ('v_label_font_size', 'node', 'int', 'Label Font Size'),
            ('v_label_font_weight', 'node', 'string', 'Label Font Weight'),
            ('v_size', 'node', 'double', 'Node Size'),
            ('v_shape', 'node', 'string', 'Node Shape'),
            ('v_border_color', 'node', 'string', 'Border Color'),
            ('v_border_width', 'node', 'double', 'Border Width'),

            # Edge visual attributes
            ('v_color', 'edge', 'string', 'Edge Color'),
            ('v_label_color', 'edge', 'string', 'Edge Label Color'),
            ('v_label_background', 'edge', 'string', 'Edge Label Background'),
            ('v_width', 'edge', 'double', 'Edge Width'),
            ('v_style', 'edge', 'string', 'Edge Style'),
        ]

        # Add keys that don't already exist
        graph_element = self.root.find('graphml:graph', self.ns)

        for key_id, key_for, key_type, key_name in visual_keys:
            if key_id not in existing_keys:
                key_elem = ET.Element('key', {
                    'id': key_id,
                    'for': key_for,
                    'attr.name': key_name,
                    'attr.type': key_type
                })
                # Insert before graph element
                self.root.insert(list(self.root).index(graph_element), key_elem)

    def style_nodes(self):
        """Add visual styling to nodes"""
        graph = self.root.find('graphml:graph', self.ns)

        for node in graph.findall('graphml:node', self.ns):
            node_id = node.get('id')

            # Get entity type
            entity_type_elem = node.find("graphml:data[@key='d1']", self.ns)
            entity_type = entity_type_elem.text if entity_type_elem is not None else 'legacy'

            # Get label
            label_elem = node.find("graphml:data[@key='d0']", self.ns)
            label = label_elem.text if label_elem is not None else node_id

            # Determine colors
            fill_color = NODE_FILL_COLORS.get(entity_type, '#CCCCCC')
            label_color = NODE_LABEL_COLORS.get(entity_type, '#000000')

            # Add visual attributes
            visual_attrs = [
                ('v_fill', fill_color),
                ('v_label_color', label_color),
                ('v_label_font_size', '14'),
                ('v_label_font_weight', 'bold'),
                ('v_size', '40.0'),
                ('v_shape', 'rectangle'),
                ('v_border_color', '#000000'),
                ('v_border_width', '2.0'),
            ]

            for key, value in visual_attrs:
                data_elem = ET.SubElement(node, 'data', {'key': key})
                data_elem.text = value

    def style_edges(self):
        """Add visual styling to edges"""
        graph = self.root.find('graphml:graph', self.ns)

        for edge in graph.findall('graphml:edge', self.ns):
            # Get relationship type
            rel_type_elem = edge.find("graphml:data[@key='d5']", self.ns)
            rel_type = rel_type_elem.text if rel_type_elem is not None else 'association'

            # Determine colors and styles
            edge_color = EDGE_COLORS.get(rel_type, '#666666')
            label_color = EDGE_LABEL_COLORS.get(rel_type, '#000000')
            edge_width = '3.0' if rel_type == 'mapping' else '2.0'
            edge_style = 'dashed' if rel_type == 'mapping' else 'solid'

            # Add visual attributes
            visual_attrs = [
                ('v_color', edge_color),
                ('v_label_color', label_color),
                ('v_label_background', '#FFFFFF'),  # White background for readability
                ('v_width', edge_width),
                ('v_style', edge_style),
            ]

            for key, value in visual_attrs:
                data_elem = ET.SubElement(edge, 'data', {'key': key})
                data_elem.text = value

    def add_yworks_extensions(self):
        """
        Add yEd-specific visual attributes for better compatibility.

        yEd uses a different format for visual attributes.
        """
        graph = self.root.find('graphml:graph', self.ns)

        # Check if we should add yWorks extensions
        # This is more complex and requires yWorks-specific namespace
        # For now, we'll stick with standard GraphML attributes

    def save(self, output_file: str):
        """Save enhanced GraphML"""
        # Pretty print the XML
        self._indent(self.root)

        # Write to file
        self.tree.write(
            output_file,
            encoding='utf-8',
            xml_declaration=True
        )

        print(f"✓ Enhanced GraphML saved to: {output_file}")

    def _indent(self, elem, level=0):
        """Add pretty-printing indentation to XML"""
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for child in elem:
                self._indent(child, level + 1)
            if not child.tail or not child.tail.strip():
                child.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i


# ============================================================================
# ALTERNATIVE: CREATE STYLED GRAPHML FROM SCRATCH
# ============================================================================

def create_styled_graphml_from_networkx(graph, output_file: str):
    """
    Create a fully styled GraphML directly from NetworkX graph.

    This is an alternative to enhancing existing GraphML.
    """
    import networkx as nx

    # Create GraphML with custom writer
    for node, attrs in graph.G.nodes(data=True):
        entity = graph.entities[node]
        entity_type = entity.entity_type.value

        # Add visual attributes to node
        attrs['v_fill'] = NODE_FILL_COLORS.get(entity_type, '#CCCCCC')
        attrs['v_label_color'] = NODE_LABEL_COLORS.get(entity_type, '#000000')
        attrs['v_label_font_size'] = 14
        attrs['v_label_font_weight'] = 'bold'
        attrs['v_size'] = 40.0
        attrs['v_shape'] = 'rectangle'
        attrs['v_border_color'] = '#000000'
        attrs['v_border_width'] = 2.0

    for u, v, attrs in graph.G.edges(data=True):
        rel_type = attrs.get('relationship_type', 'association')

        # Add visual attributes to edge
        attrs['v_color'] = EDGE_COLORS.get(rel_type, '#666666')
        attrs['v_label_color'] = EDGE_LABEL_COLORS.get(rel_type, '#000000')
        attrs['v_label_background'] = '#FFFFFF'
        attrs['v_width'] = 3.0 if rel_type == 'mapping' else 2.0
        attrs['v_style'] = 'dashed' if rel_type == 'mapping' else 'solid'

    # Write GraphML with visual attributes
    nx.write_graphml(graph.G, output_file)
    print(f"✓ Styled GraphML created: {output_file}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description='Enhance GraphML with visual styling for better label visibility'
    )
    parser.add_argument(
        'input',
        nargs='?',
        default='entity_relationship_graph.graphml',
        help='Input GraphML file (default: entity_relationship_graph.graphml)'
    )
    parser.add_argument(
        'output',
        nargs='?',
        default='entity_relationship_graph_styled.graphml',
        help='Output GraphML file (default: entity_relationship_graph_styled.graphml)'
    )

    args = parser.parse_args()

    # Check if input exists
    if not Path(args.input).exists():
        print(f"❌ Error: Input file '{args.input}' not found")
        return 1

    print("\n" + "=" * 80)
    print("GRAPHML STYLING ENHANCEMENT")
    print("=" * 80)

    print(f"\n📥 Input:  {args.input}")
    print(f"📤 Output: {args.output}")

    # Load and enhance
    print("\n🎨 Applying visual styling...")
    styler = GraphMLStyler(args.input)

    print("  • Adding visual attribute keys...")
    styler.add_visual_attribute_keys()

    print("  • Styling nodes with contrasting colors...")
    styler.style_nodes()

    print("  • Styling edges with type-specific colors...")
    styler.style_edges()

    print("\n💾 Saving enhanced GraphML...")
    styler.save(args.output)

    # Print summary
    print("\n" + "=" * 80)
    print("VISUAL STYLING APPLIED")
    print("=" * 80)

    print("\n📊 Node Styling:")
    print("  Legacy Entities:")
    print(f"    Fill Color:  {NODE_FILL_COLORS['legacy']} (Light Pink)")
    print(f"    Label Color: {NODE_LABEL_COLORS['legacy']} (Dark Red)")
    print("  HubSpot Entities:")
    print(f"    Fill Color:  {NODE_FILL_COLORS['hubspot']} (Sky Blue)")
    print(f"    Label Color: {NODE_LABEL_COLORS['hubspot']} (Navy Blue)")

    print("\n🔗 Edge Styling:")
    for rel_type, color in EDGE_COLORS.items():
        label_color = EDGE_LABEL_COLORS[rel_type]
        print(f"  {rel_type.capitalize()}:")
        print(f"    Edge Color:  {color}")
        print(f"    Label Color: {label_color}")

    print("\n💡 Next Steps:")
    print("  1. Open in Gephi, yEd, or Cytoscape")
    print("  2. Import the styled GraphML file")
    print("  3. Visual attributes will be automatically applied")
    print("  4. Labels will have contrasting colors for readability")

    print("\n✨ Enhancement complete!")
    print("=" * 80 + "\n")

    return 0


if __name__ == "__main__":
    exit(main())
