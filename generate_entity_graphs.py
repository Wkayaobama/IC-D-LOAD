#!/usr/bin/env python3
"""
Entity Relationship Graph Generator - Master Script
====================================================

This script orchestrates the complete entity relationship graph generation,
including:
- Building the graph structure
- Running network analysis
- Generating visualizations (2D, 3D, interactive)
- Exporting data in multiple formats

Usage:
    python generate_entity_graphs.py [--analysis-only] [--viz-only]

Options:
    --analysis-only    Only run network analysis (skip visualizations)
    --viz-only        Only generate visualizations (skip analysis)
    --interactive     Generate interactive HTML visualization
"""

import sys
import argparse
from pathlib import Path
from entity_relationship_graph import build_entity_relationship_graph
from entity_network_analysis import EntityNetworkAnalyzer


# ============================================================================
# INTERACTIVE VISUALIZATION
# ============================================================================

def generate_interactive_html(graph, output_file: str = "entity_relationship_graph_interactive.html"):
    """
    Generate an interactive HTML visualization using vis.js or similar.

    Args:
        graph: EntityRelationshipGraph instance
        output_file: Output HTML filename
    """
    try:
        from pyvis.network import Network
    except ImportError:
        print("⚠  Interactive visualization requires pyvis: pip install pyvis")
        return

    print("\n🌐 Generating interactive HTML visualization...")

    # Create pyvis network
    net = Network(
        height='900px',
        width='100%',
        bgcolor='#ffffff',
        font_color='#000000',
        directed=True
    )

    # Set physics layout
    net.set_options("""
    {
        "physics": {
            "enabled": true,
            "forceAtlas2Based": {
                "gravitationalConstant": -50,
                "centralGravity": 0.01,
                "springLength": 200,
                "springConstant": 0.08
            },
            "maxVelocity": 50,
            "solver": "forceAtlas2Based",
            "timestep": 0.35,
            "stabilization": {"iterations": 150}
        },
        "nodes": {
            "font": {"size": 16},
            "borderWidth": 2
        },
        "edges": {
            "font": {"size": 12, "align": "middle"},
            "arrows": {"to": {"enabled": true, "scaleFactor": 0.5}}
        }
    }
    """)

    # Add nodes
    for node_id, entity in graph.entities.items():
        # Determine color
        if entity.entity_type.value == 'legacy':
            color = '#FFB6C1'  # Pink
            group = 'legacy'
        else:
            color = '#87CEEB'  # Sky blue
            group = 'hubspot'

        # Create hover title
        title = f"""
        <b>{entity.name}</b><br>
        System: {entity.entity_type.value}<br>
        Table: {entity.table_name}<br>
        Primary Key: {entity.primary_key}
        """

        net.add_node(
            node_id,
            label=entity.name,
            title=title,
            color=color,
            group=group,
            shape='box'
        )

    # Add edges
    for u, v, data in graph.G.edges(data=True):
        cardinality = data.get('cardinality', '')
        rel_type = data.get('relationship_type', 'association')
        description = data.get('description', '')

        # Determine edge color and style
        if rel_type == 'mapping':
            color = '#FF6347'  # Red for mappings
            dashes = True
            width = 3
        elif rel_type == 'association':
            color = '#666666'  # Gray for associations
            dashes = False
            width = 2
        else:
            color = '#4169E1'  # Blue
            dashes = True
            width = 2

        # Create hover title
        title = f"{description}<br>Cardinality: {cardinality}<br>Type: {rel_type}"

        net.add_edge(
            u, v,
            label=cardinality,
            title=title,
            color=color,
            width=width,
            dashes=dashes
        )

    # Save HTML
    net.save_graph(output_file)
    print(f"  ✓ Interactive visualization saved to: {output_file}")


# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def main():
    """Main orchestration"""

    parser = argparse.ArgumentParser(
        description='Generate entity relationship graphs and analysis'
    )
    parser.add_argument(
        '--analysis-only',
        action='store_true',
        help='Only run network analysis (skip visualizations)'
    )
    parser.add_argument(
        '--viz-only',
        action='store_true',
        help='Only generate visualizations (skip analysis)'
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Generate interactive HTML visualization'
    )

    args = parser.parse_args()

    print("\n" + "=" * 80)
    print("ENTITY RELATIONSHIP GRAPH GENERATOR")
    print("=" * 80)

    # Build graph (always needed)
    print("\n🔨 Building Entity Relationship Graph...")
    graph = build_entity_relationship_graph()
    graph.print_summary()

    # Run analysis if requested
    if not args.viz_only:
        print("\n" + "=" * 80)
        print("RUNNING NETWORK ANALYSIS")
        print("=" * 80)

        analyzer = EntityNetworkAnalyzer(graph)
        report = analyzer.generate_report()
        print(report)

        # Save report
        with open("entity_network_analysis_report.txt", "w") as f:
            f.write(report)
        print("💾 Report saved to: entity_network_analysis_report.txt")

        # Export metrics
        analyzer.export_metrics_to_csv()

    # Generate visualizations if requested
    if not args.analysis_only:
        print("\n" + "=" * 80)
        print("GENERATING VISUALIZATIONS")
        print("=" * 80)

        print("\n🎨 Creating 2D visualization...")
        graph.visualize_2d("entity_relationship_graph_2d.png")

        print("\n🎨 Creating 3D visualization...")
        graph.visualize_3d("entity_relationship_graph_3d.png")

        if args.interactive:
            generate_interactive_html(graph)

        # Export graph data
        print("\n💾 Exporting graph data...")
        import networkx as nx
        import json

        nx.write_graphml(graph.G, "entity_relationship_graph.graphml")
        print("  ✓ GraphML export: entity_relationship_graph.graphml")

        graph_data = nx.node_link_data(graph.G)
        with open("entity_relationship_graph.json", "w") as f:
            json.dump(graph_data, f, indent=2)
        print("  ✓ JSON export: entity_relationship_graph.json")

    # Summary
    print("\n" + "=" * 80)
    print("✨ GENERATION COMPLETE!")
    print("=" * 80)

    print("\n📁 Generated files:")
    if not args.viz_only:
        print("  Analysis:")
        print("    • entity_network_analysis_report.txt")
        print("    • entity_network_metrics.csv")

    if not args.analysis_only:
        print("  Visualizations:")
        print("    • entity_relationship_graph_2d.png")
        print("    • entity_relationship_graph_3d.png")
        if args.interactive:
            print("    • entity_relationship_graph_interactive.html")
        print("  Data Exports:")
        print("    • entity_relationship_graph.graphml")
        print("    • entity_relationship_graph.json")

    print("\n💡 Tips:")
    print("  • Open .png files to view static visualizations")
    if args.interactive:
        print("  • Open .html file in browser for interactive exploration")
    print("  • Import .graphml into Gephi or Cytoscape for advanced analysis")
    print("  • Use .json for custom processing")
    print("\n")


if __name__ == "__main__":
    main()
