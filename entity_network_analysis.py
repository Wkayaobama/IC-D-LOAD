"""
Entity Network Analysis
=======================

Advanced network analysis of entity relationships, providing:
- Centrality metrics (which entities are most important/connected)
- Path analysis (how entities connect)
- Relationship complexity metrics
- Mapping coverage analysis

Uses NetworkX for graph algorithms and analysis.
"""

import networkx as nx
from typing import Dict, List, Tuple
from collections import defaultdict
from entity_relationship_graph import (
    build_entity_relationship_graph,
    EntityRelationshipGraph
)


# ============================================================================
# NETWORK ANALYSIS
# ============================================================================

class EntityNetworkAnalyzer:
    """Analyzes entity relationship networks"""

    def __init__(self, graph: EntityRelationshipGraph):
        self.graph = graph
        self.G = graph.G
        self.entities = graph.entities

    def compute_centrality_metrics(self) -> Dict[str, Dict[str, float]]:
        """
        Compute various centrality metrics for all entities.

        Returns:
            Dictionary of entity -> metric -> value
        """
        print("\n📊 Computing Centrality Metrics...")

        metrics = {}

        # Degree centrality (number of connections)
        degree_centrality = nx.degree_centrality(self.G)

        # In-degree centrality (how many entities depend on this one)
        in_degree_centrality = nx.in_degree_centrality(self.G)

        # Out-degree centrality (how many entities this one depends on)
        out_degree_centrality = nx.out_degree_centrality(self.G)

        # Betweenness centrality (how often entity appears on paths between others)
        betweenness_centrality = nx.betweenness_centrality(self.G)

        # Combine all metrics
        for node in self.G.nodes():
            metrics[node] = {
                'degree': degree_centrality[node],
                'in_degree': in_degree_centrality[node],
                'out_degree': out_degree_centrality[node],
                'betweenness': betweenness_centrality[node],
                'total_connections': self.G.degree(node)
            }

        return metrics

    def find_most_central_entities(self, top_n: int = 5) -> Dict[str, List[Tuple[str, float]]]:
        """
        Find the most central entities by different metrics.

        Args:
            top_n: Number of top entities to return

        Returns:
            Dictionary of metric -> list of (entity, score) tuples
        """
        metrics = self.compute_centrality_metrics()

        results = {}

        # Sort by each metric
        for metric_name in ['degree', 'in_degree', 'out_degree', 'betweenness']:
            sorted_entities = sorted(
                metrics.items(),
                key=lambda x: x[1][metric_name],
                reverse=True
            )[:top_n]

            results[metric_name] = [
                (entity, score[metric_name])
                for entity, score in sorted_entities
            ]

        return results

    def analyze_relationship_paths(self) -> Dict:
        """
        Analyze paths between legacy and HubSpot entities.

        Returns:
            Dictionary with path analysis results
        """
        print("\n🔍 Analyzing Relationship Paths...")

        legacy_nodes = [n for n in self.G.nodes() if n.startswith('legacy_')]
        hubspot_nodes = [n for n in self.G.nodes() if n.startswith('hubspot_')]

        # Find all paths from legacy to HubSpot
        mapping_paths = []

        for legacy_node in legacy_nodes:
            for hubspot_node in hubspot_nodes:
                try:
                    # Check if there's a direct path
                    if nx.has_path(self.G, legacy_node, hubspot_node):
                        shortest_path = nx.shortest_path(self.G, legacy_node, hubspot_node)
                        path_length = len(shortest_path) - 1  # Number of edges

                        mapping_paths.append({
                            'from': legacy_node,
                            'to': hubspot_node,
                            'path': shortest_path,
                            'length': path_length
                        })
                except nx.NetworkXNoPath:
                    pass

        # Analyze path characteristics
        direct_mappings = [p for p in mapping_paths if p['length'] == 1]
        indirect_mappings = [p for p in mapping_paths if p['length'] > 1]

        return {
            'total_paths': len(mapping_paths),
            'direct_mappings': len(direct_mappings),
            'indirect_mappings': len(indirect_mappings),
            'mapping_details': mapping_paths,
            'direct_mapping_list': direct_mappings
        }

    def analyze_mapping_coverage(self) -> Dict:
        """
        Analyze how well legacy entities are mapped to HubSpot.

        Returns:
            Dictionary with mapping coverage metrics
        """
        print("\n📋 Analyzing Mapping Coverage...")

        legacy_nodes = [n for n in self.G.nodes() if n.startswith('legacy_')]
        hubspot_nodes = [n for n in self.G.nodes() if n.startswith('hubspot_')]

        # Find which legacy entities have direct mappings
        legacy_mapped = set()
        hubspot_targets = set()

        for u, v, data in self.G.edges(data=True):
            if data.get('relationship_type') == 'mapping':
                if u.startswith('legacy_'):
                    legacy_mapped.add(u)
                    hubspot_targets.add(v)

        legacy_unmapped = set(legacy_nodes) - legacy_mapped

        return {
            'total_legacy': len(legacy_nodes),
            'total_hubspot': len(hubspot_nodes),
            'legacy_mapped': len(legacy_mapped),
            'legacy_unmapped': len(legacy_unmapped),
            'hubspot_used': len(hubspot_targets),
            'mapping_coverage': len(legacy_mapped) / len(legacy_nodes) * 100 if legacy_nodes else 0,
            'unmapped_entities': [self.entities[n].name for n in legacy_unmapped]
        }

    def analyze_relationship_complexity(self) -> Dict:
        """
        Analyze the complexity of relationships in each system.

        Returns:
            Dictionary with complexity metrics
        """
        print("\n🧮 Analyzing Relationship Complexity...")

        # Separate legacy and HubSpot subgraphs
        legacy_nodes = [n for n in self.G.nodes() if n.startswith('legacy_')]
        hubspot_nodes = [n for n in self.G.nodes() if n.startswith('hubspot_')]

        # Count relationships by cardinality type
        cardinality_counts = defaultdict(int)
        relationship_type_counts = defaultdict(int)

        for u, v, data in self.G.edges(data=True):
            cardinality_counts[data.get('cardinality', 'unknown')] += 1
            relationship_type_counts[data.get('relationship_type', 'unknown')] += 1

        # Analyze by system
        legacy_edges = [
            (u, v, d) for u, v, d in self.G.edges(data=True)
            if u.startswith('legacy_') and v.startswith('legacy_')
        ]

        hubspot_edges = [
            (u, v, d) for u, v, d in self.G.edges(data=True)
            if u.startswith('hubspot_') and v.startswith('hubspot_')
        ]

        mapping_edges = [
            (u, v, d) for u, v, d in self.G.edges(data=True)
            if d.get('relationship_type') == 'mapping'
        ]

        # Complexity score (higher = more complex)
        # Based on: number of entities, relationships, and many-to-many relationships
        many_to_many = sum(1 for _, _, d in self.G.edges(data=True) if 'N:N' in d.get('cardinality', ''))
        optional_relationships = sum(1 for _, _, d in self.G.edges(data=True) if '0..' in d.get('cardinality', ''))

        complexity_score = (
            len(self.G.nodes()) * 0.3 +
            len(self.G.edges()) * 0.4 +
            many_to_many * 2.0 +
            optional_relationships * 0.5
        )

        return {
            'total_entities': len(self.G.nodes()),
            'total_relationships': len(self.G.edges()),
            'legacy_internal_relationships': len(legacy_edges),
            'hubspot_internal_relationships': len(hubspot_edges),
            'cross_system_mappings': len(mapping_edges),
            'cardinality_distribution': dict(cardinality_counts),
            'relationship_type_distribution': dict(relationship_type_counts),
            'many_to_many_count': many_to_many,
            'optional_relationship_count': optional_relationships,
            'complexity_score': complexity_score
        }

    def generate_report(self) -> str:
        """
        Generate a comprehensive analysis report.

        Returns:
            Formatted report string
        """
        report_lines = []

        report_lines.append("\n" + "=" * 80)
        report_lines.append("ENTITY NETWORK ANALYSIS REPORT")
        report_lines.append("=" * 80)

        # 1. Centrality Analysis
        report_lines.append("\n" + "─" * 80)
        report_lines.append("1. CENTRALITY ANALYSIS")
        report_lines.append("─" * 80)

        central_entities = self.find_most_central_entities(top_n=5)

        report_lines.append("\n🎯 Most Connected Entities (Degree Centrality):")
        for entity, score in central_entities['degree']:
            entity_name = self.entities[entity].name
            report_lines.append(f"  • {entity_name}: {score:.3f}")

        report_lines.append("\n📥 Most Depended Upon (In-Degree Centrality):")
        for entity, score in central_entities['in_degree']:
            entity_name = self.entities[entity].name
            report_lines.append(f"  • {entity_name}: {score:.3f}")

        report_lines.append("\n📤 Most Dependencies (Out-Degree Centrality):")
        for entity, score in central_entities['out_degree']:
            entity_name = self.entities[entity].name
            report_lines.append(f"  • {entity_name}: {score:.3f}")

        report_lines.append("\n🌉 Bridge Entities (Betweenness Centrality):")
        for entity, score in central_entities['betweenness']:
            entity_name = self.entities[entity].name
            report_lines.append(f"  • {entity_name}: {score:.3f}")

        # 2. Path Analysis
        report_lines.append("\n" + "─" * 80)
        report_lines.append("2. RELATIONSHIP PATH ANALYSIS")
        report_lines.append("─" * 80)

        path_analysis = self.analyze_relationship_paths()

        report_lines.append(f"\n📊 Path Statistics:")
        report_lines.append(f"  Total Paths (Legacy → HubSpot): {path_analysis['total_paths']}")
        report_lines.append(f"  Direct Mappings: {path_analysis['direct_mappings']}")
        report_lines.append(f"  Indirect Paths: {path_analysis['indirect_mappings']}")

        report_lines.append(f"\n🔗 Direct Mapping Relationships:")
        for mapping in path_analysis['direct_mapping_list']:
            from_entity = self.entities[mapping['from']].name
            to_entity = self.entities[mapping['to']].name
            report_lines.append(f"  • {from_entity} → {to_entity}")

        # 3. Mapping Coverage
        report_lines.append("\n" + "─" * 80)
        report_lines.append("3. MAPPING COVERAGE ANALYSIS")
        report_lines.append("─" * 80)

        coverage = self.analyze_mapping_coverage()

        report_lines.append(f"\n📈 Coverage Metrics:")
        report_lines.append(f"  Total Legacy Entities: {coverage['total_legacy']}")
        report_lines.append(f"  Mapped to HubSpot: {coverage['legacy_mapped']}")
        report_lines.append(f"  Not Mapped: {coverage['legacy_unmapped']}")
        report_lines.append(f"  Coverage Rate: {coverage['mapping_coverage']:.1f}%")

        if coverage['unmapped_entities']:
            report_lines.append(f"\n⚠️  Unmapped Legacy Entities:")
            for entity in coverage['unmapped_entities']:
                report_lines.append(f"  • {entity}")
        else:
            report_lines.append(f"\n✅ All legacy entities are mapped!")

        # 4. Complexity Analysis
        report_lines.append("\n" + "─" * 80)
        report_lines.append("4. RELATIONSHIP COMPLEXITY ANALYSIS")
        report_lines.append("─" * 80)

        complexity = self.analyze_relationship_complexity()

        report_lines.append(f"\n📊 System Overview:")
        report_lines.append(f"  Total Entities: {complexity['total_entities']}")
        report_lines.append(f"  Total Relationships: {complexity['total_relationships']}")
        report_lines.append(f"  Legacy Internal: {complexity['legacy_internal_relationships']}")
        report_lines.append(f"  HubSpot Internal: {complexity['hubspot_internal_relationships']}")
        report_lines.append(f"  Cross-System Mappings: {complexity['cross_system_mappings']}")

        report_lines.append(f"\n🔢 Cardinality Distribution:")
        for cardinality, count in sorted(complexity['cardinality_distribution'].items()):
            report_lines.append(f"  • {cardinality}: {count} relationships")

        report_lines.append(f"\n📌 Relationship Type Distribution:")
        for rel_type, count in sorted(complexity['relationship_type_distribution'].items()):
            report_lines.append(f"  • {rel_type.capitalize()}: {count} relationships")

        report_lines.append(f"\n⚡ Complexity Metrics:")
        report_lines.append(f"  Many-to-Many Relationships: {complexity['many_to_many_count']}")
        report_lines.append(f"  Optional Relationships: {complexity['optional_relationship_count']}")
        report_lines.append(f"  Complexity Score: {complexity['complexity_score']:.2f}")

        # Complexity interpretation
        score = complexity['complexity_score']
        if score < 20:
            interpretation = "Low - Simple entity structure"
        elif score < 50:
            interpretation = "Medium - Moderate complexity"
        elif score < 100:
            interpretation = "High - Complex entity structure"
        else:
            interpretation = "Very High - Highly complex entity structure"

        report_lines.append(f"  Interpretation: {interpretation}")

        report_lines.append("\n" + "=" * 80 + "\n")

        return "\n".join(report_lines)

    def export_metrics_to_csv(self, output_file: str = "entity_network_metrics.csv"):
        """Export centrality metrics to CSV"""
        import csv

        metrics = self.compute_centrality_metrics()

        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Entity', 'System', 'Degree Centrality', 'In-Degree',
                'Out-Degree', 'Betweenness', 'Total Connections'
            ])

            for node, scores in metrics.items():
                entity = self.entities[node]
                writer.writerow([
                    entity.name,
                    entity.entity_type.value,
                    scores['degree'],
                    scores['in_degree'],
                    scores['out_degree'],
                    scores['betweenness'],
                    scores['total_connections']
                ])

        print(f"  ✓ Metrics exported to: {output_file}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution"""

    # Build graph
    print("\n🔨 Building Entity Relationship Graph...")
    graph = build_entity_relationship_graph()

    # Create analyzer
    print("\n🔬 Initializing Network Analyzer...")
    analyzer = EntityNetworkAnalyzer(graph)

    # Generate report
    report = analyzer.generate_report()
    print(report)

    # Save report to file
    with open("entity_network_analysis_report.txt", "w") as f:
        f.write(report)
    print("💾 Report saved to: entity_network_analysis_report.txt")

    # Export metrics
    print("\n💾 Exporting metrics...")
    analyzer.export_metrics_to_csv()

    print("\n" + "=" * 80)
    print("✨ Network Analysis Complete!")
    print("=" * 80)
    print("\nGenerated files:")
    print("  1. entity_network_analysis_report.txt - Comprehensive analysis report")
    print("  2. entity_network_metrics.csv - Centrality metrics")
    print("\n")


if __name__ == "__main__":
    main()
