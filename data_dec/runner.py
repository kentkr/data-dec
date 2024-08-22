
import argparse
import networkx as nx
from data_dec.compilation import DAG

class ProjectRunner:
    """Run the DAG"""
    def __init__(self, dag: DAG, args: argparse.Namespace) -> None:
        self.dag = dag
        self.args = args
        self.nodes = self.selected_nodes()

    def selected_nodes(self) -> list[str]:
        # sort nodes of graph, run them sequentially
        # we can make this parallelized eventually
        sorted_nodes = list(nx.topological_sort(self.dag.graph))
        selected_nodes = set(self.args.select)
        if selected_nodes:
            return [node for node in sorted_nodes if node in selected_nodes]
        return sorted_nodes

    def run(self) -> None:
        for node in self.nodes:
            # access model class of node
            self.dag.graph.nodes[node]['model'].write()
    
    # loop through each model, test it's output
    def test(self) -> None:
        for node in self.nodes:
            # access model class of node
            self.dag.graph.nodes[node]['model'].test()
    
    # loop through each model, write then test it
    def build(self) -> None:
        for node in self.nodes:
            # access model class of node
            self.dag.graph.nodes[node]['model'].write()
            self.dag.graph.nodes[node]['model'].test()

    def draw(self) -> None:
        self.dag.draw_graph(self.nodes)

