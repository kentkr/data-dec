
import networkx as nx
from data_dec.compilation import DAG

class ProjectRunner:
    """Run the DAG"""
    def __init__(self, dag: DAG) -> None:
        self.dag = dag

    def run(self) -> None:
        # sort nodes of graph, run them sequentially
        # we can make this parallelized eventually
        sorted_nodes = nx.topological_sort(self.dag.graph)
        for node in sorted_nodes:
            # access model class of node
            self.dag.graph.nodes[node]['model'].write()
    
    # loop through each model, test it's output
    def test(self) -> None:
        sorted_nodes = nx.topological_sort(self.dag.graph)
        for node in sorted_nodes:
            # access model class of node
            self.dag.graph.nodes[node]['model'].test()
    
    # loop through each model, write then test it
    def build(self) -> None:
        sorted_nodes = nx.topological_sort(self.dag.graph)
        for node in sorted_nodes:
            # access model class of node
            self.dag.graph.nodes[node]['model'].write()
            self.dag.graph.nodes[node]['model'].test()

    def draw(self) -> None:
        self.dag.draw_graph()

