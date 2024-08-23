
import argparse
from typing import Callable
import networkx as nx
from data_dec.compilation import DAG
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    def multi_thread(self, method: str) -> None:
        _method = getattr(self, method)
        futures = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            for node in self.nodes:
                # Ensure all dependencies are done
                parent_futures = [futures[parent] for parent in self.dag.graph.predecessors(node) if parent in futures]
                # If there are parent futures, wait for them to complete before processing the current node
                if parent_futures:
                    for parent_future in as_completed(parent_futures):
                        pass  # Simply waiting for all parent futures to complete
                # Submit the node's task to the executor
                future = executor.submit(_method, node)
                futures[node] = future
            # Optionally, wait for all tasks to complete
            for future in as_completed(futures.values()):
                future.result()  # Raise exception if any occurred


    def _run(self, node: str) -> None:
        self.dag.graph.nodes[node]['model'].write()
    
    # loop through each model, test it's output
    def _test(self, node: str) -> None:
        # access model class of node
        model = self.dag.graph.nodes[node]['model']
        for test in model.tests:
            print(f'Testing model {model.name!r}, test {test.name}, args {test.kwargs}')
            print(test.fn(model, **test.kwargs))
    
    # loop through each model, write then test it
    def _build(self, node: str) -> None:
        self.dag.graph.nodes[node]['model'].write()
        self.dag.graph.nodes[node]['model'].test()

    def draw(self) -> None:
        self.dag.draw_graph(self.nodes)

