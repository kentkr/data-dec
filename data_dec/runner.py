
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
                future = executor.submit(self.dag.graph.nodes[node]['model'].write)
                futures[node] = future
            # Optionally, wait for all tasks to complete
            for future in as_completed(futures.values()):
                future.result()  # Raise exception if any occurred


    def parents_complete(self, node: str, futures: dict) -> bool:
        subgraph = self.dag.graph.subgraph(self.nodes)
        parents = list(subgraph.predecessors(node))
        done = []
        for parent in parents:
            future = futures[parent]
            done.append(future.done())
        return all(done) 

    def get_new_successors(self, node: str, queue: list[str]) -> list[str]:
        subgraph = self.dag.graph.subgraph(self.nodes)
        successors = []
        for successor in subgraph.successors(node):
            if successor not in queue:
                successors.append(successor)
        return successors

    def get_starter_nodes(self) -> list[str]:
        subgraph = self.dag.graph.subgraph(self.nodes)
        return [node for node in subgraph.nodes if subgraph.in_degree(node) == 0]

    def run(self) -> None:
        queue = self.get_starter_nodes()
        futures = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            while queue:
                for node in queue:
                    if self.parents_complete(node, futures):
                        model = self.dag.graph.nodes[node]['model']
                        future = executor.submit(model.write)    
                        futures[node] = future
                        queue.remove(node)
                        queue += self.get_new_successors(node, queue)
    
    # loop through each model, test it's output
    def test(self) -> None:
        with ThreadPoolExecutor(max_workers=4) as executor:
            for node in self.nodes:
                model = self.dag.graph.nodes[node]['model']
                for test in model.tests:
                    executor.submit(test, model)
    
    # loop through each model, write then test it
    def build(self) -> None:
        queue = self.get_starter_nodes()
        test_queue = []
        futures = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            while queue:
                for node in queue:
                    if self.parents_complete(node, futures):
                        model = self.dag.graph.nodes[node]['model']
                        future = executor.submit(model.write)    
                        futures[node] = future
                        queue.remove(node)
                        queue += self.get_new_successors(node, queue)
                        test_queue.append(node)
                for node in test_queue:
                    future = futures[node]
                    if future.done():
                        model = self.dag.graph.nodes[node]['model']
                        for test in model.tests:
                            executor.submit(test, model)
                        test_queue.remove(node)

    def draw(self) -> None:
        self.dag.draw_graph(self.nodes)

