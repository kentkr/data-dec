
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
        in_progress = []
        futures = {}
        # create threadpool 
        with ThreadPoolExecutor(max_workers=4) as executor:
            while queue or in_progress:
                for node in queue:
                    # if node parents complete, submit and remove from queue
                    if self.parents_complete(node, futures):
                        model = self.dag.graph.nodes[node]['model']
                        future = executor.submit(model.write)    
                        futures[node] = future
                        queue.remove(node)
                        in_progress.append(node)
                        queue += self.get_new_successors(node, queue)
                # check result (and exception) of finished nodes
                for node in in_progress:
                    future = futures[node]
                    if future.done():
                        future.result()
                        in_progress.remove(node)
    
    # loop through each model, test it's output
    def test(self) -> None:
        with ThreadPoolExecutor(max_workers=4) as executor:
            # run all test nodes at once (limited by max workers)
            for node in self.nodes:
                model = self.dag.graph.nodes[node]['model']
                for test in model.tests:
                    executor.submit(test, model)
    
    # loop through each model, write then test it
    def build(self) -> None:
        queue = self.get_starter_nodes()
        in_progress = []
        futures = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            while queue or in_progress:
                # same as run
                for node in queue:
                    if self.parents_complete(node, futures):
                        model = self.dag.graph.nodes[node]['model']
                        future = executor.submit(model.write)    
                        futures[node] = future
                        queue.remove(node)
                        in_progress.append(node)
                        queue += self.get_new_successors(node, queue)
                # check result of done nodes, also 
                for node in in_progress:
                    future = futures[node]
                    if future.done():
                        future.result()
                        in_progress.remove(node)
                        model = self.dag.graph.nodes[node]['model']
                        for test in model.tests:
                            executor.submit(test, model)

    def draw(self) -> None:
        self.dag.draw_graph(self.nodes)

