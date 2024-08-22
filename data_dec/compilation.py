from typing import Optional
import os
import sys
import re
import networkx as nx
from data_dec.register import Register
import matplotlib.pyplot as plt
from data_dec.configuration import Project
from data_dec.entity import Model, Test, TestFunctions
from databricks.connect import DatabricksSession

# global spark session
spark = DatabricksSession.builder.profile("data-dec").getOrCreate()

class RegisterLoader:
    """
    Execute project scripts which create load info to the register
    """
    def __init__(self, project: Project) -> None:
        self.project = project

    def load_models(self) -> None:
        models_dir = os.path.join(self.project.project_dir, 'models')
        is_py_file = re.compile(r'\.py$')
        # walk through model dir and children dirs
        for dir_path, folders, files in os.walk(models_dir):
            for file in files:
                if is_py_file.search(file):
                    file_path = os.path.join(dir_path, file)
                    with open(file_path, 'r') as file:
                        # globals and sys path allows local imports
                        sys.path.append(self.project.project_dir)
                        exec(file.read(), globals())

    def load_custom_tests(self) -> None:
        models_dir = os.path.join(self.project.project_dir, 'tests')
        is_py_file = re.compile(r'\.py$')
        # walk through model dir and children dirs
        for dir_path, folders, files in os.walk(models_dir):
            for file in files:
                if is_py_file.search(file):
                    file_path = os.path.join(dir_path, file)
                    with open(file_path, 'r') as file:
                        # globals and sys path allows local imports
                        sys.path.append(self.project.project_dir)
                        exec(file.read(), globals())

    def load_project(self):
        self.load_custom_tests()
        self.load_models()


class Compiler:
    """
    Take project configurations, models, tests, and mesh that all together
    """
    def __init__(self, project) -> None:
        self.project = project
        self.register = Register
        self.models: dict[str, Model] = {}
        self.references = self.register.references
        self.compile_models()
        self.compile_tests()

    def compile_models(self) -> None:
        for model_function in self.register.models:
            model = Model(fn = model_function, database=self.project.database, schema = self.project.schema)
            self.models[model.name] = model

    def compile_tests(self) -> None:
        for model_name, tests in self.register.tests.items():
            for unconfigured_test in tests:
                fn = TestFunctions.__dict__[unconfigured_test.name]
                test = Test(model = unconfigured_test.model, name = unconfigured_test.name, fn = fn, kwargs = unconfigured_test.kwargs)
                self.models[model_name].tests.append(test)

class DAG:
    """Build a DAG off of a compiled project"""
    def __init__(self, compiler: Compiler) -> None:
        self.compiler = compiler
        self.graph = nx.DiGraph()
        self.build_graph()

    def build_graph(self) -> None:
        # add models as nodes
        for model_key, model_class in self.compiler.models.items():
            self.graph.add_node(model_key, model=model_class)
        # add references as edges
        for model_key, references in self.compiler.references.items():
            for reference in references:
                self.graph.add_edge(model_key, reference)

    def draw_graph(self, node_list: Optional[list[str]] = None) -> None:
        if node_list:
            graph = self.graph.subgraph(node_list)
        else:
            graph = self.graph
        plt.figure(figsize=(8,6))
        pos = nx.shell_layout(graph)
        nx.draw(graph, pos, with_labels=True)
        labels = {node: node for node in graph.nodes()}
        nx.draw_networkx_labels(graph, pos, labels)
        plt.show()
        plt.close()

