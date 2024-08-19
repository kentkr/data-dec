import os
from typing import Dict, Optional
import yaml
import re
from data_dec.entity import Entity 
from databricks.connect import DatabricksSession
import networkx as nx
import matplotlib.pyplot as plt

# global spark session
spark = DatabricksSession.builder.profile("data-dec").getOrCreate()

class Project:
    """Compile project infromation from yml"""
    def __init__(
            self, 
            project_dir: Optional[str] = None,
            profiles_dir: Optional[str] = None
        ) -> None:
        if project_dir:
            self.project_dir = project_dir
        else:
            self.project_dir = os.getcwd()
        if profiles_dir:
            self.profiles_dir = profiles_dir
        else:
            self.profiles_dir = os.path.expanduser('~/.dec')
        self.process_decor_yml()
        self.process_profiles_yml()

    def load_decor_yml(self) -> Dict:
        decor_path = os.path.join(self.project_dir, 'decor.yml')
        if os.path.exists(decor_path):
            with open(decor_path, 'r') as file:
                yml = yaml.safe_load(file)
        else:
            raise Exception(f'No decor.yml path found at {decor_path!r}')
        return yml

    def process_decor_yml(self) -> None:
        yml = self.load_decor_yml()
        if 'profile' in yml:
            self.profile = yml['profile']
        else:
            raise Exception(f"'profile' not found in decor.yml")

    def load_profiles_yml(self) -> Dict:
        profiles_path = os.path.join(self.profiles_dir, 'profiles.yml')
        if os.path.exists(profiles_path):
            with open(profiles_path, 'r') as file:
                yml = yaml.safe_load(file)
        else:
            raise Exception(f'No profiles.yml path found at {profiles_path}')
        return yml

    def process_profiles_yml(self) -> None:
        yml = self.load_profiles_yml() 
        profile_yml = yml[self.profile]
        self.target = profile_yml['default_target']
        # is there a good way to dynamically check if a key exists without a bazillion if statements?
        # raise an error if it doesn't?
        self.database = profile_yml['targets'][self.target]['database']
        self.database = profile_yml['targets'][self.target]['schema']

    def load_models(self) -> None:
        models_dir = os.path.join(self.project_dir, 'models')
        is_py_file = re.compile(r'\.py$')
        # walk through model dir and children dirs
        for dir_path, folders, files in os.walk(models_dir):
            for file in files:
                if is_py_file.search(file):
                    file_path = os.path.join(dir_path, file)
                    with open(file_path, 'r') as file:
                        # globals allows local imports
                        exec(file.read())

    def load_custom_tests(self) -> None:
        models_dir = os.path.join(self.project_dir, 'tests')
        is_py_file = re.compile(r'\.py$')
        # walk through model dir and children dirs
        for dir_path, folders, files in os.walk(models_dir):
            for file in files:
                if is_py_file.search(file):
                    file_path = os.path.join(dir_path, file)
                    with open(file_path, 'r') as file:
                        # globals allows local imports
                        exec(file.read())

class DAG:
    def __init__(self, entity: Entity) -> None:
        self.entity = entity
        self.graph = nx.DiGraph()
        self.build_graph()

    def build_graph(self) -> None:
        # add models as nodes
        for model_key, model_class in self.entity.models.items():
            self.graph.add_node(model_key, model=model_class)
        # add references as edges
        for model_key, references in self.entity.references.items():
            for reference in references:
                self.graph.add_edge(model_key, reference)

    def draw_graph(self) -> None:
        plt.figure(figsize=(8,6))
        pos = nx.spring_layout(self.graph)
        nx.draw(self.graph, pos, with_labels=True)
        labels = {node: node for node in self.graph.nodes()}
        nx.draw_networkx_labels(self.graph, pos, labels)
        plt.show()
        plt.close()


class ProjectRunner:
    def __init__(self, project: Project, entity: Entity) -> None:
        self.entity = entity
        self.project = project
        self.project.load_custom_tests()
        self.project.load_models()
        self.dag = DAG(entity)

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

