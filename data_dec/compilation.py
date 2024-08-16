import os
from typing import Dict
import yaml

class Project:
    """Instantiate project variables"""
    def __init__(self) -> None:
        self.project_dir = os.getcwd()
        self.profiles_dir = os.path.expanduser('~/.dec/')
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




