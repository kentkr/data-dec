
import os
from typing import Dict, Optional
import yaml

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
        self.profile = self.process_profiles_yml()
        self.decor = self.process_decor_yml()

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
        self.schema = profile_yml['targets'][self.target]['schema']

