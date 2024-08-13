
from ctypes import Union
import os
from typing import Optional
import requests 
from urllib.parse import urljoin

class Urls:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    @property
    def get_context_status(self) -> str:
        return urljoin(self.base_url, '/api/1.2/contexts/status')

    @property
    def post_create_context(self) -> str:
        return urljoin(self.base_url, '/api/1.2/contexts/create')

    @property
    def get_command_info(self) -> str:
        return urljoin(self.base_url, '/api/1.2/commands/status')

    @property
    def post_command_execution(self) -> str:
        return urljoin(self.base_url, '/api/1.2/commands/execute')

class Db:
    def __init__(self, token: str, context_id: Optional[str] = None) -> None:
        self.urls = Urls('https://forian-central-dev-engineering.cloud.databricks.com')
        self.token = token
        self.cluster_id = '0513-201611-ecr4kzv4'
        self.context_id = context_id

    def load_context_id(self) -> None:
        if not os.path.exists('.tmp_context_id'):
            pass
        with open('.tmp_context_id', 'r') as file:
            self.context_id = file.read()

    def write_context_id(self) -> None:
        with open('.tmp_context_id', 'w') as file:
             file.write(self.context_id)

    def context_is_active(self) -> bool:
        if not self.context_id:
            self.load_context_id()
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            }
        params = {
            'clusterId': self.cluster_id,
            'contextId' : self.context_id,
            }
        res = requests.get(self.urls.get_context_status, headers=headers, params=params).json()
        if res['status'] == 'Running': 
            return True 
        return False

    def create_context(self) -> None:
        if self.context_is_active():
            pass
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            }
        data = {
            'clusterId': self.cluster_id,
            'language': 'python'
            }
        res = requests.post(self.urls.post_create_context, headers=headers, json=data).json()
        self.context_id = res['id']
        self.write_context_id()

    def get_command_info(self, command_id: str) -> None:
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            }
        params = {
            'clusterId': self.cluster_id,
            'contextId' : self.context_id,
            'commandId': command_id
            }
        print(params)
        res = requests.get(self.urls.get_command_info, headers=headers, params=params).json()
        print(res)

    def execute_command(self, command) -> None:
        self.create_context()
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            }
        data = {
            'clusterId': self.cluster_id,
            'contextId': self.context_id,
            'language': 'python',
            'command': command,
            }
        print(data)
        res = requests.post(self.urls.post_command_execution, headers=headers, json=data).json()
        print(res)
        self.get_command_info(res['id'])
        return res


token = ''
db = Db(token)
r = db.execute_command('print("hi")')


