
import os
import re
import argparse
from databricks.connect import DatabricksSession
from data_dec.entity import Entity, Model
from pyspark.sql import Row, DataFrame

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest='command', required=True)
build_parser = subparsers.add_parser('build', help='Run and test models')
build_parser.add_argument('--show', help='Display the dataframes', action='store_true')
run_parser = subparsers.add_parser('run', help='Run models')
test_parser = subparsers.add_parser('test', help='Test models')
args = parser.parse_args()

# global spark session - still useful to call in each model script
spark = DatabricksSession.builder.profile("dev").getOrCreate()

# import entities
entity = Entity()

def load_models() -> None:
    models_dir = os.path.abspath(os.path.join(__file__, '../../tmp_project/models/'))
    is_py_file = re.compile(r'\.py$')
    files = os.listdir(models_dir)
    for file in files:
        if is_py_file.search(file):
            with open(os.path.join(models_dir, file), 'r') as file:
                exec(file.read())

def build() -> None:
    for model in entity.models.values():
        model.write()
        model.test()
        if args.show:
            model.show()

def main():
    load_models()
    if args.command == 'build':
        build()
    elif args.command == 'run':
        print('The run command is a work in progress')
    elif args.command == 'test':
        print('The test command is a work in progress')

if __name__ == '__main__':
    main()
