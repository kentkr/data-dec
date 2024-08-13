
import os
import re
import argparse
from databricks.connect import DatabricksSession
from data_dec.entity import Entity 

# parse arguments
parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest='command', required=True)
build_parser = subparsers.add_parser('build', help='Run and test models')
#build_parser.add_argument('--show', help='Display the dataframes', action='store_true')
run_parser = subparsers.add_parser('run', help='Run models')
test_parser = subparsers.add_parser('test', help='Test models')
args = parser.parse_args()

# global spark session - still useful to call in each model script
spark = DatabricksSession.builder.profile("dev").getOrCreate()

# base entity import
entity = Entity()

# Loop through each model file and execute - this loads the entity class
def load_models() -> None:
    models_dir = os.path.abspath(os.path.join(__file__, '../../project/models/'))
    is_py_file = re.compile(r'\.py$')
    files = os.listdir(models_dir)
    for file in files:
        if is_py_file.search(file):
            with open(os.path.join(models_dir, file), 'r') as file:
                exec(file.read())

# loop through each model, write it to db
def run() -> None:
    for model in entity.models.values():
        model.write()

# loop through each model, test it's output
def test() -> None:
    for model in entity.models.values():
        model.test()

# loop through each model, write then test it
def build() -> None:
    for model in entity.models.values():
        model.write()
        model.test()

# cli entry point
def main():
    load_models()
    if args.command == 'build':
        build()
    elif args.command == 'run':
        run()
    elif args.command == 'test':
        test()

if __name__ == '__main__':
    main()
