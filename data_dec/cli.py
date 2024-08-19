
import argparse
from data_dec.compilation import Project, ProjectRunner
from data_dec.entity import Entity
import yaml

# parse arguments
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    # group together basic commands like build or test
    subparsers = parser.add_subparsers(dest='command', required=True)
    build_parser = subparsers.add_parser('build', help='Run and test models')
    run_parser = subparsers.add_parser('run', help='Run models')
    test_parser = subparsers.add_parser('test', help='Test models')
    draw_parser = subparsers.add_parser('draw', help='Draw the DAG')
    parser.add_argument('--profiles-dir', help='Directory with your profiles.yml')
    parser.add_argument('--project-dir', help='Directory of your data-dec project')
    args = parser.parse_args()
    return args

# cli entry point
def main():
    # get arguments
    args = parse_args()
    # Class object that tracks models, tests, etc
    entity = Entity()
    # configure and run project (load models etc into entity)
    project = Project(project_dir=args.project_dir, profiles_dir=args.profiles_dir)
    runner = ProjectRunner(project, entity)
    if args.command == 'build':
        runner.build()
    elif args.command == 'run':
        runner.run()
    elif args.command == 'test':
        runner.test()
    elif args.command == 'draw':
        runner.draw()

if __name__ == '__main__':
    main()
