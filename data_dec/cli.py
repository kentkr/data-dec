
import argparse
from data_dec.compilation import Compiler, Project, ProjectRunner, RegisterLoader, DAG
from data_dec.configuration import Project
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
    project = Project(project_dir=args.project_dir, profiles_dir=args.profiles_dir)
    RegisterLoader(project).load_project()
    compiler = Compiler(project)
    dag = DAG(compiler)
    # configure and run project (load models etc into entity)
    runner = ProjectRunner(dag)
    if args.command == 'build':
        runner.build()
    elif args.command == 'run':
        runner.run()
    elif args.command == 'test':
        runner.test()
    elif args.command == 'draw':
        runner.draw()
    from code import interact
    interact(local=locals())

if __name__ == '__main__':
    main()
