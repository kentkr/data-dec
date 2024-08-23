
import argparse
from data_dec.compilation import Compiler, RegisterLoader, DAG
from data_dec.configuration import Project
from data_dec.runner import ProjectRunner
from data_dec.configuration import Project

# parse arguments
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    # args applied to whole script
    parser.add_argument('--profiles-dir', help='Directory with your profiles.yml', required=False)
    parser.add_argument('--project-dir', help='Directory of your data-dec project', required=False)
    # subparser for commands
    subparsers = parser.add_subparsers(dest='command', required=True)
    # command plags
    command_flags = argparse.ArgumentParser(add_help=False)
    command_flags.add_argument('-s', '--select', nargs='*', default=[], help='Select nodes to act on')
    # subparser with command flags
    build_parser = subparsers.add_parser('build', help='Run and test models', parents=[command_flags])
    run_parser = subparsers.add_parser('run', help='Run models', parents=[command_flags])
    test_parser = subparsers.add_parser('test', help='Test models', parents=[command_flags])
    draw_parser = subparsers.add_parser('draw', help='Draw the DAG', parents=[command_flags])
    return parser.parse_args()

# cli entry point
def main() -> None:
    # get arguments
    args = parse_args()
    project = Project(project_dir=args.project_dir, profiles_dir=args.profiles_dir)
    RegisterLoader(project).load_project()
    compiler = Compiler(project)
    dag = DAG(compiler)
    runner = ProjectRunner(dag, args)
    if args.command == 'build':
        runner.multi_thread('_build')
    elif args.command == 'run':
        runner.multi_thread('_run')
    elif args.command == 'test':
        # models are multithreaded, but tests are run sequentially
        runner.multi_thread('_test')
    elif args.command == 'draw':
        runner.draw()

if __name__ == '__main__':
    main()
