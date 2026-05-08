from src.pipeline.pipeline_manager import PipelineManager
from src.utils.cli import setup_cli_logging
from src.utils.project_paths import resolve_project_path


def main() -> None:
    setup_cli_logging('logs/system.log')
    pm = PipelineManager(str(resolve_project_path('config')))
    pm.run_system_demo()


if __name__ == '__main__':
    main()
