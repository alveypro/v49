from src.pipeline.pipeline_manager import PipelineManager
from src.utils.cli import setup_cli_logging
from src.utils.cli_output import print_header, print_kv, print_mapping
from src.utils.project_paths import resolve_project_path


def main() -> None:
    setup_cli_logging()
    pm = PipelineManager(str(resolve_project_path('config')))
    result = pm.run_training_pipeline()
    print_header('Training Complete')
    print_kv('Models trained', result.get('trained_models', []))
    print_kv('Test size', result.get('test_size', 0))
    print_mapping('Evaluation', result.get('eval_results', {}))


if __name__ == '__main__':
    main()
