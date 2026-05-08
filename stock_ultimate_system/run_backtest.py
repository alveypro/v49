import argparse

from src.pipeline.pipeline_manager import PipelineManager
from src.utils.cli import setup_cli_logging
from src.utils.cli_output import print_header, print_kv, print_mapping
from src.utils.project_paths import resolve_project_path


def main() -> None:
    setup_cli_logging()
    parser = argparse.ArgumentParser(description='Stock backtest')
    parser.add_argument('--stocks', nargs='*', default=None, help='Stock pool (e.g. 000001.SZ 600036.SH)')
    args = parser.parse_args()

    pm = PipelineManager(str(resolve_project_path('config')))
    result = pm.run_backtest_pipeline(args.stocks)

    print_header('Backtest Complete')
    print_kv('Status', result.get('status', '?'))
    print_kv('Period', f'{result.get("start_date", "")} ~ {result.get("end_date", "")}')
    print_kv('Stock pool', result.get('stock_pool', []))
    metrics = result.get('detailed_metrics', result.get('metrics', {}))
    print_mapping('Performance Metrics', metrics)
    print_mapping('Stability', result.get('stability', {}), skip_dict_values=True)

    report_path = result.get('report_path')
    if report_path:
        print_kv('Report', report_path)

    charts = result.get('charts', {})
    print_mapping('Charts', charts)


if __name__ == '__main__':
    main()
