import argparse

from src.pipeline.pipeline_manager import PipelineManager
from src.utils.cli import setup_cli_logging
from src.utils.cli_output import print_header, print_kv
from src.utils.project_paths import resolve_project_path


def main() -> None:
    setup_cli_logging()
    parser = argparse.ArgumentParser(description='Stock prediction')
    parser.add_argument('--code', default='000001.SZ', help='Stock code (e.g. 000001.SZ)')
    parser.add_argument('--batch', nargs='*', help='Multiple stock codes for batch prediction')
    args = parser.parse_args()

    pm = PipelineManager(str(resolve_project_path('config')))

    if args.batch:
        results = pm.run_batch_prediction(args.batch)
        for r in results:
            _print_result(r)
    else:
        result = pm.run_prediction_pipeline(args.code)
        _print_result(result)


def _print_result(result: dict) -> None:
    print_header(result.get('ts_code', '?'))
    sig = result.get('signal_result', {})
    print_kv('Signal', f'{sig.get("signal", "?")} | Score: {sig.get("score", 0):.1f}')
    print_kv('Reason', sig.get('reason', ''))
    fc = result.get('forecast_result', {})
    print_kv(
        'Direction prob(up/down)',
        f'{fc.get("direction_prob_up", fc.get("direction_prob", 0)):.2%}'
        f' / {fc.get("direction_prob_down", 1 - fc.get("direction_prob", 0)):.2%}'
        f' | Confidence: {fc.get("confidence", 0):.2%}',
    )
    print_kv(
        'Pred return',
        f'{fc.get("pred_return", fc.get("expected_return", 0)):.2%} '
        f'| Pred range: [{fc.get("pred_range_low", 0):.2f}, {fc.get("pred_range_high", 0):.2f}]',
    )
    ri = result.get('regime_info', {})
    print_kv('Regime', f'{ri.get("regime", "?")} | Market: {ri.get("market_trend", "?")}')
    runtime_profile = result.get('runtime_profile', {})
    if runtime_profile:
        print_kv('Runtime profile', f'{runtime_profile.get("regime", "?")} | Run: {runtime_profile.get("run_id", "")}')
    pos = result.get('position_result', {})
    print_kv('Position', f'{pos.get("position_pct", 0):.1%} | Amount: {pos.get("position_amount", 0):,.0f}')
    risk = result.get('risk_info', {})
    print_kv('Risk', f'{risk.get("risk_level", "?")} | SL: {risk.get("stop_loss", 0):.2f} | TP: {risk.get("take_profit", 0):.2f}')


if __name__ == '__main__':
    main()
