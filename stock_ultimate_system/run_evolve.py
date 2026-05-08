import argparse

from src.pipeline.pipeline_manager import PipelineManager
from src.utils.cli import setup_cli_logging
from src.utils.cli_output import print_header, print_kv, print_mapping
from src.utils.project_paths import resolve_project_path
from src.utils.update_status import record_manual_run


def main() -> None:
    parser = argparse.ArgumentParser(description="执行自动进化并生成冠军版本")
    parser.add_argument("--config-dir", default="config", help="配置目录")
    args = parser.parse_args()
    setup_cli_logging()
    config_dir = str(resolve_project_path(args.config_dir))
    result: dict = {}
    ok = False
    detail = ""
    try:
        pm = PipelineManager(config_dir)
        result = pm.run_evolution_pipeline()
        governance = result.get("version_governance", {}) or {}
        detail = str(governance.get("reason", "") or "evolution_completed")
        record_manual_run(
            run_type="evolution",
            ok=True,
            detail=detail,
            config_dir=args.config_dir,
            meta={
                "action": str(governance.get("action", "") or ""),
                "champion_version": str(governance.get("champion_version", "") or ""),
                "walk_forward_score": float((result.get("walk_forward_evaluation", {}) or {}).get("summary", {}).get("walk_forward_score", 0.0) or 0.0),
                "trade_objective_stability": float((result.get("walk_forward_evaluation", {}) or {}).get("summary", {}).get("trade_objective_stability", 0.0) or 0.0),
            },
        )
        ok = True
    except Exception as exc:
        detail = str(exc)
        record_manual_run(
            run_type="evolution",
            ok=False,
            detail=detail,
            config_dir=args.config_dir,
        )
        raise

    print_header('Evolution Complete')
    model_evo = result.get('model_evolution', {})
    print_kv('Selected models', model_evo.get('selected_models', []))
    print_kv('Model weights', model_evo.get('model_weights', {}))
    print_mapping('Tuned parameters', model_evo.get('tuned_params', {}))

    factor_evo = result.get('factor_evolution', {})
    print_kv('Active factors', len(factor_evo.get('active_factors', [])))
    print_kv('Removed factors', factor_evo.get('removed_count', 0))
    top = factor_evo.get('ranked_factors', [])[:5]
    if top:
        print('\nTop factors:')
        for name, score in top:
            print(f'  {name}: IC={score:.4f}')

    walk_forward = result.get('walk_forward_evaluation', {})
    wf_summary = walk_forward.get('summary', {})
    if wf_summary:
        print('\nWalk-forward:')
        print_kv('Pools', int(wf_summary.get('pool_count', 0)))
        print_kv('Folds', int(wf_summary.get('fold_count', 0)))
        print_kv('Walk-forward score', round(float(wf_summary.get('walk_forward_score', 0.0)), 4))
        print_kv('Trade objective mean', round(float(wf_summary.get('trade_objective_mean', 0.0)), 4))
        print_kv('Trade objective stability', round(float(wf_summary.get('trade_objective_stability', 0.0)), 4))

    governance = result.get('version_governance', {})
    if governance:
        print('\nVersion governance:')
        print_kv('Action', governance.get('action', ''))
        print_kv('Reason', governance.get('reason', ''))
        print_kv('Champion version', governance.get('champion_version', ''))


if __name__ == '__main__':
    main()
