from __future__ import annotations

import ast
from datetime import datetime
import json
from pathlib import Path
from typing import Any

import yaml

from src.utils.project_paths import resolve_project_path
from src.utils.serialization import save_json


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        import pandas as pd

        df = pd.read_csv(path)
        return df.to_dict(orient="records")
    except Exception:
        return []


def _parse_params(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = ast.literal_eval(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _load_framework_snapshot() -> dict[str, Any]:
    framework_path = resolve_project_path("config/experiment_framework.example.yaml")
    framework = _read_yaml(framework_path)
    experiment_framework = framework.get("experiment_framework", {}) if isinstance(framework, dict) else {}
    windows = experiment_framework.get("windows", {}) if isinstance(experiment_framework, dict) else {}
    governance = experiment_framework.get("governance", {}) if isinstance(experiment_framework, dict) else {}
    ranking = experiment_framework.get("ranking", {}) if isinstance(experiment_framework, dict) else {}
    gates = experiment_framework.get("gates", {}) if isinstance(experiment_framework, dict) else {}
    tracking = experiment_framework.get("tracking", {}) if isinstance(experiment_framework, dict) else {}
    return {
        "framework_file": "config/experiment_framework.example.yaml",
        "schema_version": experiment_framework.get("schema_version", 0),
        "windows": windows if isinstance(windows, dict) else {},
        "governance": governance if isinstance(governance, dict) else {},
        "ranking": ranking if isinstance(ranking, dict) else {},
        "gates": gates if isinstance(gates, dict) else {},
        "tracking": tracking if isinstance(tracking, dict) else {},
    }


def _read_json_with_local_precedence(local_path: Path, fallback_path: Path) -> dict[str, Any]:
    payload = _read_json(local_path)
    if payload:
        return payload
    return _read_json(fallback_path)


def _load_csv_rows_with_local_precedence(local_path: Path, fallback_path: Path) -> list[dict[str, Any]]:
    rows = _load_csv_rows(local_path)
    if rows:
        return rows
    return _load_csv_rows(fallback_path)


def _build_regime_coverage(
    generated_at: str,
    regime_profiles: dict[str, Any],
    daily_runtime: dict[str, Any],
) -> dict[str, Any]:
    observed_regimes = sorted(
        str(name).strip()
        for name in regime_profiles.keys()
        if str(name).strip() and str(name).strip().lower() != "unknown"
    )
    if not observed_regimes:
        observed_regimes = sorted(
            {
                str(((run.get("top_result", {}) or {}).get("dominant_regime", "") or "")).strip()
                for run in (daily_runtime.get("runs", []) or [])
                if isinstance(run, dict)
            }
            - {""}
        )
    target_regime_count = 4
    profile_count = len(observed_regimes)
    coverage_score = round(min(profile_count / target_regime_count, 1.0), 4) if profile_count > 0 else 0.0
    avg_env_score = 0.0
    if regime_profiles:
        avg_env_score = round(
            sum(_safe_float((payload or {}).get("avg_environment_score", 0.0)) for payload in regime_profiles.values())
            / max(len(regime_profiles), 1),
            4,
        )
    detail = (
        f"observed_regimes={','.join(observed_regimes)}"
        if observed_regimes
        else "no explicit regime profile artifact found"
    )
    return {
        "generated_at": generated_at,
        "regime_profile_count": profile_count,
        "target_regime_count": target_regime_count,
        "observed_regimes": observed_regimes,
        "avg_environment_score": avg_env_score,
        "regime_coverage_score": coverage_score,
        "detail": detail,
    }


def _build_sensitivity_report(
    generated_at: str,
    search_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if not search_rows:
        return {
            "generated_at": generated_at,
            "top_run_count": 0,
            "parameter_sensitivity_score": 1.0,
            "score_dispersion": 0.0,
            "param_value_dispersion": 0.0,
            "detail": "grid_backtest_latest.csv missing; sensitivity treated as unresolved",
        }

    top_rows = search_rows[: min(10, len(search_rows))]
    robust_scores = [_safe_float(row.get("robustness_score", 0.0)) for row in top_rows]
    top_score = max(robust_scores) if robust_scores else 0.0
    bottom_score = min(robust_scores) if robust_scores else 0.0
    score_dispersion = 0.0 if top_score <= 0 else min(max((top_score - bottom_score) / max(abs(top_score), 1.0), 0.0), 1.0)

    param_maps = [_parse_params(row.get("params", {})) for row in top_rows]
    param_keys = sorted({key for params in param_maps for key in params.keys()})
    per_key_dispersion: list[float] = []
    for key in param_keys:
        values = {json.dumps(params.get(key), ensure_ascii=False, sort_keys=True) for params in param_maps if key in params}
        if not values:
            continue
        per_key_dispersion.append((len(values) - 1) / max(len(param_maps) - 1, 1))
    param_value_dispersion = round(
        sum(per_key_dispersion) / len(per_key_dispersion), 4
    ) if per_key_dispersion else 0.0

    sensitivity = round(min(score_dispersion * 0.45 + param_value_dispersion * 0.55, 1.0), 4)
    return {
        "generated_at": generated_at,
        "top_run_count": len(top_rows),
        "parameter_sensitivity_score": sensitivity,
        "score_dispersion": round(score_dispersion, 4),
        "param_value_dispersion": param_value_dispersion,
        "detail": "computed from top grid backtest rows; lower is more stable",
    }


def _build_governance_decision(
    generated_at: str,
    update_status: dict[str, Any],
    daily_runtime: dict[str, Any],
    candidates_summary: dict[str, Any],
    validation_summary: dict[str, Any],
    framework_snapshot: dict[str, Any],
    regime_coverage: dict[str, Any],
    sensitivity_report: dict[str, Any],
) -> dict[str, Any]:
    gates = framework_snapshot.get("gates", {}) if isinstance(framework_snapshot, dict) else {}
    production_gates = gates.get("production", {}) if isinstance(gates, dict) else {}
    robustness_gates = gates.get("robustness", {}) if isinstance(gates, dict) else {}

    post_candidates_ok = bool((update_status.get("post_candidates", {}) or {}).get("ok"))
    post_daily_ok = bool((update_status.get("post_daily_research", {}) or {}).get("ok"))
    health_score = _safe_float((daily_runtime.get("health_score", {}) or {}).get("score", 0.0))
    min_health_score = _safe_float(production_gates.get("min_daily_health_score", 75.0), 75.0)
    rebalance_dates = _safe_int(validation_summary.get("rebalance_dates", 0))
    avg_excess_return = _safe_float(validation_summary.get("avg_excess_return_5d", 0.0))
    basket_win_rate = _safe_float(validation_summary.get("basket_win_rate_5d", 0.0))
    guardrail_mode = str(candidates_summary.get("guardrail_mode", "unknown") or "unknown")
    min_regime_coverage = _safe_float(robustness_gates.get("min_regime_coverage_score", 0.6), 0.6)
    max_parameter_sensitivity = _safe_float(robustness_gates.get("max_parameter_sensitivity", 0.35), 0.35)

    gate_status = {
        "core_pipeline_ok": post_candidates_ok and post_daily_ok,
        "validation_positive": rebalance_dates > 0 and avg_excess_return >= 0.0,
        "validation_depth_ok": rebalance_dates >= 3,
        "daily_health_ok": health_score >= min_health_score,
        "guardrail_normal": guardrail_mode == "normal",
        "regime_coverage_ok": _safe_float(regime_coverage.get("regime_coverage_score", 0.0)) >= min_regime_coverage,
        "parameter_sensitivity_ok": _safe_float(sensitivity_report.get("parameter_sensitivity_score", 1.0)) <= max_parameter_sensitivity,
    }

    reasons: list[str] = []
    decision = "observe"
    if not gate_status["core_pipeline_ok"]:
        decision = "reject"
        reasons.append("core pipeline incomplete")
    elif rebalance_dates >= 3 and avg_excess_return < 0:
        decision = "reject"
        reasons.append("validation excess return below zero")
    elif all(gate_status.values()):
        decision = "promote_to_staging"
        reasons.append("core gates passed with positive validation and stable governance metrics")
    else:
        reasons.append("hold in observation until robustness and validation gates improve")

    if basket_win_rate < 0.34 and rebalance_dates >= 3:
        reasons.append("basket win rate below 34%")
    if guardrail_mode != "normal":
        reasons.append(f"guardrail_mode={guardrail_mode}")

    return {
        "generated_at": generated_at,
        "decision": decision,
        "reason": "; ".join(reasons),
        "update_status": str(update_status.get("status", "unknown")),
        "post_candidates_ok": post_candidates_ok,
        "post_daily_research_ok": post_daily_ok,
        "health_score": round(health_score, 2),
        "validation_rebalance_dates": rebalance_dates,
        "avg_excess_return_5d": round(avg_excess_return, 4),
        "basket_win_rate_5d": round(basket_win_rate, 4),
        "guardrail_mode": guardrail_mode,
        "gate_status": gate_status,
    }


def _build_ranking_consistency(validation_payload: dict[str, Any]) -> dict[str, Any]:
    variants = validation_payload.get("variants", {}) or {}
    if not isinstance(variants, dict) or not variants:
        return {
            "ranking_consistency_score": 0.0,
            "variant_excess_spread": 0.0,
            "variant_order": [],
        }
    pairs: list[tuple[str, float]] = []
    for name, payload in variants.items():
        pairs.append((str(name), _safe_float((payload or {}).get("avg_excess_return_5d", 0.0))))
    pairs.sort(key=lambda item: item[1], reverse=True)
    values = [value for _, value in pairs]
    spread = max(values) - min(values) if values else 0.0
    score = round(max(0.0, 1.0 - min(spread / 0.03, 1.0)), 4) if values else 0.0
    return {
        "ranking_consistency_score": score,
        "variant_excess_spread": round(spread, 4),
        "variant_order": [name for name, _ in pairs],
    }


def build_experiment_artifact_bundle(output_dir: str = "data/experiments") -> dict[str, str]:
    out_dir = resolve_project_path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    update_status = _read_json(out_dir / "update_status_latest.json")
    candidates_summary = _read_json(out_dir / "candidates_basket_summary_latest.json")
    candidates_validation = _read_json(out_dir / "candidates_basket_validation_latest.json")
    daily_runtime = _read_json(out_dir / "daily_research_status_latest.json")
    framework_snapshot = _load_framework_snapshot()
    regime_profiles = _read_json_with_local_precedence(
        out_dir / "grid_backtest_regime_profiles_latest.json",
        resolve_project_path("data/experiments/grid_search/grid_backtest_regime_profiles_latest.json"),
    )
    search_rows = _load_csv_rows_with_local_precedence(
        out_dir / "grid_backtest_latest.csv",
        resolve_project_path("data/experiments/grid_search/grid_backtest_latest.csv"),
    )

    generated_at = datetime.now().isoformat()
    experiment_id = datetime.now().strftime("bundle_%Y%m%d_%H%M%S")
    validation_summary = candidates_validation.get("summary", {}) or {}
    ranking_consistency = _build_ranking_consistency(candidates_validation)
    regime_coverage = _build_regime_coverage(generated_at, regime_profiles, daily_runtime)
    sensitivity_report = _build_sensitivity_report(generated_at, search_rows)
    governance_decision = _build_governance_decision(
        generated_at,
        update_status,
        daily_runtime,
        candidates_summary,
        validation_summary,
        framework_snapshot,
        regime_coverage,
        sensitivity_report,
    )

    manifest = {
        "experiment_id": experiment_id,
        "generated_at": generated_at,
        "source": "build_experiment_artifact_bundle",
        "status": str(update_status.get("status", "unknown")),
        "daily_research_state": str(daily_runtime.get("state", "unknown")),
        "windows": framework_snapshot.get("windows", {}),
        "profiles": list(daily_runtime.get("profiles", []) or []),
        "completed_profiles": list(daily_runtime.get("completed_profiles", []) or []),
        "failed_profiles": list(daily_runtime.get("failed_profiles", []) or []),
        "research_pool_size": len(daily_runtime.get("stocks", []) or []),
        "ranking_mode": str((framework_snapshot.get("ranking", {}) or {}).get("mode", "unknown")),
        "governance_decision": governance_decision.get("decision", "observe"),
        "note": "Auto-bundled artifacts from latest runtime outputs with governance-oriented summaries.",
    }
    save_json(manifest, str(out_dir / "experiment_manifest.json"))

    config_snapshot = {
        "generated_at": generated_at,
        **framework_snapshot,
    }
    save_json(config_snapshot, str(out_dir / "config_snapshot.json"))
    save_json(governance_decision, str(out_dir / "governance_decision.json"))
    save_json(regime_coverage, str(out_dir / "regime_coverage.json"))
    save_json(sensitivity_report, str(out_dir / "sensitivity_report.json"))

    summary_md = [
        "# Experiment Summary",
        "",
        f"- generated_at: {generated_at}",
        f"- experiment_id: {experiment_id}",
        f"- update_status: {update_status.get('status', 'unknown')}",
        f"- daily_research_state: {daily_runtime.get('state', 'unknown')}",
        f"- governance_decision: {governance_decision.get('decision', 'observe')}",
        f"- candidates_count: {candidates_summary.get('candidate_count', 0)}",
        f"- research_pool_size: {len(daily_runtime.get('stocks', []) or [])}",
        "",
        "## Formal Windows",
        f"- train_window: {(framework_snapshot.get('windows', {}) or {}).get('train_window', {})}",
        f"- search_window: {(framework_snapshot.get('windows', {}) or {}).get('search_window', {})}",
        f"- validation_window: {(framework_snapshot.get('windows', {}) or {}).get('validation_window', {})}",
        f"- observation_window: {(framework_snapshot.get('windows', {}) or {}).get('observation_window', {})}",
        "",
        "该摘要由 artifact bundler 自动生成，用于治理审计连贯性。",
    ]
    _write_text(out_dir / "experiment_summary.md", "\n".join(summary_md) + "\n")

    risk_md = [
        "# Risk Review",
        "",
        f"- generated_at: {generated_at}",
        f"- risk_pressure_score: {candidates_summary.get('risk_pressure_score', 0)}",
        f"- guardrail_mode: {candidates_summary.get('guardrail_mode', 'unknown')}",
        f"- health_score: {governance_decision.get('health_score', 0)}",
        f"- regime_coverage_score: {regime_coverage.get('regime_coverage_score', 0)}",
        f"- parameter_sensitivity_score: {sensitivity_report.get('parameter_sensitivity_score', 1.0)}",
        "",
        "若 `post_daily_research_ok=false` 或验证收益为负，默认不得晋级到 production champion。",
    ]
    _write_text(out_dir / "risk_review.md", "\n".join(risk_md) + "\n")

    variant_lines = []
    for name, payload in ((candidates_validation.get("variants", {}) or {}).items()):
        variant_lines.append(
            f"- {name}: avg_excess_return_5d={_safe_float((payload or {}).get('avg_excess_return_5d', 0.0)):.4f}, "
            f"win_rate_5d={_safe_float((payload or {}).get('win_rate_5d', 0.0)):.4f}"
        )
    ranking_md = [
        "# Candidate Ranking Review",
        "",
        f"- generated_at: {generated_at}",
        f"- strategy_mode: {candidates_summary.get('strategy_mode', 'unknown')}",
        f"- validation_rebalance_dates: {validation_summary.get('rebalance_dates', 0)}",
        f"- ranking_consistency_score: {ranking_consistency.get('ranking_consistency_score', 0.0)}",
        f"- variant_excess_spread: {ranking_consistency.get('variant_excess_spread', 0.0)}",
        f"- variant_order: {', '.join(ranking_consistency.get('variant_order', [])) or '-'}",
        "",
        "## Variant Comparison",
        *(variant_lines or ["- no variant comparison available"]),
        "",
        "当前排序结果进入治理观察轨，只有在验证与稳定性同时通过时才允许晋级。",
    ]
    _write_text(out_dir / "candidate_ranking_review.md", "\n".join(ranking_md) + "\n")

    pool_meta = daily_runtime.get("research_pool_meta", {}) or {}
    stock_pool_rows = [
        "ts_code,source,requested_size,effective_size,latest_trade_date,effective_liquidity_min_turnover",
    ]
    for code in daily_runtime.get("stocks", []) or []:
        text = str(code).strip()
        if text:
            stock_pool_rows.append(
                f"{text},{pool_meta.get('source', '-')},{pool_meta.get('requested_size', 0)},{len(daily_runtime.get('stocks', []) or [])},"
                f"{pool_meta.get('latest_trade_date', '')},{pool_meta.get('effective_liquidity_min_turnover', '')}"
            )
    if len(stock_pool_rows) == 1:
        latest_csv = out_dir / "candidates_top_latest.csv"
        for row in _load_csv_rows(latest_csv):
            code = str(row.get("ts_code", "")).strip()
            if code:
                stock_pool_rows.append(f"{code},candidate_latest,0,0,,")
    _write_text(out_dir / "research_pool_snapshot.csv", "\n".join(stock_pool_rows) + "\n")

    search_result_rows = ["profile,status,run_id,planned_runs,executed_runs,robustness_score,stability_score,sharpe_ratio,total_return,validation_start,validation_end,params"]
    for run in daily_runtime.get("runs", []) or []:
        if not isinstance(run, dict):
            continue
        top_result = run.get("top_result", {}) or {}
        validation_window = top_result.get("validation_window", {}) or {}
        params_text = json.dumps(top_result.get("params", {}), ensure_ascii=False, sort_keys=True)
        search_result_rows.append(
            f"{run.get('profile', '')},{run.get('status', '')},{top_result.get('run_id', '')},{run.get('planned_runs', 0)},"
            f"{run.get('executed_runs', 0)},{_safe_float(top_result.get('robustness_score', 0.0)):.4f},"
            f"{_safe_float(top_result.get('stability_score', 0.0)):.4f},{_safe_float(top_result.get('sharpe_ratio', 0.0)):.4f},"
            f"{_safe_float(top_result.get('total_return', 0.0)):.4f},{validation_window.get('start_date', '')},"
            f"{validation_window.get('end_date', '')},\"{params_text}\""
        )
    if len(search_result_rows) == 1:
        search_result_rows.append(
            f"bundle_runtime,{update_status.get('status', 'unknown')},{experiment_id},0,0,0.0000,0.0000,0.0000,0.0000,,,"
            "\"{}\""
        )
    _write_text(out_dir / "search_results.csv", "\n".join(search_result_rows) + "\n")

    validation_rows = [
        "generated_at,rebalance_dates,avg_basket_return_5d,avg_excess_return_5d,basket_win_rate_5d,avg_universe_return_5d,avg_top1_return_5d,ranking_consistency_score,variant_excess_spread",
        (
            f"{generated_at},{validation_summary.get('rebalance_dates', 0)},"
            f"{validation_summary.get('avg_basket_return_5d', 0)},"
            f"{validation_summary.get('avg_excess_return_5d', 0)},"
            f"{validation_summary.get('basket_win_rate_5d', 0)},"
            f"{validation_summary.get('avg_universe_return_5d', 0)},"
            f"{validation_summary.get('avg_top1_return_5d', 0)},"
            f"{ranking_consistency.get('ranking_consistency_score', 0.0)},"
            f"{ranking_consistency.get('variant_excess_spread', 0.0)}"
        ),
    ]
    for name, payload in ((candidates_validation.get("variants", {}) or {}).items()):
        validation_rows.append(
            f"{generated_at}:{name},0,{_safe_float((payload or {}).get('avg_return_5d', 0.0))},"
            f"{_safe_float((payload or {}).get('avg_excess_return_5d', 0.0))},"
            f"{_safe_float((payload or {}).get('win_rate_5d', 0.0))},0,0,,"
        )
    _write_text(out_dir / "validation_results.csv", "\n".join(validation_rows) + "\n")

    signal_rows = [
        "generated_at,source,status,note",
        f"{generated_at},daily_research_status_latest.json,{daily_runtime.get('state', 'unknown')},profiles={','.join(daily_runtime.get('profiles', []) or [])}",
    ]
    for run in daily_runtime.get("runs", []) or []:
        if not isinstance(run, dict):
            continue
        top = run.get("top_result", {}) or {}
        signal_rows.append(
            f"{generated_at},profile:{run.get('profile', '')},{run.get('status', '')},"
            f"run_id={top.get('run_id', '')};dominant_regime={top.get('dominant_regime', '')};trades={top.get('total_trades', 0)}"
        )
    _write_text(out_dir / "signal_logs.csv", "\n".join(signal_rows) + "\n")

    return {
        "experiment_manifest": str(out_dir / "experiment_manifest.json"),
        "config_snapshot": str(out_dir / "config_snapshot.json"),
        "governance_decision": str(out_dir / "governance_decision.json"),
        "regime_coverage": str(out_dir / "regime_coverage.json"),
        "sensitivity_report": str(out_dir / "sensitivity_report.json"),
        "experiment_summary": str(out_dir / "experiment_summary.md"),
        "risk_review": str(out_dir / "risk_review.md"),
        "candidate_ranking_review": str(out_dir / "candidate_ranking_review.md"),
        "research_pool_snapshot": str(out_dir / "research_pool_snapshot.csv"),
        "search_results": str(out_dir / "search_results.csv"),
        "validation_results": str(out_dir / "validation_results.csv"),
        "signal_logs": str(out_dir / "signal_logs.csv"),
    }
