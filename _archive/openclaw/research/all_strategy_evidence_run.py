from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import sqlite3

from openclaw.adapters import V49Adapter
from openclaw.paths import db_path as default_db_path
from openclaw.research.backtest_param_sweep import SweepConfig, run_param_sweep
from openclaw.runtime.v49_handlers import HandlerFactory
from openclaw.services.ensemble_alpha_sleeve_service import build_ensemble_alpha_sleeve_fact_chain
from openclaw.services.ensemble_alpha_rebuild_lab_service import build_ensemble_alpha_rebuild_lab
from openclaw.services.ensemble_core_contract_service import build_ensemble_core_contract_review
from openclaw.services.ensemble_execution_cost_service import build_ensemble_execution_cost_replay
from openclaw.services.ensemble_shadow_portfolio_service import build_ensemble_shadow_portfolio
from openclaw.services.ensemble_walk_forward_benchmark_service import (
    FORMAL_POOL_BENCHMARK_STRATEGIES,
    build_ensemble_walk_forward_shadow_benchmark,
)
from openclaw.services.formal_pool_benchmark_service import build_formal_pool_benchmark_return_series
from openclaw.services.rejected_backtest_artifact_ledger_service import append_rejected_backtest_artifact
from openclaw.services.unified_strategy_recommendation_service import build_unified_system_recommendation
from strategies.registry import all_strategy_names, get_profile
from tools.strategy_optimization_stage_audit import run_stage_audit


JsonDict = Dict[str, Any]
DEFAULT_EVIDENCE_STRATEGIES = ("v5", "v8", "v9", "combo", "v4", "v6", "v7", "stable")
RESEARCH_ONLY_STRATEGIES = ("ai", "ensemble_core")
AI_RESEARCH_ONLY_REQUIREMENTS = (
    "real_runtime_backtest_handler",
    "explainable_signal_fact_chain",
    "point_in_time_inputs",
    "cost_slippage_fill_constraints",
    "stage_audit_promotion_decision",
)
ENSEMBLE_CORE_RESEARCH_ONLY_REQUIREMENTS = tuple(
    build_ensemble_core_contract_review({}).get("blocking_reasons") or []
)
REPAIR_CANDIDATE_STRATEGIES = ("v8", "stable", "combo")
LEGACY_OR_DIAGNOSTIC_STRATEGIES = ("v4", "v7", "v5", "v9", "v6")


@dataclass(frozen=True)
class AllStrategyEvidenceRunConfig:
    module_path: Path = Path("v49_app.py")
    output_dir: Path = Path("logs/openclaw/all_strategy_evidence")
    date_from: str = "2025-11-01"
    date_to: str = "2026-05-03"
    train_window_days: int = 90
    test_window_days: int = 30
    step_days: int = 30
    strategies: Sequence[str] = DEFAULT_EVIDENCE_STRATEGIES
    include_research_only: bool = True
    scan_offline_stock_limit: int = 300
    scan_limit: int = 30
    sweep_max_runs: Optional[int] = 2
    per_run_timeout_sec: Optional[int] = 180
    db_path: str = ""
    rejected_ledger: str = "logs/openclaw/rejected_backtest_artifacts.jsonl"
    operator_name: str = "all_strategy_evidence_run"


def run_all_strategy_evidence(cfg: AllStrategyEvidenceRunConfig) -> JsonDict:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    db = str(cfg.db_path or default_db_path())
    adapter = V49Adapter(module_path=cfg.module_path)
    factory = HandlerFactory(cfg.module_path)
    strategies = _normalize_strategies(cfg.strategies)
    results: JsonDict = {}
    effective_trade_date = _effective_trade_date(db_path=db, requested_date=cfg.date_to)
    effective_trade_date_iso = _iso_trade_date(effective_trade_date)
    initial_recommendation = _build_recommendation(db)
    formal_pool_strategies = [item.get("strategy") for item in initial_recommendation.get("eligible_pool") or []]

    for strategy in strategies:
        if strategy in RESEARCH_ONLY_STRATEGIES:
            results[strategy] = (
                _ensemble_core_research_only_result(db_path=db, as_of_date=effective_trade_date)
                if strategy == "ensemble_core"
                else _research_only_result(strategy)
            )
            continue
        adapter.register_scan_handler(strategy, factory.create_scan_handler(strategy))
        scan = adapter.run_scan(
            strategy,
            {
                "db_path": db,
                "trade_date": effective_trade_date_iso,
                "requested_trade_date": cfg.date_to,
                "score_threshold": int(get_profile(strategy).default_score_threshold),
                "offline_stock_limit": int(cfg.scan_offline_stock_limit),
                "limit": int(cfg.scan_limit),
            },
        )
        benchmark_contract: JsonDict = {}
        extra_runtime_params: JsonDict = {}
        if strategy == "stable":
            benchmark_contract = _build_stable_formal_pool_benchmark_contract(
                db_path=db,
                formal_pool_strategies=formal_pool_strategies,
                as_of_date=effective_trade_date,
                holding_days=int(get_profile(strategy).default_holding_days),
            )
            if benchmark_contract.get("available") is True:
                extra_runtime_params["formal_pool_returns_pct"] = list(
                    benchmark_contract.get("return_series_pct") or []
                )
                extra_runtime_params["formal_pool_benchmark_contract"] = benchmark_contract
        sweep = run_param_sweep(
            _sweep_config_for_strategy(
                cfg=cfg,
                strategy=strategy,
                db_path=db,
                date_to=effective_trade_date_iso,
                extra_runtime_params=extra_runtime_params,
            )
        )
        rejected_entry = _record_rejected_sweep_if_needed(
            strategy=strategy,
            sweep=sweep,
            ledger_path=cfg.rejected_ledger,
            operator_name=cfg.operator_name,
        )
        results[strategy] = {
            "strategy": strategy,
            "status": _strategy_status(scan=scan, sweep=sweep, rejected_entry=rejected_entry),
            "scan": _scan_summary(scan),
            "sweep": _sweep_summary(sweep),
            "rejected_entry": rejected_entry,
            **({"formal_pool_benchmark_contract": benchmark_contract} if benchmark_contract else {}),
        }

    if cfg.include_research_only:
        for strategy in all_strategy_names():
            if strategy in RESEARCH_ONLY_STRATEGIES and strategy not in results:
                results[strategy] = (
                    _ensemble_core_research_only_result(db_path=db, as_of_date=effective_trade_date)
                    if strategy == "ensemble_core"
                    else _research_only_result(strategy)
                )

    recommendation = _build_recommendation(db)
    stage_audit = run_stage_audit(
        db_path=db,
        trade_date="",
        rejected_artifacts_path=cfg.rejected_ledger,
        output_dir=str(cfg.output_dir),
    )
    payload: JsonDict = {
        "evidence_run_version": "all_strategy_evidence_run.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "requested_trade_date": str(cfg.date_to or ""),
        "effective_trade_date": str(effective_trade_date or ""),
        "effective_trade_date_iso": str(effective_trade_date_iso or ""),
        "strategies": list(results.keys()),
        "results": results,
        "rejected_ledger": cfg.rejected_ledger,
        "stage_audit_artifacts": stage_audit.get("artifacts") or {},
        "stage_audit_passed": bool(stage_audit.get("passed") is True),
        "unified_recommendation": {
            "passed": bool(recommendation.get("passed") is True),
            "blocking_reasons": list(recommendation.get("blocking_reasons") or []),
            "eligible_pool": [item.get("strategy") for item in recommendation.get("eligible_pool") or []],
            "observation_pool": [item.get("strategy") for item in recommendation.get("observation_pool") or []],
            "diagnostic_pool": [item.get("strategy") for item in recommendation.get("diagnostic_pool") or []],
            "top_strategies": [item.get("strategy") for item in recommendation.get("top_strategies") or []],
            "top_stocks": [item.get("ts_code") for item in recommendation.get("top_stocks") or []],
        },
        "strategy_optimization_backlog": _build_strategy_optimization_backlog(results),
        "top_strategy_policy": "only_emit_from_eligible_pool;do_not_pad_to_top3",
        "top_stock_policy": "only_aggregate_signal_items_from_top_strategies_with_signal_refs",
    }
    artifacts = _write_artifacts(cfg.output_dir, payload)
    payload["artifacts"] = artifacts
    _write_json(Path(artifacts["json"]), payload)
    return payload


def _effective_trade_date(*, db_path: str, requested_date: str) -> str:
    requested = str(requested_date or "").strip().replace("-", "")
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        row = conn.execute("SELECT MAX(trade_date) FROM daily_trading_data").fetchone()
    except sqlite3.Error:
        row = None
    finally:
        conn.close()
    latest = str((row or [""])[0] or "").strip().replace("-", "")
    if not latest:
        return requested
    if not requested:
        return latest
    return min(requested, latest)


def _iso_trade_date(value: str) -> str:
    raw = str(value or "").strip()
    compact = raw.replace("-", "")
    if len(compact) == 8 and compact.isdigit():
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
    return raw


def _normalize_strategies(strategies: Sequence[str]) -> list[str]:
    valid = set(all_strategy_names())
    out = []
    for raw in strategies:
        strategy = str(raw or "").strip().lower()
        if strategy and strategy in valid and strategy not in out:
            out.append(strategy)
    return out


def _sweep_config_for_strategy(
    *,
    cfg: AllStrategyEvidenceRunConfig,
    strategy: str,
    db_path: str,
    date_to: str | None = None,
    extra_runtime_params: Optional[JsonDict] = None,
) -> SweepConfig:
    profile = get_profile(strategy)
    thresholds = [int(profile.default_score_threshold)]
    if strategy == "v5":
        thresholds = [65, 70]
    elif strategy == "v4":
        thresholds = [60, 65]
    elif strategy == "v6":
        thresholds = [75, 80]
    elif strategy == "v7":
        thresholds = [60, 65]
    elif strategy == "v8":
        thresholds = [50, 55]
    elif strategy == "v9":
        thresholds = [65, 70]
    elif strategy == "combo":
        thresholds = [65]
    elif strategy == "stable":
        thresholds = [60, 65]
    sample_size = min(int(profile.default_sample_size), 50)
    holding_days = int(profile.default_holding_days)
    if strategy == "v5":
        holding_days = 3
    elif strategy in {"v6", "v8", "combo"}:
        holding_days = min(holding_days, 6)
    elif strategy == "v9":
        holding_days = 8
    max_stop_loss_pcts = [0.08]
    runtime_params: JsonDict = {}
    stop_losses = [None]
    take_profits = [None]
    if strategy == "v5":
        runtime_params = {"replay_step": 60, "max_evaluations": 180}
        stop_losses = [-0.025]
        take_profits = [0.04]
    elif strategy == "v4":
        runtime_params = {"replay_step": 60, "max_evaluations": 180}
        stop_losses = [-0.03, -0.04]
        take_profits = [0.04, 0.06]
    elif strategy == "v8":
        runtime_params = {"replay_step": 60, "max_evaluations": 180}
        max_stop_loss_pcts = [0.05, 0.06]
    elif strategy == "stable":
        runtime_params = {"stable_step": 60, "max_evaluations": 180, "stable_window": 90}
    elif strategy == "combo":
        runtime_params = {"combo_replay_step": 60, "max_evaluations": 180}
        max_stop_loss_pcts = [0.05, 0.08]
    elif strategy == "v6":
        runtime_params = {"replay_step": 10, "max_evaluations": 240}
    elif strategy == "v7":
        runtime_params = {"replay_step": 20, "max_evaluations": 240}
    runtime_params.update(dict(extra_runtime_params or {}))
    return SweepConfig(
        strategy=strategy,
        module_path=cfg.module_path,
        output_dir=cfg.output_dir,
        date_from=cfg.date_from,
        date_to=str(date_to or cfg.date_to),
        mode="rolling",
        train_window_days=int(cfg.train_window_days),
        test_window_days=int(cfg.test_window_days),
        step_days=int(cfg.step_days),
        score_thresholds=thresholds,
        sample_sizes=[sample_size],
        holding_days=[holding_days],
        max_stop_loss_pcts=max_stop_loss_pcts,
        stop_losses=stop_losses,
        take_profits=take_profits,
        db_path=db_path,
        max_runs=cfg.sweep_max_runs,
        per_run_timeout_sec=cfg.per_run_timeout_sec,
        runtime_params=runtime_params,
    )


def _build_stable_formal_pool_benchmark_contract(
    *,
    db_path: str,
    formal_pool_strategies: Sequence[Any],
    as_of_date: str,
    holding_days: int,
) -> JsonDict:
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        return build_formal_pool_benchmark_return_series(
            conn,
            strategies=[str(item or "") for item in formal_pool_strategies],
            as_of_date=str(as_of_date or ""),
            holding_days=int(holding_days or 0),
            top_n_per_strategy=5,
        )
    finally:
        conn.close()


def _record_rejected_sweep_if_needed(
    *,
    strategy: str,
    sweep: JsonDict,
    ledger_path: str,
    operator_name: str,
) -> Optional[JsonDict]:
    artifact_path = str((sweep.get("artifacts") or {}).get("json") or "")
    reasons = _rejection_reasons(sweep=sweep, artifact_path=artifact_path)
    if not reasons:
        return None
    return append_rejected_backtest_artifact(
        ledger_path,
        artifact_path=artifact_path,
        strategy=strategy,
        reason=",".join(reasons),
        reused_as_runtime_default=False,
        source_run_id=str(sweep.get("run_id") or ""),
        operator_name=operator_name,
        note="auto_recorded_by_all_strategy_evidence_run",
    )


def _rejection_reasons(*, sweep: JsonDict, artifact_path: str) -> list[str]:
    diagnostics = sweep.get("strategy_backtest_diagnostics") if isinstance(sweep.get("strategy_backtest_diagnostics"), dict) else {}
    credibility = sweep.get("backtest_credibility") if isinstance(sweep.get("backtest_credibility"), dict) else {}
    reasons: list[str] = []
    if str(sweep.get("status")) != "success":
        reasons.append("sweep_status_failed")
    if diagnostics.get("eligible_for_formal_ranking") is not True:
        reasons.append("eligible_for_formal_ranking_false")
    if diagnostics.get("credible_evidence_present") is not True:
        reasons.append("credible_evidence_false")
    if diagnostics.get("quality_floor_passed") is not True:
        reasons.append("quality_floor_failed")
    if not credibility:
        reasons.append("missing_backtest_credibility")
    if not artifact_path:
        reasons.append("missing_sweep_artifact")
    return reasons


def _strategy_status(*, scan: JsonDict, sweep: JsonDict, rejected_entry: Optional[JsonDict]) -> str:
    if str(scan.get("status")) != "success":
        return "diagnostic_scan_failed"
    diagnostics = sweep.get("strategy_backtest_diagnostics") if isinstance(sweep.get("strategy_backtest_diagnostics"), dict) else {}
    if diagnostics.get("eligible_for_formal_ranking") is True:
        return "eligible_evidence_ready"
    if diagnostics.get("credible_evidence_present") is True and diagnostics.get("quality_floor_passed") is not True:
        return "observation"
    if rejected_entry is not None:
        return "diagnostic_rejected"
    return "observation"


def _scan_summary(scan: JsonDict) -> JsonDict:
    result = scan.get("result") if isinstance(scan.get("result"), dict) else {}
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
    return {
        "run_id": str(scan.get("run_id") or ""),
        "status": str(scan.get("status") or ""),
        "item_count": int(metrics.get("count", 0) or 0),
        "data_version": str(scan.get("data_version") or ""),
        "code_version": str(scan.get("code_version") or ""),
        "param_version": str(scan.get("param_version") or ""),
        "error": str(scan.get("error") or ""),
    }


def _sweep_summary(sweep: JsonDict) -> JsonDict:
    diagnostics = sweep.get("strategy_backtest_diagnostics") if isinstance(sweep.get("strategy_backtest_diagnostics"), dict) else {}
    best = sweep.get("best") if isinstance(sweep.get("best"), dict) else {}
    return {
        "run_id": str(sweep.get("run_id") or ""),
        "status": str(sweep.get("status") or ""),
        "artifacts": sweep.get("artifacts") or {},
        "eligible_for_formal_ranking": bool(diagnostics.get("eligible_for_formal_ranking") is True),
        "credible_evidence_present": bool(diagnostics.get("credible_evidence_present") is True),
        "quality_floor_passed": bool(diagnostics.get("quality_floor_passed") is True),
        "failure_classes": list(diagnostics.get("failure_classes") or []),
        "best": {
            "score_threshold": best.get("score_threshold"),
            "sample_size": best.get("sample_size"),
            "holding_days": best.get("holding_days"),
            "win_rate": best.get("win_rate"),
            "max_drawdown": best.get("max_drawdown"),
            "signal_density": best.get("signal_density"),
        },
    }


def _research_only_result(strategy: str) -> JsonDict:
    if strategy == "ensemble_core":
        contract = build_ensemble_core_contract_review({})
        return {
            "strategy": strategy,
            "status": "research_only",
            "reason": "top_level_multi_alpha_portfolio_contract_missing",
            "promotion_blocked": True,
            "eligible_for_formal_ranking": False,
            "required_to_compete": list(contract.get("blocking_reasons") or []),
            "research_contract": contract,
            "scan": {},
            "sweep": {},
            "rejected_entry": None,
        }
    return {
        "strategy": strategy,
        "status": "research_only",
        "reason": "no_real_runtime_backtest_handler_or_explainable_fact_chain",
        "promotion_blocked": True,
        "eligible_for_formal_ranking": False,
        "required_to_compete": list(AI_RESEARCH_ONLY_REQUIREMENTS),
        "scan": {},
        "sweep": {},
        "rejected_entry": None,
    }


def _ensemble_core_research_only_result(*, db_path: str, as_of_date: str) -> JsonDict:
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        fact_chain = build_ensemble_alpha_sleeve_fact_chain(
            conn,
            as_of_date=str(as_of_date or ""),
            strategies=["v4", "v5", "v8", "v9", "combo", "v6", "v7"],
            holding_days=5,
            top_n_per_strategy=20,
        )
        shadow_portfolio = build_ensemble_shadow_portfolio(fact_chain)
        execution_cost_replay = build_ensemble_execution_cost_replay(
            conn,
            shadow_portfolio,
            as_of_date=str(as_of_date or ""),
            holding_days=5,
        )
        formal_pool_benchmark = build_formal_pool_benchmark_return_series(
            conn,
            strategies=FORMAL_POOL_BENCHMARK_STRATEGIES,
            as_of_date=str(as_of_date or ""),
            holding_days=5,
            top_n_per_strategy=5,
        )
    finally:
        conn.close()
    alpha_rebuild_lab = build_ensemble_alpha_rebuild_lab(fact_chain)
    walk_forward_benchmark = build_ensemble_walk_forward_shadow_benchmark(
        [
            {
                "as_of_date": str(as_of_date or ""),
                "shadow_portfolio": shadow_portfolio,
                "execution_cost_replay": execution_cost_replay,
                "formal_pool_benchmark": formal_pool_benchmark,
            }
        ]
    )
    contract = build_ensemble_core_contract_review(
        {
            "alpha_sleeves": [
                sleeve
                for sleeve, review in (fact_chain.get("sleeves") or {}).items()
                if int((review or {}).get("active_signal_count", 0) or 0) > 0
            ],
            "pit_inputs": _ensemble_core_pit_inputs_from_fact_chain(fact_chain),
            "attribution": _ensemble_core_attribution_from_fact_chain(fact_chain),
            "runtime_scan_handler": False,
            "runtime_backtest_handler": False,
            "walk_forward_backtest": False,
            "formal_pool_shadow_benchmark": False,
        }
    )
    return {
        "strategy": "ensemble_core",
        "status": "research_only",
        "reason": "top_level_multi_alpha_portfolio_contract_missing",
        "promotion_blocked": True,
        "eligible_for_formal_ranking": False,
        "required_to_compete": list(contract.get("blocking_reasons") or []),
        "research_contract": contract,
        "alpha_sleeve_fact_chain": fact_chain,
        "alpha_rebuild_lab": alpha_rebuild_lab,
        "shadow_portfolio": shadow_portfolio,
        "execution_cost_replay": execution_cost_replay,
        "formal_pool_benchmark": formal_pool_benchmark,
        "walk_forward_shadow_benchmark": walk_forward_benchmark,
        "scan": {},
        "sweep": {},
        "rejected_entry": None,
    }


def _ensemble_core_pit_inputs_from_fact_chain(fact_chain: JsonDict) -> list[str]:
    mapped: set[str] = set()
    for item in fact_chain.get("sample_facts") or []:
        features = (item or {}).get("tushare_pro_alpha_features") or {}
        inputs = set(str(value or "") for value in features.get("pit_inputs") or [])
        if "price_volume" in inputs:
            mapped.add("price_volume")
        if "money_flow" in inputs:
            mapped.add("money_flow")
        if "sector_heat" in inputs:
            mapped.add("sector_heat")
        if "volume_capacity" in inputs:
            mapped.add("volume_capacity")
        if "event_risk" in inputs:
            mapped.add("suspension_limit")
    return sorted(mapped)


def _ensemble_core_attribution_from_fact_chain(fact_chain: JsonDict) -> list[str]:
    out: set[str] = set()
    sleeves = fact_chain.get("sleeves") or {}
    if any((review or {}).get("ic_available") is True for review in sleeves.values()):
        out.add("alpha_ic")
    if fact_chain.get("multi_horizon_decay"):
        out.add("alpha_decay")
    correlation = fact_chain.get("sleeve_correlation") or {}
    if correlation:
        out.add("alpha_correlation")
    return sorted(out)


def _build_strategy_optimization_backlog(results: JsonDict) -> JsonDict:
    return {
        "policy": "prioritize_repair_candidates_by_evidence;do_not_optimize_all_strategies_blindly",
        "repair_candidate": [
            _backlog_item(strategy=strategy, result=results.get(strategy), intent=_repair_intent(strategy))
            for strategy in REPAIR_CANDIDATE_STRATEGIES
            if strategy in results
        ],
        "legacy_or_diagnostic": [
            _backlog_item(strategy=strategy, result=results.get(strategy), intent=_legacy_intent(strategy))
            for strategy in LEGACY_OR_DIAGNOSTIC_STRATEGIES
            if strategy in results
        ],
        "research_only": [
            _backlog_item(strategy=strategy, result=results.get(strategy), intent=_research_only_intent(strategy))
            for strategy in RESEARCH_ONLY_STRATEGIES
            if strategy in results
        ],
    }


def _backlog_item(*, strategy: str, result: Any, intent: str) -> JsonDict:
    if not isinstance(result, dict):
        return {
            "strategy": strategy,
            "status": "missing_from_evidence_run",
            "next_action": "run_strategy_evidence_chain",
            "intent": intent,
            "blocking_reasons": ["missing_evidence_result"],
        }
    sweep = result.get("sweep") if isinstance(result.get("sweep"), dict) else {}
    blocking_reasons = list(sweep.get("failure_classes") or [])
    if result.get("status") == "research_only":
        blocking_reasons = list(result.get("required_to_compete") or [])
    next_action = _next_action_for_backlog_item(strategy=strategy, result=result, blocking_reasons=blocking_reasons)
    return {
        "strategy": strategy,
        "status": str(result.get("status") or ""),
        "next_action": next_action,
        "intent": intent,
        "eligible_for_formal_ranking": bool(sweep.get("eligible_for_formal_ranking") is True),
        "credible_evidence_present": bool(sweep.get("credible_evidence_present") is True),
        "quality_floor_passed": bool(sweep.get("quality_floor_passed") is True),
        "scan_run_id": str((result.get("scan") or {}).get("run_id") or ""),
        "sweep_run_id": str(sweep.get("run_id") or ""),
        "sweep_artifact": str((sweep.get("artifacts") or {}).get("json") or ""),
        "blocking_reasons": blocking_reasons,
        **(
            {"formal_pool_benchmark_blocking_reasons": list((result.get("formal_pool_benchmark_contract") or {}).get("blocking_reasons") or [])}
            if isinstance(result.get("formal_pool_benchmark_contract"), dict)
            else {}
        ),
    }


def _next_action_for_backlog_item(*, strategy: str, result: JsonDict, blocking_reasons: Sequence[str]) -> str:
    if result.get("status") == "research_only":
        if strategy == "ensemble_core":
            return "build_multi_alpha_portfolio_contract_before_any_formal_competition"
        return "build_research_to_runtime_fact_chain_before_any_competition"
    if result.get("status") == "eligible_evidence_ready":
        return "enter_unified_formal_competition_and_collect_shadow_execution_evidence"
    if strategy == "v8":
        return "controlled_sweep_factor_distribution_and_tail_loss_stop_loss"
    if strategy == "stable":
        return "rebuild_as_defensive_allocator_overlay_and_evaluate_portfolio_drawdown_reduction"
    if strategy == "combo":
        return "explain_consensus_breakpoints_without_lowering_thresholds"
    if "zero_signal_density" in blocking_reasons:
        return "diagnose_threshold_near_misses_signal_density_and_runtime_constraints"
    if "runtime_timeout" in blocking_reasons:
        return "profile_runtime_handler_before_strategy_optimization"
    return "keep_in_diagnostic_pool_until_evidence_gap_is_closed"


def _repair_intent(strategy: str) -> str:
    if strategy == "v8":
        return "repair signal suppression and tail-loss risk under controlled sweep"
    if strategy == "stable":
        return "rebuild stable as defensive allocator overlay, not standalone alpha tuning"
    if strategy == "combo":
        return "explain consensus breakpoint without lowering agreement thresholds"
    return "repair only if evidence supports a bounded hypothesis"


def _legacy_intent(strategy: str) -> str:
    if strategy in {"v4", "v7"}:
        return "legacy baseline; explain failure and runtime cost before any promotion"
    if strategy == "v6":
        return "research observation only; PIT/noise/near-threshold review before candidate"
    return "diagnostic pool; do not blind tune for leaderboard"


def _research_only_intent(strategy: str) -> str:
    if strategy == "ensemble_core":
        return "build top-level multi-alpha risk-budgeted portfolio research line; v6/v7 are alpha donors only"
    return "build real runtime backtest handler and explainable fact chain first"


def _build_recommendation(db_path: str) -> JsonDict:
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        return build_unified_system_recommendation(conn, trade_date="")
    finally:
        conn.close()


def _write_artifacts(output_dir: Path, payload: JsonDict) -> JsonDict:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"all_strategy_evidence_run_{ts}.json"
    md_path = output_dir / f"all_strategy_evidence_run_{ts}.md"
    _write_json(json_path, payload)
    lines = [
        "# All Strategy Evidence Run",
        "",
        f"- strategies: `{','.join(payload.get('strategies') or [])}`",
        f"- eligible_pool: `{','.join(payload.get('unified_recommendation', {}).get('eligible_pool') or [])}`",
        f"- top_strategies: `{','.join(payload.get('unified_recommendation', {}).get('top_strategies') or [])}`",
        f"- top_stocks: `{','.join(payload.get('unified_recommendation', {}).get('top_stocks') or [])}`",
        f"- stage_audit_passed: `{payload.get('stage_audit_passed')}`",
        "",
        "## Optimization Backlog",
        "",
    ]
    backlog = payload.get("strategy_optimization_backlog") if isinstance(payload.get("strategy_optimization_backlog"), dict) else {}
    for bucket in ("repair_candidate", "legacy_or_diagnostic", "research_only"):
        strategies = [str(item.get("strategy") or "") for item in backlog.get(bucket) or []]
        lines.append(f"- `{bucket}`: `{','.join([s for s in strategies if s])}`")
    lines.extend([
        "",
        "## Strategy Status",
        "",
    ])
    for strategy, result in (payload.get("results") or {}).items():
        lines.append(f"- `{strategy}` status=`{result.get('status')}`")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def _write_json(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
