"""Run daily OpenClaw workflow for v49 strategy stack.

Pipeline:
1) scan
2) merge_signals
3) backtest
4) risk_check
5) generate_report
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
import json
import sys
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.dao import db_conn, resolve_db_path, latest_trade_date
from data.migrations.runner import apply_migrations
from backtest.engine import BacktestEngine
from openclaw.adapters import V49Adapter
from openclaw.publish import NotificationPublisher
from openclaw.runtime.v49_handlers import HandlerFactory
from openclaw.assistant import OpenClawStockAssistant
from openclaw.overnight_decision import (
    apply_trade_window_analysis,
    apply_feature_enrichment,
    build_validation_review,
    build_overnight_decision,
    export_execution_feedback_template,
    export_team_overnight_report,
    load_active_holdings,
    load_return_calibration,
    next_trade_date,
    persist_overnight_decision,
    refresh_realized_outcomes,
    seed_execution_feedback,
    summarize_validation_streak,
)
from openclaw.strategy_tracking import generate_scoreboard, record_signals_from_summary, refresh_performance
from openclaw.research.v4_factor_research import analyze_factor_decay, calibrate_v4_weights
from openclaw.runtime.root_dependency_bridge import load_kelly_position_manager_class
from strategies.center_config import (
    apply_risk_overrides,
    load_center_config,
    resolve_runtime_params,
    resolve_strategy_weight,
)
from strategies.registry import get_profile, ui_primary_strategies, production_strategies, all_strategy_names
from risk.engine import combine_risk
from trading_kernel.governance_audit import record_governance_audit
from tools.trading_kernel_status import build_trading_kernel_status
from utils_json_logger import setup_json_logger

DEFAULT_V49_MODULE_PATH = str(ROOT / "v49_app.py")
LOGGER = setup_json_logger("openclaw.run_daily")


def _json_safe(value: Any) -> Any:
    """Convert runtime objects (e.g. pandas/numpy) to JSON-serializable values."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "to_dict"):
        try:
            return _json_safe(value.to_dict())
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run OpenClaw daily orchestration")
    parser.add_argument(
        "--strategy",
        default="v5",
        choices=["v4", "v5", "v6", "v7", "v8", "v9", "stable", "combo", "ai"],
        help="primary strategy (production: v9/v8/v5/combo; experimental: v4/v6/v7/stable/ai)",
    )
    parser.add_argument("--module-path", default=DEFAULT_V49_MODULE_PATH, help="v49 module path")
    parser.add_argument("--policy-config", default="openclaw/config/policy.yaml", help="policy config path")
    parser.add_argument("--risk-config", default="openclaw/config/risk_thresholds.yaml", help="risk config path")
    parser.add_argument(
        "--strategy-center-config",
        default="openclaw/config/strategy_center.yaml",
        help="strategy center runtime config (best params/weights/risk overrides)",
    )
    parser.add_argument("--score-threshold", type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--holding-days", type=int, default=None)
    parser.add_argument("--offline-stock-limit", type=int, default=300)
    parser.add_argument("--output-dir", default="logs/openclaw")
    parser.add_argument("--publish", action="store_true", help="request sending notifications")
    parser.add_argument("--approve-publish", action="store_true", help="explicit human approval for publish step")
    parser.add_argument(
        "--governance-override",
        action="store_true",
        help="explicit human override for governance halt; requires operator and override reason",
    )
    parser.add_argument("--override-reason", default="", help="required reason when governance override is used")
    parser.add_argument("--operator-name", default="", help="operator name for governance override audit")
    parser.add_argument(
        "--force-publish-on-risk",
        action="store_true",
        help="override risk gate and allow publish even when risk is orange/red",
    )
    parser.add_argument("--notify-config", default="openclaw/config/notify.yaml", help="notify config path")
    parser.add_argument("--use-demo", action="store_true", help="force demo handlers")
    parser.add_argument("--apply-migrations", action="store_true", help="apply sqlite migrations before run")
    parser.add_argument("--backtest-mode", default="rolling", choices=["rolling", "single"])
    parser.add_argument("--train-window-days", type=int, default=180)
    parser.add_argument("--test-window-days", type=int, default=60)
    parser.add_argument("--step-days", type=int, default=60)
    parser.add_argument("--enable-kelly", action="store_true", help="use Kelly model for target weights")
    parser.add_argument("--run-v4-research", action="store_true", help="run v4 IC calibration and factor decay")
    args = parser.parse_args()
    if args.governance_override and not str(args.override_reason or "").strip():
        parser.error("--governance-override requires --override-reason")
    if args.governance_override and not str(args.operator_name or "").strip():
        parser.error("--governance-override requires --operator-name")
    profile = get_profile(args.strategy)
    center_cfg = load_center_config(Path(args.strategy_center_config))
    resolved_runtime = resolve_runtime_params(
        strategy=args.strategy,
        requested_score_threshold=args.score_threshold,
        requested_sample_size=args.sample_size,
        requested_holding_days=args.holding_days,
        center_config=center_cfg,
        project_root=ROOT,
    )
    args.score_threshold = int(resolved_runtime["score_threshold"])
    args.sample_size = int(resolved_runtime["sample_size"])
    args.holding_days = int(resolved_runtime["holding_days"])

    if args.apply_migrations:
        mig = apply_migrations()
        LOGGER.info("migrations_applied", extra={"extra_fields": {"count": mig.get("count", 0)}})

    policy = _load_yaml_or_default(Path(args.policy_config), _default_policy())
    thresholds = _load_yaml_or_default(Path(args.risk_config), _default_thresholds())
    notify_cfg = _load_yaml_or_default(Path(args.notify_config), _default_notify_config())

    _assert_policy(args.strategy, policy)

    prod = set(_policy_production_strategies(policy))
    if args.strategy not in prod:
        LOGGER.warning(
            "experimental_strategy",
            extra={"extra_fields": {
                "strategy": args.strategy,
                "note": "此策略为实验区策略, 不在生产默认流水线中",
                "production": sorted(prod),
            }},
        )

    adapter = V49Adapter(module_path=Path(args.module_path))

    if args.use_demo:
        _register_demo_handlers(adapter, args.strategy)
    else:
        _register_real_handlers(adapter, args.strategy)

    scan_params = {
        "score_threshold": args.score_threshold,
        "limit": 30,
        "offline_stock_limit": args.offline_stock_limit,
    }
    scan_result = adapter.run_scan(args.strategy, scan_params)
    picks = (scan_result.get("result") or {}).get("picks", [])

    merged = adapter.merge_signals(picks, _weights_for_strategy(args.strategy, center_cfg))

    date_to = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    bt_engine = BacktestEngine(adapter)
    backtest_result = bt_engine.run(
        args.strategy,
        date_from,
        date_to,
        {
            "sample_size": args.sample_size,
            "score_threshold": args.score_threshold,
            "holding_days": args.holding_days,
            "mode": args.backtest_mode,
            "train_window_days": args.train_window_days,
            "test_window_days": args.test_window_days,
            "step_days": args.step_days,
        },
    )

    summary = ((backtest_result.get("result") or {}).get("summary") or {})
    strategy_thresholds = _resolve_thresholds(args.strategy, thresholds)
    strategy_thresholds = apply_risk_overrides(args.strategy, strategy_thresholds, center_cfg)
    system_health = _collect_system_health()
    risk_v2_obj = combine_risk(
        market_stats=summary,
        thresholds=strategy_thresholds,
        system_health=system_health,
    )
    risk = {
        "market_risk": risk_v2_obj.market_risk,
        "system_risk": risk_v2_obj.system_risk,
        "risk_level": risk_v2_obj.risk_level,
        "triggered_rules": risk_v2_obj.triggered_rules,
        "recommended_actions": risk_v2_obj.recommended_actions,
        "evidence": risk_v2_obj.evidence,
    }
    validation_review = _build_validation_review(output_dir=Path(args.output_dir))
    validation_streak = summarize_validation_streak(output_dir=Path(args.output_dir), lookback_files=10)
    trading_kernel_status = _build_trading_kernel_status(output_dir=Path(args.output_dir))
    next_action = _resolve_next_action(
        risk=risk,
        validation_review=validation_review,
        trading_kernel_status=trading_kernel_status,
        validation_streak=validation_streak,
    )
    effective_next_action = _apply_governance_override(
        next_action=next_action,
        governance_override=bool(args.governance_override),
    )
    governance_override_applied = bool(args.governance_override and effective_next_action != next_action)
    fallback_plan = _build_fallback_plan(
        strategy=args.strategy,
        risk=risk,
        current_params={
            "score_threshold": args.score_threshold,
            "sample_size": args.sample_size,
            "holding_days": args.holding_days,
            "offline_stock_limit": args.offline_stock_limit,
        },
        validation_review=validation_review,
        validation_streak=validation_streak,
        trading_kernel_status=trading_kernel_status,
        next_action=effective_next_action,
        governance_override_applied=governance_override_applied,
    )

    primary_strategies = set(_policy_primary_strategies(policy))
    opportunities = merged.get("ranked_list", [])[:20]
    opportunities = _apply_positioning(
        opportunities=opportunities,
        enable_kelly=args.enable_kelly,
    )
    opportunities = _apply_ui_markers(opportunities=opportunities, primary_strategies=primary_strategies)
    # Hard safety gate: red risk should not output actionable picks by default.
    if fallback_plan.get("execution_mode") == "halted":
        opportunities = []

    if fallback_plan.get("execution_mode") == "halted" and not opportunities:
        overnight_decision = {
            "trade_date": next_trade_date(datetime.now().strftime("%Y-%m-%d")),
            "recommendations": [],
            "holding_comparisons": [],
            "position_decisions": [],
            "policy_note": "halted mode: skipped persistence for empty overnight batch",
            "persisted": {
                "recommendations": 0,
                "position_decisions": 0,
                "activation_ok": False,
                "activation_message": "skipped persistence because execution_mode=halted and opportunities=0",
            },
            "skipped_persistence": True,
        }
    else:
        overnight_decision = _build_and_persist_overnight_decision(
            opportunities=opportunities,
            risk=risk,
            output_dir=Path(args.output_dir),
            validation_review=validation_review,
        )

    scan_publish_gate = _build_scan_publish_gate(output_dir=Path(args.output_dir))
    publish_preview = _build_publish_preview(
        strategy=args.strategy,
        risk_level=str(risk.get("risk_level") or ""),
        scan_status=str(scan_result.get("status") or ""),
        backtest_status=str(backtest_result.get("status") or ""),
        picks=len(opportunities),
        report_markdown="pending",
        execution_mode=str(fallback_plan.get("execution_mode", "normal")),
        scan_publish_gate=scan_publish_gate,
        next_action=effective_next_action,
    )

    report = adapter.generate_report(
        "daily_brief",
        {
            "summary": {
                "strategy": args.strategy,
                "strategy_ui_tag": profile.ui_tag,
                "strategy_focus": "primary" if args.strategy in primary_strategies else "secondary",
                "primary_strategies": sorted(primary_strategies),
                "scan_status": scan_result.get("status"),
                "backtest_status": backtest_result.get("status"),
                "risk_level": risk.get("risk_level"),
                "count": len(opportunities),
                "execution_mode": fallback_plan.get("execution_mode", "normal"),
                "overnight_trade_date": overnight_decision.get("trade_date", ""),
                "validation_gates": validation_review.get("gates", []),
                "validation_streak": validation_streak.get("consecutive_severe_runs", 0),
            },
            "opportunities": opportunities,
            "overnight_decision": overnight_decision,
            "validation_review": validation_review,
            "validation_streak": validation_streak,
            "scan_publish_gate": scan_publish_gate,
            "publish_preview": publish_preview,
        },
        output_dir=Path(args.output_dir),
    )
    publish_preview["body"] = (
        f"scan={scan_result.get('status')} backtest={backtest_result.get('status')} "
        f"picks={len(opportunities)} execution_mode={fallback_plan.get('execution_mode', 'normal')} "
        f"report={report.get('markdown')}"
    )

    publish_result = _maybe_publish(
        enabled=args.publish,
        approved=args.approve_publish,
        force_publish_on_risk=args.force_publish_on_risk,
        policy=policy,
        notify_cfg=notify_cfg,
        summary={
            "strategy": args.strategy,
            "scan_status": scan_result.get("status"),
            "backtest_status": backtest_result.get("status"),
            "risk_level": risk.get("risk_level"),
            "report": report.get("markdown"),
            "picks": len(opportunities),
            "execution_mode": fallback_plan.get("execution_mode", "normal"),
            "next_action": effective_next_action,
            "publish_preview_title": publish_preview.get("title"),
            "publish_preview_body": publish_preview.get("body"),
        },
        scan_publish_gate=scan_publish_gate,
        next_action=effective_next_action,
    )
    validation_alert = _maybe_emit_validation_alert(
        notify_cfg=notify_cfg,
        validation_review=validation_review,
        validation_streak=validation_streak,
        strategy=args.strategy,
        output_dir=Path(args.output_dir),
    )

    run_summary = {
        "strategy": args.strategy,
        "strategy_ui_tag": profile.ui_tag,
        "strategy_is_primary": args.strategy in primary_strategies,
        "primary_strategies": sorted(primary_strategies),
        "runtime_params": {
            "score_threshold": args.score_threshold,
            "sample_size": args.sample_size,
            "holding_days": args.holding_days,
            "source": resolved_runtime.get("source", {}),
        },
        "scan": scan_result,
        "opportunities": opportunities,
        "backtest": backtest_result,
        "risk": risk,
        "validation_review": validation_review,
        "validation_streak": validation_streak,
        "trading_kernel_status": trading_kernel_status,
        "next_action": next_action,
        "effective_next_action": effective_next_action,
        "governance_override": {
            "requested": bool(args.governance_override),
            "applied": governance_override_applied,
            "operator_name": str(args.operator_name or "").strip(),
            "override_reason": str(args.override_reason or "").strip(),
        },
        "fallback_plan": fallback_plan,
        "scan_publish_gate": scan_publish_gate,
        "publish_preview": publish_preview,
        "overnight_decision": overnight_decision,
        "report": report,
        "publish": publish_result,
        "validation_alert": validation_alert,
        "policy_mode": policy.get("mode", "unknown"),
    }

    summary_path = Path(args.output_dir) / f"run_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(_json_safe(run_summary), ensure_ascii=False, indent=2), encoding="utf-8")
    tracking_result = _run_signal_tracking(summary_path=summary_path, output_dir=Path(args.output_dir))
    run_summary["tracking"] = tracking_result
    run_summary["research"] = _maybe_run_research(args=args, output_dir=Path(args.output_dir))
    governance_audit = _record_governance_audit_records(
        strategy=args.strategy,
        account_id="default",
        decision_date=str(trading_kernel_status.get("snapshot_date") or overnight_decision.get("trade_date") or ""),
        next_action=next_action,
        effective_next_action=effective_next_action,
        fallback_plan=fallback_plan,
        publish_result=publish_result,
        risk=risk,
        validation_review=validation_review,
        trading_kernel_status=trading_kernel_status,
        governance_override_requested=bool(args.governance_override),
        operator_name=str(args.operator_name or "").strip(),
        override_reason=str(args.override_reason or "").strip(),
        run_summary_path=str(summary_path),
        publish_requested=bool(args.publish),
    )
    run_summary["governance_audit"] = governance_audit
    summary_path.write_text(json.dumps(_json_safe(run_summary), ensure_ascii=False, indent=2), encoding="utf-8")
    _run_self_learning_after_daily(output_dir=Path(args.output_dir), db_path=str(resolve_db_path()))
    _append_daily_metrics_jsonl(
        output_dir=Path(args.output_dir),
        strategy=args.strategy,
        scan_result=scan_result,
        risk=risk,
        fallback_plan=fallback_plan,
        publish_result=publish_result,
    )

    LOGGER.info(
        "run_completed",
        extra={
            "extra_fields": {
                "strategy": args.strategy,
                "risk_level": risk.get("risk_level"),
                "market_risk": risk.get("market_risk"),
                "system_risk": risk.get("system_risk"),
                "summary_path": str(summary_path),
            }
        },
    )

    print(f"strategy: {args.strategy}")
    print(f"scan status: {scan_result.get('status')}")
    print(f"backtest status: {backtest_result.get('status')}")
    print(f"risk level: {risk.get('risk_level')}")
    print(f"next_action: {next_action}")
    print(f"effective_next_action: {effective_next_action}")
    print(f"report: {report.get('markdown')}")
    print(f"publish: {publish_result.get('status')}")
    print(f"summary: {summary_path}")
    return 0


def _run_self_learning_after_daily(output_dir: Path, db_path: str) -> None:
    try:
        learner = OpenClawStockAssistant(log_dir=str(output_dir), db_path=db_path)
        learner.record_module_outcome(
            module="openclaw_run_daily",
            event_type="pipeline_completed",
            payload={"status": "ok", "output_dir": str(output_dir)},
            route="stock_core",
        )
        force_weekly = datetime.now().weekday() == 6
        learner.run_self_learning_cycle(force_weekly=force_weekly)
        try:
            from trading_assistant import TradingAssistant
            ta = TradingAssistant(db_path=db_path)
            ta.apply_auto_tuning()
        except Exception:
            pass
    except Exception:
        # Keep main daily workflow robust even if self-learning hook fails.
        pass


def _build_and_persist_overnight_decision(
    *,
    opportunities: List[Dict[str, Any]],
    risk: Dict[str, Any],
    output_dir: Path,
    validation_review: Dict[str, Any],
) -> Dict[str, Any]:
    decision_date = datetime.now().strftime("%Y-%m-%d")
    payload: Dict[str, Any] = {
        "trade_date": next_trade_date(decision_date),
        "recommendations": [],
        "holding_comparisons": [],
        "position_decisions": [],
        "policy_note": "仅输出研究与人工执行建议，不包含任何规避券商监控、拆单规避或真实下单能力。",
    }
    try:
        with db_conn(str(resolve_db_path())) as conn:
            opportunities = apply_feature_enrichment(conn, opportunities=opportunities)
            active_holdings = load_active_holdings(conn)
            calibration = load_return_calibration(conn, horizon_days=1, lookback_days=180, min_samples=6)
            payload = build_overnight_decision(
                trade_date=next_trade_date(decision_date),
                opportunities=opportunities,
                active_holdings=active_holdings,
                risk=risk,
                calibration=calibration,
                top_n=2,
            )
            payload = apply_trade_window_analysis(conn, payload=payload, lookback_days=20)
            persisted = persist_overnight_decision(
                conn,
                decision_date=decision_date,
                payload=payload,
                activate=True,
                approved_by="system",
                release_note="overnight scheduled publish",
                output_dir=output_dir,
                current_primary="v5",
            )
            payload["persisted"] = persisted
            payload["feedback_seed"] = seed_execution_feedback(
                conn,
                decision_date=decision_date,
                payload=payload,
            )
            payload["realized_outcomes_refresh"] = refresh_realized_outcomes(conn, lookback_days=60)
            payload["team_report"] = export_team_overnight_report(
                output_dir=output_dir,
                decision_date=decision_date,
                payload=payload,
                risk=risk,
                validation_review=validation_review,
                validation_streak=summarize_validation_streak(output_dir=output_dir, lookback_files=10),
            )
            payload["feedback_template"] = export_execution_feedback_template(
                output_dir=output_dir,
                decision_date=decision_date,
                payload=payload,
            )
    except Exception as exc:
        payload["error"] = str(exc)
    return payload


def _register_real_handlers(adapter: V49Adapter, strategy: str) -> None:
    factory = HandlerFactory(adapter.module_path)
    adapter.register_scan_handler(strategy, factory.create_scan_handler(strategy))
    adapter.register_backtest_handler(strategy, factory.create_backtest_handler(strategy))


def _register_demo_handlers(adapter: V49Adapter, strategy: str) -> None:
    def demo_scan(params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "picks": [
                {"ts_code": "600000.SH", "score": 86.0, "strategy": strategy, "reason": "demo"},
                {"ts_code": "000001.SZ", "score": 82.0, "strategy": strategy, "reason": "demo"},
            ],
            "metrics": {"count": 2},
        }

    def demo_backtest(params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "summary": {
                "win_rate": 0.5,
                "max_drawdown": 0.08,
                "signal_density": 0.03,
            },
            "params": params,
        }

    adapter.register_scan_handler(strategy, demo_scan)
    adapter.register_backtest_handler(strategy, demo_backtest)


def _load_yaml_or_default(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return default
    try:
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else default
        import yaml  # type: ignore

        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else default
    except Exception:
        # Keep orchestration runnable even if yaml dependency is unavailable.
        try:
            data = _parse_simple_yaml(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else default
        except Exception:
            return default


def _resolve_thresholds(strategy: str, all_thresholds: Dict[str, Any]) -> Dict[str, Any]:
    base = {
        "win_rate_min": float(all_thresholds.get("win_rate_min", 0.45)),
        "max_drawdown_max": float(all_thresholds.get("max_drawdown_max", 0.12)),
        "signal_density_min": float(all_thresholds.get("signal_density_min", 0.02)),
    }
    overrides = all_thresholds.get("strategy_overrides", {})
    this = overrides.get(strategy, {}) if isinstance(overrides, dict) else {}
    for k in ["win_rate_min", "max_drawdown_max", "signal_density_min"]:
        if k in this:
            base[k] = float(this[k])
    return base


def _assert_policy(strategy: str, policy: Dict[str, Any]) -> None:
    allowed = policy.get("allowed_strategies", all_strategy_names())
    if strategy not in allowed:
        raise ValueError(f"strategy {strategy} not allowed by policy")


def _policy_production_strategies(policy: Dict[str, Any]) -> List[str]:
    vals = policy.get("production_strategies")
    if isinstance(vals, list):
        out = [str(x).strip() for x in vals if str(x).strip()]
        if out:
            return out
    return production_strategies()


def _policy_primary_strategies(policy: Dict[str, Any]) -> List[str]:
    vals = policy.get("primary_strategies")
    if isinstance(vals, list):
        out = [str(x).strip() for x in vals if str(x).strip()]
        if out:
            return out
    return ui_primary_strategies()


def _weights_for_strategy(strategy: str, center_cfg: Dict[str, Any]) -> Dict[str, float]:
    # Future extension: multi-strategy blend.
    return {strategy: resolve_strategy_weight(strategy, center_config=center_cfg, default=1.0)}


def _collect_system_health() -> Dict[str, Any]:
    health: Dict[str, Any] = {"db_reachable": False, "compile_ok": True, "db_stale_days": 9999, "db_stale_limit": 3}
    try:
        db_path = resolve_db_path()
        conn = sqlite3.connect(str(db_path))
        last = latest_trade_date(conn)
        conn.close()
        health["db_reachable"] = True
        if last:
            s = str(last)
            if "-" in s:
                d = datetime.strptime(s[:10], "%Y-%m-%d").date()
            else:
                d = datetime.strptime(s[:8], "%Y%m%d").date()
            health["db_latest_date"] = s
            health["db_stale_days"] = (datetime.now().date() - d).days
    except Exception:
        pass
    return health


def _apply_positioning(opportunities: List[Dict[str, Any]], enable_kelly: bool) -> List[Dict[str, Any]]:
    if not opportunities:
        return []

    if not enable_kelly:
        for row in opportunities:
            row["target_weight"] = 0.0
        return opportunities

    try:
        KellyPositionManager = load_kelly_position_manager_class()
    except Exception:
        for row in opportunities:
            row["target_weight"] = 0.0
        return opportunities

    manager = KellyPositionManager()
    signals = []
    for row in opportunities:
        score = float(row.get("weighted_score", row.get("score", 0.0)) or 0.0)
        star_rating = 5 if score >= 90 else 4 if score >= 80 else 3 if score >= 70 else 2 if score >= 60 else 1
        signals.append(
            {
                "ts_code": row.get("ts_code"),
                "score": score,
                "star_rating": star_rating,
            }
        )
    alloc = manager.optimize_portfolio_allocation(signals)
    by_code = {str(x.get("ts_code")): x for x in alloc.get("allocations", [])}
    for row in opportunities:
        info = by_code.get(str(row.get("ts_code")), {})
        row["target_weight"] = float(info.get("position_pct", 0.0) or 0.0)
    return opportunities


def _apply_ui_markers(opportunities: List[Dict[str, Any]], primary_strategies: set[str]) -> List[Dict[str, Any]]:
    marked: List[Dict[str, Any]] = []
    for row in opportunities:
        item = dict(row)
        used = {str(x).lower() for x in (item.get("strategies") or []) if str(x).strip()}
        has_primary = bool(used & primary_strategies)
        item["strategy_focus"] = "primary" if has_primary else "secondary"
        item["strategy_ui_tag"] = item["strategy_focus"]
        item["primary_strategies_hit"] = sorted(used & primary_strategies)
        marked.append(item)
    return marked


def _run_signal_tracking(summary_path: Path, output_dir: Path) -> Dict[str, Any]:
    try:
        r1 = record_signals_from_summary(run_summary_path=summary_path)
        r2 = refresh_performance(lookback_days=360)
        r3 = generate_scoreboard(output_dir=output_dir, lookback_days=180)
        return {"record": r1, "refresh": r2, "scoreboard": r3}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _maybe_run_research(args: argparse.Namespace, output_dir: Path) -> Dict[str, Any]:
    if args.strategy != "v4" and not args.run_v4_research:
        return {"status": "skipped"}
    try:
        ic = calibrate_v4_weights(output_dir=output_dir, lookback_days=120)
        decay = analyze_factor_decay(output_dir=output_dir, lookback_days=120)
        return {"status": "ok", "ic": ic, "decay": decay}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


def _append_daily_metrics_jsonl(
    output_dir: Path,
    strategy: str,
    scan_result: Dict[str, Any],
    risk: Dict[str, Any],
    fallback_plan: Dict[str, Any],
    publish_result: Dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    day = datetime.now().strftime("%Y%m%d")
    metrics_path = output_dir / f"metrics_daily_{day}.jsonl"
    payload = {
        "ts": datetime.now().isoformat(),
        "strategy": strategy,
        "scan_count": int((((scan_result.get("result") or {}).get("metrics") or {}).get("count") or 0)),
        "signal_density": float(((risk.get("evidence") or {}).get("market_stats") or {}).get("signal_density", 0.0) or 0.0),
        "db_latest_date": ((risk.get("evidence") or {}).get("system_health") or {}).get("db_latest_date"),
        "fallback_mode": fallback_plan.get("execution_mode", "normal"),
        "publish_status": publish_result.get("status", "unknown"),
        "risk_level": risk.get("risk_level", "unknown"),
        "market_risk": risk.get("market_risk", "unknown"),
        "system_risk": risk.get("system_risk", "unknown"),
    }
    with metrics_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _build_validation_review(output_dir: Path) -> Dict[str, Any]:
    try:
        with db_conn(str(resolve_db_path())) as conn:
            return build_validation_review(conn, output_dir=output_dir, lookback_days=14)
    except Exception as exc:
        return {"gates": ["validation_review_failed"], "error": str(exc)}


def _build_trading_kernel_status(output_dir: Path) -> Dict[str, Any]:
    try:
        db_path_str = str(resolve_db_path())
        payload = build_trading_kernel_status(
            db_path_str=db_path_str,
            account_id="default",
            snapshot_date=None,
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        raw = json.dumps(_json_safe(payload), ensure_ascii=False, indent=2)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        (output_dir / f"trading_kernel_status_{ts}.json").write_text(raw, encoding="utf-8")
        (output_dir / "trading_kernel_status_snapshot.json").write_text(raw, encoding="utf-8")
        return payload
    except Exception as exc:
        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "governance": {
                "ok": False,
                "severity": "red",
                "gates": ["trading_kernel_status_failed"],
                "issue_count": 1,
                "severe_issue_count": 1,
                "warning_issue_count": 0,
                "severe_issue_types": ["trading_kernel_status_failed"],
                "warning_issue_types": [],
            },
            "error": str(exc),
        }


def _build_fallback_plan(
    strategy: str,
    risk: Dict[str, Any],
    current_params: Dict[str, Any],
    validation_review: Dict[str, Any] | None = None,
    validation_streak: Dict[str, Any] | None = None,
    trading_kernel_status: Dict[str, Any] | None = None,
    next_action: str = "continue",
    governance_override_applied: bool = False,
) -> Dict[str, Any]:
    """Build safe fallback actions for next run without placing trades."""
    level = str((risk or {}).get("risk_level", "")).lower()
    triggered = set((risk or {}).get("triggered_rules", []) or [])
    validation_review = validation_review or {}
    validation_streak = validation_streak or {}
    trading_kernel_status = trading_kernel_status or {}
    validation_gates = set(validation_review.get("gates", []) or [])
    kernel_gates = set(((trading_kernel_status.get("governance") or {}).get("gates") or []))
    consecutive_severe_runs = int(validation_streak.get("consecutive_severe_runs", 0) or 0)
    base = {
        "strategy": strategy,
        "execution_mode": "normal",
        "auto_execute_next_run": True,
        "next_run_params": dict(current_params),
        "notes": [],
        "validation_gates": sorted(validation_gates),
        "trading_kernel_gates": sorted(kernel_gates),
        "validation_streak": consecutive_severe_runs,
        "next_action": next_action,
        "governance_override_applied": bool(governance_override_applied),
    }

    if next_action == "halt_next_run":
        base["execution_mode"] = "halted"
        base["auto_execute_next_run"] = False
        base["next_run_params"] = {}
        base["notes"] = [
            "governance action: halt_next_run",
            "manual review required before next run",
        ]
        return base

    if next_action == "manual_review_required":
        base["execution_mode"] = "degraded"
        base["auto_execute_next_run"] = False
        base["notes"].append("governance action: manual_review_required")

    if level == "red":
        base["execution_mode"] = "halted"
        base["auto_execute_next_run"] = False
        base["next_run_params"] = {}
        base["notes"] = [
            "risk red: stop automation and require manual review",
            "do not publish actionable opportunities",
        ]
        return base

    if consecutive_severe_runs >= 2:
        if governance_override_applied:
            base["execution_mode"] = "degraded"
            base["auto_execute_next_run"] = False
            base["notes"].append(
                "validation redline override: keep automation degraded and require explicit manual review"
            )
        else:
            base["execution_mode"] = "halted"
            base["auto_execute_next_run"] = False
            base["next_run_params"] = {}
            base["notes"] = [
                "validation redline escalated: consecutive severe validation runs",
                "require manual review and root-cause analysis before next run",
            ]
            return base

    if validation_gates & {"avg_realized_return_weak", "win_rate_low"}:
        if governance_override_applied:
            base["execution_mode"] = "degraded"
            base["auto_execute_next_run"] = False
            base["notes"].append(
                "validation redline override: realized execution quality remains blocked from full automation"
            )
        else:
            base["execution_mode"] = "halted"
            base["auto_execute_next_run"] = False
            base["next_run_params"] = {}
            base["notes"] = [
                "validation redline: recent realized execution quality weak",
                "require manual review before next run",
            ]
            return base

    if kernel_gates & {"trading_kernel_reconcile_blocking", "trading_kernel_status_failed"}:
        if governance_override_applied:
            base["execution_mode"] = "degraded"
            base["auto_execute_next_run"] = False
            base["notes"].append(
                "trading kernel redline override: blocked from full automation until reconcile issue is cleared"
            )
        else:
            base["execution_mode"] = "halted"
            base["auto_execute_next_run"] = False
            base["next_run_params"] = {}
            base["notes"] = [
                "trading kernel redline: reconcile blocking issue detected",
                "require manual review before next run",
            ]
            return base

    if level == "orange":
        score_threshold = int(current_params.get("score_threshold", 70))
        sample_size = int(current_params.get("sample_size", 400))
        holding_days = int(current_params.get("holding_days", 5))
        offline_limit = int(current_params.get("offline_stock_limit", 300))

        degraded = {
            "score_threshold": min(90, score_threshold + 5),
            "sample_size": max(80, int(sample_size * 0.7)),
            "holding_days": min(10, holding_days + 1),
            "offline_stock_limit": max(80, int(offline_limit * 0.7)),
        }

        if "signal_density_collapse" in triggered:
            # Keep some signal flow for diagnostics when density is too low.
            degraded["score_threshold"] = max(55, score_threshold - 5)
            degraded["sample_size"] = max(120, int(sample_size * 0.8))

        base["execution_mode"] = "degraded"
        base["next_run_params"] = degraded
        if "risk orange: run with conservative profile" not in base["notes"]:
            base["notes"].append("risk orange: run with conservative profile")

    if validation_gates & {"execution_rate_low", "feedback_pending", "realized_outcomes_lagging"}:
        score_threshold = int(base["next_run_params"].get("score_threshold", current_params.get("score_threshold", 70)))
        sample_size = int(base["next_run_params"].get("sample_size", current_params.get("sample_size", 400)))
        holding_days = int(base["next_run_params"].get("holding_days", current_params.get("holding_days", 5)))
        offline_limit = int(base["next_run_params"].get("offline_stock_limit", current_params.get("offline_stock_limit", 300)))
        base["execution_mode"] = "degraded"
        base["next_run_params"] = {
            "score_threshold": min(92, score_threshold + 3),
            "sample_size": max(60, int(sample_size * 0.6)),
            "holding_days": min(10, holding_days + 1),
            "offline_stock_limit": max(60, int(offline_limit * 0.6)),
        }
        base["notes"].append("validation gates triggered: reduce aggressiveness until execution loop is healthy")

    return base


def _maybe_publish(
    enabled: bool,
    approved: bool,
    force_publish_on_risk: bool,
    policy: Dict[str, Any],
    notify_cfg: Dict[str, Any],
    summary: Dict[str, Any],
    scan_publish_gate: Dict[str, Any] | None = None,
    next_action: str = "continue",
) -> Dict[str, Any]:
    if not enabled:
        return {"status": "skipped", "reason": "publish not requested"}

    permissions = policy.get("permissions", {})
    if not bool(permissions.get("publish", False)):
        return {"status": "blocked", "reason": "policy.permissions.publish=false"}

    requires = set(policy.get("human_approval_required", []))
    if "publish_notifications" in requires and not approved:
        return {"status": "blocked", "reason": "human approval required (--approve-publish)"}

    if not bool(notify_cfg.get("enabled", False)):
        return {"status": "blocked", "reason": "notify config disabled"}

    scan_publish_gate = scan_publish_gate or {}
    if not bool(scan_publish_gate.get("ok", True)):
        return {
            "status": "blocked",
            "reason": str(scan_publish_gate.get("reason") or "scan publish gate blocked"),
            "scan_publish_gate": scan_publish_gate,
        }

    if next_action == "halt_next_run":
        return {"status": "blocked", "reason": "governance blocked publish (next_action=halt_next_run)"}

    if next_action == "manual_review_required" and not approved:
        return {"status": "blocked", "reason": "manual review required before publish (next_action=manual_review_required)"}

    risk_level = str(summary.get("risk_level", "")).lower()
    if risk_level in {"orange", "red"} and not force_publish_on_risk:
        return {"status": "blocked", "reason": f"risk gate blocked (risk_level={risk_level})"}

    title = str(summary.get("publish_preview_title") or f"[OpenClaw] {summary['strategy']} daily brief | risk={summary['risk_level']}")
    body = str(
        summary.get("publish_preview_body")
        or (
            f"scan={summary['scan_status']} backtest={summary['backtest_status']} "
            f"picks={summary['picks']} report={summary['report']}"
        )
    )

    publisher = NotificationPublisher(notify_cfg)
    sent = publisher.publish(title=title, body=body, metadata=summary)
    return {"status": "sent" if sent.get("ok") else "failed", "detail": sent}


def _build_scan_publish_gate(output_dir: Path) -> Dict[str, Any]:
    scan_obs_path = output_dir / "scan_observability_snapshot.json"
    if not scan_obs_path.exists():
        return {
            "ok": False,
            "gate": "P6",
            "reason": "scan_observability missing: publish blocked until scan observability snapshot exists",
            "scan_observability_at": "",
            "scan_observability_age_minutes": None,
        }

    age_minutes = max(0, int((datetime.now().timestamp() - scan_obs_path.stat().st_mtime) // 60))
    updated_at = datetime.fromtimestamp(scan_obs_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    if age_minutes > 1440:
        return {
            "ok": False,
            "gate": "P9",
            "reason": "scan_observability stale >24h: publish blocked until fresh scan observability is generated",
            "scan_observability_at": updated_at,
            "scan_observability_age_minutes": age_minutes,
        }

    return {
        "ok": True,
        "gate": "",
        "reason": "scan observability fresh enough for publish gate",
        "scan_observability_at": updated_at,
        "scan_observability_age_minutes": age_minutes,
    }


def _build_publish_preview(
    *,
    strategy: str,
    risk_level: str,
    scan_status: str,
    backtest_status: str,
    picks: int,
    report_markdown: str,
    execution_mode: str,
    scan_publish_gate: Dict[str, Any],
    next_action: str,
) -> Dict[str, Any]:
    title = f"[OpenClaw] {strategy} daily brief | risk={risk_level}"
    body = (
        f"scan={scan_status} backtest={backtest_status} "
        f"picks={picks} execution_mode={execution_mode} next_action={next_action} report={report_markdown}"
    )
    return {
        "title": title,
        "body": body,
        "would_publish": bool(scan_publish_gate.get("ok", True)),
        "blocked_by_gate": str(scan_publish_gate.get("gate") or ""),
        "blocked_reason": str(scan_publish_gate.get("reason") or ""),
    }


def _resolve_next_action(
    *,
    risk: Dict[str, Any],
    validation_review: Dict[str, Any],
    trading_kernel_status: Dict[str, Any],
    validation_streak: Dict[str, Any],
) -> str:
    risk_level = str((risk or {}).get("risk_level") or "").lower()
    validation_gates = set(str(x) for x in ((validation_review or {}).get("gates") or []))
    kernel_governance = (trading_kernel_status or {}).get("governance") or {}
    kernel_gates = set(str(x) for x in (kernel_governance.get("gates") or []))
    consecutive_severe_runs = int((validation_streak or {}).get("consecutive_severe_runs", 0) or 0)

    blocking_gates = {
        "trading_kernel_reconcile_blocking",
        "trading_kernel_status_failed",
        "trading_kernel_warning_escalated",
        "consecutive_validation_redline",
        "avg_realized_return_weak",
        "win_rate_low",
    }
    warning_gates = {
        "trading_kernel_reconcile_warning",
        "trading_kernel_snapshot_empty",
        "feedback_pending",
        "execution_rate_low",
        "pick_fill_rate_low",
        "realized_outcomes_lagging",
    }

    all_gates = validation_gates | kernel_gates
    if consecutive_severe_runs >= 2 or (all_gates & blocking_gates):
        return "halt_next_run"
    if risk_level in {"orange", "red"} or (all_gates & warning_gates):
        return "manual_review_required"
    return "continue"


def _apply_governance_override(*, next_action: str, governance_override: bool) -> str:
    if governance_override and str(next_action or "") == "halt_next_run":
        # Human override may release today's flow, but the system should remain in
        # manual-review mode instead of silently restoring full automation.
        return "manual_review_required"
    return str(next_action or "continue")


def _derive_governance_final_disposition(
    *,
    next_action: str,
    effective_next_action: str,
    publish_result: Dict[str, Any],
    governance_override_requested: bool,
) -> str:
    publish_status = str((publish_result or {}).get("status") or "").lower()
    override_applied = bool(governance_override_requested and effective_next_action != next_action)

    if override_applied:
        if publish_status == "sent":
            return "manual_override_released"
        if publish_status == "blocked":
            return "manual_override_requested_but_blocked"
        return "manual_override_applied"

    if next_action == "halt_next_run":
        return "system_auto_halt"
    if next_action == "manual_review_required":
        if publish_status == "sent":
            return "manual_review_approved"
        if publish_status == "blocked":
            return "manual_review_pending"
        return "manual_review_required"
    if publish_status == "sent":
        return "system_continue_published"
    return "system_continue"


def _record_governance_audit_records(
    *,
    strategy: str,
    account_id: str,
    decision_date: str,
    next_action: str,
    effective_next_action: str,
    fallback_plan: Dict[str, Any],
    publish_result: Dict[str, Any],
    risk: Dict[str, Any],
    validation_review: Dict[str, Any],
    trading_kernel_status: Dict[str, Any],
    governance_override_requested: bool,
    operator_name: str,
    override_reason: str,
    run_summary_path: str,
    publish_requested: bool,
) -> Dict[str, Any]:
    kernel_governance = (trading_kernel_status or {}).get("governance") or {}
    validation_gates = [str(x) for x in ((validation_review or {}).get("gates") or [])]
    kernel_gates = [str(x) for x in (kernel_governance.get("gates") or [])]
    final_disposition = _derive_governance_final_disposition(
        next_action=next_action,
        effective_next_action=effective_next_action,
        publish_result=publish_result,
        governance_override_requested=governance_override_requested,
    )
    base_context = {
        "publish_reason": str((publish_result or {}).get("reason") or ""),
        "publish_detail": _json_safe((publish_result or {}).get("detail")),
        "fallback_notes": list((fallback_plan or {}).get("notes") or []),
    }
    result: Dict[str, Any] = {
        "ok": True,
        "system_recorded": False,
        "override_recorded": False,
        "final_disposition": final_disposition,
    }

    with db_conn() as conn:
        system_audit = record_governance_audit(
            conn,
            strategy=strategy,
            account_id=account_id,
            decision_date=decision_date,
            event_type="system_decision",
            system_next_action=next_action,
            effective_next_action=effective_next_action,
            final_disposition=final_disposition,
            publish_requested=publish_requested,
            publish_status=str((publish_result or {}).get("status") or ""),
            fallback_execution_mode=str((fallback_plan or {}).get("execution_mode") or ""),
            override_applied=bool(governance_override_requested and effective_next_action != next_action),
            override_reason=override_reason,
            operator_name=operator_name,
            risk_level=str((risk or {}).get("risk_level") or ""),
            validation_gates=validation_gates,
            kernel_gates=kernel_gates,
            context=base_context,
            run_summary_path=run_summary_path,
        )
        if not system_audit.get("ok"):
            conn.rollback()
            return {
                "ok": False,
                "message": str(system_audit.get("message") or "system governance audit failed"),
                "final_disposition": final_disposition,
            }
        result["system_recorded"] = True
        result["system_audit_id"] = str(system_audit.get("audit_id") or "")

        if governance_override_requested and effective_next_action != next_action:
            override_audit = record_governance_audit(
                conn,
                strategy=strategy,
                account_id=account_id,
                decision_date=decision_date,
                event_type="human_override",
                system_next_action=next_action,
                effective_next_action=effective_next_action,
                final_disposition="manual_override_release",
                publish_requested=publish_requested,
                publish_status=str((publish_result or {}).get("status") or ""),
                fallback_execution_mode=str((fallback_plan or {}).get("execution_mode") or ""),
                override_applied=True,
                override_reason=override_reason,
                operator_name=operator_name,
                risk_level=str((risk or {}).get("risk_level") or ""),
                validation_gates=validation_gates,
                kernel_gates=kernel_gates,
                context={
                    **base_context,
                    "override_scope": "governance_halt_release",
                },
                run_summary_path=run_summary_path,
            )
            if not override_audit.get("ok"):
                conn.rollback()
                return {
                    "ok": False,
                    "message": str(override_audit.get("message") or "override governance audit failed"),
                    "system_audit_id": result.get("system_audit_id", ""),
                    "final_disposition": final_disposition,
                }
            result["override_recorded"] = True
            result["override_audit_id"] = str(override_audit.get("audit_id") or "")

        conn.commit()
    return result


def _maybe_emit_validation_alert(
    *,
    notify_cfg: Dict[str, Any],
    validation_review: Dict[str, Any],
    validation_streak: Dict[str, Any],
    strategy: str,
    output_dir: Path,
) -> Dict[str, Any]:
    gates = list(validation_review.get("gates", []) or [])
    severe = [g for g in gates if g in {"avg_realized_return_weak", "win_rate_low", "execution_rate_low"}]
    if not severe:
        return {"status": "skipped", "reason": "no_severe_validation_gates"}
    if not bool(notify_cfg.get("enabled", False)):
        return {"status": "skipped", "reason": "notify config disabled", "gates": severe}
    consecutive = int(validation_streak.get("consecutive_severe_runs", 0) or 0)
    severity = "red" if consecutive >= 2 else "orange"
    title = f"[OpenClaw] validation redline | {strategy} | streak={consecutive} | {','.join(severe)}"
    body = (
        f"strategy={strategy} "
        f"execution_rate={validation_review.get('execution_rate')} "
        f"avg_realized_return_pct={validation_review.get('avg_realized_return_pct')} "
        f"win_rate={validation_review.get('win_rate')} "
        f"consecutive_severe_runs={consecutive} "
        f"output_dir={output_dir}"
    )
    publisher = NotificationPublisher(notify_cfg)
    sent = publisher.publish(
        title=title,
        body=body,
        metadata={"kind": "validation_redline", "risk_level": severity, "gates": severe, "consecutive_severe_runs": consecutive},
    )
    return {"status": "sent" if sent.get("ok") else "failed", "gates": severe, "consecutive_severe_runs": consecutive, "detail": sent}


def _default_policy() -> Dict[str, Any]:
    return {
        "mode": "read_only",
        "allowed_strategies": all_strategy_names(),
        "production_strategies": production_strategies(),
        "primary_strategies": ui_primary_strategies(),
        "permissions": {
            "read_only": True,
            "orchestrate": True,
            "publish": False,
            "trade": False,
        },
    }


def _default_thresholds() -> Dict[str, Any]:
    return {
        "win_rate_min": 0.45,
        "max_drawdown_max": 0.12,
        "signal_density_min": 0.02,
        "strategy_overrides": {},
    }


def _default_notify_config() -> Dict[str, Any]:
    return {
        "enabled": False,
        "channels": ["file", "stdout"],
        "outbox_path": "logs/openclaw/notify_outbox.log",
        "legacy_config_file": "notification_config.json",
    }


def _parse_simple_yaml(text: str) -> Dict[str, Any]:
    """Parse a minimal YAML subset (mappings/lists/scalars) used by local configs."""
    root: Dict[str, Any] = {}
    stack: List[Any] = [root]
    indents: List[int] = [0]

    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()

        while len(indents) > 1 and indent < indents[-1]:
            stack.pop()
            indents.pop()

        container = stack[-1]

        if line.startswith("- "):
            if not isinstance(container, list):
                raise ValueError("invalid list placement in YAML")
            container.append(_yaml_scalar(line[2:].strip()))
            continue

        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if value == "":
            next_is_list = False
            # Look ahead is skipped; infer from known key style.
            if key in {"channels", "allowed_strategies", "production_strategies", "primary_strategies", "experimental_strategies", "forbidden_actions", "human_approval_required"}:
                child: Any = []
                next_is_list = True
            else:
                child = {}
            if isinstance(container, dict):
                container[key] = child
            else:
                raise ValueError("invalid mapping placement in YAML")
            stack.append(child)
            indents.append(indent + 2)
            if next_is_list:
                continue
        else:
            if not isinstance(container, dict):
                raise ValueError("invalid scalar placement in YAML")
            container[key] = _yaml_scalar(value)

    return root


def _yaml_scalar(value: str) -> Any:
    low = value.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    if low == "null":
        return None
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


if __name__ == "__main__":
    raise SystemExit(main())
