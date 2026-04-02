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

from data.dao import resolve_db_path, latest_trade_date
from data.migrations.runner import apply_migrations
from backtest.engine import BacktestEngine
from openclaw.adapters import V49Adapter
from openclaw.publish import NotificationPublisher
from openclaw.runtime.v49_handlers import HandlerFactory
from openclaw.assistant import OpenClawStockAssistant
from openclaw.strategy_tracking import generate_scoreboard, record_signals_from_summary, refresh_performance
from openclaw.research.v4_factor_research import analyze_factor_decay, calibrate_v4_weights
from strategies.center_config import (
    apply_risk_overrides,
    load_center_config,
    resolve_runtime_params,
    resolve_strategy_weight,
)
from strategies.registry import get_profile, ui_primary_strategies, production_strategies, all_strategy_names
from risk.engine import combine_risk
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
    fallback_plan = _build_fallback_plan(
        strategy=args.strategy,
        risk=risk,
        current_params={
            "score_threshold": args.score_threshold,
            "sample_size": args.sample_size,
            "holding_days": args.holding_days,
            "offline_stock_limit": args.offline_stock_limit,
        },
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
            },
            "opportunities": opportunities,
        },
        output_dir=Path(args.output_dir),
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
        },
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
        "fallback_plan": fallback_plan,
        "report": report,
        "publish": publish_result,
        "policy_mode": policy.get("mode", "unknown"),
    }

    summary_path = Path(args.output_dir) / f"run_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(_json_safe(run_summary), ensure_ascii=False, indent=2), encoding="utf-8")
    tracking_result = _run_signal_tracking(summary_path=summary_path, output_dir=Path(args.output_dir))
    run_summary["tracking"] = tracking_result
    run_summary["research"] = _maybe_run_research(args=args, output_dir=Path(args.output_dir))
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
        from kelly_position_manager import KellyPositionManager
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


def _build_fallback_plan(strategy: str, risk: Dict[str, Any], current_params: Dict[str, Any]) -> Dict[str, Any]:
    """Build safe fallback actions for next run without placing trades."""
    level = str((risk or {}).get("risk_level", "")).lower()
    triggered = set((risk or {}).get("triggered_rules", []) or [])
    base = {
        "strategy": strategy,
        "execution_mode": "normal",
        "auto_execute_next_run": True,
        "next_run_params": dict(current_params),
        "notes": [],
    }

    if level == "red":
        base["execution_mode"] = "halted"
        base["auto_execute_next_run"] = False
        base["next_run_params"] = {}
        base["notes"] = [
            "risk red: stop automation and require manual review",
            "do not publish actionable opportunities",
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
        base["notes"] = ["risk orange: run with conservative profile"]

    return base


def _maybe_publish(
    enabled: bool,
    approved: bool,
    force_publish_on_risk: bool,
    policy: Dict[str, Any],
    notify_cfg: Dict[str, Any],
    summary: Dict[str, Any],
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

    risk_level = str(summary.get("risk_level", "")).lower()
    if risk_level in {"orange", "red"} and not force_publish_on_risk:
        return {"status": "blocked", "reason": f"risk gate blocked (risk_level={risk_level})"}

    title = f"[OpenClaw] {summary['strategy']} daily brief | risk={summary['risk_level']}"
    body = (
        f"scan={summary['scan_status']} backtest={summary['backtest_status']} "
        f"picks={summary['picks']} report={summary['report']}"
    )

    publisher = NotificationPublisher(notify_cfg)
    sent = publisher.publish(title=title, body=body, metadata=summary)
    return {"status": "sent" if sent.get("ok") else "failed", "detail": sent}


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
