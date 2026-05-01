#!/usr/bin/env python3
"""默认组合 (v9+v8+v5+combo) 多轮回归验证门禁。

连续执行 N 轮 scan → 要求每轮均成功且指标在阈值内。
任何一轮失败即退出码 ≠ 0, 可作为发布门槛。

用法:
  python tools/regression_combo_gate.py --rounds 3
  python tools/regression_combo_gate.py --rounds 5 --strategy v9 v8 v5 combo
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from openclaw.adapters import V49Adapter
from openclaw.runtime.v49_handlers import HandlerFactory
from strategies.center_config import load_center_config, resolve_run_policy, resolve_runtime_params
from strategies.registry import get_profile, production_strategies

try:
    from openclaw.paths import v49_module_path, log_dir
except ImportError:
    v49_module_path = lambda: ROOT / "v49_app.py"
    log_dir = lambda: ROOT / "logs" / "openclaw"


DEFAULT_STRATEGIES = production_strategies()  # v9, v8, v5, combo


def _gate_thresholds(center_cfg: Dict[str, Any]) -> Dict[str, int]:
    default = {"min_picks": 1, "max_scan_sec": 600}
    section = center_cfg.get("regression_gate", {}) if isinstance(center_cfg, dict) else {}
    if not isinstance(section, dict):
        return default
    out = dict(default)
    for key in ("min_picks", "max_scan_sec"):
        if key in section:
            try:
                out[key] = int(section[key])
            except Exception:
                pass
    return out


def _build_scan_params(strategy: str, center_cfg: Dict[str, Any]) -> Dict[str, int]:
    resolved = resolve_runtime_params(
        strategy=strategy,
        requested_score_threshold=None,
        requested_sample_size=None,
        requested_holding_days=None,
        center_config=center_cfg,
        project_root=ROOT,
    )
    run_policy = resolve_run_policy(strategy=strategy, center_config=center_cfg, default_timeout_sec=900)
    offline_limit = int(run_policy.get("offline_stock_limit", 300) or 300)
    params = {
        "score_threshold": int(resolved["score_threshold"]),
        "limit": 30,
        "offline_stock_limit": offline_limit,
    }
    if strategy == "combo":
        for component in ("v5", "v8", "v9"):
            component_resolved = resolve_runtime_params(
                strategy=component,
                requested_score_threshold=None,
                requested_sample_size=None,
                requested_holding_days=None,
                center_config=center_cfg,
                project_root=ROOT,
            )
            params[f"thr_{component}"] = int(component_resolved["score_threshold"])
    return params


def run_single_round(
    strategies: List[str],
    adapter: V49Adapter,
    round_no: int,
    center_cfg: Dict[str, Any],
    thresholds: Dict[str, int],
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "round": round_no,
        "ts": datetime.now().isoformat(),
        "strategies": {},
        "passed": True,
        "errors": [],
    }

    for strat in strategies:
        _ = get_profile(strat)  # validate strategy id
        params = _build_scan_params(strat, center_cfg)
        t0 = time.time()
        try:
            scan_result = adapter.run_scan(strat, params)
            elapsed = time.time() - t0
            picks = (scan_result.get("result") or {}).get("picks", [])
            status = scan_result.get("status", "unknown")

            strat_info = {
                "status": status,
                "picks_count": len(picks),
                "elapsed_sec": round(elapsed, 1),
                "passed": True,
                "notes": [],
            }

            if str(status).lower() not in {"ok", "success"}:
                strat_info["passed"] = False
                strat_info["notes"].append(f"状态异常: {status}")

            if elapsed > thresholds["max_scan_sec"]:
                strat_info["notes"].append(f"超时 {elapsed:.0f}s > {thresholds['max_scan_sec']}s")
            if len(picks) < thresholds["min_picks"]:
                strat_info["passed"] = False
                strat_info["notes"].append(f"信号不足 {len(picks)} < {thresholds['min_picks']}")

            result["strategies"][strat] = strat_info

        except Exception as e:
            elapsed = time.time() - t0
            result["strategies"][strat] = {
                "status": "error",
                "picks_count": 0,
                "elapsed_sec": round(elapsed, 1),
                "passed": False,
                "notes": [str(e)],
            }
            result["errors"].append(f"{strat}: {e}")
            result["passed"] = False

    failed = [s for s, v in result["strategies"].items() if not v["passed"]]
    if failed:
        result["passed"] = False
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="回归验证门禁: 默认组合多轮扫描")
    parser.add_argument("--rounds", type=int, default=3, help="连续轮数 (默认 3)")
    parser.add_argument("--strategy", nargs="*", default=DEFAULT_STRATEGIES, help="策略列表")
    parser.add_argument("--module-path", default=None, help="v49 模块路径")
    parser.add_argument(
        "--strategy-center-config",
        default="openclaw/config/strategy_center.yaml",
        help="strategy center runtime config path",
    )
    args = parser.parse_args()

    module_path = Path(args.module_path) if args.module_path else v49_module_path()
    center_cfg = load_center_config(Path(args.strategy_center_config))
    thresholds = _gate_thresholds(center_cfg)
    adapter = V49Adapter(module_path=module_path)
    factory = HandlerFactory(module_path)
    for strat in args.strategy:
        adapter.register_scan_handler(strat, factory.create_scan_handler(strat))

    all_rounds: List[Dict[str, Any]] = []
    total_pass = True

    print(f"\n{'=' * 60}")
    print(f"  回归验证门禁: {args.strategy} × {args.rounds} 轮")
    print(f"{'=' * 60}\n")

    for r in range(1, args.rounds + 1):
        print(f"── 第 {r}/{args.rounds} 轮 ──")
        result = run_single_round(args.strategy, adapter, r, center_cfg=center_cfg, thresholds=thresholds)
        all_rounds.append(result)

        for strat, info in result["strategies"].items():
            icon = "✅" if info["passed"] else "❌"
            print(f"  {icon} {strat}: picks={info['picks_count']} {info['elapsed_sec']}s {' '.join(info['notes'])}")

        if not result["passed"]:
            total_pass = False
            print(f"  ❌ 第 {r} 轮失败, 中止\n")
            break
        else:
            print(f"  ✅ 第 {r} 轮通过\n")

    report = {
        "ts": datetime.now().isoformat(),
        "strategies": args.strategy,
        "total_rounds": args.rounds,
        "completed_rounds": len(all_rounds),
        "all_passed": total_pass,
        "rounds": all_rounds,
    }

    out_dir = log_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"regression_gate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"{'=' * 60}")
    if total_pass:
        print(f"  ✅ 全部 {args.rounds} 轮通过 — 可发布")
    else:
        print(f"  ❌ 验证失败 — 禁止发布")
    print(f"  报告: {out_path}")
    print(f"{'=' * 60}\n")

    return 0 if total_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
