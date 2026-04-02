#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.research.backtest_param_sweep import SweepConfig, run_param_sweep  # noqa: E402
from strategies.center_config import load_center_config  # noqa: E402
from strategies.registry import production_strategies, experimental_strategies  # noqa: E402


JsonDict = Dict[str, Any]

DEFAULT_PLAN: Dict[str, Dict[str, List[int]]] = {
    "v5": {"score_thresholds": [60, 65, 70], "sample_sizes": [50, 80], "holding_days": [6, 8]},
    "v8": {"score_thresholds": [45, 50, 55], "sample_sizes": [50, 80], "holding_days": [6, 8]},
    "v9": {"score_thresholds": [60, 65, 70], "sample_sizes": [50, 80], "holding_days": [6, 8]},
    "combo": {"score_thresholds": [55, 60, 65], "sample_sizes": [50, 80], "holding_days": [6, 8]},
}


def _default_date_window(days: int = 730) -> Tuple[str, str]:
    d_to = datetime.now().date()
    d_from = d_to - timedelta(days=max(365, int(days)))
    return d_from.strftime("%Y-%m-%d"), d_to.strftime("%Y-%m-%d")


def _resolve_plan(center_cfg: JsonDict, strategy: str) -> Dict[str, List[int]]:
    out = dict(DEFAULT_PLAN.get(strategy, {"score_thresholds": [60, 65], "sample_sizes": [50, 80], "holding_days": [6, 8]}))
    weekly = center_cfg.get("weekly_sweep") if isinstance(center_cfg, dict) else {}
    if not isinstance(weekly, dict):
        return out
    strat_map = weekly.get("strategies", {})
    if not isinstance(strat_map, dict):
        return out
    this = strat_map.get(strategy, {})
    if not isinstance(this, dict):
        return out
    for k in ("score_thresholds", "sample_sizes", "holding_days"):
        vals = this.get(k)
        if isinstance(vals, list):
            clean = []
            for x in vals:
                try:
                    clean.append(int(x))
                except Exception:
                    pass
            if clean:
                out[k] = clean
    return out


def _compute_weights(best_by_strategy: Dict[str, JsonDict], min_weight: float = 0.10) -> Dict[str, float]:
    scores: Dict[str, float] = {}
    for s, b in best_by_strategy.items():
        try:
            scores[s] = max(0.0, float((b or {}).get("objective", 0.0)))
        except Exception:
            scores[s] = 0.0

    n = len(scores)
    if n == 0:
        return {}
    total = sum(scores.values())
    if total <= 1e-9:
        w = 1.0 / float(n)
        return {k: round(w, 4) for k in scores}

    raw = {k: v / total for k, v in scores.items()}
    floor = max(0.0, min(float(min_weight), 1.0 / float(n)))
    base_sum = floor * float(n)
    if base_sum >= 1.0 - 1e-9:
        w = 1.0 / float(n)
        return {k: round(w, 4) for k in scores}

    remainder = 1.0 - base_sum
    extras = {k: max(0.0, raw[k]) for k in raw}
    extras_sum = sum(extras.values())
    if extras_sum <= 1e-9:
        return {k: round(1.0 / float(n), 4) for k in scores}

    out = {}
    for k in raw:
        out[k] = floor + remainder * (extras[k] / extras_sum)
    # Final normalization keeps sum=1 without breaking floor materially.
    norm = sum(out.values()) or 1.0
    return {k: round(out[k] / norm, 4) for k in out}


def _write_center_config(path: Path, cfg: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import yaml  # type: ignore

        path.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return
    except Exception:
        pass
    path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report(output_dir: Path, payload: JsonDict) -> Dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    js = output_dir / f"weekly_sweep_{ts}.json"
    md = output_dir / f"weekly_sweep_{ts}.md"
    js.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Weekly Sweep",
        "",
        f"- ts: {payload.get('ts')}",
        f"- status: {payload.get('status')}",
        f"- period: {payload.get('date_from')} -> {payload.get('date_to')}",
        "",
        "| strategy | threshold | sample | hold | objective | max_drawdown | win_rate |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for s, b in (payload.get("best_by_strategy") or {}).items():
        lines.append(
            f"| {s} | {b.get('score_threshold')} | {b.get('sample_size')} | {b.get('holding_days')} | "
            f"{float(b.get('objective', 0.0)):.4f} | {float(b.get('max_drawdown', 0.0)):.4f} | {float(b.get('win_rate', 0.0)):.4f} |"
        )
    md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(js), "markdown": str(md)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run weekly backtest sweeps and feed strategy center config")
    parser.add_argument("--module-path", default=str(ROOT / "v49_app.py"))
    parser.add_argument("--center-config", default=str(ROOT / "openclaw/config/strategy_center.yaml"))
    parser.add_argument("--output-dir", default=str(ROOT / "logs/openclaw"))
    parser.add_argument("--date-from", default="")
    parser.add_argument("--date-to", default="")
    parser.add_argument("--lookback-days", type=int, default=730)
    parser.add_argument("--mode", default="single", choices=["single", "rolling"])
    parser.add_argument("--per-run-timeout-sec", type=int, default=90)
    parser.add_argument("--strategies", default="v5,v8,v9,combo")
    parser.add_argument("--write-center-config", action="store_true", default=True)
    parser.add_argument("--no-write-center-config", dest="write_center_config", action="store_false")
    args = parser.parse_args()

    center_path = Path(args.center_config)
    center_cfg = load_center_config(center_path)
    date_from, date_to = (args.date_from, args.date_to) if (args.date_from and args.date_to) else _default_date_window(args.lookback_days)

    strategies = [x.strip() for x in str(args.strategies).split(",") if x.strip()]
    valid = set(production_strategies()) | set(experimental_strategies())
    strategies = [s for s in strategies if s in valid]
    if not strategies:
        raise SystemExit("no valid strategy selected")

    results: Dict[str, JsonDict] = {}
    best_by_strategy: Dict[str, JsonDict] = {}
    failed: List[str] = []
    for s in strategies:
        plan = _resolve_plan(center_cfg, s)
        cfg = SweepConfig(
            strategy=s,
            module_path=Path(args.module_path),
            output_dir=Path(args.output_dir),
            date_from=date_from,
            date_to=date_to,
            mode=args.mode,
            train_window_days=180,
            test_window_days=60,
            step_days=60,
            score_thresholds=plan["score_thresholds"],
            sample_sizes=plan["sample_sizes"],
            holding_days=plan["holding_days"],
            per_run_timeout_sec=(int(args.per_run_timeout_sec) if int(args.per_run_timeout_sec) > 0 else None),
        )
        out = run_param_sweep(cfg)
        results[s] = out
        best = out.get("best") or {}
        if out.get("status") == "success" and isinstance(best, dict):
            best_by_strategy[s] = {
                "score_threshold": int(best.get("score_threshold", 0) or 0),
                "sample_size": int(best.get("sample_size", 0) or 0),
                "holding_days": int(best.get("holding_days", 0) or 0),
                "objective": float(best.get("objective", 0.0) or 0.0),
                "max_drawdown": float(best.get("max_drawdown", 1.0) or 1.0),
                "win_rate": float(best.get("win_rate", 0.0) or 0.0),
                "artifact_json": str(((out.get("artifacts") or {}).get("json", ""))),
            }
        else:
            failed.append(s)

    updated = False
    if args.write_center_config and best_by_strategy:
        runtime_defaults = center_cfg.get("runtime_defaults", {})
        if not isinstance(runtime_defaults, dict):
            runtime_defaults = {}
        for s, b in best_by_strategy.items():
            runtime_defaults[s] = {
                "score_threshold": b["score_threshold"],
                "sample_size": b["sample_size"],
                "holding_days": b["holding_days"],
            }
        center_cfg["runtime_defaults"] = runtime_defaults
        min_w = float(((center_cfg.get("weekly_sweep") or {}).get("min_weight", 0.10)) or 0.10)
        center_cfg["strategy_weights"] = _compute_weights(best_by_strategy, min_weight=min_w)
        center_cfg["last_weekly_sweep"] = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "date_from": date_from,
            "date_to": date_to,
            "strategies": strategies,
            "failed": failed,
            "best_by_strategy": best_by_strategy,
        }
        _write_center_config(center_path, center_cfg)
        updated = True

    status = "success" if not failed else ("partial_success" if best_by_strategy else "failed")
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "date_from": date_from,
        "date_to": date_to,
        "strategies": strategies,
        "failed": failed,
        "best_by_strategy": best_by_strategy,
        "center_config_updated": updated,
        "results": results,
    }
    artifacts = _write_report(Path(args.output_dir), payload)
    payload["artifacts"] = artifacts
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if status in {"success", "partial_success"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
