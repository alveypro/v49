#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.research.backtest_param_sweep import (
    SweepConfig,
    default_date_window,
    default_holding_grid,
    default_sample_grid,
    default_threshold_grid,
    parse_int_list,
    run_param_sweep,
)
from strategies.registry import get_profile


def main() -> int:
    parser = argparse.ArgumentParser(description="Parameter sweep for OpenClaw backtest module")
    parser.add_argument("--strategy", default="v5", help="strategy id, e.g. v5/v6/v7/v8/v9")
    parser.add_argument("--module-path", default="v49_app.py")
    parser.add_argument("--output-dir", default="logs/openclaw")
    parser.add_argument("--date-from", default="")
    parser.add_argument("--date-to", default="")
    parser.add_argument("--mode", default="rolling", choices=["rolling", "single"])
    parser.add_argument("--train-window-days", type=int, default=180)
    parser.add_argument("--test-window-days", type=int, default=60)
    parser.add_argument("--step-days", type=int, default=60)
    parser.add_argument("--score-thresholds", default="", help="comma-separated, e.g. 55,60,65")
    parser.add_argument("--sample-sizes", default="", help="comma-separated, e.g. 200,300,400")
    parser.add_argument("--holding-days", default="", help="comma-separated, e.g. 3,5,8")
    parser.add_argument("--db-path", default="", help="optional DB path override")
    parser.add_argument("--max-runs", type=int, default=0, help="limit run count for quick tests")
    parser.add_argument(
        "--per-run-timeout-sec",
        type=int,
        default=0,
        help="fail a single parameter run after timeout seconds (0 disables timeout)",
    )
    args = parser.parse_args()

    profile = get_profile(args.strategy)
    if args.date_from and args.date_to:
        date_from, date_to = args.date_from, args.date_to
    else:
        date_from, date_to = default_date_window(365)

    score_thresholds = parse_int_list(args.score_thresholds, default_threshold_grid(profile.default_score_threshold))
    sample_sizes = parse_int_list(args.sample_sizes, default_sample_grid(profile.default_sample_size))
    holding_days = parse_int_list(args.holding_days, default_holding_grid(profile.default_holding_days))

    cfg = SweepConfig(
        strategy=args.strategy,
        module_path=Path(args.module_path),
        output_dir=Path(args.output_dir),
        date_from=date_from,
        date_to=date_to,
        mode=args.mode,
        train_window_days=args.train_window_days,
        test_window_days=args.test_window_days,
        step_days=args.step_days,
        score_thresholds=score_thresholds,
        sample_sizes=sample_sizes,
        holding_days=holding_days,
        db_path=args.db_path.strip() or None,
        max_runs=(args.max_runs if args.max_runs > 0 else None),
        per_run_timeout_sec=(args.per_run_timeout_sec if args.per_run_timeout_sec > 0 else None),
    )
    out = run_param_sweep(cfg)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out.get("status") == "success" else 2


if __name__ == "__main__":
    raise SystemExit(main())
