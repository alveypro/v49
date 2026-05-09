#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.research.backtest_param_sweep import parse_float_list, parse_int_list  # noqa: E402
from openclaw.research.v8_controlled_experiment import (  # noqa: E402
    V8ControlledExperimentConfig,
    run_v8_controlled_experiment,
)


def _parse_optional_float_grid(raw: str):
    if not str(raw or "").strip():
        return [None]
    out = []
    for part in str(raw).split(","):
        text = part.strip()
        if not text:
            continue
        if text.lower() in {"none", "null"}:
            out.append(None)
        else:
            out.append(float(text))
    return out or [None]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the fixed v8 controlled experiment chain.")
    parser.add_argument("--module-path", default="v49_app.py")
    parser.add_argument("--output-dir", default="logs/openclaw/v8_controlled_experiments")
    parser.add_argument("--date-from", default="2025-11-01")
    parser.add_argument("--date-to", default="2026-05-03")
    parser.add_argument("--train-window-days", type=int, default=90)
    parser.add_argument("--test-window-days", type=int, default=30)
    parser.add_argument("--step-days", type=int, default=30)
    parser.add_argument("--score-thresholds", default="50,55,60")
    parser.add_argument("--sample-sizes", default="30")
    parser.add_argument("--holding-days", default="6")
    parser.add_argument("--max-stop-loss-pcts", default="0.06,0.08,0.10")
    parser.add_argument("--max-take-profit-pcts", default="none,0.10,0.14")
    parser.add_argument("--db-path", default="")
    parser.add_argument("--rejected-ledger", default="logs/openclaw/rejected_backtest_artifacts.jsonl")
    parser.add_argument("--max-runs", type=int, default=0)
    parser.add_argument("--per-run-timeout-sec", type=int, default=180)
    parser.add_argument("--operator-name", default="v8_controlled_experiment")
    args = parser.parse_args()

    payload = run_v8_controlled_experiment(
        V8ControlledExperimentConfig(
            module_path=Path(args.module_path),
            output_dir=Path(args.output_dir),
            date_from=args.date_from,
            date_to=args.date_to,
            train_window_days=args.train_window_days,
            test_window_days=args.test_window_days,
            step_days=args.step_days,
            score_thresholds=parse_int_list(args.score_thresholds, [50, 55, 60]),
            sample_sizes=parse_int_list(args.sample_sizes, [30]),
            holding_days=parse_int_list(args.holding_days, [6]),
            max_stop_loss_pcts=parse_float_list(args.max_stop_loss_pcts, [0.08]),
            max_take_profit_pcts=_parse_optional_float_grid(args.max_take_profit_pcts),
            db_path=args.db_path,
            rejected_ledger=args.rejected_ledger,
            max_runs=(args.max_runs if args.max_runs > 0 else None),
            per_run_timeout_sec=(args.per_run_timeout_sec if args.per_run_timeout_sec > 0 else None),
            operator_name=args.operator_name,
        )
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("status") != "rejected" else 2


if __name__ == "__main__":
    raise SystemExit(main())
