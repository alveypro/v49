#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.research.all_strategy_evidence_run import (  # noqa: E402
    DEFAULT_EVIDENCE_STRATEGIES,
    AllStrategyEvidenceRunConfig,
    run_all_strategy_evidence,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run scan/backtest evidence chain for all eligible strategy families.")
    parser.add_argument("--module-path", default="v49_app.py")
    parser.add_argument("--output-dir", default="logs/openclaw/all_strategy_evidence")
    parser.add_argument("--date-from", default="2025-11-01")
    parser.add_argument("--date-to", default="2026-05-03")
    parser.add_argument("--train-window-days", type=int, default=90)
    parser.add_argument("--test-window-days", type=int, default=30)
    parser.add_argument("--step-days", type=int, default=30)
    parser.add_argument("--strategies", default=",".join(DEFAULT_EVIDENCE_STRATEGIES))
    parser.add_argument("--offline-stock-limit", type=int, default=300)
    parser.add_argument("--scan-limit", type=int, default=30)
    parser.add_argument("--sweep-max-runs", type=int, default=2)
    parser.add_argument("--per-run-timeout-sec", type=int, default=180)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--rejected-ledger", default="logs/openclaw/rejected_backtest_artifacts.jsonl")
    parser.add_argument("--operator-name", default="all_strategy_evidence_run")
    parser.add_argument("--no-research-only", action="store_true")
    args = parser.parse_args()

    payload = run_all_strategy_evidence(
        AllStrategyEvidenceRunConfig(
            module_path=Path(args.module_path),
            output_dir=Path(args.output_dir),
            date_from=args.date_from,
            date_to=args.date_to,
            train_window_days=args.train_window_days,
            test_window_days=args.test_window_days,
            step_days=args.step_days,
            strategies=[x.strip() for x in str(args.strategies).split(",") if x.strip()],
            include_research_only=not args.no_research_only,
            scan_offline_stock_limit=args.offline_stock_limit,
            scan_limit=args.scan_limit,
            sweep_max_runs=(args.sweep_max_runs if args.sweep_max_runs > 0 else None),
            per_run_timeout_sec=(args.per_run_timeout_sec if args.per_run_timeout_sec > 0 else None),
            db_path=args.db_path,
            rejected_ledger=args.rejected_ledger,
            operator_name=args.operator_name,
        )
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
