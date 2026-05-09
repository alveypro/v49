#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_feedback_loop import run_primary_result_feedback_loop


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the governed primary result failure feedback loop.")
    parser.add_argument("--observation-json", default="data/experiments/primary_result_observation_latest.json")
    parser.add_argument("--terminal-json", default="data/experiments/primary_result_terminal_latest.json")
    parser.add_argument("--ledger-jsonl", default="artifacts/primary_result_performance/ledger.jsonl")
    parser.add_argument("--attribution-output", default="data/experiments/primary_result_failure_attribution_latest.json")
    parser.add_argument("--feedback-output", default="data/experiments/primary_result_learning_feedback_latest.json")
    parser.add_argument("--queue-dir", default="artifacts/primary_result_feedback_review_queue")
    parser.add_argument("--owner", default="system")
    parser.add_argument("--output", default="data/experiments/primary_result_feedback_loop_latest.json")
    parser.add_argument("--min-success-return", type=float, default=0.0)
    parser.add_argument("--min-excess-return", type=float, default=0.0)
    parser.add_argument("--max-drawdown-floor", type=float, default=-0.08)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        exit_code, payload = run_primary_result_feedback_loop(
            observation_path=args.observation_json,
            terminal_path=args.terminal_json,
            ledger_jsonl_path=args.ledger_jsonl,
            attribution_output_path=args.attribution_output,
            feedback_output_path=args.feedback_output,
            queue_dir=args.queue_dir,
            owner=args.owner,
            output_path=args.output,
            min_success_return=args.min_success_return,
            min_excess_return=args.min_excess_return,
            max_drawdown_floor=args.max_drawdown_floor,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "observation_status": payload["observation_status"],
                    "attribution_required": payload["attribution_required"],
                    "change_total": payload["change_total"],
                    "queue_status": payload["queue_status"],
                    "review_id": payload["review_id"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
