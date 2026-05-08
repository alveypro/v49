#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_candidate_handoff_runner import run_primary_result_candidate_handoff


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a controlled /stock candidate lifecycle handoff.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--lifecycles-dir", default="artifacts/primary_result_lifecycle")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--max-source-age-hours", type=float, default=72.0)
    parser.add_argument("--execute-handoff", action="store_true")
    parser.add_argument("--observation-window-start")
    parser.add_argument("--trade-calendar-json", default="artifacts/primary_result_trade_calendar_latest.json")
    parser.add_argument("--lifecycle-id")
    parser.add_argument("--handoff-gate-output", default="artifacts/primary_result_candidate_handoff_gate_latest.json")
    parser.add_argument("--lifecycle-evidence-output", default="data/experiments/primary_result_lifecycle_evidence_latest.json")
    parser.add_argument("--output", default="data/experiments/primary_result_candidate_handoff_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        exit_code, payload = run_primary_result_candidate_handoff(
            exp_dir=args.exp_dir,
            lifecycles_dir=args.lifecycles_dir,
            candidate_index=args.candidate_index,
            max_source_age_hours=args.max_source_age_hours,
            execute_handoff=args.execute_handoff,
            observation_window_start=args.observation_window_start,
            trade_calendar_path=args.trade_calendar_json,
            lifecycle_id=args.lifecycle_id,
            handoff_gate_output_path=args.handoff_gate_output,
            lifecycle_evidence_output_path=args.lifecycle_evidence_output,
            output_path=args.output,
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
                    "decision": payload["decision"],
                    "current_ts_code": payload["current_ts_code"],
                    "pointer_ts_code": payload["pointer_ts_code"],
                    "lifecycle_id": payload["lifecycle_id"],
                    "suggested_observation_window_start": dict(payload.get("observation_window_advice") or {}).get("suggested_window_start"),
                    "next_actions": payload["next_actions"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
