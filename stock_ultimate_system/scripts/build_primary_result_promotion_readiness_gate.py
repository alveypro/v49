#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_promotion_readiness_gate import build_primary_result_promotion_readiness_gate


def main() -> int:
    parser = argparse.ArgumentParser(description="Build /stock governed promotion readiness gate.")
    parser.add_argument("--performance-evidence-json", default="artifacts/primary_result_performance_evidence_latest.json")
    parser.add_argument("--feedback-queue-summary-json", default="artifacts/primary_result_feedback_review_queue/summary.json")
    parser.add_argument("--baseline-current-json", default="artifacts/baselines/current.json")
    parser.add_argument("--output", default="artifacts/primary_result_promotion_readiness_gate_latest.json")
    parser.add_argument("--zero-on-blocked", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = build_primary_result_promotion_readiness_gate(
        performance_evidence_path=args.performance_evidence_json,
        feedback_queue_summary_path=args.feedback_queue_summary_json,
        baseline_current_path=args.baseline_current_json,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "decision": payload["decision"],
                    "blocking_reasons": payload["blocking_reasons"],
                    "next_actions": payload["next_actions"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    if args.zero_on_blocked and payload.get("status") == "blocked":
        return 0
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
