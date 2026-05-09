#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_candidate_handoff_gate import build_primary_result_candidate_handoff_gate


def main() -> int:
    parser = argparse.ArgumentParser(description="Build /stock current candidate lifecycle handoff gate.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--lifecycles-dir", default="artifacts/primary_result_lifecycle")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--output", default="artifacts/primary_result_candidate_handoff_gate_latest.json")
    parser.add_argument("--zero-on-blocked", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = build_primary_result_candidate_handoff_gate(
        exp_dir=args.exp_dir,
        lifecycles_dir=args.lifecycles_dir,
        candidate_index=args.candidate_index,
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
