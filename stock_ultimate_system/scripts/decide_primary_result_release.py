#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_release_decision import PrimaryResultReleaseDecisionRegistry


def main() -> int:
    parser = argparse.ArgumentParser(description="Create an immutable primary result release decision.")
    parser.add_argument("--decisions-dir", default="artifacts/primary_result_release_decisions")
    parser.add_argument("--checklist-json", required=True)
    parser.add_argument("--decision", required=True, choices=["approved", "rejected"])
    parser.add_argument("--actor", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--decision-id")
    args = parser.parse_args()

    registry = PrimaryResultReleaseDecisionRegistry(decisions_dir=args.decisions_dir)
    try:
        decision = registry.create_decision(
            checklist_path=args.checklist_json,
            decision=args.decision,
            actor=args.actor,
            reason=args.reason,
            decision_id=args.decision_id,
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "decision_id": decision["decision_id"],
                    "decision": decision["decision"],
                    "checklist_id": decision["checklist_id"],
                    "baseline_promotion_allowed": decision["baseline_promotion_allowed"],
                    "decision_path": str(registry.history_dir / f"{decision['decision_id']}.json"),
                    "current_pointer_path": str(registry.current_path),
                    "decision_boundary": decision["decision_boundary"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
