#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_release_evidence_checklist import PrimaryResultReleaseEvidenceChecklistRegistry


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build an immutable release evidence checklist from an accepted feedback review item."
    )
    parser.add_argument("--checklists-dir", default="artifacts/primary_result_release_evidence_checklists")
    parser.add_argument("--review-item-json", required=True)
    parser.add_argument("--benchmark-report-json")
    parser.add_argument("--benchmark-diff-json")
    parser.add_argument("--release-gates-json")
    parser.add_argument("--release-evidence-bundle-json")
    parser.add_argument("--manifest-json")
    parser.add_argument("--baseline-policy-decision-json")
    parser.add_argument("--checklist-id")
    args = parser.parse_args()

    registry = PrimaryResultReleaseEvidenceChecklistRegistry(checklists_dir=args.checklists_dir)
    try:
        checklist = registry.create_checklist(
            review_item_path=args.review_item_json,
            benchmark_report_path=args.benchmark_report_json,
            benchmark_diff_path=args.benchmark_diff_json,
            release_gates_path=args.release_gates_json,
            release_evidence_bundle_path=args.release_evidence_bundle_json,
            manifest_path=args.manifest_json,
            baseline_policy_decision_path=args.baseline_policy_decision_json,
            checklist_id=args.checklist_id,
        )
        print(
            json.dumps(
                {
                    "status": checklist["status"],
                    "checklist_id": checklist["checklist_id"],
                    "review_id": checklist["review_id"],
                    "missing_evidence": checklist["missing_evidence"],
                    "blocking_gate_reason": checklist["blocking_gate_reason"],
                    "checklist_path": str(registry.history_dir / f"{checklist['checklist_id']}.json"),
                    "current_pointer_path": str(registry.current_path),
                    "release_boundary": checklist["release_boundary"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
