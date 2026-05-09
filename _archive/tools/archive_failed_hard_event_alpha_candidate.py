#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.research_repair_iteration_flow_service import (  # noqa: E402
    attach_repair_artifact,
    export_repair_flow_evidence_manifest,
)


DEFAULT_COMPARISON = (
    "logs/openclaw/repair_20260508_hard_event_alpha_v6_failure_comparison/"
    "v5_v6_failure_attribution_comparison_hard_event_alpha_candidate_v6_20260508_20260508_151922.json"
)
DEFAULT_GO_NO_GO = (
    "logs/openclaw/repair_20260508_hard_event_alpha_v6_failure_comparison/"
    "v7_go_no_go_review_hard_event_alpha_candidate_v6_20260508_20260508_151922.json"
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive hard_event_alpha_candidate after failed v5/v6 repair chain.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--flow-id", default="hard_event_alpha_candidate_v6_20260508")
    parser.add_argument("--comparison-artifact", default=DEFAULT_COMPARISON)
    parser.add_argument("--v7-go-no-go-artifact", default=DEFAULT_GO_NO_GO)
    parser.add_argument("--output-dir", default="logs/openclaw/repair_20260508_hard_event_alpha_failed_archive")
    parser.add_argument("--operator-name", default="hard_event_alpha_failed_archive")
    args = parser.parse_args()

    comparison = _load(args.comparison_artifact)
    go_no_go = _load(args.v7_go_no_go_artifact)
    archive = {
        "artifact_version": "failed_research_candidate_archive.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": args.operator_name,
        "strategy": "ensemble_core",
        "candidate": "hard_event_alpha_candidate",
        "archive_status": "failed_research_candidate_archived",
        "research_only": True,
        "formal_candidate_allowed": False,
        "formal_ranking_allowed": False,
        "production_candidate_allowed": False,
        "observation_watch_allowed": False,
        "source_artifacts": {
            "failure_attribution_comparison": args.comparison_artifact,
            "v7_go_no_go_review": args.v7_go_no_go_artifact,
        },
        "archive_reasons": [
            "v5_oos_failed_blocked",
            "v6_repair_review_blocked",
            "v6_oos_failed_blocked",
            "v7_no_go_until_new_alpha_body_mechanism_predeclared",
        ],
        "mechanism_diagnosis": comparison.get("mechanism_diagnosis"),
        "mechanism_gaps": comparison.get("mechanism_gaps") or [],
        "v7_decision": go_no_go.get("decision"),
        "reopen_conditions": [
            "new_alpha_body_mechanism_documented_before_any_replay",
            "new_rule_hash_predeclared_before_any_replay",
            "fixed_repair_and_oos_windows_predeclared",
            "explicit_abandon_condition_predeclared",
            "governance_approval_to_reopen_failed_research_candidate",
        ],
        "prohibited_actions": [
            "formal",
            "top",
            "production",
            "observation_packaging",
            "allocator_throttle_as_alpha",
            "cash_fallback_as_alpha",
            "posthoc_parameter_search",
        ],
    }
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = output / f"failed_research_candidate_archive_hard_event_alpha_candidate_{stamp}.json"
    archive_path.write_text(json.dumps(archive, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    conn = sqlite3.connect(str(args.db_path))
    try:
        attach_repair_artifact(
            conn,
            flow_id=args.flow_id,
            artifact_type="failed_research_candidate_archive",
            artifact_path=str(archive_path),
            summary={"archive_status": archive["archive_status"]},
        )
        manifest = export_repair_flow_evidence_manifest(conn, flow_id=args.flow_id, output_dir=args.output_dir)
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "failed_research_candidate_archive_artifact": str(archive_path),
                "repair_flow_evidence_manifest": manifest["repair_flow_evidence_manifest"],
                "archive_status": archive["archive_status"],
                "formal_candidate_allowed": False,
                "production_candidate_allowed": False,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _load(path: str) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
