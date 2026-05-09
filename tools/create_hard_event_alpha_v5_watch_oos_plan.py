#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.research_repair_iteration_flow_service import (  # noqa: E402
    create_oos_monitoring_plan_artifact,
    create_watch_risk_review_artifact,
    export_repair_flow_evidence_manifest,
)


DEFAULT_OOS_WINDOWS = "20260317,20260318,20260319,20260320,20260323,20260324,20260325,20260326,20260327,20260330"


def main() -> int:
    parser = argparse.ArgumentParser(description="Create v5 watch risk review and predeclared OOS monitoring plan.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--flow-id", default="hard_event_alpha_candidate_v5_20260508")
    parser.add_argument(
        "--evidence-manifest",
        default="logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_flow/repair_flow_evidence_manifest_hard_event_alpha_candidate_v5_20260508_20260508_120816.json",
    )
    parser.add_argument(
        "--output-dir",
        default="logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_watch_oos_plan",
    )
    parser.add_argument("--oos-window-set", default=DEFAULT_OOS_WINDOWS)
    parser.add_argument("--operator-name", default="hard_event_alpha_v5_watch_oos_plan")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path))
    try:
        review = create_watch_risk_review_artifact(
            conn,
            flow_id=args.flow_id,
            evidence_manifest_path=args.evidence_manifest,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
        plan = create_oos_monitoring_plan_artifact(
            conn,
            flow_id=args.flow_id,
            evidence_manifest_path=args.evidence_manifest,
            oos_windows=_csv(args.oos_window_set),
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
        manifest = export_repair_flow_evidence_manifest(conn, flow_id=args.flow_id, output_dir=args.output_dir)
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "flow_id": args.flow_id,
                "watch_risk_review_artifact": review["watch_risk_review_artifact"],
                "oos_monitoring_plan_artifact": plan["oos_monitoring_plan_artifact"],
                "repair_flow_evidence_manifest": manifest["repair_flow_evidence_manifest"],
                "current_stage": "watch_risk_review_plus_predeclared_oos_monitoring_plan",
                "formal_candidate_allowed": False,
                "production_candidate_allowed": False,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


if __name__ == "__main__":
    raise SystemExit(main())
