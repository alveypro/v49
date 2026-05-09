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
    close_repair_review,
    create_repair_flow,
    export_repair_flow_evidence_manifest,
    register_rule_freeze,
)


DEFAULT_WINDOWS = "20260224,20260226,20260227,20260302,20260304,20260305,20260306,20260310,20260311,20260316"


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill hard_event_alpha_candidate v5 repair flow audit memory.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--flow-id", default="hard_event_alpha_candidate_v5_20260508")
    parser.add_argument("--unthrottled-artifact", default="logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_unthrottled_benchmark/ensemble_rebuilt_candidate_shadow_benchmark_20260508_113410.json")
    parser.add_argument("--throttled-artifact", default="logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_throttled_benchmark/ensemble_rebuilt_candidate_shadow_benchmark_20260508_113410.json")
    parser.add_argument("--attribution-artifact", default="logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_attribution/ensemble_allocator_throttle_attribution_hard_event_alpha_candidate_20260508_113434.json")
    parser.add_argument("--monitor-artifact", default="logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_monitor/ensemble_observation_monitor_hard_event_alpha_candidate_20260508_113458.json")
    parser.add_argument("--repair-review-artifact", default="logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_repair_review/ensemble_risk_off_alpha_repair_review_hard_event_alpha_candidate_20260508_113521.json")
    parser.add_argument("--output-dir", default="logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_flow")
    parser.add_argument("--operator-name", default="hard_event_alpha_v5_repair_flow_backfill")
    args = parser.parse_args()

    unthrottled = _load(args.unthrottled_artifact)
    rule_freeze = unthrottled.get("rule_freeze") if isinstance(unthrottled.get("rule_freeze"), dict) else {}
    rule_spec = rule_freeze.get("rule_spec") if isinstance(rule_freeze.get("rule_spec"), dict) else {}
    rule_version = str(rule_freeze.get("rule_version") or "hard_event_alpha_candidate.neutral_consensus_turnover_guard.v5")
    rule_hash = str(rule_freeze.get("rule_hash") or "")

    conn = sqlite3.connect(str(args.db_path))
    try:
        create_repair_flow(
            conn,
            flow_id=args.flow_id,
            strategy="ensemble_core",
            candidate="hard_event_alpha_candidate",
            attempt_no=5,
            rule_version=rule_version,
            rule_hash=rule_hash,
            repair_objective="fix neutral unthrottled failure and turnover while preserving risk-off repair",
            forbidden_objectives=[
                "formal_promotion",
                "top_ranking",
                "production_enablement",
                "allocator_throttle_as_alpha",
                "cash_fallback_as_alpha",
                "ui_or_observation_packaging",
            ],
            predeclared_rules=rule_spec,
            fixed_window_set=[item.strip() for item in DEFAULT_WINDOWS.split(",")],
            benchmark_config={
                "holding_days": 5,
                "primary": "unthrottled target=1.0 neutral=1.0",
                "paired": "throttled target=0.75 neutral=0.45",
            },
            operator_name=args.operator_name,
        )
        register_rule_freeze(
            conn,
            strategy="ensemble_core",
            candidate="hard_event_alpha_candidate",
            rule_version=rule_version,
            rule_hash=rule_hash,
            rule_spec=rule_spec,
            activation_regime="risk_off,neutral",
            operator_name=args.operator_name,
            notes="v5 repair allows observation watch discussion only; formal/top/production remain forbidden",
        )
        payload = close_repair_review(
            conn,
            flow_id=args.flow_id,
            unthrottled_artifact_path=args.unthrottled_artifact,
            throttled_artifact_path=args.throttled_artifact,
            attribution_artifact_path=args.attribution_artifact,
            monitor_artifact_path=args.monitor_artifact,
            repair_review_artifact_path=args.repair_review_artifact,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()

    conn = sqlite3.connect(str(args.db_path))
    try:
        exported = export_repair_flow_evidence_manifest(conn, flow_id=args.flow_id, output_dir=args.output_dir)
    finally:
        conn.close()
    payload.update(exported)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _load(path: str) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
