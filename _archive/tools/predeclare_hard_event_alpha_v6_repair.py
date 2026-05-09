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

from openclaw.services.ensemble_rebuilt_candidate_rule_freeze_service import (  # noqa: E402
    HARD_EVENT_ALPHA_CANDIDATE_V6_RULE_VERSION,
    build_rebuilt_candidate_rule_freeze,
)
from openclaw.services.research_repair_iteration_flow_service import (  # noqa: E402
    attach_repair_artifact,
    create_oos_monitoring_plan_artifact,
    create_repair_flow,
    export_repair_flow_evidence_manifest,
    register_rule_freeze,
)


DEFAULT_REPAIR_WINDOWS = "20260317,20260318,20260319,20260320,20260323,20260324,20260325,20260326,20260327,20260330"
DEFAULT_OOS_WINDOWS = "20260331,20260401,20260402"
DEFAULT_V5_OOS_RESULT = (
    "logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_oos_monitor/"
    "oos_monitoring_result_hard_event_alpha_candidate_v5_20260508_20260508_124358.json"
)
DEFAULT_V5_OOS_MANIFEST = (
    "logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_oos_monitor/"
    "repair_flow_evidence_manifest_hard_event_alpha_candidate_v5_20260508_20260508_124358.json"
)
DEFAULT_POLICY_AUDIT = (
    "logs/openclaw/repair_20260508_hard_event_alpha_observation_sleeve_policy_audit_8w/"
    "ensemble_rebuilt_candidate_sleeve_policy_audit_20260508_005803.json"
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Predeclare hard_event_alpha_candidate v6 repair before any replay.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--flow-id", default="hard_event_alpha_candidate_v6_20260508")
    parser.add_argument("--policy-audit-json", default=DEFAULT_POLICY_AUDIT)
    parser.add_argument("--v5-oos-result", default=DEFAULT_V5_OOS_RESULT)
    parser.add_argument("--v5-oos-evidence-manifest", default=DEFAULT_V5_OOS_MANIFEST)
    parser.add_argument("--repair-window-set", default=DEFAULT_REPAIR_WINDOWS)
    parser.add_argument("--oos-window-set", default=DEFAULT_OOS_WINDOWS)
    parser.add_argument("--output-dir", default="logs/openclaw/repair_20260508_hard_event_alpha_v6_predeclared_repair")
    parser.add_argument("--operator-name", default="hard_event_alpha_v6_predeclare")
    args = parser.parse_args()

    policy_payload = _load(args.policy_audit_json)
    policy_audit = policy_payload.get("audit") if isinstance(policy_payload.get("audit"), dict) else policy_payload
    v5_result = _load(args.v5_oos_result)
    rule_freeze = build_rebuilt_candidate_rule_freeze(
        policy_audit,
        candidate="hard_event_alpha_candidate",
        rule_version=HARD_EVENT_ALPHA_CANDIDATE_V6_RULE_VERSION,
    )
    if rule_freeze.get("frozen") is not True:
        raise SystemExit(f"v6 rule freeze blocked: {rule_freeze.get('blocking_reasons')}")

    conn = sqlite3.connect(str(args.db_path))
    try:
        create_repair_flow(
            conn,
            flow_id=args.flow_id,
            strategy="ensemble_core",
            candidate="hard_event_alpha_candidate",
            parent_flow_id="hard_event_alpha_candidate_v5_20260508",
            attempt_no=6,
            rule_version=str(rule_freeze["rule_version"]),
            rule_hash=str(rule_freeze["rule_hash"]),
            repair_objective=(
                "repair v5 OOS failed_blocked alpha body: reduce over-veto, restore neutral alpha/hit, "
                "control turnover, and preserve risk-off repair"
            ),
            forbidden_objectives=[
                "formal_promotion",
                "top_ranking",
                "production_enablement",
                "allocator_throttle_as_alpha",
                "cash_fallback_as_alpha",
                "observation_packaging",
            ],
            predeclared_rules=rule_freeze.get("rule_spec") or {},
            fixed_window_set=_csv(args.repair_window_set),
            benchmark_config={
                "repair_window_source": "v5_oos_failed_blocked_windows",
                "oos_window_source": "next_available_post_repair_windows",
                "primary": "unthrottled target=1.0 neutral=1.0",
                "paired": "throttled target=0.75 neutral=0.45",
                "failure_targets": ["over_veto", "neutral_alpha", "neutral_hit", "turnover", "risk_off_regression"],
            },
            data_snapshot="v5_oos_failed_blocked_20260508_124358",
            operator_name=args.operator_name,
        )
        register_rule_freeze(
            conn,
            strategy="ensemble_core",
            candidate="hard_event_alpha_candidate",
            rule_version=str(rule_freeze["rule_version"]),
            rule_hash=str(rule_freeze["rule_hash"]),
            rule_spec=rule_freeze.get("rule_spec") or {},
            activation_regime="risk_off,neutral",
            operator_name=args.operator_name,
            notes="v6 predeclared only; no replay result and no promotion eligibility",
        )
        attach_repair_artifact(
            conn,
            flow_id=args.flow_id,
            artifact_type="failure_attribution",
            artifact_path=args.v5_oos_result,
            summary={
                "source": "v5_oos_failed_blocked",
                "blocking_reasons": v5_result.get("blocking_reasons") or [],
                "result_status": v5_result.get("result_status"),
            },
        )
        oos_plan = create_oos_monitoring_plan_artifact(
            conn,
            flow_id=args.flow_id,
            evidence_manifest_path=args.v5_oos_evidence_manifest,
            oos_windows=_csv(args.oos_window_set),
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
        manifest = export_repair_flow_evidence_manifest(conn, flow_id=args.flow_id, output_dir=args.output_dir)
    finally:
        conn.close()

    payload = {
        "flow_id": args.flow_id,
        "rule_version": rule_freeze["rule_version"],
        "rule_hash": rule_freeze["rule_hash"],
        "repair_window_set": _csv(args.repair_window_set),
        "oos_monitoring_plan_artifact": oos_plan["oos_monitoring_plan_artifact"],
        "repair_flow_evidence_manifest": manifest["repair_flow_evidence_manifest"],
        "v5_failure_attribution_artifact": args.v5_oos_result,
        "current_status": "repair_attempt_predeclared",
        "formal_candidate_allowed": False,
        "production_candidate_allowed": False,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _load(path: str) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
