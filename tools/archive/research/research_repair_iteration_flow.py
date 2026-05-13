#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.research_repair_iteration_flow_service import (  # noqa: E402
    apply_research_repair_flow_migration,
    attach_repair_artifact,
    build_repair_flow_snapshot,
    close_repair_review,
    create_oos_monitoring_plan_artifact,
    create_oos_monitoring_result_artifact,
    create_repair_flow,
    create_watch_risk_review_artifact,
    export_repair_flow_evidence_manifest,
    record_watch_risk,
    register_rule_freeze,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage research repair flow audit memory.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    sub = parser.add_subparsers(dest="cmd", required=True)

    create = sub.add_parser("create")
    create.add_argument("--flow-id", required=True)
    create.add_argument("--strategy", required=True)
    create.add_argument("--candidate", required=True)
    create.add_argument("--attempt-no", type=int, required=True)
    create.add_argument("--rule-version", required=True)
    create.add_argument("--rule-hash", required=True)
    create.add_argument("--repair-objective", required=True)
    create.add_argument("--fixed-window-set", default="")
    create.add_argument("--benchmark-config-json", default="")
    create.add_argument("--predeclared-rules-json", default="")
    create.add_argument("--operator-name", default="")
    create.add_argument("--governance-reopen-approved", action="store_true")

    freeze = sub.add_parser("register-rule-freeze")
    freeze.add_argument("--strategy", required=True)
    freeze.add_argument("--candidate", required=True)
    freeze.add_argument("--rule-version", required=True)
    freeze.add_argument("--rule-hash", required=True)
    freeze.add_argument("--rule-spec-json", required=True)
    freeze.add_argument("--activation-regime", default="")
    freeze.add_argument("--operator-name", default="")

    attach = sub.add_parser("attach-artifact")
    attach.add_argument("--flow-id", required=True)
    attach.add_argument("--artifact-type", required=True)
    attach.add_argument("--artifact-path", required=True)
    attach.add_argument("--summary-json", default="")

    close = sub.add_parser("close-review")
    close.add_argument("--flow-id", required=True)
    close.add_argument("--unthrottled-artifact", required=True)
    close.add_argument("--throttled-artifact", required=True)
    close.add_argument("--attribution-artifact", required=True)
    close.add_argument("--repair-review-artifact", required=True)
    close.add_argument("--monitor-artifact", default="")
    close.add_argument("--operator-name", default="")

    risk = sub.add_parser("register-risk")
    risk.add_argument("--flow-id", required=True)
    risk.add_argument("--strategy", required=True)
    risk.add_argument("--candidate", required=True)
    risk.add_argument("--risk-code", required=True)
    risk.add_argument("--risk-level", required=True)
    risk.add_argument("--risk-description", required=True)
    risk.add_argument("--metric-name", required=True)
    risk.add_argument("--metric-value", type=float, required=True)
    risk.add_argument("--threshold-value", type=float, required=True)
    risk.add_argument("--required-monitoring", required=True)
    risk.add_argument("--exit-condition", required=True)

    summary = sub.add_parser("summarize")
    summary.add_argument("--flow-id", required=True)

    export = sub.add_parser("export-evidence")
    export.add_argument("--flow-id", required=True)
    export.add_argument("--output-dir", required=True)

    review = sub.add_parser("create-watch-risk-review")
    review.add_argument("--flow-id", required=True)
    review.add_argument("--evidence-manifest", required=True)
    review.add_argument("--output-dir", required=True)
    review.add_argument("--operator-name", default="")

    oos = sub.add_parser("create-oos-monitoring-plan")
    oos.add_argument("--flow-id", required=True)
    oos.add_argument("--evidence-manifest", required=True)
    oos.add_argument("--oos-window-set", required=True)
    oos.add_argument("--output-dir", required=True)
    oos.add_argument("--operator-name", default="")

    oos_result = sub.add_parser("create-oos-monitoring-result")
    oos_result.add_argument("--flow-id", required=True)
    oos_result.add_argument("--oos-plan", required=True)
    oos_result.add_argument("--unthrottled-artifact", required=True)
    oos_result.add_argument("--throttled-artifact", required=True)
    oos_result.add_argument("--output-dir", required=True)
    oos_result.add_argument("--operator-name", default="")

    migrate = sub.add_parser("migrate")

    args = parser.parse_args()
    conn = sqlite3.connect(str(args.db_path))
    try:
        if args.cmd == "migrate":
            apply_research_repair_flow_migration(conn)
            payload = {"migrated": True}
        elif args.cmd == "create":
            payload = create_repair_flow(
                conn,
                flow_id=args.flow_id,
                strategy=args.strategy,
                candidate=args.candidate,
                attempt_no=args.attempt_no,
                rule_version=args.rule_version,
                rule_hash=args.rule_hash,
                repair_objective=args.repair_objective,
                fixed_window_set=_csv(args.fixed_window_set),
                benchmark_config=_json_arg(args.benchmark_config_json),
                predeclared_rules=_json_arg(args.predeclared_rules_json),
                operator_name=args.operator_name,
                governance_reopen_approved=bool(args.governance_reopen_approved),
            )
        elif args.cmd == "register-rule-freeze":
            payload = register_rule_freeze(
                conn,
                strategy=args.strategy,
                candidate=args.candidate,
                rule_version=args.rule_version,
                rule_hash=args.rule_hash,
                rule_spec=_load_json_arg(args.rule_spec_json),
                activation_regime=args.activation_regime,
                operator_name=args.operator_name,
            )
        elif args.cmd == "attach-artifact":
            payload = attach_repair_artifact(
                conn,
                flow_id=args.flow_id,
                artifact_type=args.artifact_type,
                artifact_path=args.artifact_path,
                summary=_json_arg(args.summary_json),
            )
        elif args.cmd == "close-review":
            payload = close_repair_review(
                conn,
                flow_id=args.flow_id,
                unthrottled_artifact_path=args.unthrottled_artifact,
                throttled_artifact_path=args.throttled_artifact,
                attribution_artifact_path=args.attribution_artifact,
                repair_review_artifact_path=args.repair_review_artifact,
                monitor_artifact_path=args.monitor_artifact,
                operator_name=args.operator_name,
            )
        elif args.cmd == "register-risk":
            payload = record_watch_risk(
                conn,
                flow_id=args.flow_id,
                strategy=args.strategy,
                candidate=args.candidate,
                risk_code=args.risk_code,
                risk_level=args.risk_level,
                risk_description=args.risk_description,
                metric_name=args.metric_name,
                metric_value=args.metric_value,
                threshold_value=args.threshold_value,
                required_monitoring=args.required_monitoring,
                exit_condition=args.exit_condition,
            )
        elif args.cmd == "export-evidence":
            payload = export_repair_flow_evidence_manifest(
                conn,
                flow_id=args.flow_id,
                output_dir=args.output_dir,
            )
        elif args.cmd == "create-watch-risk-review":
            payload = create_watch_risk_review_artifact(
                conn,
                flow_id=args.flow_id,
                evidence_manifest_path=args.evidence_manifest,
                output_dir=args.output_dir,
                operator_name=args.operator_name,
            )
        elif args.cmd == "create-oos-monitoring-plan":
            payload = create_oos_monitoring_plan_artifact(
                conn,
                flow_id=args.flow_id,
                evidence_manifest_path=args.evidence_manifest,
                oos_windows=_csv(args.oos_window_set),
                output_dir=args.output_dir,
                operator_name=args.operator_name,
            )
        elif args.cmd == "create-oos-monitoring-result":
            payload = create_oos_monitoring_result_artifact(
                conn,
                flow_id=args.flow_id,
                oos_plan_path=args.oos_plan,
                unthrottled_artifact_path=args.unthrottled_artifact,
                throttled_artifact_path=args.throttled_artifact,
                output_dir=args.output_dir,
                operator_name=args.operator_name,
            )
        else:
            payload = build_repair_flow_snapshot(conn, flow_id=args.flow_id)
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _json_arg(raw: str) -> dict:
    if not str(raw or "").strip():
        return {}
    payload = json.loads(str(raw))
    return payload if isinstance(payload, dict) else {}


def _load_json_arg(raw: str) -> dict:
    text = str(raw or "").strip()
    if not text:
        return {}
    p = Path(text)
    if p.exists():
        payload = json.loads(p.read_text(encoding="utf-8"))
    else:
        payload = json.loads(text)
    return payload if isinstance(payload, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
