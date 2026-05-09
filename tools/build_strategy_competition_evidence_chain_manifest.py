#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402
from openclaw.services.strategy_competition_evidence_chain_manifest_service import (  # noqa: E402
    build_strategy_competition_evidence_chain_manifest,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a full evidence-chain manifest for strategy competition Top5.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--competition-audit-artifact", default="")
    parser.add_argument("--evidence-intake-packet-artifact", default="")
    parser.add_argument("--evidence-submission-review-artifact", default="")
    parser.add_argument("--formal-validation-handoff-artifact", default="")
    parser.add_argument("--formal-validation-result-review-artifact", default="")
    parser.add_argument("--release-chain-adjudication-artifact", default="")
    parser.add_argument("--human-release-approval-artifact", default="")
    parser.add_argument("--live-order-authority-artifact", default="")
    parser.add_argument("--broker-submission-guard-artifact", default="")
    parser.add_argument("--broker-submission-response-artifact", default="")
    parser.add_argument("--broker-execution-feedback-artifact", default="")
    parser.add_argument("--post-trade-reconciliation-artifact", default="")
    parser.add_argument("--trade-lifecycle-adjudication-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_evidence_chain_manifest")
    parser.add_argument("--operator-name", default="strategy_competition_evidence_chain_manifest")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_evidence_chain_manifest(
            conn,
            output_dir=args.output_dir,
            competition_audit_artifact_path=args.competition_audit_artifact,
            evidence_intake_packet_artifact_path=args.evidence_intake_packet_artifact,
            evidence_submission_review_artifact_path=args.evidence_submission_review_artifact,
            formal_validation_handoff_artifact_path=args.formal_validation_handoff_artifact,
            formal_validation_result_review_artifact_path=args.formal_validation_result_review_artifact,
            release_chain_adjudication_artifact_path=args.release_chain_adjudication_artifact,
            human_release_approval_artifact_path=args.human_release_approval_artifact,
            live_order_authority_artifact_path=args.live_order_authority_artifact,
            broker_submission_guard_artifact_path=args.broker_submission_guard_artifact,
            broker_submission_response_artifact_path=args.broker_submission_response_artifact,
            broker_execution_feedback_artifact_path=args.broker_execution_feedback_artifact,
            post_trade_reconciliation_artifact_path=args.post_trade_reconciliation_artifact,
            trade_lifecycle_adjudication_artifact_path=args.trade_lifecycle_adjudication_artifact,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
