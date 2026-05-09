from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_audit_service import (
    build_alpha_model_cards_from_recommendation,
    build_blocked_independent_validation_stub,
    build_pre_trade_risk_controls_from_recommendation,
    build_strategy_competition_portfolio_audit,
)
from openclaw.services.strategy_competition_independent_validation_service import (
    build_strategy_competition_independent_validation,
)
from openclaw.services.strategy_competition_shadow_execution_service import build_strategy_competition_shadow_execution_plan
from openclaw.services.strategy_competition_shadow_feedback_service import build_strategy_competition_shadow_execution_evidence
from openclaw.services.unified_strategy_recommendation_service import build_unified_system_recommendation
from tests.test_strategy_competition_shadow_execution_service import _write_competition_audit


def _write_shadow_feedback(plan: dict, tmp_path: Path) -> Path:
    path = tmp_path / "shadow_feedback.json"
    path.write_text(
        json.dumps(
            {
                "artifact_version": "strategy_competition_shadow_execution_feedback.v1",
                "reports": [
                    {
                        "order_id": order["order_id"],
                        "ts_code": order["ts_code"],
                        "status": "filled",
                        "source_type": "shadow",
                        "broker_ref": f"shadow_feedback:{order['order_id']}",
                        "close_price": order["decision_price"] * 1.01,
                        "delay_sec": 300,
                        "fills": [
                            {
                                "fill_ref": "shadow_reference",
                                "fill_price": order["decision_price"],
                                "fill_qty": order["target_qty"],
                                "venue": "shadow",
                            }
                        ],
                    }
                    for order in plan["orders"]
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _validator_report(*, required_artifacts: list[str], tmp_path: Path, name: str = "external_reviewer") -> Path:
    path = tmp_path / "validator_report.json"
    path.write_text(
        json.dumps(
            {
                "artifact_version": "strategy_competition_independent_validator_decision.v1",
                "decision": "approved",
                "validator_name": name,
                "validator_role": "independent_validator",
                "conflict_of_interest_attestation": True,
                "reviewed_artifacts": required_artifacts,
                "review_scope": [
                    "fixed_candidate_pool",
                    "model_cards",
                    "top5_portfolio",
                    "shadow_execution",
                    "pre_trade_controls",
                    "promotion_boundaries",
                ],
                "validation_summary": "Reviewed fixed candidate pool, model cards, Top5 construction, shadow evidence, pre-trade controls, and promotion boundaries.",
                "risk_notes": ["approval_does_not_bypass_competition_audit"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def test_independent_validation_blocks_when_shadow_execution_is_not_passed(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    audit_path = _write_competition_audit(conn, tmp_path, pretrade=True)
    request = build_strategy_competition_independent_validation(
        conn,
        competition_audit_artifact_path=str(audit_path),
        output_dir=tmp_path / "validation",
    )

    conn.close()
    assert request["passed"] is False
    assert request["decision"] == "blocked"
    assert "independent_validator_decision_missing" in request["blocking_reasons"]
    assert "shadow_execution_not_passed" in request["blocking_reasons"]
    assert request["production_candidate_allowed"] is False


def test_independent_validation_passes_only_after_external_validator_and_upstream_evidence(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    base_audit_path = _write_competition_audit(conn, tmp_path, pretrade=True)
    plan = build_strategy_competition_shadow_execution_plan(
        conn,
        competition_audit_artifact_path=str(base_audit_path),
        output_dir=tmp_path / "shadow_plan",
    )
    feedback_path = _write_shadow_feedback(plan, tmp_path)
    shadow_evidence = build_strategy_competition_shadow_execution_evidence(
        conn,
        shadow_plan_artifact_path=plan["artifact_path"],
        shadow_feedback_artifact_path=str(feedback_path),
        output_dir=tmp_path / "shadow_evidence",
    )
    recommendation = build_unified_system_recommendation(conn, trade_date="2026-05-02")
    fixed_pool = [item["strategy"] for item in recommendation["eligible_pool"]]
    pretrade = build_pre_trade_risk_controls_from_recommendation(
        conn,
        recommendation=recommendation,
        output_path=tmp_path / "pretrade.json",
    )
    audit = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=fixed_pool,
        alpha_model_cards=build_alpha_model_cards_from_recommendation(recommendation, fixed_candidate_pool=fixed_pool),
        independent_validator=build_blocked_independent_validation_stub(),
        shadow_execution=shadow_evidence,
        pre_trade_risk_controls=pretrade,
        output_dir=tmp_path / "audit_with_shadow",
    )
    required_artifacts = [
        audit["artifact_path"],
        shadow_evidence["artifact_path"],
        pretrade["artifact"],
    ]
    validator_path = _validator_report(required_artifacts=required_artifacts, tmp_path=tmp_path)

    validation = build_strategy_competition_independent_validation(
        conn,
        competition_audit_artifact_path=audit["artifact_path"],
        validator_decision_artifact_path=str(validator_path),
        output_dir=tmp_path / "validation",
    )
    final_audit = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=fixed_pool,
        alpha_model_cards=build_alpha_model_cards_from_recommendation(recommendation, fixed_candidate_pool=fixed_pool),
        independent_validator=validation,
        shadow_execution=shadow_evidence,
        pre_trade_risk_controls=pretrade,
        output_dir=tmp_path / "final_audit",
    )

    conn.close()
    assert validation["passed"] is True
    assert validation["decision"] == "approved"
    assert validation["production_candidate_allowed"] is False
    assert final_audit["independent_validation"]["passed"] is True
    assert final_audit["shadow_execution"]["passed"] is True
    assert final_audit["pre_trade_risk_controls"]["passed"] is True
    assert final_audit["passed"] is True
    assert final_audit["production_candidate_allowed"] is True


def test_independent_validation_rejects_self_approval(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    audit_path = _write_competition_audit(conn, tmp_path, pretrade=True)
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    validator_path = _validator_report(
        required_artifacts=[audit["artifact_path"]],
        tmp_path=tmp_path,
        name="strategy_competition_independent_validation",
    )

    validation = build_strategy_competition_independent_validation(
        conn,
        competition_audit_artifact_path=str(audit_path),
        validator_decision_artifact_path=str(validator_path),
        output_dir=tmp_path / "validation",
        operator_name="strategy_competition_independent_validation",
    )

    conn.close()
    assert validation["passed"] is False
    assert "independent_validator_self_approval" in validation["blocking_reasons"]
