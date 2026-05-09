from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_shadow_execution_service import (
    build_strategy_competition_shadow_execution_plan,
)
from openclaw.services.strategy_competition_shadow_feedback_service import (
    build_strategy_competition_shadow_execution_evidence,
)
from tests.test_strategy_competition_audit_service import (
    _independent_validator,
    _seed_competition_fixture,
)
from openclaw.services.strategy_competition_audit_service import (
    build_alpha_model_cards_from_recommendation,
    build_blocked_independent_validation_stub,
    build_blocked_shadow_execution_stub,
    build_pre_trade_risk_controls_from_recommendation,
    build_strategy_competition_portfolio_audit,
)
from openclaw.services.unified_strategy_recommendation_service import build_unified_system_recommendation


def _write_competition_audit(conn: sqlite3.Connection, tmp_path: Path, *, pretrade: bool = True) -> Path:
    _seed_competition_fixture(conn)
    recommendation = build_unified_system_recommendation(conn, trade_date="2026-05-02")
    fixed_pool = [item["strategy"] for item in recommendation["eligible_pool"]]
    cards = build_alpha_model_cards_from_recommendation(recommendation, fixed_candidate_pool=fixed_pool)
    controls = (
        build_pre_trade_risk_controls_from_recommendation(conn, recommendation=recommendation)
        if pretrade
        else {"passed": False, "controls": {}, "blocking_reasons": ["fixture_pretrade_blocked"]}
    )
    artifact = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=fixed_pool,
        alpha_model_cards=cards,
        independent_validator=build_blocked_independent_validation_stub(),
        shadow_execution=build_blocked_shadow_execution_stub(),
        pre_trade_risk_controls=controls,
        output_dir=tmp_path / "audit",
    )
    return Path(artifact["artifact_path"])


def test_shadow_execution_plan_creates_pending_orders_without_passing_shadow_evidence(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    artifact_path = _write_competition_audit(conn, tmp_path, pretrade=True)

    plan = build_strategy_competition_shadow_execution_plan(
        conn,
        competition_audit_artifact_path=str(artifact_path),
        output_dir=tmp_path / "shadow",
    )

    assert plan["plan_status"] == "shadow_execution_pending"
    assert plan["passed"] is False
    assert plan["production_candidate_allowed"] is False
    assert len(plan["orders"]) == 5
    assert plan["shadow_execution"]["passed"] is False
    assert plan["shadow_execution"]["sample_count"] == 5
    assert plan["shadow_execution"]["artifact"] == plan["artifact_path"]
    assert any(reason.startswith("missing_attribution:") for reason in plan["shadow_execution"]["blocking_reasons"])

    order_count = conn.execute("SELECT COUNT(*) FROM execution_orders WHERE decision_id = ?", (plan["decision_id"],)).fetchone()[0]
    decision = conn.execute("SELECT decision_type, based_on_run_id FROM decision_events WHERE decision_id = ?", (plan["decision_id"],)).fetchone()
    conn.close()
    assert order_count == 5
    assert decision[0] == "strategy_competition_shadow_execution_plan"
    assert decision[1] == plan["plan_run_id"]
    assert Path(plan["artifact_path"]).exists()


def test_shadow_execution_plan_blocks_when_pretrade_not_passed(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    artifact_path = _write_competition_audit(conn, tmp_path, pretrade=False)

    plan = build_strategy_competition_shadow_execution_plan(
        conn,
        competition_audit_artifact_path=str(artifact_path),
        output_dir=tmp_path / "shadow",
    )

    order_count = conn.execute("SELECT COUNT(*) FROM execution_orders").fetchone()[0]
    conn.close()
    assert plan["plan_status"] == "shadow_execution_plan_blocked"
    assert "pre_trade_controls_not_passed" in plan["blocking_reasons"]
    assert order_count == 0


def test_shadow_execution_evidence_without_feedback_stays_blocked(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    artifact_path = _write_competition_audit(conn, tmp_path, pretrade=True)
    plan = build_strategy_competition_shadow_execution_plan(
        conn,
        competition_audit_artifact_path=str(artifact_path),
        output_dir=tmp_path / "shadow",
    )

    evidence = build_strategy_competition_shadow_execution_evidence(
        conn,
        shadow_plan_artifact_path=plan["artifact_path"],
        output_dir=tmp_path / "evidence",
    )

    conn.close()
    assert evidence["status"] == "shadow_execution_blocked"
    assert evidence["passed"] is False
    assert evidence["production_candidate_allowed"] is False
    assert evidence["recorded_report_count"] == 0
    assert any(reason.startswith("missing_attribution:") for reason in evidence["shadow_execution"]["blocking_reasons"])


def test_shadow_execution_feedback_records_fills_and_passes_shadow_evidence_only(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    artifact_path = _write_competition_audit(conn, tmp_path, pretrade=True)
    plan = build_strategy_competition_shadow_execution_plan(
        conn,
        competition_audit_artifact_path=str(artifact_path),
        output_dir=tmp_path / "shadow",
    )
    feedback_path = tmp_path / "feedback.json"
    feedback_path.write_text(
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
                        "delay_sec": 600,
                        "fills": [
                            {
                                "fill_ref": "shadow_close_reference",
                                "fill_price": order["decision_price"],
                                "fill_qty": order["target_qty"],
                                "fill_slippage_bp": 0,
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

    evidence = build_strategy_competition_shadow_execution_evidence(
        conn,
        shadow_plan_artifact_path=plan["artifact_path"],
        shadow_feedback_artifact_path=str(feedback_path),
        output_dir=tmp_path / "evidence",
    )
    recommendation = build_unified_system_recommendation(conn, trade_date="2026-05-02")
    fixed_pool = [item["strategy"] for item in recommendation["eligible_pool"]]
    audit = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=fixed_pool,
        alpha_model_cards=build_alpha_model_cards_from_recommendation(
            recommendation,
            fixed_candidate_pool=fixed_pool,
        ),
        independent_validator=build_blocked_independent_validation_stub(),
        shadow_execution=evidence,
        pre_trade_risk_controls=build_pre_trade_risk_controls_from_recommendation(
            conn,
            recommendation=recommendation,
        ),
        output_dir=tmp_path / "audit2",
    )

    conn.close()
    assert evidence["status"] == "shadow_execution_passed"
    assert evidence["passed"] is True
    assert evidence["production_candidate_allowed"] is False
    assert evidence["shadow_execution"]["sample_count"] == 5
    assert evidence["shadow_execution"]["status_counts"] == {"filled": 5}
    assert audit["shadow_execution"]["passed"] is True
    assert audit["pre_trade_risk_controls"]["passed"] is True
    assert audit["independent_validation"]["passed"] is False
    assert audit["passed"] is False
    assert audit["production_candidate_allowed"] is False


def test_shadow_execution_feedback_rejects_unexplained_terminal_miss(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    artifact_path = _write_competition_audit(conn, tmp_path, pretrade=True)
    plan = build_strategy_competition_shadow_execution_plan(
        conn,
        competition_audit_artifact_path=str(artifact_path),
        output_dir=tmp_path / "shadow",
    )
    feedback_path = tmp_path / "bad_feedback.json"
    feedback_path.write_text(
        json.dumps(
            {
                "artifact_version": "strategy_competition_shadow_execution_feedback.v1",
                "reports": [
                    {
                        "order_id": plan["orders"][0]["order_id"],
                        "status": "expired",
                        "source_type": "shadow",
                        "close_price": plan["orders"][0]["decision_price"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    try:
        build_strategy_competition_shadow_execution_evidence(
            conn,
            shadow_plan_artifact_path=plan["artifact_path"],
            shadow_feedback_artifact_path=str(feedback_path),
            output_dir=tmp_path / "evidence",
        )
    except ValueError as exc:
        message = str(exc)
    else:
        message = ""
    finally:
        conn.close()

    assert message.startswith("missing_shadow_miss_reason:")
