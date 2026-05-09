from __future__ import annotations

import sqlite3

from openclaw.services.decision_service import record_decision_event
from openclaw.services.lineage_service import (
    apply_professional_migrations,
    insert_signal_run,
    new_decision_id,
    new_run_id,
    replace_signal_items,
)
from openclaw.services.strategy_optimization_stage_service import build_strategy_optimization_stage_audit


def _credible_backtest() -> dict:
    return {
        "point_in_time_data": True,
        "suspension_and_limit_handling": True,
        "volume_constraint": True,
        "cost_model": True,
        "slippage_model": True,
        "in_sample_out_of_sample_split": True,
        "parameter_sensitivity": True,
        "failed_backtests_recorded": True,
        "sample": {"in_sample": "train_windows:3", "out_of_sample": "test_windows:3"},
        "metrics": {"win_rate": 0.56, "max_drawdown": 0.09, "signal_density": 0.04, "test_windows": 3},
    }


def _seed_run(
    conn: sqlite3.Connection,
    *,
    strategy: str,
    run_type: str = "scan",
    items: list[dict] | None = None,
    summary: dict | None = None,
) -> str:
    run_id = new_run_id(run_type, strategy)
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type=run_type,
        strategy=strategy,
        trade_date="2026-05-05",
        data_version="trade_date:20260505",
        code_version="git:test:stage",
        param_version=f"param:{strategy}:test",
        status="success",
        summary={"backtest_credibility": _credible_backtest()} if summary is None else summary,
    )
    replace_signal_items(conn, run_id=run_id, items=items or [{"ts_code": "000001.SZ", "score": 91, "rank_idx": 1}])
    return run_id


def test_strategy_optimization_stage_audit_passes_with_formal_facts(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    _seed_run(conn, strategy="v5")
    _seed_run(
        conn,
        strategy="stable",
        run_type="experiment",
        summary={"backtest_audit": _credible_backtest()},
        items=[{"ts_code": "000002.SZ", "score": 88, "rank_idx": 1}],
    )

    result = build_strategy_optimization_stage_audit(
        conn,
        trade_date="2026-05-05",
        rejected_artifacts=[
            {
                "artifact_path": "logs/openclaw/backtest_sweep_v8_rejected.json",
                "strategy": "v8",
                "reason": "eligible_for_formal_ranking_false",
                "reused_as_runtime_default": False,
            }
        ],
    )
    conn.close()

    assert result["passed"] is True
    assert result["blocking_reasons"] == []
    assert result["checklist"]["experimental_uses_same_backtest_gate"] is True
    assert result["checklist"]["no_formal_top_from_observation_pool"] is True
    assert {"v5", "stable"}.issubset(set(result["strategies_entered_competition"]))
    assert [item["competition_status"] for item in result["competition_pool"] if item["strategy"] in {"v5", "stable"}] == [
        "formal_eligible",
        "formal_eligible",
    ]
    assert result["eligible_strategies"] == ["v5", "stable"]
    assert result["top_strategies"] == ["v5", "stable"]
    assert result["maturity_plan"]["policy"]["formal_top_requires"]
    assert result["rejected_artifacts"][0]["reason"] == "eligible_for_formal_ranking_false"


def test_strategy_optimization_stage_audit_reports_full_eligible_pool_not_only_top3(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    for strategy in ("v5", "v8", "v9", "combo"):
        _seed_run(
            conn,
            strategy=strategy,
            items=[{"ts_code": f"00000{len(strategy)}.SZ", "score": 90, "rank_idx": 1}],
        )

    result = build_strategy_optimization_stage_audit(
        conn,
        trade_date="2026-05-05",
        rejected_artifacts=[{"artifact_path": "x.json", "strategy": "v7", "reason": "quality_floor_failed"}],
    )
    conn.close()

    assert result["passed"] is True
    assert set(result["eligible_strategies"]) == {"v5", "v8", "v9", "combo"}
    assert len(result["top_strategies"]) == 3


def test_strategy_optimization_stage_audit_separates_research_only_from_rejected_diagnostics(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)

    result = build_strategy_optimization_stage_audit(
        conn,
        unified_recommendation={
            "all_strategy_reviews": [
                {
                    "strategy": "ai",
                    "run_id": "",
                    "strategy_tier": "experimental",
                    "eligible_for_daily_top3": False,
                    "blocking_reasons": ["missing_signal_run"],
                    "backtest_component": {"passed": False, "eligible_for_formal_ranking": False},
                },
                {
                    "strategy": "v6",
                    "run_id": "run_v6",
                    "strategy_tier": "experimental",
                    "eligible_for_daily_top3": False,
                    "blocking_reasons": ["zero_signal_density"],
                    "backtest_component": {"passed": False, "eligible_for_formal_ranking": False},
                },
            ],
            "research_only_pool": [
                {
                    "strategy": "ai",
                    "run_id": "",
                    "strategy_tier": "experimental",
                    "research_only_reason": "no_real_runtime_backtest_handler_or_explainable_fact_chain",
                    "required_to_compete": ["real_runtime_backtest_handler"],
                    "blocking_reasons": ["missing_signal_run"],
                }
            ],
            "top_strategies": [],
            "top_stocks": [],
        },
        rejected_artifacts=[{"artifact_path": "x.json", "strategy": "v6", "reason": "zero_signal_density"}],
    )
    conn.close()

    assert result["research_only_pool"][0]["strategy"] == "ai"
    assert [item["strategy"] for item in result["rejected_strategies"]] == ["v6"]
    statuses = {item["strategy"]: item["competition_status"] for item in result["competition_pool"]}
    assert statuses == {"ai": "research_only", "v6": "diagnostic"}


def test_strategy_optimization_stage_audit_applies_manual_observation_record_without_formal_eligibility(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)

    result = build_strategy_optimization_stage_audit(
        conn,
        unified_recommendation={
            "all_strategy_reviews": [
                {
                    "strategy": "ensemble_core",
                    "run_id": "",
                    "strategy_tier": "experimental",
                    "eligible_for_daily_top3": False,
                    "blocking_reasons": ["missing_signal_run"],
                    "backtest_component": {"passed": False, "eligible_for_formal_ranking": False},
                }
            ],
            "research_only_pool": [
                {
                    "strategy": "ensemble_core",
                    "run_id": "",
                    "strategy_tier": "experimental",
                    "research_only_reason": "top_level_multi_alpha_portfolio_contract_missing",
                    "required_to_compete": ["formal_pool_shadow_benchmark"],
                    "blocking_reasons": ["missing_signal_run"],
                }
            ],
            "competition_pool": [
                {
                    "strategy": "ensemble_core",
                    "run_id": "",
                    "strategy_tier": "experimental",
                    "competition_status": "research_only",
                    "competes_for_formal_top": False,
                }
            ],
            "top_strategies": [],
            "top_stocks": [],
        },
        rejected_artifacts=[{"artifact_path": "x.json", "strategy": "v6", "reason": "zero_signal_density"}],
        observation_promotion_records=[
            {
                "record_version": "ensemble_observation_pool_record.v1",
                "record_id": "dec_apply",
                "status": "applied",
                "strategy": "ensemble_core",
                "candidate": "hard_event_alpha_candidate",
                "from_pool": "research_only",
                "to_pool": "observation",
                "source_promotion_decision_id": "dec_ready",
                "source_promotion_decision_artifact": "decision.json",
                "formal_pool_eligible": False,
                "formal_ranking_allowed": False,
            }
        ],
    )
    conn.close()

    assert result["passed"] is True
    assert result["research_only_pool"] == []
    assert result["observation_pool"][0]["strategy"] == "ensemble_core"
    assert result["observation_pool"][0]["reason"] == "manual_research_only_to_observation_transition"
    statuses = {item["strategy"]: item["competition_status"] for item in result["competition_pool"]}
    assert statuses == {"ensemble_core": "observation"}
    assert result["competition_pool"][0]["competes_for_formal_top"] is False
    assert result["top_strategies"] == []


def test_strategy_optimization_stage_audit_blocks_observation_pool_formal_top(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = _seed_run(
        conn,
        strategy="v7",
        run_type="scan",
        summary={},
        items=[{"ts_code": "000007.SZ", "score": 96, "rank_idx": 1}],
    )
    _seed_run(
        conn,
        strategy="v7",
        run_type="backtest",
        items=[],
        summary={
            "backtest_credibility": _credible_backtest(),
            "strategy_backtest_diagnostics": {
                "diagnostic_version": "strategy_backtest_diagnostics.v1",
                "credible_evidence_present": True,
                "quality_floor_passed": False,
                "eligible_for_formal_ranking": False,
                "failure_classes": ["weak_out_of_sample_win_rate"],
            },
        },
    )
    malicious_recommendation = {
        "all_strategy_reviews": [
            {
                "strategy": "v7",
                "run_id": run_id,
                "strategy_tier": "experimental",
                "eligible_for_daily_top3": False,
                "blocking_reasons": ["backtest_quality_floor_not_passed"],
                "backtest_component": {
                    "passed": True,
                    "quality_floor_passed": False,
                    "eligible_for_formal_ranking": False,
                    "blocking_reasons": [],
                },
            }
        ],
        "top_strategies": [
            {
                "strategy": "v7",
                "run_id": run_id,
                "strategy_tier": "experimental",
                "eligible_for_daily_top3": True,
                "backtest_component": {
                    "passed": True,
                    "quality_floor_passed": False,
                    "eligible_for_formal_ranking": False,
                },
            }
        ],
        "top_stocks": [
            {
                "ts_code": "000007.SZ",
                "signal_refs": [{"strategy": "v7", "run_id": run_id}],
            }
        ],
    }

    result = build_strategy_optimization_stage_audit(
        conn,
        unified_recommendation=malicious_recommendation,
        rejected_artifacts=[{"artifact_path": "x.json", "strategy": "v7", "reason": "quality_floor_failed"}],
    )
    conn.close()

    assert result["passed"] is False
    assert "formal_top_contains_observation_or_blocked_strategy" in result["blocking_reasons"]
    assert result["observation_pool"][0]["strategy"] == "v7"
    assert result["formal_top_violations"][0]["strategy"] == "v7"


def test_strategy_optimization_stage_audit_blocks_promotion_without_execution_evidence(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = _seed_run(conn, strategy="stable", run_type="experiment", summary={"backtest_audit": _credible_backtest()})
    record_decision_event(
        conn,
        decision_id=new_decision_id(),
        decision_type="experiment_promote_candidate",
        based_on_run_id=run_id,
        approval_reason_codes=["manual_promotion_without_execution"],
        approval_note="should be blocked by stage audit",
        operator_name="test",
        decision_payload={"source": "test"},
    )

    result = build_strategy_optimization_stage_audit(
        conn,
        rejected_artifacts=[{"artifact_path": "x.json", "strategy": "v8", "reason": "quality_floor_failed"}],
    )
    conn.close()

    assert result["passed"] is False
    assert "promotion_decision_missing_execution_evidence" in result["blocking_reasons"]
    assert result["promotion_decision_violations"][0]["run_id"] == run_id


def test_strategy_optimization_stage_audit_blocks_promotion_with_incomplete_execution_fields(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = _seed_run(conn, strategy="stable", run_type="experiment", summary={"backtest_audit": _credible_backtest()})
    record_decision_event(
        conn,
        decision_id=new_decision_id(),
        decision_type="experiment_promote_candidate",
        based_on_run_id=run_id,
        approval_reason_codes=["manual_promotion_without_complete_execution_fields"],
        approval_note="should be blocked by stage audit",
        operator_name="test",
        decision_payload={
            "execution_evidence": {
                "passed": True,
                "sample_count": 1,
                "linked_run_ids": [run_id],
                "cases": [{"order_id": "ord_missing_fields", "status": "filled", "fill_count": 0}],
            }
        },
    )

    result = build_strategy_optimization_stage_audit(
        conn,
        rejected_artifacts=[{"artifact_path": "x.json", "strategy": "v8", "reason": "quality_floor_failed"}],
    )
    conn.close()

    assert result["passed"] is False
    assert "promotion_decision_missing_execution_evidence" in result["blocking_reasons"]
    assert "missing_decision_id:ord_missing_fields" in result["promotion_decision_violations"][0]["evidence_issues"]
    assert "missing_attribution:ord_missing_fields" in result["promotion_decision_violations"][0]["evidence_issues"]


def test_strategy_optimization_stage_audit_blocks_reused_rejected_artifact(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    _seed_run(conn, strategy="v5")

    result = build_strategy_optimization_stage_audit(
        conn,
        rejected_artifacts=[
            {
                "artifact_path": "logs/openclaw/backtest_sweep_v5_failed.json",
                "strategy": "v5",
                "reason": "eligible_for_formal_ranking_false",
                "reused_as_runtime_default": True,
            }
        ],
    )
    conn.close()

    assert result["passed"] is False
    assert "rejected_artifact_reused_or_missing_reason" in result["blocking_reasons"]
    assert result["rejected_artifact_violations"][0]["reused_as_runtime_default"] is True
