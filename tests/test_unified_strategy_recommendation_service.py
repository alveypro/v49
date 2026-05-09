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
from openclaw.services.unified_strategy_recommendation_service import build_unified_system_recommendation


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
        "sample": {"in_sample": "2024", "out_of_sample": "2025"},
        "metrics": {"annual_return": 0.18, "max_drawdown": 0.09, "signal_density": 0.04, "test_windows": 3},
    }


def _seed_run(
    conn: sqlite3.Connection,
    *,
    strategy: str,
    items: list[dict],
    trade_date: str = "2026-05-02",
    run_type: str = "scan",
    summary: dict | None = None,
    status: str = "success",
    artifact_path: str = "",
) -> str:
    run_id = new_run_id(run_type, strategy)
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type=run_type,
        strategy=strategy,
        trade_date=trade_date,
        data_version=f"trade_date:{trade_date}",
        code_version="git:test:unified",
        param_version=f"param:{strategy}:test",
        status=status,
        artifact_path=artifact_path,
        summary={"backtest_credibility": _credible_backtest()} if summary is None else summary,
    )
    replace_signal_items(conn, run_id=run_id, items=items)
    return run_id


def _record_release_decision(conn: sqlite3.Connection, *, run_id: str) -> str:
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="daily_strategy_review",
        based_on_run_id=run_id,
        risk_gate_state={"risk_level": "green"},
        release_gate_state={"release_id": "rel_unified_test", "passed": True},
        approval_reason_codes=["daily_review"],
        approval_note="unified recommendation fixture",
        operator_name="test",
        decision_payload={"source": "unified_strategy_recommendation"},
    )
    return decision_id


def test_build_unified_system_recommendation_preserves_stage_and_returns_top_lists(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    v5_run = _seed_run(
        conn,
        strategy="v5",
        items=[
            {"ts_code": "000001.SZ", "score": 93, "rank_idx": 1, "reason_codes": ["v5_strength"]},
            {"ts_code": "000002.SZ", "score": 88, "rank_idx": 2, "reason_codes": ["v5_quality"]},
        ],
        summary={
            "backtest_credibility": _credible_backtest(),
            "execution_evidence": {"mode": "quasi_live", "sample_count": 2, "linked_decision_ids": ["dec_exec_v5"]},
        },
    )
    combo_run = _seed_run(
        conn,
        strategy="combo",
        items=[
            {"ts_code": "000001.SZ", "score": 91, "rank_idx": 1, "reason_codes": ["combo_consensus"]},
            {"ts_code": "000003.SZ", "score": 87, "rank_idx": 2, "reason_codes": ["combo_rank"]},
        ],
    )
    stable_run = _seed_run(
        conn,
        strategy="stable",
        run_type="experiment",
        items=[
            {"ts_code": "000004.SZ", "score": 95, "rank_idx": 1, "reason_codes": ["stable_experiment"]},
            {"ts_code": "000001.SZ", "score": 80, "rank_idx": 2, "reason_codes": ["stable_overlap"]},
        ],
        summary={
            "governance_type": "experiment_strategy_evidence",
            "backtest_audit": _credible_backtest(),
            "execution_evidence": {"mode": "shadow", "sample_count": 1, "linked_decision_ids": ["dec_exec_stable"]},
        },
    )
    _seed_run(
        conn,
        strategy="v8",
        items=[{"ts_code": "000005.SZ", "score": 99, "rank_idx": 1, "reason_codes": ["missing_backtest"]}],
        summary={},
    )
    decision_id = _record_release_decision(conn, run_id=v5_run)

    result = build_unified_system_recommendation(conn, trade_date="2026-05-02")
    conn.close()

    assert result["passed"] is True
    assert result["blocking_reasons"] == []
    assert result["policy"]["unified_user_output"] is True
    assert result["policy"]["single_competition_dimension"] is True
    assert result["policy"]["preserve_internal_strategy_stage"] is True
    assert result["policy"]["governance_labels_are_informational"] is True
    assert result["policy"]["all_registered_strategies_reviewed"] is True
    assert [item["strategy"] for item in result["top_strategies"]] == ["v5", "stable", "combo"]
    assert [item["strategy"] for item in result["eligible_pool"]] == ["v5", "stable", "combo"]
    assert "ai" in {item["strategy"] for item in result["research_only_pool"]}
    assert "ensemble_core" in {item["strategy"] for item in result["research_only_pool"]}
    assert result["top_strategies"][0]["strategy_stage"] == "primary"
    assert result["top_strategies"][0]["strategy_tier"] == "production"
    assert result["top_strategies"][0]["decision_state"]["decision_id"] == decision_id
    stable = [item for item in result["top_strategies"] if item["strategy"] == "stable"][0]
    assert stable["strategy_tier"] == "experimental"
    assert stable["run_type"] == "experiment"
    assert result["top_stocks"][0]["ts_code"] == "000001.SZ"
    assert result["top_stocks"][0]["consensus_count"] == 3
    blocked_v8 = [item for item in result["all_strategy_reviews"] if item["strategy"] == "v8"][0]
    assert blocked_v8["eligible_for_daily_top3"] is False
    assert "backtest_credibility_not_passed" in blocked_v8["blocking_reasons"]


def test_build_unified_system_recommendation_allows_experimental_strategy_to_compete_on_same_fact_requirements(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    _seed_run(
        conn,
        strategy="stable",
        run_type="scan",
        items=[{"ts_code": "000001.SZ", "score": 99, "rank_idx": 1}],
        summary={"backtest_credibility": _credible_backtest()},
    )

    result = build_unified_system_recommendation(conn)
    conn.close()

    assert result["passed"] is True
    assert result["blocking_reasons"] == []
    review = result["top_strategies"][0]
    assert review["strategy"] == "stable"
    assert review["strategy_tier"] == "experimental"
    assert review["eligible_for_daily_top3"] is True
    assert review["blocking_reasons"] == []


def test_build_unified_system_recommendation_uses_latest_strategy_backtest_for_scan_qualification(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    backtest_run = _seed_run(
        conn,
        strategy="v8",
        run_type="backtest",
        items=[],
        summary={"backtest_credibility": _credible_backtest()},
    )
    scan_run = _seed_run(
        conn,
        strategy="v8",
        run_type="scan",
        items=[{"ts_code": "000008.SZ", "score": 92, "rank_idx": 1, "reason_codes": ["v8_signal"]}],
        summary={},
    )

    result = build_unified_system_recommendation(conn)
    conn.close()

    assert result["passed"] is True
    assert [item["strategy"] for item in result["top_strategies"]] == ["v8"]
    review = result["top_strategies"][0]
    assert review["run_id"] == scan_run
    assert review["backtest_component"]["passed"] is True
    assert review["backtest_component"]["source"] == "latest_strategy_backtest"
    assert review["backtest_component"]["source_run_id"] == backtest_run
    assert review["backtest_component"]["current_run_blocking_reasons"]
    assert result["top_stocks"][0]["ts_code"] == "000008.SZ"


def test_build_unified_system_recommendation_prefers_diagnostic_sweep_over_newer_empty_backtest(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    sweep_run = _seed_run(
        conn,
        strategy="v7",
        run_type="backtest",
        items=[],
        status="failed",
        artifact_path="logs/openclaw/backtest_sweep_v7_failed.json",
        summary={
            "backtest_credibility": {
                **_credible_backtest(),
                "parameter_sensitivity": False,
                "metrics": {"win_rate": 0.0, "max_drawdown": 0.0, "signal_density": 0.0, "test_windows": 0},
            },
            "strategy_backtest_diagnostics": {
                "diagnostic_version": "strategy_backtest_diagnostics.v1",
                "credible_evidence_present": False,
                "quality_floor_passed": False,
                "eligible_for_formal_ranking": False,
                "failure_classes": ["zero_signal_density", "no_successful_test_windows"],
            },
        },
    )
    raw_runs = [
        _seed_run(
            conn,
            strategy="v7",
            run_type="backtest",
            items=[],
            summary={"meta": {}, "metrics": {}, "params": {"window": idx}},
        )
        for idx in range(25)
    ]
    _seed_run(
        conn,
        strategy="v7",
        run_type="scan",
        items=[{"ts_code": "000007.SZ", "score": 96, "rank_idx": 1, "reason_codes": ["v7_signal"]}],
        summary={},
    )

    result = build_unified_system_recommendation(conn)
    conn.close()

    review = [item for item in result["all_strategy_reviews"] if item["strategy"] == "v7"][0]
    assert review["backtest_component"]["source_run_id"] == sweep_run
    assert review["backtest_component"]["source_run_id"] not in raw_runs
    assert review["backtest_component"]["source_run_status"] == "failed"
    assert review["backtest_component"]["source_artifact_path"].endswith("backtest_sweep_v7_failed.json")
    assert review["backtest_component"]["diagnostic_version"] == "strategy_backtest_diagnostics.v1"
    assert review["eligible_for_daily_top3"] is False
    assert "backtest_credibility_not_passed" in review["blocking_reasons"]


def test_build_unified_system_recommendation_does_not_let_newer_rejected_research_sweep_override_formal_evidence(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    formal_run = _seed_run(
        conn,
        strategy="v4",
        run_type="backtest",
        items=[],
        trade_date="2026-05-06",
        artifact_path="logs/openclaw/formal_backtest_sweep_v4.json",
        summary={
            "backtest_credibility": _credible_backtest(),
            "strategy_backtest_diagnostics": {
                "diagnostic_version": "strategy_backtest_diagnostics.v1",
                "credible_evidence_present": True,
                "quality_floor_passed": True,
                "eligible_for_formal_ranking": True,
                "failure_classes": [],
            },
        },
    )
    rejected_research_run = _seed_run(
        conn,
        strategy="v4",
        run_type="backtest",
        items=[],
        trade_date="2026-03-04",
        artifact_path="logs/openclaw/research_backtest_sweep_v4.json",
        summary={
            "backtest_credibility": {
                **_credible_backtest(),
                "parameter_sensitivity": False,
            },
            "strategy_backtest_diagnostics": {
                "diagnostic_version": "strategy_backtest_diagnostics.v1",
                "credible_evidence_present": False,
                "quality_floor_passed": True,
                "eligible_for_formal_ranking": False,
                "failure_classes": [],
            },
        },
    )
    scan_run = _seed_run(
        conn,
        strategy="v4",
        run_type="scan",
        items=[{"ts_code": "000004.SZ", "score": 88, "rank_idx": 1, "reason_codes": ["v4_signal"]}],
        summary={},
    )

    result = build_unified_system_recommendation(conn)
    conn.close()

    review = result["top_strategies"][0]
    assert review["strategy"] == "v4"
    assert review["run_id"] == scan_run
    assert review["backtest_component"]["source_run_id"] == formal_run
    assert review["backtest_component"]["source_run_id"] != rejected_research_run
    assert review["backtest_component"]["eligible_for_formal_ranking"] is True
    assert review["backtest_component"]["source_artifact_path"].endswith("formal_backtest_sweep_v4.json")


def test_build_unified_system_recommendation_blocks_credible_but_low_quality_backtest(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
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
    _seed_run(
        conn,
        strategy="v7",
        run_type="scan",
        items=[{"ts_code": "000007.SZ", "score": 96, "rank_idx": 1, "reason_codes": ["v7_signal"]}],
        summary={},
    )

    result = build_unified_system_recommendation(conn)
    conn.close()

    assert result["passed"] is False
    review = result["all_strategy_reviews"][0]
    assert review["strategy"] == "v7"
    assert review["backtest_component"]["passed"] is True
    assert review["backtest_component"]["quality_floor_passed"] is False
    assert review["eligible_for_daily_top3"] is False
    assert "backtest_quality_floor_not_passed" in review["blocking_reasons"]


def test_build_unified_system_recommendation_reviews_all_registered_strategies_even_without_runs(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)

    result = build_unified_system_recommendation(conn)
    conn.close()

    reviewed = {item["strategy"]: item for item in result["all_strategy_reviews"]}
    assert {"v5", "v8", "v9", "combo", "v4", "v6", "v7", "stable", "ai", "ensemble_core"}.issubset(reviewed)
    assert result["top_strategies"] == []
    assert result["top_stocks"] == []
    assert {"v5", "v8", "v9", "combo", "v4", "v6", "v7", "stable", "ai", "ensemble_core"}.issubset(
        {item["strategy"] for item in result["competition_pool"]}
    )
    assert {item["competition_status"] for item in result["competition_pool"]} >= {"diagnostic", "research_only"}
    assert result["eligible_pool"] == []
    assert result["observation_pool"] == []
    assert len(result["diagnostic_pool"]) >= 8
    assert [item["strategy"] for item in result["research_only_pool"]] == ["ai", "ensemble_core"]
    assert "missing_signal_run" in reviewed["v5"]["blocking_reasons"]
    assert "missing_signal_items" in reviewed["ai"]["blocking_reasons"]
    assert result["research_only_pool"][0]["research_only_reason"] == "no_real_runtime_backtest_handler_or_explainable_fact_chain"
    assert result["research_only_pool"][1]["research_only_reason"] == "top_level_multi_alpha_portfolio_contract_missing"
    assert "missing_alpha_sleeves:momentum" in result["research_only_pool"][1]["required_to_compete"]
    assert "no_successful_signal_runs" in result["blocking_reasons"]


def test_build_unified_system_recommendation_does_not_count_linked_decision_ids_as_execution_evidence(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    _seed_run(
        conn,
        strategy="v5",
        items=[{"ts_code": "000001.SZ", "score": 93, "rank_idx": 1}],
        summary={
            "backtest_credibility": _credible_backtest(),
            "execution_evidence": {
                "mode": "quasi_live",
                "sample_count": 2,
                "linked_decision_ids": ["dec_legacy_only"],
            },
        },
    )

    result = build_unified_system_recommendation(conn)
    conn.close()

    review = result["top_strategies"][0]
    assert review["strategy"] == "v5"
    assert review["eligible_for_daily_top3"] is True
    assert review["execution_component"]["passed"] is False
    assert review["execution_component"]["score"] == 0.0
    assert "missing_linked_run_ids" in review["execution_component"]["blocking_reasons"]
    assert "missing_execution_cases" in review["execution_component"]["blocking_reasons"]


def test_build_unified_system_recommendation_counts_traceable_execution_summary(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = _seed_run(
        conn,
        strategy="v5",
        items=[{"ts_code": "000001.SZ", "score": 93, "rank_idx": 1}],
        summary={
            "backtest_credibility": _credible_backtest(),
            "execution_evidence": {
                "passed": True,
                "minimum_source_tier": "broker",
                "total_orders": 1,
                "linked_run_ids": ["placeholder"],
                "cases": [
                    {
                        "order_id": "ord_1",
                        "decision_id": "dec_1",
                        "based_on_run_id": "placeholder",
                        "has_attribution": True,
                        "slippage_bp": 2.4,
                    }
                ],
            },
        },
    )

    result = build_unified_system_recommendation(conn)
    conn.close()

    review = result["top_strategies"][0]
    assert review["run_id"] == run_id
    assert review["execution_component"]["passed"] is True
    assert review["execution_component"]["score"] == 100.0
    assert review["execution_component"]["sample_count"] == 1
