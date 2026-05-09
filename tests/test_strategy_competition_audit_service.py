from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.lineage_service import (
    apply_professional_migrations,
    insert_signal_run,
    new_run_id,
    replace_signal_items,
)
from openclaw.services.strategy_competition_audit_service import build_strategy_competition_portfolio_audit
from openclaw.services.strategy_competition_audit_service import (
    build_alpha_model_cards_from_recommendation,
    build_blocked_independent_validation_stub,
    build_blocked_pre_trade_controls_stub,
    build_blocked_shadow_execution_stub,
    build_pre_trade_risk_controls_from_recommendation,
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


def _seed_stock_facts(conn: sqlite3.Connection) -> None:
    stocks = [
        ("000001.SZ", "平安银行", "银行"),
        ("000002.SZ", "万科A", "地产"),
        ("000003.SZ", "测试科技", "科技"),
        ("000004.SZ", "测试医药", "医药"),
        ("000005.SZ", "测试制造", "制造"),
    ]
    for idx, (code, name, industry) in enumerate(stocks, start=1):
        conn.execute(
            """
            INSERT OR REPLACE INTO stock_basic (ts_code, name, industry, circ_mv, total_mv)
            VALUES (?, ?, ?, ?, ?)
            """,
            (code, name, industry, 1000000.0 + idx, 2000000.0 + idx),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_trading_data
            (ts_code, trade_date, open_price, high_price, low_price, close_price, vol, amount, pct_chg, turnover_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (code, "20260502", 10.0, 10.5, 9.8, 10.2, 100000.0, 3000000.0 + idx, 0.5, 1.2),
        )
    conn.commit()


def _seed_strategy(conn: sqlite3.Connection, strategy: str, items: list[dict]) -> str:
    run_id = new_run_id("scan", strategy)
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy=strategy,
        trade_date="2026-05-02",
        data_version=f"data:{strategy}:20260502",
        code_version=f"code:{strategy}:abc123",
        param_version=f"param:{strategy}:v1",
        status="success",
        summary={"backtest_credibility": _credible_backtest()},
    )
    replace_signal_items(conn, run_id=run_id, items=items)
    return run_id


def _model_cards() -> dict:
    out = {}
    for strategy in ("v5", "v9", "combo"):
        out[strategy] = {
            "alpha_id": strategy,
            "status": "formal_eligible",
            "model_card": {"owner": "research", "description": f"{strategy} alpha card"},
            "hypothesis": f"{strategy} predeclared alpha hypothesis",
            "rule_hash": f"rule_hash_{strategy}",
            "data_hash": f"data_hash_{strategy}",
            "code_hash": f"code_hash_{strategy}",
            "evidence_manifest": f"logs/openclaw/{strategy}_manifest.json",
        }
    return out


def _independent_validator() -> dict:
    return {
        "validator_name": "independent_risk_reviewer",
        "validator_role": "independent_validator",
        "decision": "approved",
        "conflict_of_interest_attestation": True,
        "reviewed_artifacts": ["recommendation", "portfolio_audit", "risk_controls"],
    }


def _shadow_execution() -> dict:
    return {"passed": True, "sample_count": 5, "artifact": "logs/openclaw/shadow_execution.json"}


def _pre_trade_controls() -> dict:
    return {
        "passed": True,
        "controls": {
            "single_name_weight_limit": True,
            "industry_weight_limit": True,
            "liquidity_check": True,
            "price_limit_check": True,
            "suspension_check": True,
            "turnover_budget": True,
            "order_value_limit": True,
            "risk_contribution_limit": True,
            "size_factor_exposure_limit": True,
            "liquidity_factor_exposure_limit": True,
        },
    }


def _seed_competition_fixture(conn: sqlite3.Connection) -> None:
    apply_professional_migrations(conn)
    _seed_stock_facts(conn)
    _seed_strategy(
        conn,
        "v5",
        [
            {"ts_code": "000001.SZ", "score": 95, "rank_idx": 1, "reason_codes": ["v5_strength"]},
            {"ts_code": "000002.SZ", "score": 91, "rank_idx": 2, "reason_codes": ["v5_value"]},
        ],
    )
    _seed_strategy(
        conn,
        "v9",
        [
            {"ts_code": "000003.SZ", "score": 94, "rank_idx": 1, "reason_codes": ["v9_quality"]},
            {"ts_code": "000001.SZ", "score": 88, "rank_idx": 2, "reason_codes": ["v9_overlap"]},
        ],
    )
    _seed_strategy(
        conn,
        "combo",
        [
            {"ts_code": "000004.SZ", "score": 93, "rank_idx": 1, "reason_codes": ["combo_consensus"]},
            {"ts_code": "000005.SZ", "score": 89, "rank_idx": 2, "reason_codes": ["combo_rank"]},
        ],
    )


def test_strategy_competition_audit_passes_only_with_full_industry_benchmark_evidence(tmp_db: Path):
    conn = sqlite3.connect(str(tmp_db))
    _seed_competition_fixture(conn)

    artifact = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=["v5", "v9", "combo"],
        alpha_model_cards=_model_cards(),
        independent_validator=_independent_validator(),
        shadow_execution=_shadow_execution(),
        pre_trade_risk_controls=_pre_trade_controls(),
        output_dir=tmp_db.parent / "audit",
    )

    assert artifact["passed"] is True
    assert artifact["result_status"] == "industry_benchmark_competition_passed"
    assert len(artifact["top5_portfolio_audit"]) == 5
    assert artifact["formal_top_allowed"] is True
    assert artifact["production_candidate_allowed"] is True
    assert artifact["ranking_method_hash"]
    assert artifact["top5_portfolio_audit"][0]["source"]["signal_refs"]
    assert artifact["top5_portfolio_audit"][0]["risk"]["industry"]
    assert "estimated_cost_bps" in artifact["top5_portfolio_audit"][0]["cost"]
    assert artifact["cost_summary"]["cost_model"] == "predeclared_base_plus_participation_impact"
    assert artifact["benchmark_contract"]["artifact_version"] == "benchmark_industry_contract.v1"
    assert artifact["benchmark_contract"]["contract_hash"]
    assert artifact["benchmark_contract"]["industry_weights_hash"]
    assert artifact["benchmark_contract"]["constituent_hash"]
    assert artifact["risk_summary"]["weight_plan"]["method"] in {
        "qp_cvxpy_risk_cost_factor_neutral_v1",
        "multifactor_hard_constraint_weighting_v4",
    }
    assert artifact["risk_summary"]["risk_budget"]["max_single_risk_contribution"] == 0.4
    assert artifact["risk_summary"]["risk_budget"]["risk_contribution_model"] == "adaptive_shrunk_industry_block_covariance_v3"
    assert artifact["risk_summary"]["risk_budget"]["base_shrinkage"] == 0.35
    assert 0.15 <= artifact["risk_summary"]["risk_budget"]["shrinkage_intensity"] <= 0.8
    assert artifact["risk_summary"]["risk_budget"]["cross_industry_correlation_scale"] == 0.6
    assert artifact["risk_summary"]["risk_budget"]["factor_exposure_summary"]["size"]["within_limit"] is True
    assert artifact["risk_summary"]["risk_budget"]["factor_exposure_summary"]["liquidity"]["within_limit"] is True
    weights = [float(item.get("weight") or 0.0) for item in artifact["top5_portfolio_audit"]]
    assert abs(sum(weights) - 1.0) < 1e-6
    assert max(weights) <= 0.25
    assert len({round(item, 6) for item in weights}) > 1
    max_share = max(
        float(item.get("risk", {}).get("risk_contribution_share") or 0.0)
        for item in artifact["top5_portfolio_audit"]
    )
    assert max_share <= 0.4
    assert Path(artifact["artifact_path"]).exists()

    rows = conn.execute("SELECT COUNT(*) FROM portfolio_competition_runs").fetchone()
    conn.close()
    assert rows[0] == 1


def test_strategy_competition_audit_blocks_missing_validator_and_pre_trade_controls(tmp_db: Path):
    conn = sqlite3.connect(str(tmp_db))
    _seed_competition_fixture(conn)

    artifact = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=["v5", "v9", "combo"],
        alpha_model_cards=_model_cards(),
        independent_validator={},
        shadow_execution={},
        pre_trade_risk_controls={},
    )
    conn.close()

    assert artifact["passed"] is False
    assert artifact["production_candidate_allowed"] is False
    assert "independent_validator_missing" in artifact["blocking_reasons"]
    assert "shadow_execution_missing" in artifact["blocking_reasons"]
    assert "pre_trade_risk_controls_missing" in artifact["blocking_reasons"]


def test_strategy_competition_audit_applies_external_benchmark_industry_neutrality(tmp_db: Path):
    conn = sqlite3.connect(str(tmp_db))
    _seed_competition_fixture(conn)
    artifact = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=["v5", "v9", "combo"],
        alpha_model_cards=_model_cards(),
        independent_validator=_independent_validator(),
        shadow_execution=_shadow_execution(),
        pre_trade_risk_controls=_pre_trade_controls(),
        portfolio_constraints={
            "max_single_name_weight": 0.7,
            "max_industry_weight": 0.7,
            "industry_neutral_tolerance": 0.03,
            "benchmark_industry_source": "external_index_contract",
            "benchmark_industry_weights": {
                "银行": 0.40,
                "地产": 0.20,
                "科技": 0.15,
                "医药": 0.15,
                "制造": 0.10,
            },
        },
    )
    conn.close()

    plan = artifact["risk_summary"]["weight_plan"]
    assert plan["method"] in {"qp_cvxpy_risk_cost_factor_neutral_v1", "multifactor_hard_constraint_weighting_v4"}
    if plan["method"] == "qp_cvxpy_risk_cost_factor_neutral_v1":
        assert plan["industry_neutral_source"] == "external_index_contract"
        assert plan["industry_neutral_within_tolerance"] is True
        targets = plan["industry_neutral_targets"]
        assert targets["银行"] == 0.4
        assert targets["地产"] == 0.2
        assert targets["科技"] == 0.15
        assert targets["医药"] == 0.15
        assert targets["制造"] == 0.1


def test_strategy_competition_audit_blocks_research_only_or_failed_candidate_in_fixed_pool(tmp_db: Path):
    conn = sqlite3.connect(str(tmp_db))
    _seed_competition_fixture(conn)
    cards = _model_cards()
    cards["hard_event_alpha_candidate"] = {
        "status": "failed",
        "model_card": {"description": "failed hard event candidate"},
        "hypothesis": "failed candidate should never enter competition",
        "rule_hash": "rule_hash_failed",
        "data_hash": "data_hash_failed",
        "code_hash": "code_hash_failed",
    }

    artifact = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=["v5", "v9", "combo", "hard_event_alpha_candidate"],
        alpha_model_cards=cards,
        independent_validator=_independent_validator(),
        shadow_execution=_shadow_execution(),
        pre_trade_risk_controls=_pre_trade_controls(),
    )
    conn.close()

    assert artifact["passed"] is False
    assert any("fixed_candidate_missing_from_recommendation:hard_event_alpha_candidate" == reason for reason in artifact["blocking_reasons"])
    assert any("model_card_status_not_competition_eligible:hard_event_alpha_candidate" == reason for reason in artifact["blocking_reasons"])


def test_current_recommendation_model_cards_do_not_bypass_missing_real_approvals(tmp_db: Path):
    conn = sqlite3.connect(str(tmp_db))
    _seed_competition_fixture(conn)
    recommendation = build_unified_system_recommendation(conn, trade_date="2026-05-02")
    fixed_pool = [item["strategy"] for item in recommendation["eligible_pool"]]
    cards = build_alpha_model_cards_from_recommendation(recommendation, fixed_candidate_pool=fixed_pool)

    artifact = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=fixed_pool,
        alpha_model_cards=cards,
        independent_validator=build_blocked_independent_validation_stub(),
        shadow_execution=build_blocked_shadow_execution_stub(),
        pre_trade_risk_controls=build_blocked_pre_trade_controls_stub(),
    )
    conn.close()

    assert set(cards) == {"v5", "v9", "combo"}
    assert all(cards[strategy]["rule_hash"] for strategy in cards)
    assert artifact["passed"] is False
    assert artifact["top5_portfolio_audit"]
    assert "independent_validator_not_approved" in artifact["blocking_reasons"]
    assert "shadow_execution_not_passed" in artifact["blocking_reasons"]
    assert "pre_trade_risk_controls_not_passed" in artifact["blocking_reasons"]


def test_pre_trade_risk_controls_can_be_derived_without_granting_production(tmp_db: Path):
    conn = sqlite3.connect(str(tmp_db))
    _seed_competition_fixture(conn)
    recommendation = build_unified_system_recommendation(conn, trade_date="2026-05-02")
    fixed_pool = [item["strategy"] for item in recommendation["eligible_pool"]]
    cards = build_alpha_model_cards_from_recommendation(recommendation, fixed_candidate_pool=fixed_pool)
    controls = build_pre_trade_risk_controls_from_recommendation(
        conn,
        recommendation=recommendation,
        portfolio_constraints={"portfolio_notional": 500000.0, "max_order_value": 125000.0},
    )

    artifact = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=fixed_pool,
        alpha_model_cards=cards,
        independent_validator=build_blocked_independent_validation_stub(),
        shadow_execution=build_blocked_shadow_execution_stub(),
        pre_trade_risk_controls=controls,
    )
    conn.close()

    assert controls["passed"] is True
    assert all(controls["controls"].values())
    assert len(controls["orders"]) == 5
    assert controls["weight_plan"]["method"] in {
        "qp_cvxpy_risk_cost_factor_neutral_v1",
        "multifactor_hard_constraint_weighting_v4",
    }
    assert controls["risk_budget"]["max_single_risk_contribution"] == 0.4
    assert controls["risk_budget"]["risk_contribution_model"] == "adaptive_shrunk_industry_block_covariance_v3"
    assert controls["risk_budget"]["factor_exposure_summary"]["size"]["within_limit"] is True
    assert controls["weight_plan"]["cash_weight"] <= 0.01
    assert artifact["passed"] is False
    assert artifact["production_candidate_allowed"] is False
    assert "pre_trade_risk_controls_not_passed" not in artifact["blocking_reasons"]
    assert "independent_validator_not_approved" in artifact["blocking_reasons"]
    assert "shadow_execution_not_passed" in artifact["blocking_reasons"]


def test_pre_trade_risk_controls_block_limit_up_candidate(tmp_db: Path):
    conn = sqlite3.connect(str(tmp_db))
    _seed_competition_fixture(conn)
    conn.execute("UPDATE daily_trading_data SET pct_chg = 9.8 WHERE ts_code = '000001.SZ'")
    conn.commit()
    recommendation = build_unified_system_recommendation(conn, trade_date="2026-05-02")

    controls = build_pre_trade_risk_controls_from_recommendation(conn, recommendation=recommendation)
    conn.close()

    assert controls["passed"] is False
    assert any(reason.endswith(":price_limit_check") for reason in controls["blocking_reasons"])
    assert controls["controls"]["price_limit_check"] is False


def test_competition_audit_accepts_shadow_execution_plan_artifact_shape(tmp_db: Path):
    conn = sqlite3.connect(str(tmp_db))
    _seed_competition_fixture(conn)
    recommendation = build_unified_system_recommendation(conn, trade_date="2026-05-02")
    fixed_pool = [item["strategy"] for item in recommendation["eligible_pool"]]
    cards = build_alpha_model_cards_from_recommendation(recommendation, fixed_candidate_pool=fixed_pool)
    controls = build_pre_trade_risk_controls_from_recommendation(conn, recommendation=recommendation)
    shadow_plan = {
        "artifact_version": "strategy_competition_shadow_execution_plan.v1",
        "artifact_path": "logs/openclaw/shadow_plan.json",
        "plan_status": "shadow_execution_pending",
        "shadow_execution": {
            "passed": False,
            "sample_count": 5,
            "artifact": "logs/openclaw/shadow_plan.json",
            "blocking_reasons": ["missing_attribution:ord_1"],
        },
    }

    artifact = build_strategy_competition_portfolio_audit(
        conn,
        trade_date="2026-05-02",
        fixed_candidate_pool=fixed_pool,
        alpha_model_cards=cards,
        independent_validator=build_blocked_independent_validation_stub(),
        shadow_execution=shadow_plan,
        pre_trade_risk_controls=controls,
    )
    conn.close()

    assert artifact["shadow_execution"]["sample_count"] == 5
    assert artifact["shadow_execution"]["source_plan_status"] == "shadow_execution_pending"
    assert artifact["shadow_execution"]["source_plan_artifact"] == "logs/openclaw/shadow_plan.json"
    assert artifact["shadow_execution"]["blocking_reasons"] == ["shadow_execution_not_passed"]
