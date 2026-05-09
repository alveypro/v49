from __future__ import annotations

import json
from pathlib import Path

from openclaw.research.all_strategy_evidence_run import (
    AllStrategyEvidenceRunConfig,
    DEFAULT_EVIDENCE_STRATEGIES,
    _effective_trade_date,
    _iso_trade_date,
    run_all_strategy_evidence,
    _sweep_config_for_strategy,
)


def test_default_evidence_strategies_cover_legacy_runtime_handlers():
    assert "v4" in DEFAULT_EVIDENCE_STRATEGIES
    assert "v7" in DEFAULT_EVIDENCE_STRATEGIES
    assert "ai" not in DEFAULT_EVIDENCE_STRATEGIES
    assert "ensemble_core" not in DEFAULT_EVIDENCE_STRATEGIES


def test_legacy_diagnostic_strategies_get_parameter_sensitivity_sweep_space(tmp_path: Path):
    cfg = AllStrategyEvidenceRunConfig(output_dir=tmp_path)

    v4_cfg = _sweep_config_for_strategy(cfg=cfg, strategy="v4", db_path=str(tmp_path / "test.db"))
    assert v4_cfg.score_thresholds == [60, 65]
    assert v4_cfg.stop_losses == [-0.03, -0.04]
    assert v4_cfg.take_profits == [0.04, 0.06]
    assert _sweep_config_for_strategy(cfg=cfg, strategy="v6", db_path=str(tmp_path / "test.db")).score_thresholds == [75, 80]
    assert _sweep_config_for_strategy(cfg=cfg, strategy="v7", db_path=str(tmp_path / "test.db")).score_thresholds == [60, 65]
    assert _sweep_config_for_strategy(cfg=cfg, strategy="v6", db_path=str(tmp_path / "test.db")).runtime_params == {
        "replay_step": 10,
        "max_evaluations": 240,
    }
    assert _sweep_config_for_strategy(cfg=cfg, strategy="v7", db_path=str(tmp_path / "test.db")).runtime_params == {
        "replay_step": 20,
        "max_evaluations": 240,
    }


def test_stable_evidence_run_keeps_default_holding_period_for_defensive_alpha(tmp_path: Path):
    cfg = AllStrategyEvidenceRunConfig(output_dir=tmp_path)

    stable_cfg = _sweep_config_for_strategy(cfg=cfg, strategy="stable", db_path=str(tmp_path / "test.db"))

    assert stable_cfg.holding_days == [10]
    assert stable_cfg.runtime_params["stable_window"] == 90


def test_stable_sweep_config_accepts_formal_pool_benchmark_runtime_params(tmp_path: Path):
    cfg = AllStrategyEvidenceRunConfig(output_dir=tmp_path)

    stable_cfg = _sweep_config_for_strategy(
        cfg=cfg,
        strategy="stable",
        db_path=str(tmp_path / "test.db"),
        extra_runtime_params={"formal_pool_returns_pct": [1.0, -0.5]},
    )

    assert stable_cfg.runtime_params["formal_pool_returns_pct"] == [1.0, -0.5]


def test_effective_trade_date_clamps_to_available_market_data(tmp_path: Path):
    db = tmp_path / "test.db"
    import sqlite3

    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE daily_trading_data(trade_date TEXT)")
    conn.execute("INSERT INTO daily_trading_data(trade_date) VALUES ('20260402')")
    conn.commit()
    conn.close()

    assert _effective_trade_date(db_path=str(db), requested_date="2026-05-06") == "20260402"
    assert _effective_trade_date(db_path=str(db), requested_date="2026-03-01") == "20260301"


def test_iso_trade_date_normalizes_compact_dates_for_backtest_windows():
    assert _iso_trade_date("20260402") == "2026-04-02"
    assert _iso_trade_date("2026-04-02") == "2026-04-02"


def test_all_strategy_evidence_run_records_scan_sweep_and_rejection(monkeypatch, tmp_path: Path):
    class FakeAdapter:
        def __init__(self, module_path):
            self.handlers = {}
            self.scan_params = {}

        def register_scan_handler(self, strategy, handler):
            self.handlers[strategy] = handler

        def run_scan(self, strategy, params):
            self.scan_params[strategy] = dict(params)
            return {
                "run_id": f"run_scan_{strategy}",
                "status": "success",
                "data_version": "data:v1",
                "code_version": "code:v1",
                "param_version": "param:v1",
                "result": {"metrics": {"count": 2}, "picks": [{"ts_code": "000001.SZ", "score": 90}]},
            }

    class FakeFactory:
        def __init__(self, module_path):
            pass

        def create_scan_handler(self, strategy):
            return lambda params: {"picks": []}

    sweep_json = tmp_path / "sweep_v5.json"
    sweep_json.write_text('{"strategy":"v5"}', encoding="utf-8")

    seen_sweep_dates = {}

    def fake_sweep(cfg):
        seen_sweep_dates[cfg.strategy] = cfg.date_to
        return {
            "run_id": f"sweep_{cfg.strategy}",
            "status": "failed",
            "artifacts": {"json": str(sweep_json)},
            "best": {"status": "failed"},
            "backtest_credibility": {},
            "strategy_backtest_diagnostics": {
                "eligible_for_formal_ranking": False,
                "credible_evidence_present": False,
                "quality_floor_passed": False,
                "failure_classes": ["no_successful_parameter_run"],
            },
        }

    def fake_stage_audit(**kwargs):
        return {"passed": True, "artifacts": {"json": str(tmp_path / "audit.json"), "markdown": str(tmp_path / "audit.md")}}

    def fake_recommendation(conn, trade_date=""):
        return {
            "passed": False,
            "blocking_reasons": ["no_strategy_passed_daily_top3_gate"],
            "eligible_pool": [],
            "observation_pool": [],
            "diagnostic_pool": [{"strategy": "v5"}],
            "top_strategies": [],
            "top_stocks": [],
        }

    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.V49Adapter", FakeAdapter)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.HandlerFactory", FakeFactory)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.run_param_sweep", fake_sweep)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.run_stage_audit", fake_stage_audit)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.build_unified_system_recommendation", fake_recommendation)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run._effective_trade_date", lambda **kwargs: "20260402")

    ledger = tmp_path / "rejected.jsonl"
    payload = run_all_strategy_evidence(
        AllStrategyEvidenceRunConfig(
            output_dir=tmp_path,
            strategies=["v5"],
            include_research_only=True,
            db_path=str(tmp_path / "test.db"),
            rejected_ledger=str(ledger),
        )
    )

    assert payload["results"]["v5"]["scan"]["run_id"] == "run_scan_v5"
    assert payload["effective_trade_date"] == "20260402"
    assert payload["effective_trade_date_iso"] == "2026-04-02"
    assert seen_sweep_dates["v5"] == "2026-04-02"
    assert payload["results"]["v5"]["status"] == "diagnostic_rejected"
    assert payload["results"]["ai"]["status"] == "research_only"
    assert payload["results"]["ai"]["promotion_blocked"] is True
    assert payload["results"]["ai"]["eligible_for_formal_ranking"] is False
    assert "real_runtime_backtest_handler" in payload["results"]["ai"]["required_to_compete"]
    assert payload["results"]["ensemble_core"]["status"] == "research_only"
    assert payload["results"]["ensemble_core"]["reason"] == "top_level_multi_alpha_portfolio_contract_missing"
    assert "missing_alpha_sleeves:momentum" in payload["results"]["ensemble_core"]["required_to_compete"]
    assert payload["results"]["ensemble_core"]["alpha_sleeve_fact_chain"]["research_only"] is True
    assert "portfolio_weights" not in payload["results"]["ensemble_core"]["alpha_sleeve_fact_chain"]
    assert payload["results"]["ensemble_core"]["shadow_portfolio"]["research_only"] is True
    assert payload["results"]["ensemble_core"]["shadow_portfolio"]["not_for_production"] is True
    assert payload["results"]["ensemble_core"]["execution_cost_replay"]["research_only"] is True
    assert payload["results"]["ensemble_core"]["execution_cost_replay"]["not_for_production"] is True
    assert "missing_shadow_weights" in payload["results"]["ensemble_core"]["execution_cost_replay"]["blocking_reasons"]
    assert payload["results"]["ensemble_core"]["alpha_rebuild_lab"]["research_only"] is True
    assert "candidate_reviews" in payload["results"]["ensemble_core"]["alpha_rebuild_lab"]
    assert payload["results"]["ensemble_core"]["walk_forward_shadow_benchmark"]["research_only"] is True
    assert payload["results"]["ensemble_core"]["walk_forward_shadow_benchmark"]["passed"] is False
    assert any(
        reason.startswith("insufficient_walk_forward_windows:")
        for reason in payload["results"]["ensemble_core"]["walk_forward_shadow_benchmark"]["blocking_reasons"]
    )
    assert "shadow_portfolio" not in payload["unified_recommendation"]
    assert [item["strategy"] for item in payload["strategy_optimization_backlog"]["legacy_or_diagnostic"]] == ["v5"]
    assert [item["strategy"] for item in payload["strategy_optimization_backlog"]["research_only"]] == ["ai", "ensemble_core"]
    assert Path(payload["artifacts"]["json"]).exists()
    rows = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["strategy"] == "v5"
    assert "eligible_for_formal_ranking_false" in rows[0]["reason"]


def test_all_strategy_evidence_run_keeps_eligible_strategy_out_of_rejected_ledger(monkeypatch, tmp_path: Path):
    class FakeAdapter:
        def __init__(self, module_path):
            pass

        def register_scan_handler(self, strategy, handler):
            pass

        def run_scan(self, strategy, params):
            return {
                "run_id": f"run_scan_{strategy}",
                "status": "success",
                "result": {"metrics": {"count": 1}},
            }

    class FakeFactory:
        def __init__(self, module_path):
            pass

        def create_scan_handler(self, strategy):
            return lambda params: {"picks": []}

    sweep_json = tmp_path / "sweep_v8.json"
    sweep_json.write_text('{"strategy":"v8","strategy_backtest_diagnostics":{}}', encoding="utf-8")

    def fake_sweep(cfg):
        return {
            "run_id": f"sweep_{cfg.strategy}",
            "status": "success",
            "artifacts": {"json": str(sweep_json)},
            "best": {"status": "success", "win_rate": 0.55, "max_drawdown": 0.10, "signal_density": 0.05},
            "backtest_credibility": {"point_in_time_data": True},
            "strategy_backtest_diagnostics": {
                "eligible_for_formal_ranking": True,
                "credible_evidence_present": True,
                "quality_floor_passed": True,
                "failure_classes": [],
            },
        }

    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.V49Adapter", FakeAdapter)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.HandlerFactory", FakeFactory)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.run_param_sweep", fake_sweep)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.run_stage_audit", lambda **kwargs: {"passed": True, "artifacts": {}})
    monkeypatch.setattr(
        "openclaw.research.all_strategy_evidence_run.build_unified_system_recommendation",
        lambda conn, trade_date="": {
            "passed": True,
            "blocking_reasons": [],
            "eligible_pool": [{"strategy": "v8"}],
            "observation_pool": [],
            "diagnostic_pool": [],
            "top_strategies": [{"strategy": "v8"}],
            "top_stocks": [{"ts_code": "000001.SZ"}],
        },
    )

    ledger = tmp_path / "rejected.jsonl"
    payload = run_all_strategy_evidence(
        AllStrategyEvidenceRunConfig(
            output_dir=tmp_path,
            strategies=["v8"],
            include_research_only=False,
            db_path=str(tmp_path / "test.db"),
            rejected_ledger=str(ledger),
        )
    )

    assert payload["results"]["v8"]["status"] == "eligible_evidence_ready"
    assert payload["unified_recommendation"]["eligible_pool"] == ["v8"]
    assert payload["strategy_optimization_backlog"]["repair_candidate"][0]["strategy"] == "v8"
    assert (
        payload["strategy_optimization_backlog"]["repair_candidate"][0]["next_action"]
        == "enter_unified_formal_competition_and_collect_shadow_execution_evidence"
    )
    assert not ledger.exists()


def test_all_strategy_evidence_run_marks_credible_low_quality_as_observation(monkeypatch, tmp_path: Path):
    class FakeAdapter:
        def __init__(self, module_path):
            pass

        def register_scan_handler(self, strategy, handler):
            pass

        def run_scan(self, strategy, params):
            return {
                "run_id": f"run_scan_{strategy}",
                "status": "success",
                "result": {"metrics": {"count": 1}},
            }

    class FakeFactory:
        def __init__(self, module_path):
            pass

        def create_scan_handler(self, strategy):
            return lambda params: {"picks": []}

    sweep_json = tmp_path / "sweep_stable.json"
    sweep_json.write_text('{"strategy":"stable"}', encoding="utf-8")

    def fake_sweep(cfg):
        return {
            "run_id": f"sweep_{cfg.strategy}",
            "status": "success",
            "artifacts": {"json": str(sweep_json)},
            "best": {"status": "success", "win_rate": 0.0, "max_drawdown": 0.0, "signal_density": 0.02},
            "backtest_credibility": {"point_in_time_data": True},
            "strategy_backtest_diagnostics": {
                "eligible_for_formal_ranking": False,
                "credible_evidence_present": True,
                "quality_floor_passed": False,
                "failure_classes": ["weak_out_of_sample_win_rate"],
            },
        }

    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.V49Adapter", FakeAdapter)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.HandlerFactory", FakeFactory)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.run_param_sweep", fake_sweep)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.run_stage_audit", lambda **kwargs: {"passed": True, "artifacts": {}})
    monkeypatch.setattr(
        "openclaw.research.all_strategy_evidence_run.build_unified_system_recommendation",
        lambda conn, trade_date="": {
            "passed": True,
            "blocking_reasons": [],
            "eligible_pool": [],
            "observation_pool": [{"strategy": "stable"}],
            "diagnostic_pool": [],
            "top_strategies": [],
            "top_stocks": [],
        },
    )

    payload = run_all_strategy_evidence(
        AllStrategyEvidenceRunConfig(
            output_dir=tmp_path,
            strategies=["stable"],
            include_research_only=False,
            db_path=str(tmp_path / "test.db"),
            rejected_ledger=str(tmp_path / "rejected.jsonl"),
        )
    )

    assert payload["results"]["stable"]["status"] == "observation"
    assert payload["strategy_optimization_backlog"]["repair_candidate"][0]["status"] == "observation"
    assert (
        payload["strategy_optimization_backlog"]["repair_candidate"][0]["next_action"]
        == "rebuild_as_defensive_allocator_overlay_and_evaluate_portfolio_drawdown_reduction"
    )


def test_all_strategy_evidence_run_attaches_stable_benchmark_contract(monkeypatch, tmp_path: Path):
    class FakeAdapter:
        def __init__(self, module_path):
            pass

        def register_scan_handler(self, strategy, handler):
            pass

        def run_scan(self, strategy, params):
            return {
                "run_id": f"run_scan_{strategy}",
                "status": "success",
                "result": {"metrics": {"count": 1}},
            }

    class FakeFactory:
        def __init__(self, module_path):
            pass

        def create_scan_handler(self, strategy):
            return lambda params: {"picks": []}

    seen_runtime_params = {}

    def fake_benchmark(**kwargs):
        return {
            "available": True,
            "return_series_pct": [1.0, -0.5, 0.25],
            "blocking_reasons": [],
            "contract": {"benchmark_version": "formal_pool_benchmark_return_series.v1"},
        }

    def fake_sweep(cfg):
        seen_runtime_params.update(cfg.runtime_params)
        sweep_json = tmp_path / "sweep_stable.json"
        sweep_json.write_text('{"strategy":"stable"}', encoding="utf-8")
        return {
            "run_id": f"sweep_{cfg.strategy}",
            "status": "success",
            "artifacts": {"json": str(sweep_json)},
            "best": {"status": "success", "win_rate": 0.0, "max_drawdown": 0.0, "signal_density": 0.02},
            "backtest_credibility": {"point_in_time_data": True},
            "strategy_backtest_diagnostics": {
                "eligible_for_formal_ranking": False,
                "credible_evidence_present": True,
                "quality_floor_passed": False,
                "failure_classes": ["weak_out_of_sample_win_rate"],
            },
        }

    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.V49Adapter", FakeAdapter)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.HandlerFactory", FakeFactory)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.run_param_sweep", fake_sweep)
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run.run_stage_audit", lambda **kwargs: {"passed": True, "artifacts": {}})
    monkeypatch.setattr(
        "openclaw.research.all_strategy_evidence_run.build_unified_system_recommendation",
        lambda conn, trade_date="": {
            "passed": True,
            "blocking_reasons": [],
            "eligible_pool": [{"strategy": "v5"}],
            "observation_pool": [{"strategy": "stable"}],
            "diagnostic_pool": [],
            "top_strategies": [{"strategy": "v5"}],
            "top_stocks": [],
        },
    )
    monkeypatch.setattr("openclaw.research.all_strategy_evidence_run._build_stable_formal_pool_benchmark_contract", fake_benchmark)

    payload = run_all_strategy_evidence(
        AllStrategyEvidenceRunConfig(
            output_dir=tmp_path,
            strategies=["stable"],
            include_research_only=False,
            db_path=str(tmp_path / "test.db"),
            rejected_ledger=str(tmp_path / "rejected.jsonl"),
        )
    )

    assert seen_runtime_params["formal_pool_returns_pct"] == [1.0, -0.5, 0.25]
    assert payload["results"]["stable"]["formal_pool_benchmark_contract"]["available"] is True
