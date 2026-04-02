"""Tests for risk.engine — dual risk evaluation and combine_risk."""
from __future__ import annotations

from risk.engine import DualRiskResult, combine_risk, evaluate_market_risk, evaluate_system_risk


class TestEvaluateMarketRisk:
    def test_green_when_all_ok(self):
        stats = {"win_rate": 0.55, "max_drawdown": 0.08, "signal_density": 0.05}
        thresholds = {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02}
        level, rules = evaluate_market_risk(stats, thresholds)
        assert level == "green"
        assert rules == []

    def test_orange_on_win_rate_breach(self):
        stats = {"win_rate": 0.40, "max_drawdown": 0.08, "signal_density": 0.05}
        thresholds = {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02}
        level, rules = evaluate_market_risk(stats, thresholds)
        assert level == "orange"
        assert "win_rate_breach" in rules

    def test_red_on_drawdown_breach(self):
        stats = {"win_rate": 0.55, "max_drawdown": 0.20, "signal_density": 0.05}
        thresholds = {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02}
        level, rules = evaluate_market_risk(stats, thresholds)
        assert level == "red"
        assert "drawdown_breach" in rules

    def test_orange_on_density_collapse(self):
        stats = {"win_rate": 0.55, "max_drawdown": 0.08, "signal_density": 0.01}
        thresholds = {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02}
        level, rules = evaluate_market_risk(stats, thresholds)
        assert level == "orange"
        assert "signal_density_collapse" in rules

    def test_red_overrides_density_collapse(self):
        stats = {"win_rate": 0.55, "max_drawdown": 0.20, "signal_density": 0.01}
        thresholds = {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02}
        level, rules = evaluate_market_risk(stats, thresholds)
        assert level == "red"
        assert "drawdown_breach" in rules
        assert "signal_density_collapse" in rules

    def test_handles_missing_keys(self):
        level, rules = evaluate_market_risk({}, {})
        assert level == "green"
        assert rules == []


class TestEvaluateSystemRisk:
    def test_green_when_healthy(self):
        health = {"db_reachable": True, "compile_ok": True, "db_stale_days": 0, "db_stale_limit": 3}
        level, rules = evaluate_system_risk(health)
        assert level == "green"

    def test_red_on_db_unreachable(self):
        health = {"db_reachable": False, "compile_ok": True}
        level, rules = evaluate_system_risk(health)
        assert level == "red"
        assert "db_unreachable" in rules

    def test_red_on_compile_failed(self):
        health = {"db_reachable": True, "compile_ok": False}
        level, rules = evaluate_system_risk(health)
        assert level == "red"
        assert "compile_failed" in rules

    def test_yellow_on_stale_db(self):
        health = {"db_reachable": True, "compile_ok": True, "db_stale_days": 5, "db_stale_limit": 3}
        level, rules = evaluate_system_risk(health)
        assert level == "yellow"
        assert "db_stale" in rules


class TestCombineRisk:
    def test_all_green(self):
        result = combine_risk(
            market_stats={"win_rate": 0.55, "max_drawdown": 0.05, "signal_density": 0.05},
            thresholds={"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02},
            system_health={"db_reachable": True, "compile_ok": True, "db_stale_days": 0},
        )
        assert isinstance(result, DualRiskResult)
        assert result.risk_level == "green"
        assert result.market_risk == "green"
        assert result.system_risk == "green"
        assert "keep current mode" in result.recommended_actions

    def test_worst_of_both_wins(self):
        result = combine_risk(
            market_stats={"win_rate": 0.55, "max_drawdown": 0.05, "signal_density": 0.05},
            thresholds={"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02},
            system_health={"db_reachable": False, "compile_ok": True},
        )
        assert result.risk_level == "red"
        assert result.market_risk == "green"
        assert result.system_risk == "red"

    def test_evidence_included(self):
        result = combine_risk(
            market_stats={"win_rate": 0.55},
            thresholds={},
            system_health={"db_reachable": True},
        )
        assert "market_stats" in result.evidence
        assert "system_health" in result.evidence
