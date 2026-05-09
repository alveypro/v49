"""Tests for backtest.engine — BacktestEngine and rolling window logic."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pytest

from backtest.engine import BacktestEngine, _aggregate, _build_windows, RollingWindow
from openclaw.adapters.v49_adapter import V49Adapter


def _make_engine_with_handler(summary: Dict[str, Any] | None = None) -> BacktestEngine:
    summary = summary or {"win_rate": 0.55, "max_drawdown": 0.08, "signal_density": 0.03}
    adapter = V49Adapter(module_path=Path("/tmp/fake.py"))
    adapter.register_backtest_handler("v7", lambda params: {"summary": summary})
    return BacktestEngine(adapter)


class TestBuildWindows:
    def test_basic_windows(self):
        windows = _build_windows("2025-01-01", "2025-12-31", 180, 60, 60)
        assert len(windows) > 0
        assert all(isinstance(w, RollingWindow) for w in windows)
        roles = [w.role for w in windows]
        assert "train" in roles
        assert "test" in roles

    def test_returns_empty_on_invalid_params(self):
        assert _build_windows("2025-01-01", "2025-12-31", 0, 60, 60) == []
        assert _build_windows("2025-01-01", "2025-12-31", 180, 0, 60) == []
        assert _build_windows("2025-01-01", "2025-12-31", 180, 60, 0) == []

    def test_returns_empty_when_range_too_short(self):
        windows = _build_windows("2025-01-01", "2025-02-01", 180, 60, 60)
        assert windows == []

    def test_window_dates_are_ordered(self):
        windows = _build_windows("2024-01-01", "2025-12-31", 180, 60, 60)
        for w in windows:
            assert w.date_from <= w.date_to


class TestAggregate:
    def test_empty_returns_defaults(self):
        result = _aggregate([])
        assert result["win_rate"] == 0.0
        assert result["max_drawdown"] == 1.0

    def test_averages_correctly(self):
        summaries = [
            {
                "win_rate": 0.50,
                "max_drawdown": 0.10,
                "signal_density": 0.04,
                "tradeability_filter_enabled": True,
                "volume_constraint_enabled": True,
                "trading_cost": {"slippage_bp": 10.0},
                "risk_control": {"max_stop_loss_pct": 0.08},
            },
            {
                "win_rate": 0.60,
                "max_drawdown": 0.06,
                "signal_density": 0.06,
                "tradeability_filter_enabled": True,
                "volume_constraint_enabled": True,
                "trading_cost": {"slippage_bp": 10.0},
            },
        ]
        result = _aggregate(summaries)
        assert abs(result["win_rate"] - 0.55) < 1e-9
        assert abs(result["max_drawdown"] - 0.08) < 1e-9
        assert abs(result["signal_density"] - 0.05) < 1e-9
        assert result["samples"] == 2
        assert result["tradeability_filter_enabled"] is True
        assert result["volume_constraint_enabled"] is True
        assert result["trading_cost"]["slippage_bp"] == 10.0
        assert result["risk_control"] == {"max_stop_loss_pct": 0.08}

    def test_aggregate_preserves_window_risk_diagnostics(self):
        summaries = [
            {
                "win_rate": 0.50,
                "max_drawdown": 0.10,
                "signal_density": 0.04,
                "risk_diagnostics": {
                    "exit_reason_counts": {"stop_loss": 1},
                    "tail_loss_count_5pct": 1,
                    "tail_loss_count_8pct": 0,
                    "worst_return_pct": -6.0,
                    "worst_trades": [{"ts_code": "000001.SZ", "future_return": -6.0}],
                },
            },
            {
                "win_rate": 0.60,
                "max_drawdown": 0.06,
                "signal_density": 0.06,
                "risk_diagnostics": {
                    "exit_reason_counts": {"holding_period": 2},
                    "tail_loss_count_5pct": 0,
                    "tail_loss_count_8pct": 1,
                    "worst_return_pct": -9.0,
                    "worst_trades": [{"ts_code": "000002.SZ", "future_return": -9.0}],
                },
            },
        ]

        result = _aggregate(summaries)

        risk = result["risk_diagnostics"]
        assert risk["window_count"] == 2
        assert risk["exit_reason_counts"] == {"stop_loss": 1, "holding_period": 2}
        assert risk["tail_loss_count_5pct"] == 1
        assert risk["tail_loss_count_8pct"] == 1
        assert risk["worst_return_pct"] == -9.0
        assert risk["worst_trades"][0]["ts_code"] == "000002.SZ"

    def test_aggregate_preserves_stable_defensive_allocator_review(self):
        summaries = [
            {
                "win_rate": 0.0,
                "max_drawdown": 0.02,
                "signal_density": 0.02,
                "defensive_allocator": {
                    "available": True,
                    "contract": {"role": "defensive_allocator_overlay"},
                    "promotion_eligible": False,
                    "blocking_reasons": ["missing_formal_pool_benchmark_return_series"],
                    "drawdown_reduction": 0.04,
                    "excess_return_pct": -1.2,
                    "overlay_max_drawdown": 0.01,
                    "full_exposure_max_drawdown": 0.05,
                },
            },
            {
                "win_rate": 0.0,
                "max_drawdown": 0.03,
                "signal_density": 0.02,
                "defensive_allocator": {
                    "available": True,
                    "contract": {"role": "defensive_allocator_overlay"},
                    "promotion_eligible": False,
                    "blocking_reasons": ["non_negative_excess_return_not_proven"],
                    "drawdown_reduction": 0.02,
                    "excess_return_pct": -0.8,
                    "overlay_max_drawdown": 0.02,
                    "full_exposure_max_drawdown": 0.04,
                },
            },
        ]

        result = _aggregate(summaries)

        allocator = result["defensive_allocator"]
        assert allocator["contract"]["role"] == "defensive_allocator_overlay"
        assert allocator["promotion_eligible"] is False
        assert allocator["allocator_candidate_eligible"] is False
        assert allocator["success_metric_passed"] is False
        assert allocator["avg_drawdown_reduction"] == 0.03
        assert allocator["avg_excess_return_pct"] == -1.0
        assert allocator["blocking_reasons"] == [
            "missing_formal_pool_benchmark_return_series",
            "non_negative_excess_return_not_proven",
        ]


class TestBacktestEngine:
    def test_single_mode(self):
        engine = _make_engine_with_handler()
        result = engine.run("v7", "2025-01-01", "2025-06-01", {"mode": "single"})
        assert result["status"] == "success"
        summary = result["result"]["summary"]
        assert summary["win_rate"] == 0.55
        assert "trading_cost" in summary

    def test_rolling_falls_back_to_single_when_range_too_short(self):
        engine = _make_engine_with_handler()
        result = engine.run("v7", "2025-01-01", "2025-02-01", {
            "mode": "rolling",
            "train_window_days": 180,
            "test_window_days": 60,
            "step_days": 60,
        })
        assert result["status"] == "success"

    def test_enrich_summary_adds_trading_cost(self):
        engine = _make_engine_with_handler()
        enriched = engine._enrich_summary(
            {"win_rate": 0.5, "signal_density": 0.03},
            {"holding_days": 5},
        )
        assert "trading_cost" in enriched
        cost = enriched["trading_cost"]
        assert cost["base_round_trip_bp"] > 0
        assert cost["expected_cost_pct"] >= 0

    def test_rolling_failed_when_all_windows_fail(self):
        adapter = V49Adapter(module_path=Path("/tmp/fake.py"))

        def _always_fail(_params: Dict[str, Any]) -> Dict[str, Any]:
            raise RuntimeError("boom")

        adapter.register_backtest_handler("v7", _always_fail)
        engine = BacktestEngine(adapter)
        result = engine.run(
            "v7",
            "2025-01-01",
            "2025-12-31",
            {
                "mode": "rolling",
                "train_window_days": 180,
                "test_window_days": 60,
                "step_days": 60,
            },
        )
        assert result["status"] == "failed"
        assert "0 successful test windows" in result.get("error", "")
        assert result["result"]["rolling"]["failed_windows"]
        assert result["backtest_credibility"]["failed_backtests_recorded"] is True

    def test_rolling_failed_windows_preserve_handler_diagnostics(self):
        adapter = V49Adapter(module_path=Path("/tmp/fake.py"))

        def _fail_with_diagnostics(_params: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "status": "failed",
                "summary": {"win_rate": 0.0, "max_drawdown": 1.0, "signal_density": 0.0},
                "raw": {
                    "success": False,
                    "error": "no signal",
                    "backtest_diagnostics": {
                        "type": "score_distribution",
                        "strategy": "v8",
                        "evaluated": 10,
                        "passed_threshold": 0,
                        "max_score": 39.5,
                    },
                },
            }

        adapter.register_backtest_handler("v8", _fail_with_diagnostics)
        engine = BacktestEngine(adapter)
        result = engine.run(
            "v8",
            "2025-01-01",
            "2025-12-31",
            {
                "mode": "rolling",
                "train_window_days": 180,
                "test_window_days": 60,
                "step_days": 60,
            },
        )

        failed = result["result"]["rolling"]["failed_windows"]
        assert failed
        assert failed[0]["backtest_diagnostics"]["type"] == "score_distribution"
        assert failed[0]["backtest_diagnostics"]["max_score"] == 39.5

    def test_rolling_applies_global_evaluation_budget_across_windows(self):
        adapter = V49Adapter(module_path=Path("/tmp/fake.py"))
        limits: list[int] = []

        def _window_backtest(params: Dict[str, Any]) -> Dict[str, Any]:
            limit = int(params.get("max_evaluations", 0) or 0)
            limits.append(limit)
            return {
                "summary": {"win_rate": 0.50, "max_drawdown": 0.10, "signal_density": 0.01},
                "raw": {
                    "success": True,
                    "backtest_diagnostics": {
                        "type": "score_distribution",
                        "evaluated": limit,
                        "passed_threshold": 0,
                    },
                },
            }

        adapter.register_backtest_handler("v6", _window_backtest)
        engine = BacktestEngine(adapter)
        result = engine.run(
            "v6",
            "2025-01-01",
            "2025-12-31",
            {
                "mode": "rolling",
                "train_window_days": 180,
                "test_window_days": 60,
                "step_days": 60,
                "max_evaluations": 30,
                "max_evaluations_global": 35,
            },
        )

        assert limits == [30, 5]
        rolling = result["result"]["rolling"]
        assert rolling["evaluation_budget"] == {"global_max_evaluations": 35, "remaining": 0}
        assert rolling["failed_windows"][-1]["error"] == "global evaluation budget exhausted"
        assert rolling["train_windows"] == 1
        assert rolling["test_windows"] == 1
