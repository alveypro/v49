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
            {"win_rate": 0.50, "max_drawdown": 0.10, "signal_density": 0.04},
            {"win_rate": 0.60, "max_drawdown": 0.06, "signal_density": 0.06},
        ]
        result = _aggregate(summaries)
        assert abs(result["win_rate"] - 0.55) < 1e-9
        assert abs(result["max_drawdown"] - 0.08) < 1e-9
        assert abs(result["signal_density"] - 0.05) < 1e-9
        assert result["samples"] == 2


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
