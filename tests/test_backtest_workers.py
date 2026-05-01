from __future__ import annotations

import pandas as pd

from openclaw.runtime.backtest_workers import run_comparison_backtest_worker, run_single_backtest_worker


class FakeAnalyzer:
    def __init__(self) -> None:
        self.calls = []

    def backtest_bottom_breakthrough(self, df, *, sample_size, holding_days):
        self.calls.append(("v5", sample_size, holding_days))
        return {"success": True, "stats": _stats(50, 1.0, total_signals=10)}

    def backtest_v8_ultimate(self, df, *, sample_size, holding_days, score_threshold):
        self.calls.append(("v8", sample_size, holding_days, score_threshold))
        return {"success": True, "stats": _stats(55, 1.5, total_signals=20)}

    def backtest_v9_midterm(self, df, *, sample_size, holding_days, score_threshold):
        self.calls.append(("v9", sample_size, holding_days, score_threshold))
        return {"success": True, "stats": _stats(60, 2.0, total_signals=30)}

    def backtest_combo_production(self, df, *, sample_size, holding_days, combo_threshold, min_agree):
        self.calls.append(("combo", sample_size, holding_days, combo_threshold, min_agree))
        return {"success": True, "stats": _stats(65, 2.5, total_signals=40)}


def _stats(win_rate: float, avg_return: float, *, total_signals: int) -> dict:
    return {
        "total_signals": total_signals,
        "analyzed_stocks": total_signals * 2,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "median_return": avg_return,
        "max_return": avg_return + 1,
        "min_return": avg_return - 1,
        "sharpe_ratio": 1.0,
        "sortino_ratio": 1.2,
        "max_drawdown": -3.0,
        "profit_loss_ratio": 1.5,
        "avg_holding_days": 10,
        "annualized_return": avg_return * 10,
        "volatility": 8,
    }


def _history_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2026-01-01"},
            {"ts_code": "000002.SZ", "trade_date": "2026-01-02"},
            {"ts_code": "600000.SH", "trade_date": "2026-01-03"},
        ]
    )


def test_single_backtest_worker_routes_strategy_and_full_market_sample():
    analyzer = FakeAnalyzer()
    result = run_single_backtest_worker(
        {
            "strategy": "组合策略（生产共识）",
            "full_market_mode": True,
            "sample_size": 999,
            "holding_days": 7,
            "score_threshold": 70,
        },
        analyzer_factory=lambda: analyzer,
        load_history_df=lambda **kwargs: _history_df(),
    )

    assert result["success"] is True
    assert result["result"]["stats"]["win_rate"] == 65
    assert analyzer.calls == [("combo", 3, 7, 70.0, 2)]


def test_comparison_backtest_worker_returns_all_strategy_stats():
    result = run_comparison_backtest_worker(
        {"sample_size": 100, "validation_mode": "快速全样本"},
        analyzer_factory=FakeAnalyzer,
        load_history_df=lambda **kwargs: _history_df(),
    )

    assert result["success"] is True
    assert set(result["results"]) == {"v5.0 趋势趋势版", "v8.0 进阶版", "v9.0 中线均衡版", "组合策略（生产共识）"}
    assert result["results"]["组合策略（生产共识）"]["total_signals"] == 40
    assert result["meta"] == {"validation_mode": "快速全样本"}
