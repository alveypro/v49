from __future__ import annotations

import math

import pandas as pd

from openclaw.runtime.backtest_stats import calculate_backtest_stats


def test_calculate_backtest_stats_freezes_core_metrics():
    frame = pd.DataFrame(
        [
            {"trade_date": "2026-01-01", "future_return": 5.0, "signal_strength": 72, "holding_days_realized": 3},
            {"trade_date": "2026-01-02", "future_return": -2.0, "signal_strength": 68, "holding_days_realized": 4},
            {"trade_date": "2026-01-03", "future_return": 3.0, "signal_strength": 91, "holding_days_realized": 5},
        ]
    )

    stats = calculate_backtest_stats(frame, analyzed_count=12, holding_days=5)

    assert stats["total_signals"] == 3
    assert stats["analyzed_stocks"] == 12
    assert abs(stats["win_rate"] - 66.6666666667) < 1e-6
    assert stats["avg_holding_days"] == 4.0
    assert stats["max_consecutive_wins"] == 1
    assert stats["max_consecutive_losses"] == 1
    assert stats["strength_performance"]["70-75"]["count"] == 1
    assert stats["strength_performance"]["90+"]["win_rate"] == 100.0
    assert len(stats["cumulative_returns"]) == 3


def test_calculate_backtest_stats_uses_score_as_signal_strength_fallback():
    frame = pd.DataFrame(
        [
            {"future_return": 1.0, "score": 61},
            {"future_return": -1.0, "score": 66},
        ]
    )

    stats = calculate_backtest_stats(frame, analyzed_count=2, holding_days=5)

    assert stats["strength_performance"]["60-65"]["count"] == 1
    assert stats["strength_performance"]["65-70"]["count"] == 1


def test_calculate_backtest_stats_handles_all_winning_sample():
    frame = pd.DataFrame(
        [
            {"future_return": 1.0, "signal_strength": 70},
            {"future_return": 2.0, "signal_strength": 75},
        ]
    )

    stats = calculate_backtest_stats(frame, analyzed_count=2, holding_days=5)

    assert stats["avg_loss"] == 0
    assert stats["avg_win"] == 1.5
    assert math.isinf(stats["profit_loss_ratio"])
    assert stats["max_consecutive_wins"] == 2
