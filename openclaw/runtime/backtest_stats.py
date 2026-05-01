from __future__ import annotations

from typing import Any, Dict

import pandas as pd


def calculate_backtest_stats(backtest_df: pd.DataFrame, *, analyzed_count: int, holding_days: int) -> Dict[str, Any]:
    if "signal_strength" not in backtest_df.columns:
        backtest_df = backtest_df.copy()
        backtest_df["signal_strength"] = backtest_df["score"] if "score" in backtest_df.columns else 0.0

    stats: Dict[str, Any] = {
        "total_signals": len(backtest_df),
        "analyzed_stocks": analyzed_count,
        "avg_return": float(backtest_df["future_return"].mean()),
        "median_return": float(backtest_df["future_return"].median()),
        "win_rate": float((backtest_df["future_return"] > 0).sum() / len(backtest_df) * 100),
        "max_return": float(backtest_df["future_return"].max()),
        "min_return": float(backtest_df["future_return"].min()),
        "avg_holding_days": float(backtest_df["holding_days_realized"].mean()) if "holding_days_realized" in backtest_df.columns else holding_days,
    }
    if "round_trip_cost_pct" in backtest_df.columns:
        stats["avg_round_trip_cost_pct"] = float(backtest_df["round_trip_cost_pct"].mean())
    if "gross_return" in backtest_df.columns:
        stats["avg_gross_return"] = float(backtest_df["gross_return"].mean())

    std_return = backtest_df["future_return"].std()
    stats["sharpe_ratio"] = float(stats["avg_return"] / std_return) if std_return > 0 else 0
    stats["volatility"] = float(std_return)

    winning_trades = backtest_df[backtest_df["future_return"] > 0]
    losing_trades = backtest_df[backtest_df["future_return"] <= 0]
    avg_win = winning_trades["future_return"].mean() if len(winning_trades) > 0 else 0
    avg_loss = abs(losing_trades["future_return"].mean()) if len(losing_trades) > 0 else 0
    if len(losing_trades) > 0:
        stats["profit_loss_ratio"] = float(avg_win / avg_loss) if avg_loss > 0 else float("inf")
    else:
        stats["profit_loss_ratio"] = float("inf")

    stats["avg_win"] = float(avg_win) if len(winning_trades) > 0 else 0
    stats["avg_loss"] = float(avg_loss) if len(losing_trades) > 0 else 0

    cumulative_returns = (1 + backtest_df["future_return"] / 100).cumprod()
    running_max = cumulative_returns.expanding().max()
    drawdown = (cumulative_returns - running_max) / running_max * 100
    stats["max_drawdown"] = float(drawdown.min())

    downside_returns = backtest_df[backtest_df["future_return"] < 0]["future_return"]
    downside_std = downside_returns.std() if len(downside_returns) > 0 else 0
    stats["sortino_ratio"] = float(stats["avg_return"] / downside_std) if downside_std > 0 else 0

    annualized_return = stats["avg_return"] * (252 / holding_days)
    stats["calmar_ratio"] = float(abs(annualized_return / stats["max_drawdown"])) if stats["max_drawdown"] != 0 else 0
    stats["annualized_return"] = float(annualized_return)

    backtest_df_sorted = backtest_df.sort_values("trade_date") if "trade_date" in backtest_df.columns else backtest_df
    returns_list = backtest_df_sorted["future_return"].tolist()
    max_consecutive_wins = 0
    max_consecutive_losses = 0
    current_wins = 0
    current_losses = 0
    for ret in returns_list:
        if ret > 0:
            current_wins += 1
            current_losses = 0
            max_consecutive_wins = max(max_consecutive_wins, current_wins)
        else:
            current_losses += 1
            current_wins = 0
            max_consecutive_losses = max(max_consecutive_losses, current_losses)

    stats["max_consecutive_wins"] = max_consecutive_wins
    stats["max_consecutive_losses"] = max_consecutive_losses
    stats["return_25_percentile"] = float(backtest_df["future_return"].quantile(0.25))
    stats["return_75_percentile"] = float(backtest_df["future_return"].quantile(0.75))

    win_rate_decimal = stats["win_rate"] / 100
    stats["expected_value"] = float(win_rate_decimal * stats["avg_win"] + (1 - win_rate_decimal) * stats["avg_loss"])

    strength_bins = [0, 60, 65, 70, 75, 80, 85, 90, 100]
    strength_labels = ["<60", "60-65", "65-70", "70-75", "75-80", "80-85", "85-90", "90+"]
    backtest_df["strength_bin"] = pd.cut(
        backtest_df["signal_strength"],
        bins=strength_bins,
        labels=strength_labels,
        include_lowest=True,
    )

    strength_performance = {}
    for label in strength_labels:
        subset = backtest_df[backtest_df["strength_bin"] == label]
        if len(subset) > 0:
            strength_performance[label] = {
                "count": int(len(subset)),
                "avg_return": float(subset["future_return"].mean()),
                "win_rate": float((subset["future_return"] > 0).sum() / len(subset) * 100),
                "max_return": float(subset["future_return"].max()),
                "min_return": float(subset["future_return"].min()),
            }
    stats["strength_performance"] = strength_performance
    stats["cumulative_returns"] = cumulative_returns.tolist()[-100:] if len(cumulative_returns) > 100 else cumulative_returns.tolist()
    return stats
