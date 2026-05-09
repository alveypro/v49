from __future__ import annotations

from typing import Any

import numpy as np


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def trade_objective_from_predictions(
    y_true,
    prob_up,
    realized_returns=None,
    *,
    top_quantile: float = 0.2,
) -> dict[str, float]:
    y_arr = np.asarray(y_true, dtype=float)
    prob_arr = np.asarray(prob_up, dtype=float)
    if y_arr.size == 0 or prob_arr.size == 0 or y_arr.size != prob_arr.size:
        return {
            "trade_objective": 0.0,
            "coverage": 0.0,
            "precision_at_top": 0.0,
            "avg_return_at_top": 0.0,
            "return_sharpe_proxy": 0.0,
        }

    top_n = max(int(np.ceil(len(prob_arr) * float(top_quantile))), 1)
    ranked_idx = np.argsort(prob_arr)[::-1][:top_n]
    top_hits = y_arr[ranked_idx]
    precision_at_top = float(np.mean(top_hits > 0.5)) if top_hits.size else 0.0
    coverage = float(top_n / len(prob_arr))

    avg_return_at_top = 0.0
    return_sharpe_proxy = 0.0
    if realized_returns is not None:
        ret_arr = np.asarray(realized_returns, dtype=float)
        if ret_arr.size == prob_arr.size:
            picked_returns = ret_arr[ranked_idx]
            avg_return_at_top = float(np.mean(picked_returns)) if picked_returns.size else 0.0
            std = float(np.std(picked_returns)) if picked_returns.size else 0.0
            if std > 1e-12:
                return_sharpe_proxy = float(np.mean(picked_returns) / std)

    confidence_edge = float(np.mean(np.abs(prob_arr[ranked_idx] - 0.5) * 2.0)) if ranked_idx.size else 0.0
    trade_objective = (
        precision_at_top * 0.35
        + avg_return_at_top * 6.0
        + return_sharpe_proxy * 0.20
        + confidence_edge * 0.10
        - coverage * 0.05
    )
    return {
        "trade_objective": float(trade_objective),
        "coverage": coverage,
        "precision_at_top": precision_at_top,
        "avg_return_at_top": float(avg_return_at_top),
        "return_sharpe_proxy": float(return_sharpe_proxy),
    }


def summarize_trade_metrics(metric_rows: list[dict[str, Any]]) -> dict[str, float]:
    if not metric_rows:
        return {
            "trade_objective_mean": 0.0,
            "trade_objective_std": 0.0,
            "trade_objective_stability": 0.0,
            "precision_at_top_mean": 0.0,
            "avg_return_at_top_mean": 0.0,
            "return_sharpe_proxy_mean": 0.0,
            "sample_count": 0.0,
        }

    objective_values = np.asarray([_safe_float(row.get("trade_objective")) for row in metric_rows], dtype=float)
    precision_values = np.asarray([_safe_float(row.get("precision_at_top")) for row in metric_rows], dtype=float)
    return_values = np.asarray([_safe_float(row.get("avg_return_at_top")) for row in metric_rows], dtype=float)
    sharpe_values = np.asarray([_safe_float(row.get("return_sharpe_proxy")) for row in metric_rows], dtype=float)
    std = float(np.std(objective_values)) if len(objective_values) > 1 else 0.0
    stability = float(max(0.0, 1.0 - std / max(abs(float(np.mean(objective_values))), 0.1)))
    return {
        "trade_objective_mean": float(np.mean(objective_values)),
        "trade_objective_std": std,
        "trade_objective_stability": stability,
        "precision_at_top_mean": float(np.mean(precision_values)),
        "avg_return_at_top_mean": float(np.mean(return_values)),
        "return_sharpe_proxy_mean": float(np.mean(sharpe_values)),
        "sample_count": float(len(metric_rows)),
    }
