from __future__ import annotations

from typing import Dict


def estimate_round_trip_cost(
    holding_days: int,
    signal_density: float,
    commission_bp: float = 8.0,
    slippage_bp: float = 10.0,
    stamp_duty_bp: float = 10.0,
) -> Dict[str, float]:
    """Estimate transaction cost with a lightweight turnover model.

    The model intentionally stays simple and deterministic for daily ops:
    - round trip base cost = commission (buy+sell) + slippage (buy+sell) + stamp duty (sell)
    - effective turnover grows with signal density and shrinks with longer holding days
    """
    hold = max(1, int(holding_days))
    density = max(0.0, float(signal_density))

    base_round_trip_bp = max(0.0, commission_bp) * 2.0 + max(0.0, slippage_bp) * 2.0 + max(0.0, stamp_duty_bp)
    turnover_proxy = min(2.5, max(0.2, density * 10.0)) / hold
    expected_cost_bp = base_round_trip_bp * turnover_proxy

    return {
        "base_round_trip_bp": round(base_round_trip_bp, 3),
        "turnover_proxy": round(turnover_proxy, 4),
        "expected_cost_bp": round(expected_cost_bp, 3),
        "expected_cost_pct": round(expected_cost_bp / 100.0, 4),
    }
