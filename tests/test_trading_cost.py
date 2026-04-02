"""Tests for risk.trading_cost — transaction cost estimation."""
from __future__ import annotations

from risk.trading_cost import estimate_round_trip_cost


class TestEstimateRoundTripCost:
    def test_basic_cost(self):
        cost = estimate_round_trip_cost(holding_days=5, signal_density=0.03)
        assert cost["base_round_trip_bp"] > 0
        assert cost["turnover_proxy"] > 0
        assert cost["expected_cost_bp"] > 0
        assert cost["expected_cost_pct"] > 0

    def test_longer_holding_reduces_cost(self):
        short = estimate_round_trip_cost(holding_days=3, signal_density=0.05)
        long = estimate_round_trip_cost(holding_days=15, signal_density=0.05)
        assert long["expected_cost_bp"] < short["expected_cost_bp"]

    def test_zero_density_produces_minimum_cost(self):
        cost = estimate_round_trip_cost(holding_days=5, signal_density=0.0)
        assert cost["turnover_proxy"] >= 0
        assert cost["expected_cost_bp"] >= 0

    def test_custom_commission(self):
        low = estimate_round_trip_cost(holding_days=5, signal_density=0.03, commission_bp=3.0)
        high = estimate_round_trip_cost(holding_days=5, signal_density=0.03, commission_bp=15.0)
        assert high["base_round_trip_bp"] > low["base_round_trip_bp"]

    def test_holding_days_clamped_to_one(self):
        cost = estimate_round_trip_cost(holding_days=0, signal_density=0.03)
        assert cost["turnover_proxy"] > 0
