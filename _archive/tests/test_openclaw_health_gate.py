from __future__ import annotations

import importlib.util
from pathlib import Path


_P = Path(__file__).resolve().parents[1] / "tools" / "openclaw_health_gate.py"
_SPEC = importlib.util.spec_from_file_location("openclaw_health_gate_for_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gate)


def test_risk_limits_for_strategy_override():
    cfg = {"risk_overrides": {"v9": {"win_rate_min": 0.38, "max_drawdown_max": 0.45, "signal_density_min": 0.05}}}
    out = gate._risk_limits_for_strategy("v9", cfg)
    assert out["win_rate_min"] == 0.38
    assert out["max_drawdown_max"] == 0.45
    assert out["signal_density_min"] == 0.05


def test_metric_breaches_detects_all():
    limits = {"win_rate_min": 0.5, "max_drawdown_max": 0.3, "signal_density_min": 0.1}
    stats = {"win_rate": 0.42, "max_drawdown": 0.35, "signal_density": 0.05}
    out = gate._metric_breaches(stats, limits)
    assert set(out) == {"win_rate", "max_drawdown", "signal_density"}


def test_metric_breaches_no_breach():
    limits = {"win_rate_min": 0.4, "max_drawdown_max": 0.5, "signal_density_min": 0.01}
    stats = {"win_rate": 0.42, "max_drawdown": 0.35, "signal_density": 0.05}
    out = gate._metric_breaches(stats, limits)
    assert out == []
