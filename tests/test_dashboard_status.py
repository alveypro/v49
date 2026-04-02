from __future__ import annotations

import importlib.util
from pathlib import Path


_P = Path(__file__).resolve().parents[1] / "tools" / "dashboard_status.py"
_SPEC = importlib.util.spec_from_file_location("dashboard_status_for_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
dash = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(dash)


def test_effective_strategy_view_from_center_defaults():
    cfg = {
        "auto_apply_backtest_best": False,
        "runtime_defaults": {"v9": {"score_threshold": 66, "sample_size": 55, "holding_days": 9}},
        "risk_overrides": {"v9": {"win_rate_min": 0.38, "max_drawdown_max": 0.45, "signal_density_min": 0.05}},
        "strategy_weights": {"v9": 0.2},
    }
    row = dash._effective_strategy_view("v9", cfg)
    assert row["score_threshold"] == 66
    assert row["sample_size"] == 55
    assert row["holding_days"] == 9
    assert row["source"] == "center_runtime_default"
    assert abs(row["weight"] - 0.2) < 1e-9
    assert "wr>=0.38" in row["risk_text"]


def test_build_dashboard_contains_center_columns(monkeypatch):
    cfg = {
        "auto_apply_backtest_best": False,
        "runtime_defaults": {"v5": {"score_threshold": 65, "sample_size": 50, "holding_days": 8}},
        "strategy_weights": {"v5": 0.4},
    }
    monkeypatch.setattr(dash, "_load_strategy_center", lambda: cfg)
    out = dash.build_dashboard()
    assert "参数来源" in out
    assert "风控线" in out
