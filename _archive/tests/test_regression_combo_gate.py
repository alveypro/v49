from __future__ import annotations

import importlib.util
from pathlib import Path


_P = Path(__file__).resolve().parents[1] / "tools" / "regression_combo_gate.py"
_SPEC = importlib.util.spec_from_file_location("regression_combo_gate_for_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gate)


def test_gate_thresholds_from_center_config():
    cfg = {"regression_gate": {"min_picks": 2, "max_scan_sec": 300}}
    out = gate._gate_thresholds(cfg)
    assert out["min_picks"] == 2
    assert out["max_scan_sec"] == 300


def test_build_scan_params_uses_center_runtime_and_run_policy():
    cfg = {
        "auto_apply_backtest_best": False,
        "runtime_defaults": {"v9": {"score_threshold": 66, "sample_size": 50, "holding_days": 8}},
        "run_policy": {"v9": {"offline_stock_limit": 40}},
    }
    out = gate._build_scan_params("v9", cfg)
    assert out["score_threshold"] == 66
    assert out["offline_stock_limit"] == 40
    assert out["limit"] == 30
