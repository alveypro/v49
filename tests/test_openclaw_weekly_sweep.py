from __future__ import annotations

import importlib.util
from pathlib import Path


_P = Path(__file__).resolve().parents[1] / "tools" / "openclaw_weekly_sweep.py"
_SPEC = importlib.util.spec_from_file_location("openclaw_weekly_sweep_for_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
weekly = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(weekly)


def test_resolve_plan_prefers_center_cfg():
    cfg = {
        "weekly_sweep": {
            "strategies": {
                "v9": {
                    "score_thresholds": [62, 67],
                    "sample_sizes": [60],
                    "holding_days": [7, 9],
                }
            }
        }
    }
    out = weekly._resolve_plan(cfg, "v9")
    assert out["score_thresholds"] == [62, 67]
    assert out["sample_sizes"] == [60]
    assert out["holding_days"] == [7, 9]


def test_compute_weights_uses_objective_and_floor():
    best = {
        "v5": {"objective": 60.0},
        "v8": {"objective": 30.0},
        "v9": {"objective": 10.0},
        "combo": {"objective": 0.0},
    }
    out = weekly._compute_weights(best, min_weight=0.1)
    assert abs(sum(out.values()) - 1.0) < 1e-6
    assert out["v5"] > out["v8"] > out["v9"]
    assert out["combo"] >= 0.1 - 1e-6
