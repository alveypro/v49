from __future__ import annotations

import json
from pathlib import Path

from strategies.center_config import (
    apply_risk_overrides,
    default_center_config,
    find_latest_backtest_best,
    resolve_run_policy,
    resolve_runtime_params,
)


def test_find_latest_backtest_best_uses_newest_success(tmp_path: Path):
    old = tmp_path / "backtest_sweep_v9_20260101_000000.json"
    new = tmp_path / "backtest_sweep_v9_20260102_000000.json"
    old.write_text(
        json.dumps({"best": {"status": "success", "score_threshold": 60, "sample_size": 300, "holding_days": 10}}),
        encoding="utf-8",
    )
    new.write_text(
        json.dumps({"best": {"status": "success", "score_threshold": 65, "sample_size": 50, "holding_days": 8}}),
        encoding="utf-8",
    )

    best = find_latest_backtest_best("v9", tmp_path)
    assert best is not None
    assert best["score_threshold"] == 65
    assert best["sample_size"] == 50
    assert best["holding_days"] == 8


def test_resolve_runtime_params_prefers_cli_overrides(tmp_path: Path):
    cfg = default_center_config()
    cfg["runtime_defaults"] = {"v9": {"score_threshold": 62, "sample_size": 80, "holding_days": 9}}

    (tmp_path / "logs" / "openclaw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs" / "openclaw" / "backtest_sweep_v9_20260102_000000.json").write_text(
        json.dumps({"best": {"status": "success", "score_threshold": 65, "sample_size": 50, "holding_days": 8}}),
        encoding="utf-8",
    )
    cfg["backtest_best_dir"] = "logs/openclaw"

    out = resolve_runtime_params(
        strategy="v9",
        requested_score_threshold=66,
        requested_sample_size=None,
        requested_holding_days=7,
        center_config=cfg,
        project_root=tmp_path,
    )
    assert out["score_threshold"] == 66
    assert out["sample_size"] == 50
    assert out["holding_days"] == 7
    assert out["source"]["score_threshold"] == "cli_override"
    assert out["source"]["sample_size"] == "latest_backtest_best"
    assert out["source"]["holding_days"] == "cli_override"


def test_apply_risk_overrides():
    base = {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02}
    cfg = {"risk_overrides": {"combo": {"max_drawdown_max": 0.35, "win_rate_min": 0.38}}}
    out = apply_risk_overrides("combo", base, cfg)
    assert out["win_rate_min"] == 0.38
    assert out["max_drawdown_max"] == 0.35
    assert out["signal_density_min"] == 0.02


def test_resolve_run_policy_merges_default_and_strategy():
    cfg = {
        "run_policy": {
            "default": {"timeout_sec": 900, "retry_on_no_picks": 1, "no_picks_retry_max": 2},
            "combo": {"timeout_sec": 840, "sample_size": 80, "retry_on_no_picks": 0},
        }
    }
    out = resolve_run_policy("combo", cfg, default_timeout_sec=600)
    assert out["timeout_sec"] == 840
    assert out["sample_size"] == 80
    assert out["retry_on_no_picks"] == 0
    assert out["no_picks_retry_max"] == 2
