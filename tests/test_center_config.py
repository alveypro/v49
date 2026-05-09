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


def _credible_payload(best: dict, *, eligible: bool = True) -> dict:
    return {
        "best": best,
        "backtest_credibility": {
            "point_in_time_data": True,
            "suspension_and_limit_handling": True,
            "volume_constraint": True,
            "cost_model": True,
            "slippage_model": True,
            "in_sample_out_of_sample_split": True,
            "parameter_sensitivity": True,
            "failed_backtests_recorded": True,
            "metrics": {"signal_density": best.get("signal_density", 0.02), "test_windows": 3},
        },
        "strategy_backtest_diagnostics": {"eligible_for_formal_ranking": eligible},
    }


def test_find_latest_backtest_best_uses_newest_success(tmp_path: Path):
    old = tmp_path / "backtest_sweep_v9_20260101_000000.json"
    new = tmp_path / "backtest_sweep_v9_20260102_000000.json"
    old.write_text(
        json.dumps(
            _credible_payload(
                {"status": "success", "score_threshold": 60, "sample_size": 300, "holding_days": 10, "win_rate": 0.5, "max_drawdown": 0.1, "signal_density": 0.02}
            )
        ),
        encoding="utf-8",
    )
    new.write_text(
        json.dumps(
            _credible_payload(
                {"status": "success", "score_threshold": 65, "sample_size": 50, "holding_days": 8, "win_rate": 0.6, "max_drawdown": 0.1, "signal_density": 0.03}
            )
        ),
        encoding="utf-8",
    )

    best = find_latest_backtest_best("v9", tmp_path)
    assert best is not None
    assert best["score_threshold"] == 65
    assert best["sample_size"] == 50
    assert best["holding_days"] == 8


def test_find_latest_backtest_best_skips_unusable_evidence(tmp_path: Path):
    unusable = tmp_path / "backtest_sweep_v8_20260103_000000.json"
    usable = tmp_path / "backtest_sweep_v8_20260102_000000.json"
    unusable.write_text(
        json.dumps(
            {
                "best": {
                    "status": "success",
                    "score_threshold": 50,
                    "sample_size": 50,
                    "holding_days": 6,
                    "win_rate": 0.88,
                    "max_drawdown": 0.30,
                    "signal_density": 0.05,
                },
                "backtest_credibility": {
                    "point_in_time_data": True,
                    "suspension_and_limit_handling": True,
                    "volume_constraint": True,
                    "cost_model": True,
                    "slippage_model": True,
                    "in_sample_out_of_sample_split": True,
                    "parameter_sensitivity": True,
                    "failed_backtests_recorded": True,
                    "metrics": {"signal_density": 0.05, "test_windows": 3},
                },
                "strategy_backtest_diagnostics": {"eligible_for_formal_ranking": False},
            }
        ),
        encoding="utf-8",
    )
    usable.write_text(
        json.dumps(
            {
                "best": {
                    "status": "success",
                    "score_threshold": 55,
                    "sample_size": 80,
                    "holding_days": 8,
                    "win_rate": 0.56,
                    "max_drawdown": 0.12,
                    "signal_density": 0.08,
                },
                "backtest_credibility": {
                    "point_in_time_data": True,
                    "suspension_and_limit_handling": True,
                    "volume_constraint": True,
                    "cost_model": True,
                    "slippage_model": True,
                    "in_sample_out_of_sample_split": True,
                    "parameter_sensitivity": True,
                    "failed_backtests_recorded": True,
                    "metrics": {"signal_density": 0.08, "test_windows": 3},
                },
                "strategy_backtest_diagnostics": {"eligible_for_formal_ranking": True},
            }
        ),
        encoding="utf-8",
    )
    unusable.touch()

    best = find_latest_backtest_best("v8", tmp_path)

    assert best is not None
    assert best["score_threshold"] == 55
    assert best["sample_size"] == 80


def test_find_latest_backtest_best_searches_repair_probe_subdirectories(tmp_path: Path):
    nested = tmp_path / "v5_repair_probe"
    nested.mkdir()
    artifact = nested / "backtest_sweep_v5_20260103_000000.json"
    artifact.write_text(
        json.dumps(
            _credible_payload(
                {
                    "status": "success",
                    "score_threshold": 70,
                    "sample_size": 50,
                    "holding_days": 3,
                    "win_rate": 1.0,
                    "max_drawdown": 0.0,
                    "signal_density": 0.02,
                }
            )
        ),
        encoding="utf-8",
    )

    best = find_latest_backtest_best("v5", tmp_path)

    assert best is not None
    assert best["score_threshold"] == 70
    assert best["sample_size"] == 50
    assert best["holding_days"] == 3
    assert best["artifact"] == str(artifact)


def test_find_latest_backtest_best_skips_rejected_artifact_ledger_entry(tmp_path: Path):
    rejected = tmp_path / "rejected.jsonl"
    blocked = tmp_path / "backtest_sweep_combo_20260103_000000.json"
    usable = tmp_path / "backtest_sweep_combo_20260102_000000.json"
    blocked.write_text(
        json.dumps(
            {
                "best": {"status": "success", "score_threshold": 65, "sample_size": 50, "holding_days": 6},
                "backtest_credibility": {
                    "point_in_time_data": True,
                    "suspension_and_limit_handling": True,
                    "volume_constraint": True,
                    "cost_model": True,
                    "slippage_model": True,
                    "in_sample_out_of_sample_split": True,
                    "parameter_sensitivity": True,
                    "failed_backtests_recorded": True,
                    "metrics": {"signal_density": 0.02, "test_windows": 3},
                },
                "strategy_backtest_diagnostics": {"eligible_for_formal_ranking": True},
            }
        ),
        encoding="utf-8",
    )
    usable.write_text(
        json.dumps(
            {
                "best": {"status": "success", "score_threshold": 68, "sample_size": 80, "holding_days": 8},
                "backtest_credibility": {
                    "point_in_time_data": True,
                    "suspension_and_limit_handling": True,
                    "volume_constraint": True,
                    "cost_model": True,
                    "slippage_model": True,
                    "in_sample_out_of_sample_split": True,
                    "parameter_sensitivity": True,
                    "failed_backtests_recorded": True,
                    "metrics": {"signal_density": 0.02, "test_windows": 3},
                },
                "strategy_backtest_diagnostics": {"eligible_for_formal_ranking": True},
            }
        ),
        encoding="utf-8",
    )
    rejected.write_text(
        json.dumps(
            {
                "artifact_path": str(blocked),
                "strategy": "combo",
                "reason": "consensus_breakpoint_unexplained",
                "reused_as_runtime_default": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    best = find_latest_backtest_best("combo", tmp_path, rejected_artifacts_path=str(rejected))

    assert best is not None
    assert best["artifact"] == str(usable)


def test_resolve_runtime_params_prefers_cli_overrides(tmp_path: Path):
    cfg = default_center_config()
    cfg["runtime_defaults"] = {"v9": {"score_threshold": 62, "sample_size": 80, "holding_days": 9}}

    (tmp_path / "logs" / "openclaw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs" / "openclaw" / "backtest_sweep_v9_20260102_000000.json").write_text(
        json.dumps(
            _credible_payload(
                {"status": "success", "score_threshold": 65, "sample_size": 50, "holding_days": 8, "win_rate": 0.6, "max_drawdown": 0.1, "signal_density": 0.02}
            )
        ),
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


def test_resolve_runtime_params_does_not_auto_lower_score_threshold(tmp_path: Path):
    cfg = default_center_config()
    cfg["runtime_defaults"] = {"v5": {"score_threshold": 70, "sample_size": 80, "holding_days": 5}}

    (tmp_path / "logs" / "openclaw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs" / "openclaw" / "backtest_sweep_v5_20260102_000000.json").write_text(
        json.dumps(
            _credible_payload(
                {"status": "success", "score_threshold": 60, "sample_size": 50, "holding_days": 8, "win_rate": 0.6, "max_drawdown": 0.1, "signal_density": 0.02}
            )
        ),
        encoding="utf-8",
    )
    cfg["backtest_best_dir"] = "logs/openclaw"

    out = resolve_runtime_params(
        strategy="v5",
        requested_score_threshold=None,
        requested_sample_size=None,
        requested_holding_days=None,
        center_config=cfg,
        project_root=tmp_path,
    )

    assert out["score_threshold"] == 70
    assert out["sample_size"] == 50
    assert out["holding_days"] == 8
    assert out["source"]["score_threshold"] == "threshold_floor"


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
