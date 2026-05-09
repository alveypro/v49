import json
from pathlib import Path

from src.utils import experiment_artifact_bundle as artifact_bundle
from src.utils.experiment_artifact_bundle import build_experiment_artifact_bundle


def test_build_experiment_artifact_bundle(tmp_path):
    out_dir = tmp_path / "data" / "experiments"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "update_status_latest.json").write_text(
        '{"status":"success","post_candidates":{"ok":true},"post_daily_research":{"ok":true}}',
        encoding="utf-8",
    )
    (out_dir / "daily_research_status_latest.json").write_text(
        json.dumps(
            {
                "state": "completed",
                "profiles": ["short", "medium"],
                "completed_profiles": ["short", "medium"],
                "stocks": ["000001.SZ", "600000.SH"],
                "health_score": {"score": 88.0},
                "research_pool_meta": {
                    "source": "sqlite_research_pool",
                    "requested_size": 50,
                    "latest_trade_date": "2026-04-09",
                    "effective_liquidity_min_turnover": 1200000,
                },
                "runs": [
                    {
                        "profile": "medium",
                        "status": "ok",
                        "planned_runs": 4,
                        "executed_runs": 4,
                        "top_result": {
                            "run_id": "r1",
                            "robustness_score": 0.23,
                            "stability_score": 0.61,
                            "sharpe_ratio": 0.91,
                            "total_return": 0.14,
                            "total_trades": 35,
                            "dominant_regime": "trend",
                            "validation_window": {"start_date": "2025-01-01", "end_date": "2025-06-30"},
                            "params": {"buy_score": 36},
                        },
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (out_dir / "candidates_basket_summary_latest.json").write_text(
        '{"candidate_count":10,"guardrail_mode":"normal","risk_pressure_score":12.5,"strategy_mode":"diversified"}',
        encoding="utf-8",
    )
    (out_dir / "candidates_basket_validation_latest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "rebalance_dates": 3,
                    "avg_basket_return_5d": 0.02,
                    "avg_excess_return_5d": 0.01,
                    "basket_win_rate_5d": 0.67,
                    "avg_universe_return_5d": 0.01,
                    "avg_top1_return_5d": 0.03,
                },
                "variants": {
                    "diversified": {"avg_excess_return_5d": 0.01, "win_rate_5d": 0.67},
                    "raw": {"avg_excess_return_5d": 0.008, "win_rate_5d": 0.5},
                    "top1": {"avg_excess_return_5d": 0.011, "win_rate_5d": 0.67},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (out_dir / "candidates_top_latest.csv").write_text("ts_code\n000001.SZ\n600000.SH\n", encoding="utf-8")
    (out_dir / "grid_backtest_regime_profiles_latest.json").write_text(
        '{"trend":{"avg_environment_score":0.71},"range":{"avg_environment_score":0.43}}',
        encoding="utf-8",
    )
    (out_dir / "grid_backtest_latest.csv").write_text(
        (
            "run_id,robustness_score,params\n"
            "\"r1\",0.23,\"{'buy_score': 36, 'stop_loss_pct': 0.05}\"\n"
            "\"r2\",0.21,\"{'buy_score': 38, 'stop_loss_pct': 0.05}\"\n"
            "\"r3\",0.18,\"{'buy_score': 36, 'stop_loss_pct': 0.04}\"\n"
        ),
        encoding="utf-8",
    )

    outputs = build_experiment_artifact_bundle(output_dir=str(out_dir))

    assert len(outputs) == 12
    for _, path in outputs.items():
        assert Path(path).exists()
    assert (out_dir / "experiment_manifest.json").exists()
    assert (out_dir / "signal_logs.csv").exists()
    governance = json.loads((out_dir / "governance_decision.json").read_text(encoding="utf-8"))
    assert governance["decision"] == "observe"
    assert governance["gate_status"]["core_pipeline_ok"] is True
    regime = json.loads((out_dir / "regime_coverage.json").read_text(encoding="utf-8"))
    assert regime["regime_coverage_score"] == 0.5
    sensitivity = json.loads((out_dir / "sensitivity_report.json").read_text(encoding="utf-8"))
    assert 0.0 <= sensitivity["parameter_sensitivity_score"] <= 1.0
    ranking_review = (out_dir / "candidate_ranking_review.md").read_text(encoding="utf-8")
    assert "ranking_consistency_score" in ranking_review
    search_results = (out_dir / "search_results.csv").read_text(encoding="utf-8")
    assert "validation_start,validation_end" in search_results


def test_build_experiment_artifact_bundle_prefers_output_dir_artifacts(monkeypatch, tmp_path):
    project_root = tmp_path / "project"
    out_dir = project_root / "custom_bundle"
    shared_grid_dir = project_root / "data" / "experiments" / "grid_search"
    out_dir.mkdir(parents=True, exist_ok=True)
    shared_grid_dir.mkdir(parents=True, exist_ok=True)

    def fake_resolve_project_path(relative_path: str) -> Path:
        return project_root / relative_path

    monkeypatch.setattr(artifact_bundle, "resolve_project_path", fake_resolve_project_path)

    (out_dir / "update_status_latest.json").write_text(
        '{"status":"success","post_candidates":{"ok":true},"post_daily_research":{"ok":true}}',
        encoding="utf-8",
    )
    (out_dir / "daily_research_status_latest.json").write_text(
        json.dumps(
            {
                "state": "completed",
                "profiles": ["medium"],
                "completed_profiles": ["medium"],
                "stocks": ["000001.SZ"],
                "health_score": {"score": 91.0},
                "runs": [
                    {
                        "profile": "medium",
                        "status": "ok",
                        "planned_runs": 2,
                        "executed_runs": 2,
                        "top_result": {
                            "run_id": "local-r1",
                            "robustness_score": 0.25,
                            "stability_score": 0.62,
                            "sharpe_ratio": 1.01,
                            "total_return": 0.16,
                            "total_trades": 18,
                            "dominant_regime": "trend",
                            "validation_window": {"start_date": "2025-01-01", "end_date": "2025-06-30"},
                            "params": {"buy_score": 36},
                        },
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (out_dir / "candidates_basket_summary_latest.json").write_text(
        '{"candidate_count":6,"guardrail_mode":"normal","risk_pressure_score":10.0,"strategy_mode":"diversified"}',
        encoding="utf-8",
    )
    (out_dir / "candidates_basket_validation_latest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "rebalance_dates": 3,
                    "avg_basket_return_5d": 0.02,
                    "avg_excess_return_5d": 0.01,
                    "basket_win_rate_5d": 0.67,
                    "avg_universe_return_5d": 0.01,
                    "avg_top1_return_5d": 0.03,
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (out_dir / "grid_backtest_regime_profiles_latest.json").write_text(
        '{"trend":{"avg_environment_score":0.71},"range":{"avg_environment_score":0.43}}',
        encoding="utf-8",
    )
    (out_dir / "grid_backtest_latest.csv").write_text(
        (
            "run_id,robustness_score,params\n"
            "\"local-r1\",0.25,\"{'buy_score': 36}\"\n"
            "\"local-r2\",0.22,\"{'buy_score': 38}\"\n"
        ),
        encoding="utf-8",
    )

    (shared_grid_dir / "grid_backtest_regime_profiles_latest.json").write_text(
        '{"trend":{"avg_environment_score":0.51}}',
        encoding="utf-8",
    )
    (shared_grid_dir / "grid_backtest_latest.csv").write_text(
        (
            "run_id,robustness_score,params\n"
            "\"shared-r1\",0.19,\"{'buy_score': 40}\"\n"
            "\"shared-r2\",0.19,\"{'buy_score': 40}\"\n"
        ),
        encoding="utf-8",
    )

    build_experiment_artifact_bundle(output_dir=str(out_dir))

    regime = json.loads((out_dir / "regime_coverage.json").read_text(encoding="utf-8"))
    sensitivity = json.loads((out_dir / "sensitivity_report.json").read_text(encoding="utf-8"))

    assert regime["observed_regimes"] == ["range", "trend"]
    assert regime["regime_coverage_score"] == 0.5
    assert sensitivity["top_run_count"] == 2
    assert sensitivity["parameter_sensitivity_score"] > 0.0
