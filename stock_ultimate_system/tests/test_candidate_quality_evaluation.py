from __future__ import annotations

import json
from pathlib import Path

from src.candidate_quality_baseline_registry import CandidateQualityBaselineRegistry
from src.candidate_quality_evaluation import (
    build_candidate_quality_evaluation,
    write_candidate_quality_evaluation_artifacts,
)


def _write_pointer_chain(base: Path) -> None:
    artifacts_dir = base / "artifacts"
    (artifacts_dir / "current_result_pointer").mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "current_result_pointer" / "current.json").write_text(
        json.dumps(
            {
                "pointer_version": "current_result_pointer.v1",
                "pointer_snapshot_id": "pointer-1",
                "result_id": "primary:300750.SZ",
                "run_id": "run-1",
                "lifecycle_id": "life-1",
                "artifact_ids": ["artifact:a", "artifact:b"],
                "as_of_date": "2026-05-01",
                "source_scope": "stock",
                "snapshot_path": str(artifacts_dir / "current_result_pointer" / "history" / "pointer-1.json"),
                "updated_at": "2026-05-01T00:00:00+00:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _write_multiwindow_payload(exp_dir: Path) -> None:
    (exp_dir / "candidate_quality_multiwindow_latest.json").write_text(
        json.dumps(
            {
                "sample_density": {
                    "60d": {"sample_total": 3, "minimum_required": 3, "status": "passed"},
                    "120d": {"sample_total": 5, "minimum_required": 5, "status": "passed"},
                },
                "windows": {
                    "60d": {
                        "top1": {"avg_return": 0.051, "avg_excess_return": 0.022, "win_rate": 0.63},
                        "top3": {"avg_return": 0.044, "avg_excess_return": 0.019, "win_rate": 0.61},
                        "top5": {"avg_return": 0.039, "avg_excess_return": 0.017, "win_rate": 0.59},
                        "top10": {"avg_return": 0.031, "avg_excess_return": 0.011, "win_rate": 0.55},
                    },
                    "120d": {
                        "top1": {"avg_return": 0.082, "avg_excess_return": 0.034, "win_rate": 0.66},
                        "top3": {"avg_return": 0.071, "avg_excess_return": 0.029, "win_rate": 0.64},
                        "top5": {"avg_return": 0.064, "avg_excess_return": 0.024, "win_rate": 0.61},
                        "top10": {"avg_return": 0.052, "avg_excess_return": 0.018, "win_rate": 0.57},
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_candidate_quality_evaluation_writes_formal_artifacts(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    _write_pointer_chain(tmp_path)

    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))
    monkeypatch.setenv("STOCK_ULTIMATE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    (exp_dir / "candidates_basket_validation_latest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "rebalance_dates": 8,
                    "avg_basket_return_5d": 0.018,
                    "avg_excess_return_5d": 0.011,
                    "basket_win_rate_5d": 0.625,
                    "avg_universe_return_5d": 0.007,
                    "avg_top1_return_5d": 0.027,
                },
                "variants": {
                    "top1": {"avg_excess_return_5d": 0.014, "win_rate_5d": 0.625},
                    "top3": {"avg_return_5d": 0.021, "avg_excess_return_5d": 0.012, "win_rate_5d": 0.625},
                    "top10": {"avg_return_5d": 0.013, "avg_excess_return_5d": 0.006, "win_rate_5d": 0.5},
                },
                "ranking_consistency": {"ranking_consistency_score": 0.88},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (exp_dir / "grid_backtest_regime_profiles_latest.json").write_text(
        json.dumps(
            {
                "bull": {"avg_environment_score": 0.72},
                "bear": {"avg_environment_score": 0.51},
                "range": {"avg_environment_score": 0.46},
                "high_vol": {"avg_environment_score": 0.39},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (exp_dir / "stock_primary_result_benchmark_report.json").write_text(
        json.dumps({"benchmark_version": "v1", "sample_total": 7}, ensure_ascii=False),
        encoding="utf-8",
    )
    (exp_dir / "primary_result_failure_attribution_latest.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "primary_failure_category": None,
                "contributing_categories": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_multiwindow_payload(exp_dir)
    previous_summary_path = exp_dir / "previous_candidate_quality_summary.json"
    previous_summary_path.write_text(
        json.dumps(
            {
                "evaluation_id": "prev-1",
                "generated_at": "2026-04-01T00:00:00+00:00",
                "pass_or_fail": "passed",
                "result_id": "primary:300750.SZ",
                "run_id": "run-0",
                "source_scope": "stock",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry = CandidateQualityBaselineRegistry(baselines_dir=tmp_path / "artifacts" / "candidate_quality_baselines")
    registry.register(summary_path=previous_summary_path, baseline_id="baseline-prev-1")

    payload = build_candidate_quality_evaluation()
    outputs = write_candidate_quality_evaluation_artifacts(payload)

    assert payload["pass_or_fail"] == "passed"
    assert payload["sample_count"] == 8
    assert payload["bucket_metrics"]["top1"]["avg_return"] == 0.027
    assert payload["bucket_metrics"]["top3"]["avg_excess_return"] == 0.012
    assert payload["bucket_window_metrics"]["60d"]["top3"]["avg_return"] == 0.044
    assert payload["bucket_window_metrics"]["120d"]["top10"]["avg_excess_return"] == 0.018
    assert payload["multiwindow_sample_density"]["120d"]["status"] == "passed"
    assert payload["regime_breakdown"]["bull"]["environment_score"] == 0.72
    assert payload["previous_formal_baseline_summary"]["evaluation_id"] == "prev-1"
    assert Path(outputs["candidate_quality_summary"]).exists()
    assert Path(outputs["candidate_quality_benchmark_table"]).exists()
    assert Path(outputs["candidate_quality_failure_attribution"]).exists()


def test_candidate_quality_evaluation_blocks_when_required_inputs_are_missing(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    _write_pointer_chain(tmp_path)

    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))
    monkeypatch.setenv("STOCK_ULTIMATE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    (exp_dir / "candidates_basket_validation_latest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "rebalance_dates": 3,
                    "avg_basket_return_5d": 0.012,
                    "avg_excess_return_5d": 0.004,
                    "basket_win_rate_5d": 0.5,
                    "avg_universe_return_5d": 0.008,
                    "avg_top1_return_5d": 0.016,
                },
                "variants": {
                    "top1": {"avg_excess_return_5d": 0.005, "win_rate_5d": 0.5},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = build_candidate_quality_evaluation()

    assert payload["pass_or_fail"] == "blocked"
    assert "benchmark_report_missing" in payload["blocking_reasons"]
    assert "previous_formal_baseline_missing" in payload["blocking_reasons"]
    assert "failure_attribution_missing" in payload["blocking_reasons"]
    assert "regime_breakdown_missing:bull" in payload["blocking_reasons"]
    assert "missing_20d_top3_avg_return" in payload["blocking_reasons"]
    assert "missing_20d_top10_avg_excess_return" in payload["blocking_reasons"]
    assert "missing_60d_top1_avg_return" in payload["blocking_reasons"]
    assert "missing_120d_top10_avg_excess_return" in payload["blocking_reasons"]


def test_candidate_quality_evaluation_uses_failure_attribution_summary_from_artifacts_when_latest_missing(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts_dir = tmp_path / "artifacts"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "primary_result_failure_attribution").mkdir(parents=True, exist_ok=True)
    _write_pointer_chain(tmp_path)

    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))
    monkeypatch.setenv("STOCK_ULTIMATE_ARTIFACTS_DIR", str(artifacts_dir))

    (exp_dir / "candidates_basket_validation_latest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "rebalance_dates": 8,
                    "avg_basket_return_5d": 0.018,
                    "avg_excess_return_5d": 0.011,
                    "basket_win_rate_5d": 0.625,
                    "avg_universe_return_5d": 0.007,
                    "avg_top1_return_5d": 0.027,
                },
                "variants": {
                    "top1": {"avg_excess_return_5d": 0.014, "win_rate_5d": 0.625},
                    "top3": {"avg_return_5d": 0.021, "avg_excess_return_5d": 0.012, "win_rate_5d": 0.625},
                    "top10": {"avg_return_5d": 0.013, "avg_excess_return_5d": 0.006, "win_rate_5d": 0.5},
                },
                "ranking_consistency": {"ranking_consistency_score": 0.88},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_multiwindow_payload(exp_dir)
    (exp_dir / "grid_backtest_regime_profiles_latest.json").write_text(
        json.dumps(
            {
                "bull": {"avg_environment_score": 0.72},
                "bear": {"avg_environment_score": 0.51},
                "range": {"avg_environment_score": 0.46},
                "high_vol": {"avg_environment_score": 0.39},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (exp_dir / "stock_primary_result_benchmark_report.json").write_text(
        json.dumps({"benchmark_version": "v1", "sample_total": 7}, ensure_ascii=False),
        encoding="utf-8",
    )
    previous_summary_path = exp_dir / "previous_candidate_quality_summary.json"
    previous_summary_path.write_text(
        json.dumps(
            {
                "evaluation_id": "prev-1",
                "generated_at": "2026-04-01T00:00:00+00:00",
                "pass_or_fail": "passed",
                "result_id": "primary:300750.SZ",
                "run_id": "run-0",
                "source_scope": "stock",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry = CandidateQualityBaselineRegistry(baselines_dir=tmp_path / "artifacts" / "candidate_quality_baselines")
    registry.register(summary_path=previous_summary_path, baseline_id="baseline-prev-1")
    (artifacts_dir / "primary_result_failure_attribution" / "summary.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "false_positive_cases": 2,
                "evidence_insufficient_cases": 1,
                "regime_mismatch_cases": 1,
                "contributing_categories": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = build_candidate_quality_evaluation()

    assert payload["pass_or_fail"] == "passed"
    assert payload["failure_attribution_summary"]["false_positive_cases"] == 2
    assert payload["failure_attribution_summary"]["evidence_insufficient_cases"] == 1
    assert payload["multiwindow_sample_density"]["60d"]["status"] == "passed"


def test_candidate_quality_evaluation_blocks_when_multiwindow_sample_density_is_insufficient(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    _write_pointer_chain(tmp_path)

    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))
    monkeypatch.setenv("STOCK_ULTIMATE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    (exp_dir / "candidates_basket_validation_latest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "rebalance_dates": 8,
                    "avg_basket_return_5d": 0.018,
                    "avg_excess_return_5d": 0.011,
                    "basket_win_rate_5d": 0.625,
                    "avg_universe_return_5d": 0.007,
                    "avg_top1_return_5d": 0.027,
                },
                "variants": {
                    "top1": {"avg_excess_return_5d": 0.014, "win_rate_5d": 0.625},
                    "top3": {"avg_return_5d": 0.021, "avg_excess_return_5d": 0.012, "win_rate_5d": 0.625},
                    "top10": {"avg_return_5d": 0.013, "avg_excess_return_5d": 0.006, "win_rate_5d": 0.5},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (exp_dir / "candidate_quality_multiwindow_latest.json").write_text(
        json.dumps(
            {
                "sample_density": {
                    "60d": {"sample_total": 2, "minimum_required": 3, "status": "blocked"},
                    "120d": {"sample_total": 4, "minimum_required": 5, "status": "blocked"},
                },
                "windows": {
                    "60d": {
                        "top1": {"avg_return": 0.051, "avg_excess_return": 0.022, "win_rate": 0.63},
                        "top3": {"avg_return": 0.044, "avg_excess_return": 0.019, "win_rate": 0.61},
                        "top5": {"avg_return": 0.039, "avg_excess_return": 0.017, "win_rate": 0.59},
                        "top10": {"avg_return": 0.031, "avg_excess_return": 0.011, "win_rate": 0.55},
                    },
                    "120d": {
                        "top1": {"avg_return": 0.082, "avg_excess_return": 0.034, "win_rate": 0.66},
                        "top3": {"avg_return": 0.071, "avg_excess_return": 0.029, "win_rate": 0.64},
                        "top5": {"avg_return": 0.064, "avg_excess_return": 0.024, "win_rate": 0.61},
                        "top10": {"avg_return": 0.052, "avg_excess_return": 0.018, "win_rate": 0.57},
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (exp_dir / "grid_backtest_regime_profiles_latest.json").write_text(
        json.dumps(
            {
                "bull": {"avg_environment_score": 0.72},
                "bear": {"avg_environment_score": 0.51},
                "range": {"avg_environment_score": 0.46},
                "high_vol": {"avg_environment_score": 0.39},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (exp_dir / "stock_primary_result_benchmark_report.json").write_text(
        json.dumps({"benchmark_version": "v1", "sample_total": 7}, ensure_ascii=False),
        encoding="utf-8",
    )
    (exp_dir / "primary_result_failure_attribution_latest.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "primary_failure_category": None,
                "contributing_categories": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    previous_summary_path = exp_dir / "previous_candidate_quality_summary.json"
    previous_summary_path.write_text(
        json.dumps(
            {
                "evaluation_id": "prev-1",
                "generated_at": "2026-04-01T00:00:00+00:00",
                "pass_or_fail": "passed",
                "result_id": "primary:300750.SZ",
                "run_id": "run-0",
                "source_scope": "stock",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry = CandidateQualityBaselineRegistry(baselines_dir=tmp_path / "artifacts" / "candidate_quality_baselines")
    registry.register(summary_path=previous_summary_path, baseline_id="baseline-prev-1")

    payload = build_candidate_quality_evaluation()

    assert payload["pass_or_fail"] == "blocked"
    assert "60d_sample_density_insufficient" in payload["blocking_reasons"]
    assert "120d_sample_density_insufficient" in payload["blocking_reasons"]
