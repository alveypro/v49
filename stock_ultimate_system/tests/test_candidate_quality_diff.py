from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from src.candidate_quality_baseline_registry import CandidateQualityBaselineRegistry
from src.candidate_quality_diff import build_candidate_quality_diff


def _write_summary(
    path: Path,
    *,
    evaluation_id: str,
    top1_20d: float,
    top3_20d: float,
    top1_60d: float,
    top3_60d: float,
    top1_120d: float,
    top3_120d: float,
    top10_120d: float = 0.018,
    false_positive_cases: int = 3,
    missed_winner_cases: int = 2,
    rank_too_low_cases: int = 1,
    risk_gate_blocked_but_later_strong_cases: int = 1,
    evidence_insufficient_cases: int = 1,
    regime_mismatch_cases: int = 1,
    risk_control_failure_cases: int = 1,
    benchmark_underperformance_cases: int = 1,
    negative_absolute_return_cases: int = 1,
    source_risk_mismatch_cases: int = 1,
    weak_source_signal_cases: int = 1,
    weak_success_cases: int = 0,
    unclassified_failure_cases: int = 0,
    sample_density_60d: tuple[int, int, str] = (3, 3, "passed"),
    sample_density_120d: tuple[int, int, str] = (5, 5, "passed"),
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "evaluation_id": evaluation_id,
        "generated_at": "2026-05-01T00:00:00+00:00",
        "run_id": "run-1",
        "result_id": "primary:300750.SZ",
        "source_scope": "stock",
        "multiwindow_sample_density": {
            "60d": {
                "sample_total": sample_density_60d[0],
                "minimum_required": sample_density_60d[1],
                "status": sample_density_60d[2],
            },
            "120d": {
                "sample_total": sample_density_120d[0],
                "minimum_required": sample_density_120d[1],
                "status": sample_density_120d[2],
            },
        },
        "bucket_window_metrics": {
            "20d": {
                "top1": {"avg_return": top1_20d + 0.01, "avg_excess_return": top1_20d, "win_rate": 0.63},
                "top3": {"avg_return": top3_20d + 0.01, "avg_excess_return": top3_20d, "win_rate": 0.61},
                "top5": {"avg_return": 0.017, "avg_excess_return": 0.008, "win_rate": 0.59},
                "top10": {"avg_return": 0.015, "avg_excess_return": 0.006, "win_rate": 0.55},
            },
            "60d": {
                "top1": {"avg_return": top1_60d + 0.01, "avg_excess_return": top1_60d, "win_rate": 0.66},
                "top3": {"avg_return": top3_60d + 0.01, "avg_excess_return": top3_60d, "win_rate": 0.64},
                "top5": {"avg_return": 0.031, "avg_excess_return": 0.014, "win_rate": 0.60},
                "top10": {"avg_return": 0.023, "avg_excess_return": 0.01, "win_rate": 0.57},
            },
            "120d": {
                "top1": {"avg_return": top1_120d + 0.01, "avg_excess_return": top1_120d, "win_rate": 0.68},
                "top3": {"avg_return": top3_120d + 0.01, "avg_excess_return": top3_120d, "win_rate": 0.66},
                "top5": {"avg_return": 0.048, "avg_excess_return": 0.021, "win_rate": 0.62},
                "top10": {"avg_return": 0.041, "avg_excess_return": top10_120d, "win_rate": 0.58},
            },
        },
        "failure_attribution_summary": {
            "false_positive_cases": false_positive_cases,
            "missed_winner_cases": missed_winner_cases,
            "rank_too_low_cases": rank_too_low_cases,
            "risk_gate_blocked_but_later_strong_cases": risk_gate_blocked_but_later_strong_cases,
            "evidence_insufficient_cases": evidence_insufficient_cases,
            "regime_mismatch_cases": regime_mismatch_cases,
            "risk_control_failure_cases": risk_control_failure_cases,
            "benchmark_underperformance_cases": benchmark_underperformance_cases,
            "negative_absolute_return_cases": negative_absolute_return_cases,
            "source_risk_mismatch_cases": source_risk_mismatch_cases,
            "weak_source_signal_cases": weak_source_signal_cases,
            "weak_success_cases": weak_success_cases,
            "unclassified_failure_cases": unclassified_failure_cases,
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_failure_summary(path: Path, *, false_positive_cases: int, evidence_insufficient_cases: int = 1, regime_mismatch_cases: int = 1) -> None:
    path.write_text(
        json.dumps(
            {
                "false_positive_cases": false_positive_cases,
                "missed_winner_cases": 2,
                "rank_too_low_cases": 1,
                "risk_gate_blocked_but_later_strong_cases": 1,
                "evidence_insufficient_cases": evidence_insufficient_cases,
                "regime_mismatch_cases": regime_mismatch_cases,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_candidate_quality_diff_builds_current_vs_previous_comparison(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = tmp_path / "artifacts"

    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))
    monkeypatch.setenv("STOCK_ULTIMATE_ARTIFACTS_DIR", str(artifacts_dir))

    current_summary_path = exp_dir / "candidate_quality_summary.json"
    previous_summary_path = exp_dir / "previous_candidate_quality_summary.json"
    _write_summary(
        current_summary_path,
        evaluation_id="current-eval",
        top1_20d=0.018,
        top3_20d=0.012,
        top1_60d=0.025,
        top3_60d=0.019,
        top1_120d=0.031,
        top3_120d=0.024,
        false_positive_cases=2,
    )
    _write_summary(
        previous_summary_path,
        evaluation_id="prev-eval",
        top1_20d=0.011,
        top3_20d=0.008,
        top1_60d=0.016,
        top3_60d=0.011,
        top1_120d=0.020,
        top3_120d=0.015,
        top10_120d=0.013,
        false_positive_cases=3,
    )

    registry = CandidateQualityBaselineRegistry(baselines_dir=artifacts_dir / "candidate_quality_baselines")
    registry.register(summary_path=previous_summary_path, baseline_id="baseline-prev-eval")

    payload = build_candidate_quality_diff()

    assert payload["pass_or_fail"] == "passed"
    assert payload["previous_evaluation_id"] == "prev-eval"
    assert payload["bucket_window_deltas"]["60d"]["top3"]["avg_excess_return"] == 0.008
    assert payload["failure_summary_deltas"]["false_positive_cases"] == -1
    assert payload["failure_summary_deltas"]["risk_control_failure_cases"] == 0
    assert payload["sample_density_delta"]["120d"]["sample_total_delta"] == 0
    assert payload["failure_deterioration"]["has_deterioration"] is False
    assert payload["failure_deterioration"]["top_improved_categories"][0]["field"] == "false_positive_cases"
    assert payload["recommended_remediation_actions"] == []
    assert payload["iteration_schedule"] == []
    assert payload["improvement_gate"]["has_two_window_stable_advantage"] is True
    assert payload["improvement_gate"]["has_top1_or_top3_excess_advantage"] is True
    assert payload["improvement_gate"]["no_new_failure_degradation"] is True
    assert payload["improvement_gate"]["pass_or_fail"] == "passed"


def test_candidate_quality_diff_blocks_when_previous_baseline_or_failure_summary_is_missing(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = tmp_path / "artifacts"

    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))
    monkeypatch.setenv("STOCK_ULTIMATE_ARTIFACTS_DIR", str(artifacts_dir))

    current_summary_path = exp_dir / "candidate_quality_summary.json"
    _write_summary(
        current_summary_path,
        evaluation_id="current-eval",
        top1_20d=0.018,
        top3_20d=0.012,
        top1_60d=0.025,
        top3_60d=0.019,
        top1_120d=0.031,
        top3_120d=0.024,
        false_positive_cases=2,
        sample_density_60d=(2, 3, "blocked"),
        sample_density_120d=(4, 5, "blocked"),
    )

    payload = build_candidate_quality_diff()

    assert payload["pass_or_fail"] == "blocked"
    assert "previous_formal_baseline_missing" in payload["blocking_reasons"]
    assert "60d_sample_density_insufficient" in payload["blocking_reasons"]
    assert "120d_sample_density_insufficient" in payload["blocking_reasons"]
    assert "previous_failure_summary_missing:false_positive_cases" in payload["blocking_reasons"]
    assert payload["failure_deterioration"]["has_deterioration"] is False
    assert payload["improvement_gate"]["pass_or_fail"] == "blocked"


def test_candidate_quality_diff_explains_failure_deterioration_by_priority_field(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = tmp_path / "artifacts"

    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))
    monkeypatch.setenv("STOCK_ULTIMATE_ARTIFACTS_DIR", str(artifacts_dir))

    current_summary_path = exp_dir / "candidate_quality_summary.json"
    previous_summary_path = exp_dir / "previous_candidate_quality_summary.json"
    _write_summary(
        current_summary_path,
        evaluation_id="current-eval",
        top1_20d=0.018,
        top3_20d=0.012,
        top1_60d=0.025,
        top3_60d=0.019,
        top1_120d=0.031,
        top3_120d=0.024,
        false_positive_cases=3,
        risk_control_failure_cases=2,
    )
    _write_summary(
        previous_summary_path,
        evaluation_id="prev-eval",
        top1_20d=0.011,
        top3_20d=0.008,
        top1_60d=0.016,
        top3_60d=0.011,
        top1_120d=0.020,
        top3_120d=0.015,
        top10_120d=0.013,
        false_positive_cases=2,
        risk_control_failure_cases=1,
    )

    registry = CandidateQualityBaselineRegistry(baselines_dir=artifacts_dir / "candidate_quality_baselines")
    registry.register(summary_path=previous_summary_path, baseline_id="baseline-prev-eval")

    payload = build_candidate_quality_diff()

    assert payload["failure_deterioration"]["has_deterioration"] is True
    assert payload["failure_deterioration"]["top_deteriorated_categories"][0]["field"] == "risk_control_failure_cases"
    assert payload["failure_deterioration"]["top_deteriorated_categories"][1]["field"] == "false_positive_cases"
    assert payload["recommended_remediation_actions"][0] == "prioritize risk-control review items before ranking or timing adjustments"
    assert any("false-positive deterioration" in action for action in payload["recommended_remediation_actions"])
    assert payload["iteration_schedule"][0]["failure_field"] == "risk_control_failure_cases"
    assert payload["iteration_schedule"][0]["priority_band"] == "critical"
    assert payload["iteration_schedule"][1]["failure_field"] == "false_positive_cases"
    assert payload["iteration_schedule"][1]["priority_band"] == "high"


def test_build_candidate_quality_diff_cli_writes_artifact(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "build_candidate_quality_diff.py"
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = tmp_path / "artifacts"

    current_summary_path = exp_dir / "candidate_quality_summary.json"
    previous_summary_path = exp_dir / "previous_candidate_quality_summary.json"
    _write_summary(
        current_summary_path,
        evaluation_id="current-eval",
        top1_20d=0.018,
        top3_20d=0.012,
        top1_60d=0.025,
        top3_60d=0.019,
        top1_120d=0.031,
        top3_120d=0.024,
        false_positive_cases=2,
    )
    _write_summary(
        previous_summary_path,
        evaluation_id="prev-eval",
        top1_20d=0.011,
        top3_20d=0.008,
        top1_60d=0.016,
        top3_60d=0.011,
        top1_120d=0.020,
        top3_120d=0.015,
        top10_120d=0.013,
        false_positive_cases=3,
    )

    registry = CandidateQualityBaselineRegistry(baselines_dir=artifacts_dir / "candidate_quality_baselines")
    registry.register(summary_path=previous_summary_path, baseline_id="baseline-prev-eval")

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--exp-dir",
            str(exp_dir),
            "--artifacts-dir",
            str(artifacts_dir),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    assert payload["status"] == "passed"
    assert payload["candidate_quality_diff"]["previous_evaluation_id"] == "prev-eval"
    assert Path(payload["output_path"]).exists()
