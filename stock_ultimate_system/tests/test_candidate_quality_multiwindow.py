from __future__ import annotations

import json

from src.candidate_quality_multiwindow import (
    build_candidate_quality_multiwindow,
    write_candidate_quality_multiwindow_artifact,
)


def test_candidate_quality_multiwindow_writes_artifact_when_source_exists(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    (exp_dir / "candidate_quality_multiwindow_source.json").write_text(
        json.dumps(
            {
                "sample_totals": {"60d": 3, "120d": 5},
                "sample_density": {
                    "60d": {"sample_total": 3, "minimum_required": 3, "status": "passed"},
                    "120d": {"sample_total": 5, "minimum_required": 5, "status": "passed"},
                },
                "windows": {
                    "60d": {
                        "top1": {"avg_return": 0.05, "avg_excess_return": 0.02, "win_rate": 0.6},
                        "top3": {"avg_return": 0.04, "avg_excess_return": 0.018, "win_rate": 0.58},
                        "top5": {"avg_return": 0.035, "avg_excess_return": 0.014, "win_rate": 0.55},
                        "top10": {"avg_return": 0.03, "avg_excess_return": 0.011, "win_rate": 0.53},
                    },
                    "120d": {
                        "top1": {"avg_return": 0.08, "avg_excess_return": 0.03, "win_rate": 0.65},
                        "top3": {"avg_return": 0.07, "avg_excess_return": 0.027, "win_rate": 0.63},
                        "top5": {"avg_return": 0.06, "avg_excess_return": 0.023, "win_rate": 0.6},
                        "top10": {"avg_return": 0.05, "avg_excess_return": 0.018, "win_rate": 0.57},
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = build_candidate_quality_multiwindow()
    output_path = write_candidate_quality_multiwindow_artifact(payload)

    assert payload["status"] == "passed"
    assert payload["sample_density"]["60d"]["status"] == "passed"
    assert payload["windows"]["60d"]["top3"]["avg_return"] == 0.04
    assert payload["windows"]["120d"]["top10"]["avg_excess_return"] == 0.018
    assert output_path.endswith("candidate_quality_multiwindow_latest.json")


def test_candidate_quality_multiwindow_blocks_when_source_missing(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    payload = build_candidate_quality_multiwindow()

    assert payload["status"] == "blocked"
    assert "multiwindow_source_missing" in payload["blocking_reasons"]
    assert "60d_sample_density_insufficient" in payload["blocking_reasons"]
    assert "missing_60d_top1_avg_return" in payload["blocking_reasons"]
    assert "missing_120d_top10_avg_excess_return" in payload["blocking_reasons"]
