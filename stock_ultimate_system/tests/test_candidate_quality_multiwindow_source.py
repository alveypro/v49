from __future__ import annotations

import json

from src.candidate_quality_multiwindow_source import (
    build_candidate_quality_multiwindow_source,
    write_candidate_quality_multiwindow_source_artifact,
)


def _write_validation(
    exp_dir,
    *,
    stamp: str,
    top1: float,
    top5: float,
    top5_excess: float,
    top3: float | None,
    top10: float | None,
) -> None:
    payload = {
        "summary": {
            "rebalance_dates": 1,
            "avg_basket_return_5d": top5,
            "basket_win_rate_5d": 0.56,
            "avg_universe_return_5d": 0.01,
            "avg_excess_return_5d": top5_excess,
            "avg_top1_return_5d": top1,
        },
        "records": [],
    }
    variants = {
        "top1": {"avg_return_5d": top1, "avg_excess_return_5d": 0.02, "win_rate_5d": 0.61},
    }
    if top3 is not None:
        variants["top3"] = {"avg_return_5d": top3, "avg_excess_return_5d": 0.018, "win_rate_5d": 0.59}
    if top10 is not None:
        variants["top10"] = {"avg_return_5d": top10, "avg_excess_return_5d": 0.01, "win_rate_5d": 0.53}
    if variants:
        payload["variants"] = variants

    (exp_dir / f"candidates_basket_validation_{stamp}.json").write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_candidate_quality_multiwindow_source_writes_source_artifact(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    _write_validation(exp_dir, stamp="20260401_090000", top1=0.05, top5=0.035, top5_excess=0.014, top3=0.041, top10=0.028)
    _write_validation(exp_dir, stamp="20260415_090000", top1=0.06, top5=0.042, top5_excess=0.018, top3=0.047, top10=0.032)
    _write_validation(exp_dir, stamp="20260430_090000", top1=0.07, top5=0.048, top5_excess=0.021, top3=0.055, top10=0.038)
    _write_validation(exp_dir, stamp="20260301_090000", top1=0.045, top5=0.031, top5_excess=0.012, top3=0.038, top10=0.025)
    _write_validation(exp_dir, stamp="20260210_090000", top1=0.041, top5=0.029, top5_excess=0.011, top3=0.035, top10=0.023)

    payload = build_candidate_quality_multiwindow_source()
    output_path = write_candidate_quality_multiwindow_source_artifact(payload)

    assert payload["status"] == "passed"
    assert payload["source_mode"] == "validation_history"
    assert payload["sample_totals"]["60d"] == 3
    assert payload["sample_totals"]["120d"] == 5
    assert payload["sample_density"]["60d"]["status"] == "passed"
    assert payload["sample_density"]["120d"]["status"] == "passed"
    assert payload["windows"]["60d"]["top3"]["avg_return"] == 0.047667
    assert payload["windows"]["120d"]["top10"]["avg_return"] == 0.0292
    assert output_path.endswith("candidate_quality_multiwindow_source.json")


def test_candidate_quality_multiwindow_source_blocks_when_window_inputs_are_missing(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    _write_validation(exp_dir, stamp="20260430_090000", top1=0.07, top5=0.048, top5_excess=0.021, top3=None, top10=None)

    payload = build_candidate_quality_multiwindow_source()

    assert payload["status"] == "blocked"
    assert payload["source_mode"] == "validation_history"
    assert payload["sample_density"]["60d"]["status"] == "blocked"
    assert "60d_sample_density_insufficient" in payload["blocking_reasons"]
    assert "missing_60d_top3_avg_return" in payload["blocking_reasons"]
    assert "missing_120d_top10_avg_excess_return" in payload["blocking_reasons"]


def test_candidate_quality_multiwindow_source_deduplicates_multiple_validation_runs_on_same_day(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    _write_validation(exp_dir, stamp="20260430_090000", top1=0.05, top5=0.035, top5_excess=0.014, top3=0.041, top10=0.028)
    _write_validation(exp_dir, stamp="20260430_160000", top1=0.08, top5=0.050, top5_excess=0.020, top3=0.058, top10=0.040)
    _write_validation(exp_dir, stamp="20260415_090000", top1=0.06, top5=0.042, top5_excess=0.018, top3=0.047, top10=0.032)
    _write_validation(exp_dir, stamp="20260401_090000", top1=0.07, top5=0.048, top5_excess=0.021, top3=0.055, top10=0.038)
    _write_validation(exp_dir, stamp="20260301_090000", top1=0.045, top5=0.031, top5_excess=0.012, top3=0.038, top10=0.025)
    _write_validation(exp_dir, stamp="20260210_090000", top1=0.041, top5=0.029, top5_excess=0.011, top3=0.035, top10=0.023)

    payload = build_candidate_quality_multiwindow_source()

    assert payload["sample_totals"]["60d"] == 3
    assert payload["sample_totals"]["120d"] == 5
    assert payload["windows"]["60d"]["top1"]["avg_return"] == 0.07
