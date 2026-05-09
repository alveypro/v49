import json
import subprocess
import sys
from pathlib import Path

from src.candidate_quality_density_progress import (
    build_candidate_quality_density_progress,
    write_candidate_quality_density_progress_artifact,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_candidate_quality_density_progress_tracks_remaining_samples_and_dates(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    _write_json(
        exp_dir / "candidate_quality_summary.json",
        {
            "multiwindow_sample_density": {
                "60d": {"sample_total": 3, "minimum_required": 3, "status": "passed"},
                "120d": {"sample_total": 4, "minimum_required": 5, "status": "blocked"},
            }
        },
    )
    _write_json(exp_dir / "candidate_quality_multiwindow_source.json", {"source_mode": "validation_history"})
    for stamp in ("20260110_090000", "20260210_090000", "20260310_090000", "20260410_090000"):
        _write_json(exp_dir / f"candidates_basket_validation_{stamp}.json", {"summary": {}})

    payload = build_candidate_quality_density_progress()
    output_path = write_candidate_quality_density_progress_artifact(payload)

    assert payload["status"] == "blocked"
    assert payload["progress"]["120d"]["remaining_samples_needed"] == 1
    assert payload["progress"]["120d"]["progress_ratio"] == 0.8
    assert payload["progress"]["120d"]["latest_validation_date"] == "2026-04-10"
    assert payload["progress"]["120d"]["earliest_validation_date"] == "2026-01-10"
    assert output_path.endswith("candidate_quality_density_progress.json")


def test_build_candidate_quality_density_progress_cli_outputs_json(tmp_path, monkeypatch):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "build_candidate_quality_density_progress.py"
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    _write_json(
        exp_dir / "candidate_quality_summary.json",
        {
            "multiwindow_sample_density": {
                "60d": {"sample_total": 3, "minimum_required": 3, "status": "passed"},
                "120d": {"sample_total": 5, "minimum_required": 5, "status": "passed"},
            }
        },
    )
    _write_json(exp_dir / "candidate_quality_multiwindow_source.json", {"source_mode": "validation_history"})

    completed = subprocess.run(
        [sys.executable, str(script), "--exp-dir", str(exp_dir), "--json"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "passed"
    assert Path(payload["output_path"]).exists()
