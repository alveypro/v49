from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from src.candidate_quality_validation_history_archive import build_candidate_quality_validation_history_archive


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_candidate_quality_validation_history_archive_writes_dated_snapshot(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    latest_payload = {
        "summary": {"rebalance_dates": 8, "avg_basket_return_5d": 0.018},
        "variants": {"top3": {"avg_return_5d": 0.02}},
    }
    _write_json(exp_dir / "candidates_basket_validation_latest.json", latest_payload)

    exit_code, payload = build_candidate_quality_validation_history_archive()

    assert exit_code == 0
    assert payload["status"] == "passed"
    output_path = Path(str(payload["output_path"]))
    assert output_path.exists()
    assert output_path.name.startswith("candidates_basket_validation_")
    assert json.loads(output_path.read_text(encoding="utf-8")) == latest_payload


def test_candidate_quality_validation_history_archive_blocks_placeholder_latest(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    _write_json(exp_dir / "candidates_basket_validation_latest.json", {"summary": {"rebalance_dates": 0}})

    _, payload = build_candidate_quality_validation_history_archive()

    assert payload["status"] == "blocked"
    assert "candidate_quality_validation_rebalance_dates_missing_or_zero" in payload["blocking_reasons"]


def test_archive_candidate_quality_validation_history_cli_outputs_json(tmp_path, monkeypatch):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "archive_candidate_quality_validation_history.py"
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("STOCK_ULTIMATE_EXPERIMENTS_DIR", str(exp_dir))

    _write_json(exp_dir / "candidates_basket_validation_latest.json", {"summary": {"rebalance_dates": 5}})

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
