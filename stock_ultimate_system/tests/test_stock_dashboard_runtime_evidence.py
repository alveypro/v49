import json
from pathlib import Path

from src.stock_dashboard_runtime_evidence import (
    DAILY_CLOSURE_LATEST_FILENAME,
    OBSERVATION_WAIT_STATUS_FILENAME,
    load_dashboard_runtime_evidence,
    load_json_object,
)


def test_load_json_object_returns_dict_only(tmp_path: Path):
    valid = tmp_path / "valid.json"
    valid.write_text(json.dumps({"status": "ok"}), encoding="utf-8")
    array_payload = tmp_path / "array.json"
    array_payload.write_text("[]", encoding="utf-8")
    invalid = tmp_path / "invalid.json"
    invalid.write_text("{", encoding="utf-8")

    assert load_json_object(valid) == {"status": "ok"}
    assert load_json_object(array_payload) == {}
    assert load_json_object(invalid) == {}
    assert load_json_object(tmp_path / "missing.json") == {}


def test_load_dashboard_runtime_evidence_reads_observation_and_closure(tmp_path: Path):
    artifacts_root = tmp_path / "artifacts"
    exp_dir = tmp_path / "data" / "experiments"
    artifacts_root.mkdir()
    exp_dir.mkdir(parents=True)
    (artifacts_root / OBSERVATION_WAIT_STATUS_FILENAME).write_text(
        json.dumps({"current_date": "2026-05-13"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (exp_dir / DAILY_CLOSURE_LATEST_FILENAME).write_text(
        json.dumps({"status": "closed_success"}, ensure_ascii=False),
        encoding="utf-8",
    )

    evidence = load_dashboard_runtime_evidence(artifacts_root=artifacts_root, exp_dir=exp_dir)

    assert evidence.observation_wait_status == {"current_date": "2026-05-13"}
    assert evidence.daily_closure_latest == {"status": "closed_success"}


def test_load_dashboard_runtime_evidence_fails_empty_on_missing_files(tmp_path: Path):
    evidence = load_dashboard_runtime_evidence(
        artifacts_root=tmp_path / "missing_artifacts",
        exp_dir=tmp_path / "missing_exp",
    )

    assert evidence.observation_wait_status == {}
    assert evidence.daily_closure_latest == {}
