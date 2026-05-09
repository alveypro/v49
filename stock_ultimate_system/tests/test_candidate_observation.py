import json
import subprocess
import sys
from pathlib import Path

from src.candidate_quality.observation import (
    append_candidate_observation_ledger,
    build_candidate_observation_snapshot,
    freeze_candidate_observation_snapshot,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    candidates = tmp_path / "candidates_top_latest.csv"
    candidates.write_text(
        "rank,ts_code,stock_name,industry,signal,final_score,reason,data_quality_level,data_quality_score\n"
        "1,000001.SZ,平安银行,银行,buy,99.5,测试理由,pass,98.0\n",
        encoding="utf-8",
    )
    lineage = tmp_path / "candidate_lineage_latest.json"
    lineage.write_text(
        json.dumps(
            {
                "schema_version": "candidate_lineage.v1",
                "status": "passed",
                "run_id": "candidate-run-001",
                "data_as_of": "20260506",
                "candidates": [{"ts_code": "000001.SZ", "lineage_hash": "abc123"}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    gate = tmp_path / "candidate_data_quality_gate_latest.json"
    gate.write_text(
        json.dumps(
            {
                "schema_version": "candidate_data_quality_gate.v1",
                "status": "passed",
                "blocked_count": 0,
                "candidates": [{"ts_code": "000001.SZ", "quality_level": "pass", "quality_score": 98.0}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    backtest = tmp_path / "realistic_backtest_latest.json"
    backtest.write_text(
        json.dumps(
            {
                "schema_version": "candidate_realistic_backtest.v1",
                "status": "failed",
                "candidates": [
                    {
                        "ts_code": "000001.SZ",
                        "status": "blocked",
                        "blocking_reasons": ["insufficient_future_trade_dates"],
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return {"candidates": candidates, "lineage": lineage, "gate": gate, "backtest": backtest}


def test_candidate_observation_snapshot_freeze_is_immutable(tmp_path):
    paths = _write_sources(tmp_path)
    snapshot = build_candidate_observation_snapshot(
        candidates_csv_path=paths["candidates"],
        lineage_path=paths["lineage"],
        data_quality_gate_path=paths["gate"],
        realistic_backtest_path=paths["backtest"],
    )

    snapshot_path, frozen = freeze_candidate_observation_snapshot(snapshot, output_dir=tmp_path)
    original_text = Path(snapshot_path).read_text(encoding="utf-8")
    changed = {**snapshot, "items": []}
    snapshot_path_2, frozen_2 = freeze_candidate_observation_snapshot(changed, output_dir=tmp_path)

    assert snapshot_path_2 == snapshot_path
    assert frozen["freeze_status"] == "frozen"
    assert frozen_2["freeze_status"] == "already_frozen"
    assert Path(snapshot_path).read_text(encoding="utf-8") == original_text
    assert (tmp_path / "candidate_observation_snapshot_latest.json").exists()


def test_candidate_observation_ledger_appends_and_skips_duplicates(tmp_path):
    paths = _write_sources(tmp_path)
    snapshot = build_candidate_observation_snapshot(
        candidates_csv_path=paths["candidates"],
        lineage_path=paths["lineage"],
        data_quality_gate_path=paths["gate"],
        realistic_backtest_path=paths["backtest"],
    )
    _snapshot_path, frozen = freeze_candidate_observation_snapshot(snapshot, output_dir=tmp_path)
    ledger = tmp_path / "candidate_observation_ledger.jsonl"

    first = append_candidate_observation_ledger(frozen, ledger_path=ledger)
    second = append_candidate_observation_ledger(frozen, ledger_path=ledger)

    assert first["appended_count"] == 1
    assert second["appended_count"] == 0
    assert second["skipped_duplicate_count"] == 1
    lines = ledger.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["observation_id"] == "20260506:000001.SZ"
    assert entry["hit_status"] == "pending"


def test_freeze_candidate_observation_cli(tmp_path):
    paths = _write_sources(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "freeze_candidate_observation.py"),
            "--exp-dir",
            str(tmp_path),
            "--output-dir",
            str(tmp_path),
            "--ledger-jsonl",
            str(tmp_path / "candidate_observation_ledger.jsonl"),
            "--candidates-csv",
            str(paths["candidates"]),
            "--lineage",
            str(paths["lineage"]),
            "--data-quality-gate",
            str(paths["gate"]),
            "--realistic-backtest",
            str(paths["backtest"]),
            "--json",
        ],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["status"] == "frozen"
    assert payload["ledger_appended_count"] == 1
    assert (tmp_path / "candidate_observation_snapshot_20260506.json").exists()
