from __future__ import annotations

from pathlib import Path

from openclaw.services.rejected_backtest_artifact_ledger_service import (
    append_rejected_backtest_artifact,
    load_rejected_backtest_artifacts,
    merge_rejected_backtest_artifacts,
)


def test_append_and_load_rejected_backtest_artifact_jsonl(tmp_path: Path):
    artifact = tmp_path / "backtest_sweep_v8_failed.json"
    artifact.write_text('{"best":{"status":"success"}}', encoding="utf-8")
    ledger = tmp_path / "rejected.jsonl"

    entry = append_rejected_backtest_artifact(
        str(ledger),
        artifact_path=str(artifact),
        strategy="V8",
        reason="eligible_for_formal_ranking_false",
        source_run_id="run_backtest_v8_test",
        operator_name="audit",
    )
    loaded = load_rejected_backtest_artifacts(str(ledger))

    assert entry["strategy"] == "v8"
    assert entry["artifact_sha256"]
    assert loaded == [entry]


def test_merge_rejected_backtest_artifacts_dedupes_latest():
    old = {
        "artifact_path": "logs/openclaw/backtest_sweep_v5_failed.json",
        "strategy": "v5",
        "reason": "quality_floor_failed",
        "rejected_at": "2026-05-01 10:00:00",
    }
    new = {
        "artifact_path": "logs/openclaw/backtest_sweep_v5_failed.json",
        "strategy": "v5",
        "reason": "eligible_for_formal_ranking_false",
        "rejected_at": "2026-05-02 10:00:00",
    }

    merged = merge_rejected_backtest_artifacts([old], [new])

    assert len(merged) == 1
    assert merged[0]["reason"] == "eligible_for_formal_ranking_false"


def test_load_rejected_backtest_artifact_requires_reason(tmp_path: Path):
    ledger = tmp_path / "bad.jsonl"
    ledger.write_text('{"artifact_path":"x.json","strategy":"v8"}\n', encoding="utf-8")

    try:
        load_rejected_backtest_artifacts(str(ledger))
    except ValueError as exc:
        message = str(exc)
    else:
        message = ""

    assert message == "missing_reason"


def test_load_rejected_backtest_artifacts_missing_file_returns_empty(tmp_path: Path):
    assert load_rejected_backtest_artifacts(str(tmp_path / "missing.jsonl")) == []
