from __future__ import annotations

import json
from pathlib import Path

from openclaw.research.v8_controlled_experiment import (
    V8ControlledExperimentConfig,
    run_v8_controlled_experiment,
)


def test_v8_controlled_experiment_keeps_eligible_artifact_out_of_rejected_ledger(monkeypatch, tmp_path: Path):
    sweep_json = tmp_path / "backtest_sweep_v8.json"
    sweep_json.write_text('{"v8_suppression_diagnostics":true}', encoding="utf-8")

    def fake_sweep(cfg):
        assert cfg.strategy == "v8"
        assert list(cfg.max_stop_loss_pcts) == [0.06, 0.08]
        return {
            "run_id": "sweep_v8_test",
            "status": "success",
            "artifacts": {"json": str(sweep_json), "markdown": str(tmp_path / "sweep.md")},
            "backtest_credibility": {"point_in_time_data": True},
            "strategy_backtest_diagnostics": {
                "eligible_for_formal_ranking": True,
                "credible_evidence_present": True,
                "quality_floor_passed": True,
            },
        }

    def fake_stage_audit(**kwargs):
        return {"passed": True, "artifacts": {"json": str(tmp_path / "audit.json"), "markdown": str(tmp_path / "audit.md")}}

    monkeypatch.setattr("openclaw.research.v8_controlled_experiment.run_param_sweep", fake_sweep)
    monkeypatch.setattr("openclaw.research.v8_controlled_experiment.run_stage_audit", fake_stage_audit)

    ledger = tmp_path / "rejected.jsonl"
    payload = run_v8_controlled_experiment(
        V8ControlledExperimentConfig(
            output_dir=tmp_path,
            db_path=str(tmp_path / "test.db"),
            rejected_ledger=str(ledger),
            max_stop_loss_pcts=[0.06, 0.08],
            max_take_profit_pcts=[None],
        )
    )

    assert payload["status"] == "candidate_discussion_only"
    assert payload["rejected_entry"] is None
    assert not ledger.exists()
    assert Path(payload["artifacts"]["json"]).exists()


def test_v8_controlled_experiment_records_rejected_artifact_and_runs_audit(monkeypatch, tmp_path: Path):
    sweep_json = tmp_path / "backtest_sweep_v8_failed.json"
    sweep_json.write_text('{"strategy":"v8"}', encoding="utf-8")
    audit_calls = []

    def fake_sweep(cfg):
        return {
            "run_id": "sweep_v8_failed",
            "status": "failed",
            "artifacts": {"json": str(sweep_json)},
            "backtest_credibility": {},
            "strategy_backtest_diagnostics": {
                "eligible_for_formal_ranking": False,
                "credible_evidence_present": False,
                "quality_floor_passed": False,
            },
        }

    def fake_stage_audit(**kwargs):
        audit_calls.append(kwargs)
        return {"passed": True, "artifacts": {"json": str(tmp_path / "audit.json"), "markdown": str(tmp_path / "audit.md")}}

    monkeypatch.setattr("openclaw.research.v8_controlled_experiment.run_param_sweep", fake_sweep)
    monkeypatch.setattr("openclaw.research.v8_controlled_experiment.run_stage_audit", fake_stage_audit)

    ledger = tmp_path / "rejected.jsonl"
    payload = run_v8_controlled_experiment(
        V8ControlledExperimentConfig(
            output_dir=tmp_path,
            db_path=str(tmp_path / "test.db"),
            rejected_ledger=str(ledger),
        )
    )

    assert payload["status"] == "rejected"
    assert payload["rejected_entry"]["reason"]
    assert "missing_v8_suppression_diagnostics" in payload["rejected_entry"]["reason"]
    assert audit_calls[0]["rejected_artifacts_path"] == str(ledger)
    loaded = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()]
    assert loaded[0]["source_run_id"] == "sweep_v8_failed"
