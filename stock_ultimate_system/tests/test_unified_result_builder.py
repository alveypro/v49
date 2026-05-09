import json
from pathlib import Path

from src.artifact_registry import ArtifactRegistry
from src.current_result_pointer import CurrentResultPointerStore
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry
from src.unified_result_builder import (
    build_primary_result,
    build_primary_result_api_payload,
    resolve_result_lifecycle_stage,
)


def test_resolve_result_lifecycle_stage_prefers_unique_primary_stage():
    assert resolve_result_lifecycle_stage(
        research_status="completed",
        candidate_status="candidate",
        audit_status=None,
        execution_status=None,
    ) == "L2"
    assert resolve_result_lifecycle_stage(
        research_status="completed",
        candidate_status="candidate",
        audit_status="in_review",
        execution_status=None,
    ) == "L3"
    assert resolve_result_lifecycle_stage(
        research_status="completed",
        candidate_status="candidate",
        audit_status="passed",
        execution_status="running",
    ) == "L4"


def test_build_primary_result_degrades_without_guessing_missing_governance_fields(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text(
        '{"state":"completed","ended_at":"2026-04-12T01:11:19"}',
        encoding="utf-8",
    )
    (exp_dir / "buylist_latest.json").write_text(
        '{"items":[{"ts_code":"000001.SZ","stock_name":"平安银行"}]}',
        encoding="utf-8",
    )
    (exp_dir / "governance_audit_latest.json").write_text(
        '{"summary":{"overall_status":"pass"}}',
        encoding="utf-8",
    )
    (exp_dir / "t1_execution_checklist_latest.json").write_text(
        '{"summary":{"overall_status":"pass"}}',
        encoding="utf-8",
    )
    (exp_dir / "t12_rollback_drill_latest.json").write_text(
        '{"triggered": false}',
        encoding="utf-8",
    )

    result = build_primary_result(exp_dir)

    assert result.result_lifecycle_stage == "L2"
    assert result.result_type == "candidate"
    assert result.candidate_status == "shortlisted"
    assert result.audit_status is None
    assert result.execution_status is None
    assert result.rollback_status is None
    assert result.terminal_outcome is None
    assert "审核状态暂缺" in (result.data_sync_note or "")


def test_terminal_outcome_does_not_change_primary_stage():
    stage = resolve_result_lifecycle_stage(
        research_status="completed",
        candidate_status="candidate",
        audit_status=None,
        execution_status=None,
    )
    assert stage == "L2"
    terminal_outcome = "expired"
    assert terminal_outcome == "expired"
    assert stage == "L2"


def test_builder_explanation_fields_degrade_without_guessing(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")

    result = build_primary_result(exp_dir)
    assert result.history_summary is not None
    assert "历史候选记录" not in (result.history_summary or "")
    assert "候选记录 shortlisted" in (result.history_summary or "")
    assert result.history_source_file == "buylist_latest.json"
    assert result.history_generation_mode == "degraded"
    assert result.disabled_reason is None
    assert result.invalid_reason is None


def test_result_type_cannot_infer_result_lifecycle_stage(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")

    payload = build_primary_result_api_payload(exp_dir)

    assert payload["result_lifecycle_stage"] == "L2"
    assert payload["result_type"] == "candidate"
    assert payload["result_type"] != payload["result_lifecycle_stage"]


def test_null_values_do_not_trigger_inference(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text('{"summary":{"overall_status":"pass"}}', encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text('{"summary":{"overall_status":"pass"}}', encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")

    payload = build_primary_result_api_payload(exp_dir)

    assert payload["schema_version"] == "primary_result_v1"
    assert payload["result_lifecycle_stage"] == "L2"
    assert payload["audit_status"] is None
    assert payload["execution_status"] is None
    assert payload["rollback_status"] is None
    assert payload["terminal_outcome"] is None


def test_build_primary_result_fail_closes_when_pointer_required_but_missing(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )

    result = build_primary_result(exp_dir, require_current_pointer=True)

    assert result.result_id == "primary:unavailable"
    assert result.disabled_reason is not None
    assert "current_result_pointer" in result.disabled_reason
    assert result.history_generation_mode == "blocked"
    assert "fail closed" in (result.data_sync_note or "")


def test_build_primary_result_prefers_pointer_first_identity_when_available(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n"
        "000002.SZ,万科A,地产,watch,medium,120\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")

    artifacts_dir = tmp_path / "artifacts"
    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )

    result = build_primary_result(exp_dir, candidate_index=1, require_current_pointer=True)

    assert result.result_id == "primary:000001.SZ"
    assert result.ts_code == "000001.SZ"
    assert result.stock_name == "平安银行"
    assert result.result_lifecycle_stage == "L3"


def test_build_primary_result_prefers_pointer_artifact_chain_over_global_latest_alias(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "primary_result_audit_latest.json").write_text(
        '{"result_id":"primary:999999.SZ","ts_code":"999999.SZ","audit_status":"failed"}',
        encoding="utf-8",
    )

    chain_dir = tmp_path / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)
    audit_path = chain_dir / "primary_result_audit.json"
    audit_path.write_text(
        '{"result_id":"primary:000001.SZ","ts_code":"000001.SZ","audit_status":"passed"}',
        encoding="utf-8",
    )

    artifacts_dir = tmp_path / "artifacts"
    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ArtifactRegistry(artifacts_dir / "artifact_registry.jsonl").register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=audit_path,
        producer="test",
        artifact_id="run-001:audit",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L3",
        artifact_ids=["run-001:audit"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["run-001:audit"],
        as_of_date="2026-04-28",
    )

    result = build_primary_result(exp_dir, require_current_pointer=True)

    assert result.audit_status == "passed"
    assert result.run_id == "run-001"
    assert result.lifecycle_id == "lifecycle-001"
    assert result.artifact_ids == ["run-001:audit"]


def test_build_primary_result_fail_closes_when_pointer_artifact_chain_is_broken(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )

    artifacts_dir = tmp_path / "artifacts"
    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    (artifacts_dir / "artifact_registry.jsonl").write_text("", encoding="utf-8")
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L3",
        artifact_ids=["run-001:audit"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["run-001:audit"],
        as_of_date="2026-04-28",
    )

    result = build_primary_result(exp_dir, require_current_pointer=True)

    assert result.history_generation_mode == "blocked"
    assert "artifact_registry" in (result.disabled_reason or "")


def test_build_primary_result_fail_closes_when_pointer_snapshot_path_points_outside_managed_chain(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )

    artifacts_dir = tmp_path / "artifacts"
    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L3",
        artifact_ids=["run-001:audit"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["run-001:audit"],
        as_of_date="2026-04-28",
    )
    current_path = artifacts_dir / "current_result_pointer" / "current.json"
    current_pointer = json.loads(current_path.read_text(encoding="utf-8"))
    current_pointer["snapshot_path"] = "/private/var/folders/db/pytest-of-mac/pytest-999/current_result_pointer/history/pointer-001.json"
    current_path.write_text(json.dumps(current_pointer, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    result = build_primary_result(exp_dir, require_current_pointer=True)

    assert result.history_generation_mode == "blocked"
    assert "snapshot_path" in (result.disabled_reason or "")


def test_build_primary_result_fail_closes_when_l4_chain_would_fallback_to_same_result_old_run_latest(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "primary_result_execution_latest.json").write_text(
        json.dumps(
            {
                "result_id": "primary:000001.SZ",
                "run_id": "run-old",
                "ts_code": "000001.SZ",
                "execution_status": "ready",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    artifacts_dir = tmp_path / "artifacts"
    chain_dir = tmp_path / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)
    audit_path = chain_dir / "primary_result_audit.json"
    audit_path.write_text(
        '{"result_id":"primary:000001.SZ","run_id":"run-001","ts_code":"000001.SZ","audit_status":"passed"}',
        encoding="utf-8",
    )

    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ArtifactRegistry(artifacts_dir / "artifact_registry.jsonl").register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=audit_path,
        producer="test",
        artifact_id="run-001:audit",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L4",
        artifact_ids=["run-001:audit"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["run-001:audit"],
        as_of_date="2026-04-28",
    )

    result = build_primary_result(exp_dir, require_current_pointer=True)

    assert result.history_generation_mode == "blocked"
    assert "lifecycle artifact" in (result.disabled_reason or "")
    assert "primary_result_execution" in (result.data_sync_note or "")


def test_build_primary_result_fail_closes_when_same_run_artifact_registry_entry_has_wrong_type(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )

    artifacts_dir = tmp_path / "artifacts"
    chain_dir = tmp_path / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)
    audit_path = chain_dir / "primary_result_audit.json"
    audit_path.write_text(
        '{"result_id":"primary:000001.SZ","run_id":"run-001","ts_code":"000001.SZ","audit_status":"passed"}',
        encoding="utf-8",
    )
    execution_path = chain_dir / "primary_result_execution.json"
    execution_path.write_text(
        '{"result_id":"primary:000001.SZ","run_id":"run-001","ts_code":"000001.SZ","execution_status":"ready"}',
        encoding="utf-8",
    )
    rollback_path = chain_dir / "primary_result_rollback.json"
    rollback_path.write_text(
        '{"result_id":"primary:000001.SZ","run_id":"run-001","ts_code":"000001.SZ","rollback_status":"not_required"}',
        encoding="utf-8",
    )
    observation_path = chain_dir / "primary_result_observation.json"
    observation_path.write_text(
        '{"result_id":"primary:000001.SZ","run_id":"run-001","ts_code":"000001.SZ","observation_status":"observing"}',
        encoding="utf-8",
    )

    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    registry = ArtifactRegistry(artifacts_dir / "artifact_registry.jsonl")
    registry.register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=audit_path,
        producer="test",
        artifact_id="run-001:audit",
        result_id="primary:000001.SZ",
    )
    registry.register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=execution_path,
        producer="test",
        artifact_id="run-001:execution-wrong-type",
        result_id="primary:000001.SZ",
    )
    registry.register_artifact(
        artifact_type="primary_result_rollback",
        run_id="run-001",
        path=rollback_path,
        producer="test",
        artifact_id="run-001:rollback",
        result_id="primary:000001.SZ",
    )
    registry.register_artifact(
        artifact_type="primary_result_observation",
        run_id="run-001",
        path=observation_path,
        producer="test",
        artifact_id="run-001:observation",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L4",
        artifact_ids=["run-001:audit", "run-001:execution-wrong-type", "run-001:rollback", "run-001:observation"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["run-001:audit", "run-001:execution-wrong-type", "run-001:rollback", "run-001:observation"],
        as_of_date="2026-04-28",
    )

    result = build_primary_result(exp_dir, require_current_pointer=True)

    assert result.history_generation_mode == "blocked"
    assert "lifecycle artifact" in (result.disabled_reason or "")
    assert "primary_result_execution" in (result.data_sync_note or "")


def test_build_primary_result_fail_closes_when_pointer_chain_references_old_run_artifact_of_same_result(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    artifacts_dir = tmp_path / "artifacts"

    registry = ArtifactRegistry(artifacts_dir / "artifact_registry.jsonl")
    audit_path = exp_dir / "primary_result_audit_latest.json"
    execution_path = exp_dir / "primary_result_execution_latest.json"
    rollback_path = exp_dir / "primary_result_rollback_latest.json"
    observation_path = exp_dir / "primary_result_observation_latest.json"
    for path in (audit_path, execution_path, rollback_path, observation_path):
        path.write_text("{}", encoding="utf-8")

    registry.register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-current",
        path=audit_path,
        producer="test",
        artifact_id="artifact:audit-current",
        result_id="primary:000001.SZ",
    )
    registry.register_artifact(
        artifact_type="primary_result_execution",
        run_id="run-old",
        path=execution_path,
        producer="test",
        artifact_id="artifact:execution-old",
        result_id="primary:000001.SZ",
    )
    registry.register_artifact(
        artifact_type="primary_result_rollback",
        run_id="run-current",
        path=rollback_path,
        producer="test",
        artifact_id="artifact:rollback-current",
        result_id="primary:000001.SZ",
    )
    registry.register_artifact(
        artifact_type="primary_result_observation",
        run_id="run-current",
        path=observation_path,
        producer="test",
        artifact_id="artifact:observation-current",
        result_id="primary:000001.SZ",
    )

    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-current",
        result_id="primary:000001.SZ",
        run_id="run-current",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L4",
        artifact_ids=[
            "artifact:audit-current",
            "artifact:execution-old",
            "artifact:rollback-current",
            "artifact:observation-current",
        ],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-current",
        result_id="primary:000001.SZ",
        run_id="run-current",
        lifecycle_id="life-current",
        artifact_ids=[
            "artifact:audit-current",
            "artifact:execution-old",
            "artifact:rollback-current",
            "artifact:observation-current",
        ],
        as_of_date="2026-04-15",
    )

    result = build_primary_result(exp_dir=exp_dir, candidate_index=0, require_current_pointer=True)

    assert result.history_generation_mode == "blocked"
    assert "artifact_registry 与 current_result_pointer 不一致" in (result.disabled_reason or "")
    assert "artifact run_id mismatch: artifact:execution-old" in (result.data_sync_note or "")
