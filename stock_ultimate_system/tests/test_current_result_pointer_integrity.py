import json

from scripts.check_current_result_pointer_integrity import check_current_result_pointer_integrity
from src.artifact_registry import ArtifactRegistry
from src.current_result_pointer import CurrentResultPointerStore
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry


def test_current_result_pointer_integrity_passes_on_consistent_chain(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a", "artifact:b"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a", "artifact:b"],
        as_of_date="2026-04-28",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
    )

    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["problems"] == []


def test_current_result_pointer_integrity_fails_on_missing_run_registry_entry(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"

    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-missing",
        ts_code="000001.SZ",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-missing",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert any("run registry entry missing" in problem for problem in payload["problems"])


def test_current_result_pointer_integrity_fails_on_missing_artifact_registry_entry(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    artifact_registry_path.write_text("", encoding="utf-8")
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert any("artifact registry entry missing" in problem for problem in payload["problems"])


def test_current_result_pointer_integrity_fails_on_pytest_derived_snapshot_path(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )
    current_path = pointer_dir / "current.json"
    current_payload = json.loads(current_path.read_text(encoding="utf-8"))
    current_payload["snapshot_path"] = "/private/var/folders/db/pytest-of-mac/pytest-999/current_result_pointer/history/pointer-001.json"
    current_path.write_text(json.dumps(current_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert any("temporary or pytest-derived path" in problem for problem in payload["problems"])


def test_current_result_pointer_integrity_fails_on_tampered_result_registry_current_history_entry(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="test",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )

    tampered_history_path = results_dir / "history" / "result-record-001.json"
    tampered_payload = json.loads(tampered_history_path.read_text(encoding="utf-8"))
    tampered_payload["record_id"] = "result-record-tampered"
    tampered_payload["run_id"] = "run-old"
    tampered_history_path.write_text(json.dumps(tampered_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "result registry current pointer record_id does not match history entry" in payload["problems"]
    assert "result registry current pointer run_id does not match current result pointer" in payload["problems"]


def test_current_result_pointer_integrity_fails_on_tampered_run_registry_current_history_entry(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="test",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )

    tampered_history_path = runs_dir / "history" / "run-001.json"
    tampered_payload = json.loads(tampered_history_path.read_text(encoding="utf-8"))
    tampered_payload["run_id"] = "run-old"
    tampered_payload["run_type"] = "legacy_research"
    tampered_history_path.write_text(json.dumps(tampered_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "run registry current pointer run_id does not match history entry" in payload["problems"]
    assert "run registry current pointer run_type does not match history entry" in payload["problems"]
    assert "run registry current pointer history entry does not match current result pointer run_id" in payload["problems"]


def test_current_result_pointer_integrity_fails_on_registered_artifact_sha256_drift(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="test",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )
    artifact_path.write_text('{"ok": false, "tampered": true}\n', encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "artifact registry sha256 mismatch for artifact_id=artifact:a" in payload["problems"]


def test_current_result_pointer_integrity_fails_on_wrong_artifact_order_for_l4_stage(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    registry = ArtifactRegistry(artifact_registry_path)
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", chain_dir / "lifecycle.json"),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        path.write_text(
            json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-001", "ts_code": "000001.SZ"}, ensure_ascii=False),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-001",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    wrong_order = [
        "artifact:audit",
        "artifact:rollback",
        "artifact:execution",
        "artifact:observation",
        "artifact:lifecycle-evidence",
    ]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=wrong_order,
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=wrong_order,
        as_of_date="2026-04-28",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "artifact_ids order/types do not match lifecycle stage sequence" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_result_and_pointer_advance_to_new_run_but_run_registry_current_stays_old(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)

    run_registry = RunRegistry(runs_dir=runs_dir)
    run_registry.register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-old",
        data_snapshot_id="data-old",
        code_revision="rev-old",
        make_current=True,
    )
    run_registry.register(
        run_id="run-002",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-new",
        data_snapshot_id="data-new",
        code_revision="rev-new",
        make_current=False,
    )

    registry = ArtifactRegistry(artifact_registry_path)
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", chain_dir / "lifecycle.json"),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        path.write_text(
            json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-002", "ts_code": "000001.SZ"}, ensure_ascii=False),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-002",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    artifact_ids = [artifact_id for artifact_id, _, _ in ordered_specs]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-002",
        result_id="primary:000001.SZ",
        run_id="run-002",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=artifact_ids,
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-002",
        result_id="primary:000001.SZ",
        run_id="run-002",
        lifecycle_id="lifecycle-002",
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "run registry current pointer does not match current result pointer run_id" in payload["problems"]
    assert "run registry current pointer history entry does not match current result pointer run_id" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_run_registry_current_run_type_is_not_main_chain_lifecycle(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)

    run_registry = RunRegistry(runs_dir=runs_dir)
    run_registry.register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    run_current_path = runs_dir / "current.json"
    run_current = json.loads(run_current_path.read_text(encoding="utf-8"))
    run_current["run_type"] = "daily_research"
    run_current_path.write_text(json.dumps(run_current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    registry = ArtifactRegistry(artifact_registry_path)
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", chain_dir / "lifecycle.json"),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        path.write_text(
            json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-001", "ts_code": "000001.SZ"}, ensure_ascii=False),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-001",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    artifact_ids = [artifact_id for artifact_id, _, _ in ordered_specs]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=artifact_ids,
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "run registry current pointer run_type does not match main chain lifecycle type" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_run_registry_history_metadata_drifts_from_pointer_identity(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
        },
    )

    registry = ArtifactRegistry(artifact_registry_path)
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", chain_dir / "lifecycle.json"),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        path.write_text(
            json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-001", "ts_code": "000001.SZ"}, ensure_ascii=False),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-001",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    artifact_ids = [artifact_id for artifact_id, _, _ in ordered_specs]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=artifact_ids,
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
    )

    run_history_path = runs_dir / "history" / "run-001.json"
    run_history = json.loads(run_history_path.read_text(encoding="utf-8"))
    run_history["metadata"]["lifecycle_id"] = "lifecycle-old"
    run_history["metadata"]["result_id"] = "primary:999999.SZ"
    run_history["metadata"]["ts_code"] = "999999.SZ"
    run_history_path.write_text(json.dumps(run_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "run registry history metadata lifecycle_id does not match current result pointer" in payload["problems"]
    assert "run registry history metadata result_id does not match current result pointer" in payload["problems"]
    assert "run registry history metadata ts_code does not match result registry current ts_code" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_pointer_snapshot_metadata_result_record_id_drifts(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )

    registry = ArtifactRegistry(artifact_registry_path)
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", chain_dir / "lifecycle.json"),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        path.write_text(
            json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-001", "ts_code": "000001.SZ"}, ensure_ascii=False),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-001",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    artifact_ids = [artifact_id for artifact_id, _, _ in ordered_specs]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=artifact_ids,
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
        metadata={"result_registry_record_id": "result-record-001"},
    )

    snapshot_path = pointer_dir / "history" / "pointer-001.json"
    snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot_payload["metadata"]["result_registry_record_id"] = "result-record-old"
    snapshot_path.write_text(json.dumps(snapshot_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "current result pointer snapshot metadata result_registry_record_id does not match result registry current record" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_result_registry_metadata_artifact_registry_path_drifts(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )

    registry = ArtifactRegistry(artifact_registry_path)
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", chain_dir / "lifecycle.json"),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        path.write_text(
            json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-001", "ts_code": "000001.SZ"}, ensure_ascii=False),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-001",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    artifact_ids = [artifact_id for artifact_id, _, _ in ordered_specs]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=artifact_ids,
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "artifact_registry_path": str(tmp_path / "artifacts" / "artifact_registry-old.jsonl"),
        },
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
        metadata={"result_registry_record_id": "result-record-001"},
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "result registry metadata artifact_registry_path does not match active artifact registry" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_pointer_snapshot_metadata_paths_drift(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )

    registry = ArtifactRegistry(artifact_registry_path)
    lifecycle_evidence_path = chain_dir / "lifecycle.json"
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", lifecycle_evidence_path),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        path.write_text(
            json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-001", "ts_code": "000001.SZ"}, ensure_ascii=False),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-001",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    artifact_ids = [artifact_id for artifact_id, _, _ in ordered_specs]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=artifact_ids,
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "evidence_path": str(lifecycle_evidence_path),
            "artifact_registry_path": str(artifact_registry_path),
        },
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
        metadata={
            "result_registry_record_id": "result-record-001",
            "evidence_path": str(chain_dir / "wrong-lifecycle.json"),
            "artifact_registry_path": str(tmp_path / "artifacts" / "artifact_registry-old.jsonl"),
        },
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "current result pointer snapshot metadata evidence_path does not match result registry current evidence_path" in payload["problems"]
    assert "current result pointer snapshot metadata artifact_registry_path does not match active artifact registry" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_result_registry_repair_mode_crosses_formal_boundary(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )

    registry = ArtifactRegistry(artifact_registry_path)
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", chain_dir / "lifecycle.json"),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        path.write_text(
            json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-001", "ts_code": "000001.SZ"}, ensure_ascii=False),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-001",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    artifact_ids = [artifact_id for artifact_id, _, _ in ordered_specs]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=artifact_ids,
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "repair_mode": "bootstrap",
            "artifact_registry_path": str(artifact_registry_path),
        },
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
        metadata={"result_registry_record_id": "result-record-001"},
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "result registry metadata repair_mode is not allowed for current formal main chain" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_run_registry_metadata_mirrors_drift(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)
    lifecycle_evidence_path = chain_dir / "lifecycle.json"

    lifecycle_evidence_path.write_text("{}", encoding="utf-8")
    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
            "evidence_path": str(chain_dir / "wrong-lifecycle.json"),
            "artifact_registry_path": str(tmp_path / "artifacts" / "artifact_registry-old.jsonl"),
            "repair_mode": "bootstrap",
        },
    )

    registry = ArtifactRegistry(artifact_registry_path)
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", lifecycle_evidence_path),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        path.write_text(
            json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-001", "ts_code": "000001.SZ"}, ensure_ascii=False),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-001",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    artifact_ids = [artifact_id for artifact_id, _, _ in ordered_specs]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=artifact_ids,
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "evidence_path": str(lifecycle_evidence_path),
            "artifact_registry_path": str(artifact_registry_path),
        },
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
        metadata={"result_registry_record_id": "result-record-001"},
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "run registry history metadata evidence_path does not match result registry current evidence_path" in payload["problems"]
    assert "run registry history metadata artifact_registry_path does not match active artifact registry" in payload["problems"]
    assert "run registry history metadata repair_mode is not allowed for current formal main chain" in payload["problems"]


def test_current_result_pointer_integrity_fails_on_temporal_progression_drift(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    chain_dir = tmp_path / "artifacts" / "chain"
    chain_dir.mkdir(parents=True, exist_ok=True)
    lifecycle_evidence_path = chain_dir / "lifecycle.json"
    lifecycle_evidence_path.write_text(
        json.dumps({"completed_at": "2026-04-28T07:00:00Z"}, ensure_ascii=False),
        encoding="utf-8",
    )

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
        created_at="2026-04-28T08:00:00Z",
    )
    registry = ArtifactRegistry(artifact_registry_path)
    ordered_specs = [
        ("artifact:audit", "primary_result_audit", chain_dir / "audit.json"),
        ("artifact:execution", "primary_result_execution", chain_dir / "execution.json"),
        ("artifact:rollback", "primary_result_rollback", chain_dir / "rollback.json"),
        ("artifact:observation", "primary_result_observation", chain_dir / "observation.json"),
        ("artifact:lifecycle-evidence", "primary_result_lifecycle_evidence", lifecycle_evidence_path),
    ]
    for artifact_id, artifact_type, path in ordered_specs:
        if path != lifecycle_evidence_path:
            path.write_text(
                json.dumps({"result_id": "primary:000001.SZ", "run_id": "run-001", "ts_code": "000001.SZ"}, ensure_ascii=False),
                encoding="utf-8",
            )
        registry.register_artifact(
            artifact_type=artifact_type,
            run_id="run-001",
            path=path,
            producer="test",
            artifact_id=artifact_id,
            result_id="primary:000001.SZ",
        )

    artifact_ids = [artifact_id for artifact_id, _, _ in ordered_specs]
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L4",
        artifact_ids=artifact_ids,
        make_current=True,
        registered_at="2026-04-28T07:30:00Z",
        metadata={"evidence_path": str(lifecycle_evidence_path)},
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
        updated_at="2026-04-28T07:45:00Z",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "run registry current updated_at is after result registry current updated_at" in payload["problems"]
    assert "result registry current updated_at does not match current result pointer updated_at" in payload["problems"]
    assert "result registry current updated_at does not match lifecycle evidence completed_at" in payload["problems"]
    assert "current result pointer updated_at does not match lifecycle evidence completed_at" in payload["problems"]
    assert "run registry current updated_at is after lifecycle evidence completed_at" in payload["problems"]


def test_current_result_pointer_integrity_flags_non_main_chain_current_pointer_divergence(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-sidecar",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-sidecar",
        data_snapshot_id="data-sidecar",
        code_revision="rev-sidecar",
        make_current=False,
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="test",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )
    run_current_path = runs_dir / "current.json"
    run_current = json.loads(run_current_path.read_text(encoding="utf-8"))
    run_current["run_id"] = "run-sidecar"
    run_current["run_type"] = "daily_research"
    run_current["entry_path"] = str(runs_dir / "history" / "run-sidecar.json")
    run_current_path.write_text(json.dumps(run_current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "run registry current pointer diverged to a non-main-chain run while formal main-chain pointer remains active" in payload["problems"]


def test_current_result_pointer_integrity_fails_on_source_scope_and_chronological_latest_drift(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="test",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    registry = ResultRegistry(results_dir=results_dir)
    registry.register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        source_scope="stock",
        registered_at="2026-04-28T09:00:00Z",
        make_current=True,
    )
    registry.register(
        record_id="result-record-002",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        source_scope="apex",
        registered_at="2026-04-28T10:00:00Z",
        make_current=False,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
        source_scope="stock",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "result registry current record_id does not match chronological latest record for result_id" in payload["problems"]
    assert "result registry latest record source_scope does not match current result pointer" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_run_registry_source_scope_and_current_history_semantics_drift(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
            "source_scope": "apex",
        },
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="test",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    registry = ResultRegistry(results_dir=results_dir)
    registry.register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        source_scope="stock",
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
        source_scope="stock",
    )
    result_current_path = results_dir / "current.json"
    result_current = json.loads(result_current_path.read_text(encoding="utf-8"))
    result_history_path = results_dir / "history" / f"{result_current['record_id']}.json"
    result_history = json.loads(result_history_path.read_text(encoding="utf-8"))
    result_history["source_scope"] = "apex"
    result_history_path.write_text(json.dumps(result_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "result registry current history source_scope does not match current result pointer" in payload["problems"]
    assert "result registry latest record source_scope does not match current result pointer" in payload["problems"]
    assert "run registry history metadata source_scope does not match current result pointer" in payload["problems"]


def test_current_result_pointer_integrity_flags_temporal_alignment_with_non_main_chain_run_semantics(
    tmp_path,
):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        created_at="2026-04-28T07:30:00Z",
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
            "source_scope": "apex",
            "repair_mode": "bootstrap",
        },
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="test",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        source_scope="stock",
        registered_at="2026-04-28T07:30:00Z",
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
        source_scope="stock",
        updated_at="2026-04-28T07:30:00Z",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "run registry history metadata repair_mode is not allowed for current formal main chain" in payload["problems"]
    assert "run registry history metadata source_scope does not match current result pointer" in payload["problems"]
    assert "run registry current updated_at is chronologically aligned but history metadata carries non-main-chain semantics" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_pointer_snapshot_metadata_source_scope_drifts(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
            "source_scope": "stock",
        },
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="scripts.run_primary_result_lifecycle",
        code_revision="rev-001",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        source_scope="stock",
        make_current=True,
        metadata={"source_scope": "stock"},
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
        source_scope="stock",
        metadata={"result_registry_record_id": "result-record-001", "source_scope": "stock"},
    )
    snapshot_path = pointer_dir / "history" / "pointer-001.json"
    snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot_payload["metadata"]["source_scope"] = "apex"
    snapshot_path.write_text(json.dumps(snapshot_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "current result pointer snapshot metadata source_scope does not match active current result pointer" in payload["problems"]
    assert "current result pointer snapshot metadata source_scope does not match result registry current source_scope" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_artifact_producer_and_code_revision_drift(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
            "source_scope": "stock",
        },
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="scripts.repair_primary_result_chain",
        code_revision="rev-sidecar",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        source_scope="stock",
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
        source_scope="stock",
    )

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "artifact registry producer is not allowed for current formal main chain: artifact:a" in payload["problems"]
    assert "artifact registry code_revision does not match main chain run history: artifact:a" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_pointer_snapshot_metadata_producer_and_code_revision_drift(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="scripts.run_primary_result_lifecycle",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
            "source_scope": "stock",
        },
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="scripts.run_primary_result_lifecycle",
        code_revision="rev-001",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    ResultRegistry(results_dir=results_dir).register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        source_scope="stock",
        make_current=True,
        metadata={
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "rev-001",
        },
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
        source_scope="stock",
        metadata={
            "result_registry_record_id": "result-record-001",
            "source_scope": "stock",
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "rev-001",
        },
    )
    snapshot_path = pointer_dir / "history" / "pointer-001.json"
    snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot_payload["metadata"]["producer"] = "scripts.repair_primary_result_chain"
    snapshot_payload["metadata"]["code_revision"] = "rev-sidecar"
    snapshot_path.write_text(json.dumps(snapshot_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "current result pointer snapshot metadata producer does not match main chain run history" in payload["problems"]
    assert "current result pointer snapshot metadata code_revision does not match main chain run history" in payload["problems"]
    assert "current result pointer snapshot metadata producer does not match result registry current metadata producer" in payload["problems"]
    assert "current result pointer snapshot metadata code_revision does not match result registry current metadata code_revision" in payload["problems"]


def test_current_result_pointer_integrity_fails_when_result_registry_metadata_producer_and_code_revision_drift(tmp_path):
    pointer_dir = tmp_path / "artifacts" / "current_result_pointer"
    results_dir = tmp_path / "artifacts" / "result_registry"
    runs_dir = tmp_path / "artifacts" / "run_registry"
    artifact_registry_path = tmp_path / "artifacts" / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifacts" / "artifact-a.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")

    RunRegistry(runs_dir=runs_dir).register(
        run_id="run-001",
        run_type="primary_result_lifecycle",
        producer="scripts.run_primary_result_lifecycle",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
        metadata={
            "lifecycle_id": "lifecycle-001",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
            "source_scope": "stock",
        },
    )
    ArtifactRegistry(artifact_registry_path).register_artifact(
        artifact_type="primary_result_audit",
        run_id="run-001",
        path=artifact_path,
        producer="scripts.run_primary_result_lifecycle",
        code_revision="rev-001",
        artifact_id="artifact:a",
        result_id="primary:000001.SZ",
    )
    result_registry = ResultRegistry(results_dir=results_dir)
    result_registry.register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        lifecycle_stage="L3",
        artifact_ids=["artifact:a"],
        source_scope="stock",
        make_current=True,
        metadata={
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "rev-001",
        },
    )
    CurrentResultPointerStore(pointer_dir=pointer_dir).point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
        source_scope="stock",
        metadata={
            "result_registry_record_id": "result-record-001",
            "source_scope": "stock",
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "rev-001",
        },
    )
    current_history_path = results_dir / "history" / "result-record-001.json"
    current_history_payload = json.loads(current_history_path.read_text(encoding="utf-8"))
    current_history_payload["metadata"]["producer"] = "scripts.repair_primary_result_chain"
    current_history_payload["metadata"]["code_revision"] = "rev-sidecar"
    current_history_path.write_text(json.dumps(current_history_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )

    assert exit_code == 1
    assert payload["ok"] is False
    assert "result registry metadata producer does not match main chain run history" in payload["problems"]
    assert "result registry metadata code_revision does not match main chain run history" in payload["problems"]
    assert "current result pointer snapshot metadata producer does not match result registry current metadata producer" in payload["problems"]
    assert "current result pointer snapshot metadata code_revision does not match result registry current metadata code_revision" in payload["problems"]
