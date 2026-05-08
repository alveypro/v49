import json

import pytest

from src.artifact_registry import ArtifactRegistry, ARTIFACT_REGISTRY_SCHEMA_VERSION, sha256_file


def test_artifact_registry_registers_and_queries_jsonl(tmp_path):
    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_text('{"ok": true}\n', encoding="utf-8")
    registry = ArtifactRegistry(tmp_path / "artifact_registry.jsonl")

    entry = registry.register_artifact(
        artifact_type="release_pipeline_summary",
        run_id="stock-release-test001",
        path=artifact_path,
        producer="test",
        created_at="2026-04-14T09:00:00+00:00",
        result_id="primary:000001.SZ",
        parent_artifact_ids=["upstream:a", "upstream:b"],
        metadata={"kind": "summary"},
    )

    payload = entry.as_dict()
    assert payload["schema_version"] == ARTIFACT_REGISTRY_SCHEMA_VERSION
    assert payload["artifact_type"] == "release_pipeline_summary"
    assert payload["run_id"] == "stock-release-test001"
    assert payload["result_id"] == "primary:000001.SZ"
    assert payload["parent_artifact_ids"] == ["upstream:a", "upstream:b"]
    assert payload["sha256"] == sha256_file(artifact_path)
    assert registry.list_entries(run_id="stock-release-test001") == [payload]
    assert registry.list_entries(artifact_type="release_pipeline_summary") == [payload]
    assert registry.list_entries(result_id="primary:000001.SZ") == [payload]
    assert registry.list_entries(run_id="stock-release-other") == []


def test_artifact_registry_rejects_duplicate_artifact_id(tmp_path):
    artifact_a = tmp_path / "artifact-a.json"
    artifact_b = tmp_path / "artifact-b.json"
    artifact_a.write_text('{"artifact":"a"}\n', encoding="utf-8")
    artifact_b.write_text('{"artifact":"b"}\n', encoding="utf-8")
    registry = ArtifactRegistry(tmp_path / "artifact_registry.jsonl")

    registry.register_artifact(
        artifact_type="release_pipeline_summary",
        run_id="stock-release-test001",
        artifact_id="artifact:conflict",
        path=artifact_a,
        producer="test",
    )

    with pytest.raises(FileExistsError, match="artifact_id=artifact:conflict"):
        registry.register_artifact(
            artifact_type="release_pipeline_summary",
            run_id="stock-release-test002",
            artifact_id="artifact:conflict",
            path=artifact_b,
            producer="test",
        )


def test_artifact_registry_rejects_duplicate_registered_path(tmp_path):
    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_text('{"artifact":"shared"}\n', encoding="utf-8")
    registry = ArtifactRegistry(tmp_path / "artifact_registry.jsonl")

    registry.register_artifact(
        artifact_type="release_pipeline_summary",
        run_id="stock-release-test001",
        artifact_id="artifact:first",
        path=artifact_path,
        producer="test",
    )

    with pytest.raises(FileExistsError, match="artifact registry path already registered"):
        registry.register_artifact(
            artifact_type="release_pipeline_summary",
            run_id="stock-release-test001",
            artifact_id="artifact:second",
            path=artifact_path,
            producer="test",
        )


def test_artifact_registry_allows_same_path_across_different_runs(tmp_path):
    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_text('{"artifact":"shared"}\n', encoding="utf-8")
    registry = ArtifactRegistry(tmp_path / "artifact_registry.jsonl")

    first = registry.register_artifact(
        artifact_type="release_pipeline_summary",
        run_id="stock-release-test001",
        artifact_id="artifact:first",
        path=artifact_path,
        producer="test",
    )
    second = registry.register_artifact(
        artifact_type="release_pipeline_summary",
        run_id="stock-release-test002",
        artifact_id="artifact:second",
        path=artifact_path,
        producer="test",
    )

    assert first.path == second.path
    assert registry.list_entries() == [first.as_dict(), second.as_dict()]


def test_artifact_registry_get_entry_rejects_conflicting_duplicate_artifact_id_history(tmp_path):
    registry_path = tmp_path / "artifact_registry.jsonl"
    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_text('{"artifact":"shared"}\n', encoding="utf-8")
    payload = {
        "schema_version": ARTIFACT_REGISTRY_SCHEMA_VERSION,
        "artifact_id": "artifact:conflict",
        "artifact_type": "release_pipeline_summary",
        "run_id": "stock-release-test001",
        "path": str(artifact_path),
        "sha256": sha256_file(artifact_path),
        "created_at": "2026-05-01T00:00:00+00:00",
        "producer": "test",
        "result_id": "primary:000001.SZ",
        "parent_artifact_ids": [],
        "metadata": {},
    }
    registry_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
        + json.dumps({**payload, "run_id": "stock-release-test002"}, ensure_ascii=False, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="conflicting entries for artifact_id=artifact:conflict"):
        ArtifactRegistry(registry_path).get_entry("artifact:conflict")
