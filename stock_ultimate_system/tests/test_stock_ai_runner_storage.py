import json

from src.stock_ai_runner_storage import (
    STOCK_AI_RUNNER_HISTORY_LIMIT,
    STOCK_AI_RUNNER_RECENT_ATTEMPT_LIMIT,
    StockAIRunnerStorage,
)


def test_stock_ai_runner_storage_persists_ledger_and_provider_summary(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")

    persisted = storage.persist(
        result_id="primary:000001.SZ",
        provider_name="echo_summary",
        final_status="ready",
        attempt_ledger=[
            {"state": "ready", "request_id": "req-1", "provider_name": "echo_summary", "reason": ""},
            {"state": "running", "request_id": "req-1", "provider_name": "echo_summary", "reason": ""},
            {"state": "ok", "request_id": "req-1", "provider_name": "echo_summary", "reason": ""},
        ],
        telemetry_buffer=[
            {
                "provider_name": "echo_summary",
                "request_id": "req-1",
                "timeout_ms": 1200,
                "retry_count": 0,
                "status": "ok",
                "status_code": 200,
                "response_bytes": 64,
                "elapsed_ms": 8,
                "network_mode": "offline_only",
                "response_schema_version": "stock_ai_provider_adapter.v1",
            }
        ],
    )

    assert persisted["ledger_entries"] == 3
    assert persisted["telemetry_entries"] == 1
    assert persisted["read_model_path"].endswith("read_model.json")
    summary = json.loads(storage.provider_summary_path.read_text(encoding="utf-8"))
    assert summary["providers"]["echo_summary"]["total_calls"] == 1
    assert summary["providers"]["echo_summary"]["error_calls"] == 0
    read_model = json.loads(storage.read_model_path.read_text(encoding="utf-8"))
    assert read_model["provider_rollups"]["echo_summary"]["total_calls"] == 1
    assert read_model["provider_latest_status"]["echo_summary"]["state"] == "ok"
    assert read_model["recent_attempts"][-1]["state"] == "ok"
    assert read_model["failure_buckets"] == {}
    assert read_model["failure_top_causes"] == []


def test_stock_ai_runner_storage_truncates_history_window(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")

    for idx in range(STOCK_AI_RUNNER_HISTORY_LIMIT + 5):
        storage.persist(
            result_id=f"primary:{idx}",
            provider_name="echo_summary",
            final_status="ready",
            attempt_ledger=[
                {"state": "ok", "request_id": f"req-{idx}", "provider_name": "echo_summary", "reason": ""}
            ],
            telemetry_buffer=[
                {
                    "provider_name": "echo_summary",
                    "request_id": f"req-{idx}",
                    "timeout_ms": 1200,
                    "retry_count": 0,
                    "status": "ok",
                    "status_code": 200,
                    "response_bytes": 32,
                    "elapsed_ms": idx,
                    "network_mode": "offline_only",
                    "response_schema_version": "stock_ai_provider_adapter.v1",
                }
            ],
        )

    ledger_lines = storage.ledger_path.read_text(encoding="utf-8").splitlines()
    telemetry_lines = storage.telemetry_path.read_text(encoding="utf-8").splitlines()
    assert len(ledger_lines) == STOCK_AI_RUNNER_HISTORY_LIMIT
    assert len(telemetry_lines) == STOCK_AI_RUNNER_HISTORY_LIMIT


def test_stock_ai_runner_storage_builds_recent_attempts_and_failure_buckets(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")

    storage.persist(
        result_id="primary:000001.SZ",
        provider_name="echo_summary",
        final_status="degraded",
        attempt_ledger=[
            {"state": "ready", "request_id": "req-1", "provider_name": "echo_summary", "reason": ""},
            {"state": "running", "request_id": "req-1", "provider_name": "echo_summary", "reason": ""},
            {"state": "timeout", "request_id": "req-1", "provider_name": "echo_summary", "reason": "provider_timeout"},
        ],
        telemetry_buffer=[
            {
                "provider_name": "echo_summary",
                "request_id": "req-1",
                "timeout_ms": 1200,
                "retry_count": 0,
                "status": "timeout",
                "status_code": 408,
                "response_bytes": 0,
                "elapsed_ms": 1200,
                "network_mode": "offline_only",
                "response_schema_version": "",
            }
        ],
    )
    read_model = storage.build_read_model()

    assert len(read_model["recent_attempts"]) <= STOCK_AI_RUNNER_RECENT_ATTEMPT_LIMIT
    assert read_model["recent_attempts"][-1]["state"] == "timeout"
    assert read_model["failure_buckets"]["timeout"] == 1
    assert read_model["provider_rollups"]["echo_summary"]["timeout_calls"] == 1
    assert read_model["provider_latest_status"]["echo_summary"]["is_problem"] is True
    assert read_model["failure_top_causes"][0]["reason"] == "provider_timeout"
    assert read_model["result_recent_attempts"]["primary:000001.SZ"][-1]["state"] == "timeout"
