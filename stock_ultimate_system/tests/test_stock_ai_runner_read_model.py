from src.stock_ai_runner_read_model import (
    load_stock_ai_runner_read_model,
    read_stock_ai_failure_top_causes,
    read_stock_ai_provider_health_rollups,
    read_stock_ai_provider_latest_health_snapshot,
    read_stock_ai_provider_latest_status,
    read_stock_ai_result_replay,
)
from src.stock_ai_runner_storage import StockAIRunnerStorage


def _persist_attempt(
    storage: StockAIRunnerStorage,
    *,
    result_id: str,
    provider_name: str,
    final_status: str,
    failure_state: str | None = None,
    failure_reason: str = "",
    elapsed_ms: int = 10,
    recorded_at: str = "",
) -> None:
    terminal_state = failure_state or "ok"
    terminal_reason = failure_reason if failure_state else ""
    status_code = 200 if not failure_state else (408 if failure_state == "timeout" else 400)
    telemetry_status = "ok" if not failure_state else failure_state
    storage.persist(
        result_id=result_id,
        provider_name=provider_name,
        final_status=final_status,
        attempt_ledger=[
            {
                "state": "ready",
                "request_id": f"{provider_name}-{result_id}",
                "provider_name": provider_name,
                "reason": "",
                "recorded_at": recorded_at,
            },
            {
                "state": "running",
                "request_id": f"{provider_name}-{result_id}",
                "provider_name": provider_name,
                "reason": "",
                "recorded_at": recorded_at,
            },
            {
                "state": terminal_state,
                "request_id": f"{provider_name}-{result_id}",
                "provider_name": provider_name,
                "reason": terminal_reason,
                "recorded_at": recorded_at,
            },
        ],
        telemetry_buffer=[
            {
                "provider_name": provider_name,
                "request_id": f"{provider_name}-{result_id}",
                "timeout_ms": 1200,
                "retry_count": 0,
                "status": telemetry_status,
                "status_code": status_code,
                "response_bytes": 64,
                "elapsed_ms": elapsed_ms,
                "network_mode": "offline_only",
                "response_schema_version": "stock_ai_provider_adapter.v1",
                "recorded_at": recorded_at,
            }
        ],
    )


def test_read_stock_ai_provider_latest_status_surfaces_problem_provider_first(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000001.SZ",
        provider_name="echo_summary",
        final_status="ready",
    )
    _persist_attempt(
        storage,
        result_id="primary:000002.SZ",
        provider_name="local_preview",
        final_status="blocked",
        failure_state="blocked",
        failure_reason="provider_status_code_error:503",
    )

    rows = read_stock_ai_provider_latest_status(storage.storage_dir)

    assert rows[0]["provider_name"] == "local_preview"
    assert rows[0]["latest_state"] == "blocked"
    assert rows[0]["is_problem"] is True
    assert rows[0]["last_status_code"] == 400
    assert rows[0]["last_result_id"] == "primary:000002.SZ"
    assert rows[1]["provider_name"] == "echo_summary"
    assert rows[1]["latest_state"] == "ok"


def test_read_stock_ai_provider_latest_health_snapshot_has_fixed_contract(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000010.SZ",
        provider_name="echo_summary",
        final_status="ready",
        elapsed_ms=18,
    )

    rows = read_stock_ai_provider_latest_health_snapshot(storage.storage_dir)

    assert rows[0].keys() >= {
        "provider_name",
        "latest_state",
        "is_problem",
        "last_request_id",
        "last_result_id",
        "last_reason",
        "last_status_code",
        "last_elapsed_ms",
    }
    assert rows[0]["last_elapsed_ms"] == 18


def test_read_stock_ai_provider_health_rollups_exposes_recent_provider_degradation(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000011.SZ",
        provider_name="echo_summary",
        final_status="ready",
        recorded_at="2026-04-29T09:00:00Z",
    )
    _persist_attempt(
        storage,
        result_id="primary:000012.SZ",
        provider_name="echo_summary",
        final_status="degraded",
        failure_state="timeout",
        failure_reason="provider_timeout",
        elapsed_ms=1200,
        recorded_at="2026-04-29T09:10:00Z",
    )
    _persist_attempt(
        storage,
        result_id="primary:000013.SZ",
        provider_name="local_preview",
        final_status="ready",
        recorded_at="2026-04-29T09:20:00Z",
    )

    rollups = read_stock_ai_provider_health_rollups(storage.storage_dir, window=2)

    assert rollups[0]["provider_name"] == "echo_summary"
    assert rollups[0]["is_degrading"] is True
    assert rollups[0]["timeout_rate"] == 0.5
    assert rollups[0]["latest_state"] == "timeout"
    assert rollups[1]["provider_name"] == "local_preview"
    assert rollups[1]["success_rate"] == 1.0


def test_read_stock_ai_failure_top_causes_returns_ranked_causes(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000001.SZ",
        provider_name="echo_summary",
        final_status="degraded",
        failure_state="timeout",
        failure_reason="provider_timeout",
    )
    _persist_attempt(
        storage,
        result_id="primary:000002.SZ",
        provider_name="local_preview",
        final_status="degraded",
        failure_state="timeout",
        failure_reason="provider_timeout",
    )
    _persist_attempt(
        storage,
        result_id="primary:000003.SZ",
        provider_name="local_preview",
        final_status="blocked",
        failure_state="blocked",
        failure_reason="provider_status_code_error:503",
    )

    causes = read_stock_ai_failure_top_causes(storage.storage_dir, top_n=2)

    assert causes[0]["reason"] == "provider_timeout"
    assert causes[0]["count"] == 2
    assert causes[1]["reason"] == "provider_status_code_error:503"
    assert causes[1]["count"] == 1


def test_read_stock_ai_result_replay_returns_recent_attempts_for_result(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000001.SZ",
        provider_name="echo_summary",
        final_status="ready",
        recorded_at="2026-04-29T09:00:00Z",
    )
    _persist_attempt(
        storage,
        result_id="primary:000001.SZ",
        provider_name="echo_summary",
        final_status="degraded",
        failure_state="timeout",
        failure_reason="provider_timeout",
        elapsed_ms=1200,
        recorded_at="2026-04-29T09:30:00Z",
    )

    replay = read_stock_ai_result_replay(storage.storage_dir, result_id="primary:000001.SZ", window=4)

    assert replay["result_id"] == "primary:000001.SZ"
    assert replay["window"] == 4
    assert replay["attempt_count"] == 4
    assert replay["latest_state"] == "timeout"
    assert replay["failure_counts"]["timeout"] == 1
    assert replay["attempts"][-1]["reason"] == "provider_timeout"


def test_read_stock_ai_result_replay_supports_recorded_at_range_filter(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000021.SZ",
        provider_name="echo_summary",
        final_status="ready",
        recorded_at="2026-04-29T09:00:00Z",
    )
    _persist_attempt(
        storage,
        result_id="primary:000021.SZ",
        provider_name="echo_summary",
        final_status="degraded",
        failure_state="blocked",
        failure_reason="provider_status_code_error:503",
        recorded_at="2026-04-29T10:00:00Z",
    )

    replay = read_stock_ai_result_replay(
        storage.storage_dir,
        result_id="primary:000021.SZ",
        window=8,
        recorded_at_from="2026-04-29T09:30:00Z",
        recorded_at_to="2026-04-29T10:30:00Z",
    )

    assert replay["recorded_at_from"] == "2026-04-29T09:30:00Z"
    assert replay["recorded_at_to"] == "2026-04-29T10:30:00Z"
    assert replay["attempt_count"] == 3
    assert replay["latest_state"] == "blocked"
    assert replay["failure_counts"]["blocked"] == 1
    assert all(item["recorded_at"] == "2026-04-29T10:00:00Z" for item in replay["attempts"])


def test_load_stock_ai_runner_read_model_uses_persisted_read_model_file(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000009.SZ",
        provider_name="echo_summary",
        final_status="ready",
    )

    read_model = load_stock_ai_runner_read_model(storage.storage_dir)

    assert "provider_latest_health" in read_model
    assert "provider_latest_status" in read_model
    assert "failure_top_causes" in read_model
    assert "result_recent_attempts" in read_model
