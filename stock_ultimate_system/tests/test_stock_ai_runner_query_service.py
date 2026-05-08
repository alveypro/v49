from src.stock_ai_runner_query_service import StockAIRunnerQueryService
from src.stock_ai_runner_storage import StockAIRunnerStorage


def _persist_attempt(
    storage: StockAIRunnerStorage,
    *,
    result_id: str,
    provider_name: str,
    final_status: str,
    status: str = "ok",
    status_code: int = 200,
    reason: str = "",
    recorded_at: str = "",
    elapsed_ms: int = 10,
) -> None:
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
                "state": status,
                "request_id": f"{provider_name}-{result_id}",
                "provider_name": provider_name,
                "reason": reason,
                "recorded_at": recorded_at,
            },
        ],
        telemetry_buffer=[
            {
                "provider_name": provider_name,
                "request_id": f"{provider_name}-{result_id}",
                "timeout_ms": 1200,
                "retry_count": 0,
                "status": status,
                "status_code": status_code,
                "response_bytes": 64,
                "elapsed_ms": elapsed_ms,
                "network_mode": "offline_only",
                "response_schema_version": "stock_ai_provider_adapter.v1",
                "recorded_at": recorded_at,
            }
        ],
    )


def test_stock_ai_runner_query_service_exposes_stable_read_entrypoints(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000001.SZ",
        provider_name="echo_summary",
        final_status="ready",
        status="ok",
        status_code=200,
        recorded_at="2026-04-29T09:00:00Z",
    )
    _persist_attempt(
        storage,
        result_id="primary:000002.SZ",
        provider_name="echo_summary",
        final_status="degraded",
        status="timeout",
        status_code=408,
        reason="provider_timeout",
        recorded_at="2026-04-29T09:10:00Z",
        elapsed_ms=1200,
    )
    _persist_attempt(
        storage,
        result_id="primary:000003.SZ",
        provider_name="local_preview",
        final_status="blocked",
        status="blocked",
        status_code=503,
        reason="provider_status_code_error:503",
        recorded_at="2026-04-29T09:20:00Z",
    )
    service = StockAIRunnerQueryService(storage.storage_dir)

    latest_health = service.read_latest_health()
    health_rollups = service.read_health_rollups(window=2)
    trend_summaries = service.read_trend_summaries(short_window=2, long_window=4)
    top_causes = service.read_failure_top_causes(top_n=2)
    replay = service.read_result_replay(
        result_id="primary:000002.SZ",
        window=4,
        recorded_at_from="2026-04-29T09:00:00Z",
        recorded_at_to="2026-04-29T09:30:00Z",
    )

    assert latest_health[0]["provider_name"] in {"echo_summary", "local_preview"}
    assert health_rollups[0].keys() >= {"provider_name", "window", "success_rate", "timeout_rate", "blocked_rate"}
    assert trend_summaries[0].keys() >= {
        "provider_name",
        "short_window",
        "long_window",
        "timeout_trend_delta",
        "blocked_trend_delta",
        "is_worsening",
        "is_flapping",
    }
    assert top_causes[0]["count"] >= 1
    assert replay["result_id"] == "primary:000002.SZ"
    assert replay["recorded_at_to"] == "2026-04-29T09:30:00Z"


def test_stock_ai_runner_query_service_detects_worsening_provider_trend(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000101.SZ",
        provider_name="echo_summary",
        final_status="ready",
        status="ok",
        status_code=200,
        recorded_at="2026-04-29T09:00:00Z",
    )
    _persist_attempt(
        storage,
        result_id="primary:000102.SZ",
        provider_name="echo_summary",
        final_status="ready",
        status="ok",
        status_code=200,
        recorded_at="2026-04-29T09:05:00Z",
    )
    _persist_attempt(
        storage,
        result_id="primary:000103.SZ",
        provider_name="echo_summary",
        final_status="degraded",
        status="timeout",
        status_code=408,
        reason="provider_timeout",
        recorded_at="2026-04-29T09:10:00Z",
    )
    _persist_attempt(
        storage,
        result_id="primary:000104.SZ",
        provider_name="echo_summary",
        final_status="blocked",
        status="blocked",
        status_code=503,
        reason="provider_status_code_error:503",
        recorded_at="2026-04-29T09:15:00Z",
    )
    service = StockAIRunnerQueryService(storage.storage_dir)

    trends = service.read_trend_summaries(short_window=2, long_window=4)

    assert trends[0]["provider_name"] == "echo_summary"
    assert trends[0]["is_worsening"] is True
    assert trends[0]["blocked_trend_delta"] > 0 or trends[0]["timeout_trend_delta"] > 0


def test_stock_ai_runner_query_service_builds_provider_detail_drilldown(tmp_path):
    storage = StockAIRunnerStorage.from_path(tmp_path / "artifacts" / "stock_ai_runner")
    _persist_attempt(
        storage,
        result_id="primary:000201.SZ",
        provider_name="echo_summary",
        final_status="ready",
        status="ok",
        status_code=200,
        recorded_at="2026-04-29T09:00:00Z",
    )
    _persist_attempt(
        storage,
        result_id="primary:000202.SZ",
        provider_name="echo_summary",
        final_status="degraded",
        status="timeout",
        status_code=408,
        reason="provider_timeout",
        recorded_at="2026-04-29T09:10:00Z",
    )
    service = StockAIRunnerQueryService(storage.storage_dir)

    detail = service.read_provider_detail(
        provider_name="echo_summary",
        replay_window=6,
        recorded_at_from="2026-04-29T09:00:00Z",
        recorded_at_to="2026-04-29T10:00:00Z",
        health_window=4,
        trend_short_window=2,
        trend_long_window=4,
    )

    assert detail["provider_name"] == "echo_summary"
    assert detail["latest_health"]["provider_name"] == "echo_summary"
    assert detail["health_rollup"]["provider_name"] == "echo_summary"
    assert detail["trend_summary"]["provider_name"] == "echo_summary"
    assert detail["provider_replay"]["provider_name"] == "echo_summary"
    assert "primary:000202.SZ" in detail["provider_replay"]["result_ids"]
