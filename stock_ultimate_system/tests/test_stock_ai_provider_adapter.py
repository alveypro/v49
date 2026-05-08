from src.stock_ai_provider_adapter import (
    STOCK_AI_PROVIDER_DEFAULT_RETRY_COUNT,
    STOCK_AI_PROVIDER_RESPONSE_SIZE_LIMIT_BYTES,
    STOCK_AI_PROVIDER_TELEMETRY_RING_BUFFER_LIMIT,
    STOCK_AI_PROVIDER_TIMEOUT_MS,
    StockAIProviderAttemptError,
    build_stock_ai_provider_telemetry_summary,
    build_stock_ai_provider_request,
    invoke_stock_ai_provider_stub,
    parse_stock_ai_provider_response,
    run_stock_ai_provider_attempt,
)


def test_build_stock_ai_provider_request_wraps_payload_and_provider_name():
    request = build_stock_ai_provider_request(
        payload={"schema_version": "stock_ai_explainer_input.v1", "result_id": "primary:000001.SZ"},
        provider_name="echo_summary",
    )

    assert request["adapter_schema_version"] == "stock_ai_provider_adapter.v1"
    assert request["request_id"].startswith("stock-ai-req-")
    assert request["provider_name"] == "echo_summary"
    assert request["timeout_ms"] == STOCK_AI_PROVIDER_TIMEOUT_MS
    assert request["retry_count"] == STOCK_AI_PROVIDER_DEFAULT_RETRY_COUNT
    assert request["max_response_bytes"] == STOCK_AI_PROVIDER_RESPONSE_SIZE_LIMIT_BYTES
    assert request["network_mode"] == "offline_only"
    assert request["status"] == "ready"
    assert request["payload"]["result_id"] == "primary:000001.SZ"


def test_parse_stock_ai_provider_response_requires_payload_dict():
    payload, telemetry = parse_stock_ai_provider_response(
        {
            "adapter_schema_version": "stock_ai_provider_adapter.v1",
            "provider_name": "echo_summary",
            "request_id": "stock-ai-req-001",
            "timeout_ms": 1200,
            "retry_count": 0,
            "status": "ok",
            "payload": {"schema_version": "stock_ai_explainer_output.v1"},
        }
    )

    assert payload["schema_version"] == "stock_ai_explainer_output.v1"
    assert telemetry["provider_name"] == "echo_summary"
    assert telemetry["request_id"] == "stock-ai-req-001"
    assert telemetry["status"] == "ok"
    assert telemetry["status_code"] == 200


def test_invoke_stock_ai_provider_stub_blocks_unknown_provider():
    try:
        invoke_stock_ai_provider_stub(provider_name="unknown", payload={}, providers={})
    except ValueError as exc:
        assert "unknown_provider_stub" in str(exc)
    else:
        raise AssertionError("expected unknown provider to raise")


def test_invoke_stock_ai_provider_stub_returns_payload_and_telemetry():
    payload, telemetry = invoke_stock_ai_provider_stub(
        provider_name="echo_summary",
        payload={"schema_version": "stock_ai_explainer_input.v1", "summary_lines": ["a"], "result_id": "primary:1", "as_of_date": "2026-04-28", "run_id": "run-001"},
        providers={
            "echo_summary": lambda request: {
                "adapter_schema_version": "stock_ai_provider_adapter.v1",
                "provider_name": request["provider_name"],
                "request_id": request["request_id"],
                "timeout_ms": request["timeout_ms"],
                "retry_count": request["retry_count"],
                "status": "ok",
                "payload": {"schema_version": "stock_ai_explainer_output.v1"},
            }
        },
    )

    assert payload["schema_version"] == "stock_ai_explainer_output.v1"
    assert telemetry["provider_name"] == "echo_summary"
    assert telemetry["request_id"].startswith("stock-ai-req-")
    assert telemetry["timeout_ms"] == STOCK_AI_PROVIDER_TIMEOUT_MS
    assert telemetry["retry_count"] == STOCK_AI_PROVIDER_DEFAULT_RETRY_COUNT
    assert telemetry["network_mode"] == "offline_only"


def test_invoke_stock_ai_provider_stub_blocks_error_status_code():
    try:
        invoke_stock_ai_provider_stub(
            provider_name="error_stub",
            payload={"schema_version": "stock_ai_explainer_input.v1"},
            providers={
                "error_stub": lambda request: {
                    "adapter_schema_version": "stock_ai_provider_adapter.v1",
                    "provider_name": request["provider_name"],
                    "request_id": request["request_id"],
                    "timeout_ms": request["timeout_ms"],
                    "retry_count": request["retry_count"],
                    "status": "error",
                    "status_code": 503,
                    "payload": {"schema_version": "stock_ai_explainer_output.v1"},
                }
            },
        )
    except ValueError as exc:
        assert "provider_status_code_error:503" in str(exc)
    else:
        raise AssertionError("expected error status code to raise")


def test_invoke_stock_ai_provider_stub_blocks_oversized_response():
    try:
        invoke_stock_ai_provider_stub(
            provider_name="oversized_stub",
            payload={"schema_version": "stock_ai_explainer_input.v1"},
            providers={
                "oversized_stub": lambda request: {
                    "adapter_schema_version": "stock_ai_provider_adapter.v1",
                    "provider_name": request["provider_name"],
                    "request_id": request["request_id"],
                    "timeout_ms": request["timeout_ms"],
                    "retry_count": request["retry_count"],
                    "status": "ok",
                    "status_code": 200,
                    "payload": {
                        "schema_version": "stock_ai_explainer_output.v1",
                        "blob": "X" * (request["max_response_bytes"] + 32),
                    },
                }
            },
        )
    except ValueError as exc:
        assert "provider_response_too_large" in str(exc)
    else:
        raise AssertionError("expected oversized response to raise")


def test_build_stock_ai_provider_telemetry_summary_rolls_up_counts():
    summary = build_stock_ai_provider_telemetry_summary(
        [
            {"status": "ok", "status_code": 200, "elapsed_ms": 10, "response_bytes": 120},
            {"status": "timeout", "status_code": 408, "elapsed_ms": 1201, "response_bytes": 0},
            {"status": "error", "status_code": 503, "elapsed_ms": 4, "response_bytes": 40},
        ]
    )

    assert summary["total_calls"] == 3
    assert summary["error_calls"] == 2
    assert summary["timeout_calls"] == 1
    assert summary["blocked_calls"] == 2
    assert summary["max_elapsed_ms"] == 1201
    assert summary["max_response_bytes"] == 120


def test_run_stock_ai_provider_attempt_records_ready_running_ok():
    payload, telemetry, ledger, buffer_items = run_stock_ai_provider_attempt(
        provider_name="echo_summary",
        payload={"schema_version": "stock_ai_explainer_input.v1", "result_id": "primary:1", "as_of_date": "2026-04-28", "run_id": "run-001"},
        providers={
            "echo_summary": lambda request: {
                "adapter_schema_version": "stock_ai_provider_adapter.v1",
                "provider_name": request["provider_name"],
                "request_id": request["request_id"],
                "timeout_ms": request["timeout_ms"],
                "retry_count": request["retry_count"],
                "status": "ok",
                "status_code": 200,
                "payload": {"schema_version": "stock_ai_explainer_output.v1"},
            }
        },
    )

    assert payload["schema_version"] == "stock_ai_explainer_output.v1"
    assert telemetry["status"] == "ok"
    assert [item["state"] for item in ledger] == ["ready", "running", "ok"]
    assert len(buffer_items) == 1


def test_run_stock_ai_provider_attempt_records_timeout_state():
    try:
        run_stock_ai_provider_attempt(
            provider_name="timeout_stub",
            payload={"schema_version": "stock_ai_explainer_input.v1"},
            providers={
                "timeout_stub": lambda request: {
                    "adapter_schema_version": "stock_ai_provider_adapter.v1",
                    "provider_name": request["provider_name"],
                    "request_id": request["request_id"],
                    "timeout_ms": request["timeout_ms"],
                    "retry_count": request["retry_count"],
                    "status": "timeout",
                    "status_code": 408,
                    "payload": {"schema_version": "stock_ai_explainer_output.v1"},
                }
            },
        )
    except StockAIProviderAttemptError as exc:
        assert exc.telemetry["status"] == "timeout"
        assert [item["state"] for item in exc.ledger] == ["ready", "running", "timeout"]
    else:
        raise AssertionError("expected timeout")


def test_run_stock_ai_provider_attempt_records_blocked_state():
    try:
        run_stock_ai_provider_attempt(
            provider_name="blocked_stub",
            payload={"schema_version": "stock_ai_explainer_input.v1"},
            providers={
                "blocked_stub": lambda request: {
                    "adapter_schema_version": "stock_ai_provider_adapter.v1",
                    "provider_name": request["provider_name"],
                    "request_id": request["request_id"],
                    "timeout_ms": request["timeout_ms"],
                    "retry_count": request["retry_count"],
                    "status": "error",
                    "status_code": 503,
                    "payload": {"schema_version": "stock_ai_explainer_output.v1"},
                }
            },
        )
    except StockAIProviderAttemptError as exc:
        assert "provider_status_code_error:503" in str(exc)
        assert exc.telemetry["status"] == "blocked"
        assert [item["state"] for item in exc.ledger] == ["ready", "running", "blocked"]
    else:
        raise AssertionError("expected blocked state")


def test_telemetry_ring_buffer_has_limit():
    summary = build_stock_ai_provider_telemetry_summary(
        [{"status": "ok", "status_code": 200, "elapsed_ms": idx, "response_bytes": idx} for idx in range(STOCK_AI_PROVIDER_TELEMETRY_RING_BUFFER_LIMIT + 3)]
    )
    assert summary["total_calls"] == STOCK_AI_PROVIDER_TELEMETRY_RING_BUFFER_LIMIT + 3
