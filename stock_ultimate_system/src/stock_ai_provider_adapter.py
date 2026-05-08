from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Callable


STOCK_AI_PROVIDER_ADAPTER_SCHEMA_VERSION = "stock_ai_provider_adapter.v1"
STOCK_AI_PROVIDER_TIMEOUT_MS = 1200
STOCK_AI_PROVIDER_DEFAULT_RETRY_COUNT = 0
STOCK_AI_PROVIDER_RESPONSE_SIZE_LIMIT_BYTES = 8192
STOCK_AI_PROVIDER_TELEMETRY_RING_BUFFER_LIMIT = 8

StockAIProviderStub = Callable[[dict[str, Any]], dict[str, Any]]


class StockAIProviderAttemptError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        telemetry: dict[str, Any],
        ledger: list[dict[str, Any]],
        telemetry_buffer: list[dict[str, Any]],
    ) -> None:
        super().__init__(message)
        self.telemetry = telemetry
        self.ledger = ledger
        self.telemetry_buffer = telemetry_buffer


def _build_request_id(provider_name: str, payload: dict[str, Any]) -> str:
    raw = f"{provider_name}|{payload.get('result_id', '')}|{payload.get('as_of_date', '')}|{payload.get('run_id', '')}"
    return "stock-ai-req-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def build_stock_ai_provider_request(*, payload: dict[str, Any], provider_name: str) -> dict[str, Any]:
    normalized_provider_name = str(provider_name or "").strip()
    return {
        "adapter_schema_version": STOCK_AI_PROVIDER_ADAPTER_SCHEMA_VERSION,
        "request_id": _build_request_id(normalized_provider_name, payload),
        "provider_name": normalized_provider_name,
        "input_schema_version": str(payload.get("schema_version", "") or ""),
        "timeout_ms": STOCK_AI_PROVIDER_TIMEOUT_MS,
        "retry_count": STOCK_AI_PROVIDER_DEFAULT_RETRY_COUNT,
        "max_response_bytes": STOCK_AI_PROVIDER_RESPONSE_SIZE_LIMIT_BYTES,
        "network_mode": "offline_only",
        "status": "ready",
        "payload": dict(payload),
    }


def parse_stock_ai_provider_response(response: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(response, dict):
        raise ValueError("provider_response_must_be_dict")
    if "payload" not in response:
        raise ValueError("provider_response_missing_payload")
    payload = response.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("provider_response_payload_must_be_dict")
    telemetry = {
        "provider_name": str(response.get("provider_name", "") or ""),
        "request_id": str(response.get("request_id", "") or ""),
        "timeout_ms": response.get("timeout_ms", STOCK_AI_PROVIDER_TIMEOUT_MS),
        "retry_count": response.get("retry_count", STOCK_AI_PROVIDER_DEFAULT_RETRY_COUNT),
        "status": str(response.get("status", "ok") or "ok"),
        "status_code": int(response.get("status_code", 200) or 200),
        "response_bytes": int(response.get("response_bytes", 0) or 0),
        "elapsed_ms": int(response.get("elapsed_ms", 0) or 0),
        "response_schema_version": str(response.get("adapter_schema_version", "") or ""),
    }
    return payload, telemetry


def build_stock_ai_provider_telemetry_summary(telemetry_items: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(telemetry_items)
    error_count = sum(1 for item in telemetry_items if str(item.get("status", "")).lower() not in {"ok", "ready"})
    timeout_count = sum(
        1
        for item in telemetry_items
        if str(item.get("status", "")).lower() == "timeout" or int(item.get("status_code", 0) or 0) == 408
    )
    blocked_count = sum(1 for item in telemetry_items if int(item.get("status_code", 0) or 0) >= 400)
    max_elapsed_ms = max((int(item.get("elapsed_ms", 0) or 0) for item in telemetry_items), default=0)
    max_response_bytes = max((int(item.get("response_bytes", 0) or 0) for item in telemetry_items), default=0)
    return {
        "total_calls": total,
        "error_calls": error_count,
        "timeout_calls": timeout_count,
        "blocked_calls": blocked_count,
        "max_elapsed_ms": max_elapsed_ms,
        "max_response_bytes": max_response_bytes,
    }


def _append_ring_buffer(
    items: list[dict[str, Any]],
    entry: dict[str, Any],
    *,
    limit: int = STOCK_AI_PROVIDER_TELEMETRY_RING_BUFFER_LIMIT,
) -> list[dict[str, Any]]:
    next_items = [*items, dict(entry)]
    if len(next_items) > limit:
        next_items = next_items[-limit:]
    return next_items


def _attempt_entry(*, state: str, request_id: str, provider_name: str, reason: str = "") -> dict[str, Any]:
    return {
        "state": state,
        "request_id": request_id,
        "provider_name": provider_name,
        "reason": reason,
    }


def invoke_stock_ai_provider_stub(
    *,
    provider_name: str,
    payload: dict[str, Any],
    providers: dict[str, StockAIProviderStub],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if provider_name not in providers:
        raise ValueError(f"unknown_provider_stub:{provider_name}")
    request = build_stock_ai_provider_request(payload=payload, provider_name=provider_name)
    started = time.perf_counter()
    response = providers[provider_name](request)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    if not isinstance(response, dict):
        raise ValueError("provider_response_must_be_dict")
    encoded_response = json.dumps(response, ensure_ascii=False).encode("utf-8")
    response_bytes = len(encoded_response)
    timeout_ms = int(request["timeout_ms"])
    max_response_bytes = int(request["max_response_bytes"])
    status = str(response.get("status", "ok") or "ok").lower()
    status_code = int(response.get("status_code", 200) or 200)
    if response_bytes > max_response_bytes:
        raise ValueError("provider_response_too_large")
    if status == "timeout" or status_code == 408 or elapsed_ms > timeout_ms:
        raise TimeoutError("provider_timeout_guard")
    if status_code >= 400:
        raise ValueError(f"provider_status_code_error:{status_code}")
    response = dict(response)
    response["response_bytes"] = response_bytes
    response["elapsed_ms"] = response.get("elapsed_ms", elapsed_ms)
    parsed_payload, telemetry = parse_stock_ai_provider_response(response)
    telemetry["provider_name"] = telemetry["provider_name"] or request["provider_name"]
    telemetry["request_id"] = telemetry["request_id"] or request["request_id"]
    telemetry["timeout_ms"] = telemetry["timeout_ms"] or request["timeout_ms"]
    telemetry["retry_count"] = telemetry["retry_count"] if telemetry["retry_count"] is not None else request["retry_count"]
    telemetry["status_code"] = telemetry["status_code"] or status_code
    telemetry["response_bytes"] = telemetry["response_bytes"] or response_bytes
    telemetry["elapsed_ms"] = telemetry["elapsed_ms"] or elapsed_ms
    telemetry["network_mode"] = request["network_mode"]
    return parsed_payload, telemetry


def run_stock_ai_provider_attempt(
    *,
    provider_name: str,
    payload: dict[str, Any],
    providers: dict[str, StockAIProviderStub],
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    request = build_stock_ai_provider_request(payload=payload, provider_name=provider_name)
    ledger = [
        _attempt_entry(state="ready", request_id=str(request["request_id"]), provider_name=str(request["provider_name"])),
        _attempt_entry(state="running", request_id=str(request["request_id"]), provider_name=str(request["provider_name"])),
    ]
    telemetry_buffer: list[dict[str, Any]] = []
    try:
        parsed_payload, telemetry = invoke_stock_ai_provider_stub(
            provider_name=provider_name,
            payload=payload,
            providers=providers,
        )
    except TimeoutError as exc:
        telemetry = {
            "provider_name": str(request["provider_name"]),
            "request_id": str(request["request_id"]),
            "timeout_ms": int(request["timeout_ms"]),
            "retry_count": int(request["retry_count"]),
            "status": "timeout",
            "status_code": 408,
            "response_bytes": 0,
            "elapsed_ms": int(request["timeout_ms"]),
            "response_schema_version": "",
            "network_mode": str(request["network_mode"]),
        }
        telemetry_buffer = _append_ring_buffer(telemetry_buffer, telemetry)
        ledger.append(
            _attempt_entry(
                state="timeout",
                request_id=str(request["request_id"]),
                provider_name=str(request["provider_name"]),
                reason=str(exc),
            )
        )
        raise StockAIProviderAttemptError(
            str(exc),
            telemetry=telemetry,
            ledger=ledger,
            telemetry_buffer=telemetry_buffer,
        ) from exc
    except ValueError as exc:
        reason = str(exc)
        state = "blocked" if any(
            token in reason for token in ("provider_status_code_error", "provider_response_too_large")
        ) else "error"
        telemetry = {
            "provider_name": str(request["provider_name"]),
            "request_id": str(request["request_id"]),
            "timeout_ms": int(request["timeout_ms"]),
            "retry_count": int(request["retry_count"]),
            "status": state,
            "status_code": 400 if state == "blocked" else 500,
            "response_bytes": 0,
            "elapsed_ms": 0,
            "response_schema_version": "",
            "network_mode": str(request["network_mode"]),
        }
        telemetry_buffer = _append_ring_buffer(telemetry_buffer, telemetry)
        ledger.append(
            _attempt_entry(
                state=state,
                request_id=str(request["request_id"]),
                provider_name=str(request["provider_name"]),
                reason=reason,
            )
        )
        raise StockAIProviderAttemptError(
            reason,
            telemetry=telemetry,
            ledger=ledger,
            telemetry_buffer=telemetry_buffer,
        ) from exc
    except Exception as exc:
        telemetry = {
            "provider_name": str(request["provider_name"]),
            "request_id": str(request["request_id"]),
            "timeout_ms": int(request["timeout_ms"]),
            "retry_count": int(request["retry_count"]),
            "status": "error",
            "status_code": 500,
            "response_bytes": 0,
            "elapsed_ms": 0,
            "response_schema_version": "",
            "network_mode": str(request["network_mode"]),
        }
        telemetry_buffer = _append_ring_buffer(telemetry_buffer, telemetry)
        ledger.append(
            _attempt_entry(
                state="error",
                request_id=str(request["request_id"]),
                provider_name=str(request["provider_name"]),
                reason=str(exc),
            )
        )
        raise StockAIProviderAttemptError(
            str(exc),
            telemetry=telemetry,
            ledger=ledger,
            telemetry_buffer=telemetry_buffer,
        ) from exc
    telemetry_buffer = _append_ring_buffer(telemetry_buffer, telemetry)
    ledger.append(
        _attempt_entry(
            state="ok",
            request_id=str(request["request_id"]),
            provider_name=str(request["provider_name"]),
        )
    )
    return parsed_payload, telemetry, ledger, telemetry_buffer
