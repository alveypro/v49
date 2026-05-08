from __future__ import annotations

import hashlib
import os
from typing import Any, Callable

from src.stock_ai_provider_adapter import (
    STOCK_AI_PROVIDER_ADAPTER_SCHEMA_VERSION,
    StockAIProviderAttemptError,
    build_stock_ai_provider_telemetry_summary,
    run_stock_ai_provider_attempt,
)
from src.stock_ai_runner_storage import StockAIRunnerStorage


STOCK_AI_EXPLAINER_INPUT_SCHEMA_VERSION = "stock_ai_explainer_input.v1"
STOCK_AI_EXPLAINER_OUTPUT_SCHEMA_VERSION = "stock_ai_explainer_output.v1"
STOCK_AI_EXPLAINER_PROVIDER_STUB_VERSION = "stock_ai_explainer_provider_stub.v1"

STOCK_AI_EXPLAINER_ALLOWED_INPUT_FIELDS = frozenset(
    {
        "schema_version",
        "result_id",
        "run_id",
        "lifecycle_id",
        "as_of_date",
        "ts_code",
        "stock_name",
        "result_lifecycle_stage",
        "result_type",
        "audit_status",
        "execution_status",
        "terminal_outcome",
        "disabled_reason",
        "invalid_reason",
        "headline_tone",
        "headline_detail",
        "summary_lines",
        "data_sync_note",
        "history_source_file",
        "history_generation_mode",
        "decision_semantics",
        "blocker_semantics",
        "execution_semantics",
        "evidence_semantics",
        "governance_semantics",
        "observation_wait_status",
        "current_basket_pointer_status",
        "current_basket_pointer_basket_id",
        "current_basket_pointer_updated_at",
        "latest_basket_attempt_status",
        "latest_basket_attempt_generated_at",
        "latest_basket_attempt_blocking_reason",
        "source_field_whitelist",
    }
)
STOCK_AI_EXPLAINER_REQUIRED_INPUT_FIELDS = frozenset(
    {
        "schema_version",
        "result_id",
        "run_id",
        "lifecycle_id",
        "as_of_date",
        "ts_code",
        "stock_name",
        "result_lifecycle_stage",
        "headline_tone",
        "headline_detail",
        "summary_lines",
        "source_field_whitelist",
    }
)
STOCK_AI_EXPLAINER_FORBIDDEN_INPUT_FIELDS = frozenset(
    {
        "current_result_pointer",
        "result_registry_current",
        "run_registry_current",
        "artifact_registry_current",
        "stock_entry_guard",
        "entry_guard",
        "release_decision",
        "rollback_decision",
        "promotion_decision",
        "update_current_pointer",
        "rewrite_pointer",
        "should_release",
        "should_rollback",
        "should_promote",
        "t12_secondary_conclusion",
    }
)

STOCK_AI_EXPLAINER_ALLOWED_OUTPUT_FIELDS = frozenset(
    {
        "schema_version",
        "trace_id",
        "output_level",
        "output_role",
        "display_label",
        "summary_title",
        "summary_text",
        "supporting_facts",
        "risk_flags",
        "source_field_refs",
    }
)
STOCK_AI_EXPLAINER_REQUIRED_OUTPUT_FIELDS = frozenset(
    {
        "schema_version",
        "trace_id",
        "output_level",
        "output_role",
        "display_label",
        "summary_title",
        "summary_text",
        "supporting_facts",
        "risk_flags",
        "source_field_refs",
    }
)
STOCK_AI_EXPLAINER_FORBIDDEN_OUTPUT_FIELDS = frozenset(
    {
        "current_result_pointer",
        "result_registry_current",
        "run_registry_current",
        "artifact_registry_current",
        "approve_release",
        "rollback_decision",
        "release_decision",
        "promotion_decision",
        "update_current_pointer",
        "rewrite_pointer",
        "should_release",
        "should_rollback",
        "should_promote",
    }
)

_TRUE_VALUES = {"1", "true", "yes", "on"}
StockAIExplainerProvider = Callable[[dict[str, Any]], dict[str, Any]]
_FALSE_VISIBLE_RESULT = {
    "enabled": False,
    "visible": False,
    "status": "disabled",
    "degraded": False,
    "reason": "ai_stock_explainer_disabled",
    "trace_id": "",
    "display_label": "AI 解释",
    "summary_title": "",
    "summary_text": "",
    "risk_flags": [],
    "supporting_facts": [],
    "source_field_refs": [],
}


def _provider_stub_name() -> str:
    return _text(os.getenv("STOCK_AI_PROVIDER_STUB"), "local_preview")


def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in _TRUE_VALUES


def _text(value: object, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _normalize_lines(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def is_stock_ai_explainer_scope(base_path: str) -> bool:
    raw = str(base_path or "").strip()
    if not raw:
        return False
    normalized = "/" + raw.strip("/")
    return normalized.lower() == "/stock"


def resolve_stock_ai_explainer_flags() -> dict[str, bool]:
    global_enabled = _flag("STOCK_AI_F0_GLOBAL")
    stock_enabled = _flag("STOCK_AI_F1_STOCK_EXPLAINER")
    a0_enabled = _flag("STOCK_AI_F2_A0_DISPLAY")
    a1_enabled = _flag("STOCK_AI_F2_A1_EXPLAINER")
    enabled = global_enabled and stock_enabled and a1_enabled
    return {
        "global_enabled": global_enabled,
        "stock_enabled": stock_enabled,
        "a0_enabled": a0_enabled,
        "a1_enabled": a1_enabled,
        "enabled": enabled,
    }


def build_stock_ai_explainer_input(
    *,
    primary_result: dict[str, Any],
    primary_result_query: dict[str, Any],
    decision_semantics: dict[str, Any],
    blocker_semantics: dict[str, Any],
    execution_semantics: dict[str, Any],
    evidence_semantics: dict[str, Any],
    governance_semantics: dict[str, Any],
    current_basket_pointer_status: str,
    current_basket_pointer_basket_id: str,
    current_basket_pointer_updated_at: str,
    latest_basket_attempt_status: str,
    latest_basket_attempt_generated_at: str,
    latest_basket_attempt_blocking_reason: str,
    observation_wait_status: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema_version": STOCK_AI_EXPLAINER_INPUT_SCHEMA_VERSION,
        "result_id": _text(primary_result.get("result_id"), "primary:unavailable"),
        "run_id": _text(primary_result.get("run_id"), "run_id unavailable"),
        "lifecycle_id": _text(primary_result.get("lifecycle_id"), "lifecycle_id unavailable"),
        "as_of_date": _text(primary_result.get("as_of_date"), "as_of_date unavailable"),
        "ts_code": _text(primary_result.get("ts_code"), "对象暂缺"),
        "stock_name": _text(primary_result.get("stock_name"), "名称暂缺"),
        "result_lifecycle_stage": _text(primary_result.get("result_lifecycle_stage"), "阶段待确认"),
        "result_type": _text(primary_result.get("result_type")),
        "audit_status": _text(primary_result.get("audit_status")),
        "execution_status": _text(primary_result.get("execution_status")),
        "terminal_outcome": _text(primary_result.get("terminal_outcome")),
        "disabled_reason": _text(primary_result.get("disabled_reason")),
        "invalid_reason": _text(primary_result.get("invalid_reason")),
        "headline_tone": _text(primary_result_query.get("headline_tone")),
        "headline_detail": _text(primary_result_query.get("headline_detail")),
        "summary_lines": _normalize_lines(primary_result_query.get("summary_lines")),
        "data_sync_note": _text(primary_result.get("data_sync_note")),
        "history_source_file": _text(primary_result.get("history_source_file")),
        "history_generation_mode": _text(primary_result.get("history_generation_mode")),
        "decision_semantics": dict(decision_semantics or {}),
        "blocker_semantics": dict(blocker_semantics or {}),
        "execution_semantics": dict(execution_semantics or {}),
        "evidence_semantics": dict(evidence_semantics or {}),
        "governance_semantics": dict(governance_semantics or {}),
        "observation_wait_status": dict(observation_wait_status or {}),
        "current_basket_pointer_status": _text(current_basket_pointer_status, "-"),
        "current_basket_pointer_basket_id": _text(current_basket_pointer_basket_id, "-"),
        "current_basket_pointer_updated_at": _text(current_basket_pointer_updated_at, "-"),
        "latest_basket_attempt_status": _text(latest_basket_attempt_status, "-"),
        "latest_basket_attempt_generated_at": _text(latest_basket_attempt_generated_at, "-"),
        "latest_basket_attempt_blocking_reason": _text(latest_basket_attempt_blocking_reason, "-"),
    }
    payload["source_field_whitelist"] = sorted(
        key for key in payload.keys() if key != "source_field_whitelist"
    )
    return payload


def validate_stock_ai_explainer_input(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    keys = set(payload.keys())
    unexpected = sorted(keys - STOCK_AI_EXPLAINER_ALLOWED_INPUT_FIELDS)
    missing = sorted(field for field in STOCK_AI_EXPLAINER_REQUIRED_INPUT_FIELDS if field not in payload)
    forbidden = sorted(key for key in keys if key in STOCK_AI_EXPLAINER_FORBIDDEN_INPUT_FIELDS)
    if unexpected:
        errors.append(f"unexpected_input_fields:{','.join(unexpected)}")
    if missing:
        errors.append(f"missing_input_fields:{','.join(missing)}")
    if forbidden:
        errors.append(f"forbidden_input_fields:{','.join(forbidden)}")
    if payload.get("schema_version") != STOCK_AI_EXPLAINER_INPUT_SCHEMA_VERSION:
        errors.append("invalid_input_schema_version")
    if not _normalize_lines(payload.get("summary_lines")):
        errors.append("summary_lines_must_not_be_empty")
    source_whitelist = payload.get("source_field_whitelist")
    if not isinstance(source_whitelist, list) or not source_whitelist:
        errors.append("source_field_whitelist_missing")
    elif sorted(str(item) for item in source_whitelist) != sorted(str(item) for item in keys if item != "source_field_whitelist"):
        errors.append("source_field_whitelist_mismatch")
    return errors


def _build_trace_id(result_id: str, as_of_date: str, run_id: str) -> str:
    raw = f"{result_id}|{as_of_date}|{run_id}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    return f"stock-ai-{digest}"


def _local_stock_ai_provider(request: dict[str, Any]) -> dict[str, Any]:
    payload = dict(request.get("payload") or {})
    result_id = _text(payload.get("result_id"), "primary:unavailable")
    ts_code = _text(payload.get("ts_code"), "对象暂缺")
    stock_name = _text(payload.get("stock_name"), "名称暂缺")
    stage = _text(payload.get("result_lifecycle_stage"), "阶段待确认")
    disabled_reason = _text(payload.get("disabled_reason"))
    invalid_reason = _text(payload.get("invalid_reason"))
    latest_attempt_status = _text(payload.get("latest_basket_attempt_status"), "-")
    latest_attempt_reason = _text(payload.get("latest_basket_attempt_blocking_reason"), "-")
    execution_semantics = payload.get("execution_semantics") or {}
    governance_semantics = payload.get("governance_semantics") or {}
    decision_action = _text(execution_semantics.get("decision_action"), "继续观察")
    gate_status = _text(governance_semantics.get("gate_overall_status"), "待确认")
    summary_text = (
        f"{result_id} 对应 {ts_code} {stock_name}，当前处于 {stage}。"
        f" 页面仍以唯一 pointer 主结果为准，当前动作保持 {decision_action}，门禁状态 {gate_status}。"
    )
    risk_flags: list[str] = []
    if disabled_reason:
        summary_text += f" 当前存在制度阻断：{disabled_reason}。"
        risk_flags.append("制度阻断")
    elif invalid_reason:
        summary_text += f" 当前结果已终局：{invalid_reason}。"
        risk_flags.append("结果终局")
    elif latest_attempt_status == "blocked":
        summary_text += f" 最新候选篮尝试被阻断：{latest_attempt_reason}。"
        risk_flags.append("候选篮阻断")
    return {
        "adapter_schema_version": STOCK_AI_PROVIDER_ADAPTER_SCHEMA_VERSION,
        "provider_name": request.get("provider_name", "local_preview"),
        "request_id": request.get("request_id", ""),
        "timeout_ms": request.get("timeout_ms", 0),
        "retry_count": request.get("retry_count", 0),
        "status": "ok",
        "payload": {
            "schema_version": STOCK_AI_EXPLAINER_OUTPUT_SCHEMA_VERSION,
            "trace_id": _build_trace_id(
                _text(payload.get("result_id")),
                _text(payload.get("as_of_date")),
                _text(payload.get("run_id")),
            ),
            "output_level": "A1",
            "output_role": "R-A1",
            "display_label": "AI 解释",
            "summary_title": "AI 解释只做说明增强",
            "summary_text": summary_text,
            "supporting_facts": [
                _text(payload.get("headline_tone")),
                _text(payload.get("headline_detail")),
            ],
            "risk_flags": risk_flags,
            "source_field_refs": [
                "result_id",
                "ts_code",
                "stock_name",
                "result_lifecycle_stage",
                "disabled_reason",
                "invalid_reason",
                "headline_tone",
                "headline_detail",
            ],
        }
    }


def _echo_stock_ai_provider(request: dict[str, Any]) -> dict[str, Any]:
    payload = dict(request.get("payload") or {})
    summary_lines = _normalize_lines(payload.get("summary_lines"))
    return {
        "adapter_schema_version": STOCK_AI_PROVIDER_ADAPTER_SCHEMA_VERSION,
        "provider_name": request.get("provider_name", "echo_summary"),
        "request_id": request.get("request_id", ""),
        "timeout_ms": request.get("timeout_ms", 0),
        "retry_count": request.get("retry_count", 0),
        "status": "ok",
        "payload": {
            "schema_version": STOCK_AI_EXPLAINER_OUTPUT_SCHEMA_VERSION,
            "trace_id": _build_trace_id(
                _text(payload.get("result_id")),
                _text(payload.get("as_of_date")),
                _text(payload.get("run_id")),
            ),
            "output_level": "A1",
            "output_role": "R-A1",
            "display_label": "AI 解释",
            "summary_title": "AI 解释受控回声预览",
            "summary_text": "；".join(summary_lines[:2]) or "暂无可解释摘要。",
            "supporting_facts": [
                _text(payload.get("headline_tone")),
                _text(payload.get("headline_detail")),
            ],
            "risk_flags": [],
            "source_field_refs": [
                "headline_tone",
                "headline_detail",
                "summary_lines",
            ],
        }
    }


def _timeout_stock_ai_provider(_: dict[str, Any]) -> dict[str, Any]:
    raise TimeoutError("provider_timeout")


def _empty_response_stock_ai_provider(_: dict[str, Any]) -> dict[str, Any]:
    return {}


def _non_dict_response_stock_ai_provider(_: dict[str, Any]) -> dict[str, Any]:
    return "not-a-dict"  # type: ignore[return-value]


def _missing_payload_stock_ai_provider(_: dict[str, Any]) -> dict[str, Any]:
    return {"adapter_schema_version": STOCK_AI_EXPLAINER_PROVIDER_STUB_VERSION}


def _payload_not_dict_stock_ai_provider(_: dict[str, Any]) -> dict[str, Any]:
    return {"payload": "not-a-dict"}


def _missing_required_fields_stock_ai_provider(_: dict[str, Any]) -> dict[str, Any]:
    return {
        "payload": {
            "schema_version": STOCK_AI_EXPLAINER_OUTPUT_SCHEMA_VERSION,
            "trace_id": "stock-ai-missing-fields",
            "output_level": "A1",
        }
    }


def _invalid_schema_stock_ai_provider(_: dict[str, Any]) -> dict[str, Any]:
    return {
        "payload": {
            "schema_version": "stock_ai_explainer_output.v0",
            "trace_id": "stock-ai-invalid-schema",
            "output_level": "A1",
            "output_role": "R-A1",
            "display_label": "AI 解释",
            "summary_title": "invalid schema",
            "summary_text": "invalid schema",
            "supporting_facts": [],
            "risk_flags": [],
            "source_field_refs": [],
        }
    }


def _forbidden_output_field_stock_ai_provider(_: dict[str, Any]) -> dict[str, Any]:
    return {
        "payload": {
            "schema_version": STOCK_AI_EXPLAINER_OUTPUT_SCHEMA_VERSION,
            "trace_id": "stock-ai-forbidden-field",
            "output_level": "A1",
            "output_role": "R-A1",
            "display_label": "AI 解释",
            "summary_title": "forbidden field",
            "summary_text": "forbidden field",
            "supporting_facts": [],
            "risk_flags": [],
            "source_field_refs": [],
            "release_decision": "approve",
        }
    }


def _status_code_error_stock_ai_provider(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "adapter_schema_version": STOCK_AI_PROVIDER_ADAPTER_SCHEMA_VERSION,
        "provider_name": request.get("provider_name", "status_code_error_stub"),
        "request_id": request.get("request_id", ""),
        "timeout_ms": request.get("timeout_ms", 0),
        "retry_count": request.get("retry_count", 0),
        "status": "error",
        "status_code": 503,
        "payload": {"schema_version": STOCK_AI_EXPLAINER_OUTPUT_SCHEMA_VERSION},
    }


def _oversized_response_stock_ai_provider(request: dict[str, Any]) -> dict[str, Any]:
    oversized_text = "X" * (int(request.get("max_response_bytes", 0) or 0) + 64)
    return {
        "adapter_schema_version": STOCK_AI_PROVIDER_ADAPTER_SCHEMA_VERSION,
        "provider_name": request.get("provider_name", "oversized_response_stub"),
        "request_id": request.get("request_id", ""),
        "timeout_ms": request.get("timeout_ms", 0),
        "retry_count": request.get("retry_count", 0),
        "status": "ok",
        "status_code": 200,
        "payload": {
            "schema_version": STOCK_AI_EXPLAINER_OUTPUT_SCHEMA_VERSION,
            "trace_id": "oversized",
            "output_level": "A1",
            "output_role": "R-A1",
            "display_label": "AI 解释",
            "summary_title": "oversized",
            "summary_text": oversized_text,
            "supporting_facts": [],
            "risk_flags": [],
            "source_field_refs": [],
        },
    }


def resolve_stock_ai_explainer_provider(provider: StockAIExplainerProvider | None = None) -> tuple[str, StockAIExplainerProvider]:
    if provider is not None:
        return ("injected", provider)
    stub_name = _provider_stub_name()
    providers: dict[str, StockAIExplainerProvider] = {
        "local_preview": _local_stock_ai_provider,
        "echo_summary": _echo_stock_ai_provider,
        "timeout_stub": _timeout_stock_ai_provider,
        "empty_response_stub": _empty_response_stock_ai_provider,
        "non_dict_response_stub": _non_dict_response_stock_ai_provider,
        "missing_payload_stub": _missing_payload_stock_ai_provider,
        "payload_not_dict_stub": _payload_not_dict_stock_ai_provider,
        "missing_required_fields_stub": _missing_required_fields_stock_ai_provider,
        "invalid_schema_stub": _invalid_schema_stock_ai_provider,
        "forbidden_output_field_stub": _forbidden_output_field_stock_ai_provider,
        "status_code_error_stub": _status_code_error_stock_ai_provider,
        "oversized_response_stub": _oversized_response_stock_ai_provider,
    }
    if stub_name not in providers:
        raise ValueError(f"unknown_provider_stub:{stub_name}")
    return (stub_name, providers[stub_name])


def validate_stock_ai_explainer_output(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    keys = set(payload.keys())
    unexpected = sorted(keys - STOCK_AI_EXPLAINER_ALLOWED_OUTPUT_FIELDS)
    missing = sorted(field for field in STOCK_AI_EXPLAINER_REQUIRED_OUTPUT_FIELDS if field not in payload)
    forbidden = sorted(key for key in keys if key in STOCK_AI_EXPLAINER_FORBIDDEN_OUTPUT_FIELDS)
    if unexpected:
        errors.append(f"unexpected_output_fields:{','.join(unexpected)}")
    if missing:
        errors.append(f"missing_output_fields:{','.join(missing)}")
    if forbidden:
        errors.append(f"forbidden_output_fields:{','.join(forbidden)}")
    if payload.get("schema_version") != STOCK_AI_EXPLAINER_OUTPUT_SCHEMA_VERSION:
        errors.append("invalid_output_schema_version")
    if _text(payload.get("output_level")) != "A1":
        errors.append("invalid_output_level")
    if _text(payload.get("output_role")) != "R-A1":
        errors.append("invalid_output_role")
    if not _text(payload.get("summary_text")):
        errors.append("summary_text_must_not_be_empty")
    if not isinstance(payload.get("source_field_refs"), list):
        errors.append("source_field_refs_must_be_list")
    return errors


def build_stock_ai_explainer_result(
    *,
    base_path: str,
    primary_result: dict[str, Any],
    primary_result_query: dict[str, Any],
    decision_semantics: dict[str, Any],
    blocker_semantics: dict[str, Any],
    execution_semantics: dict[str, Any],
    evidence_semantics: dict[str, Any],
    governance_semantics: dict[str, Any],
    current_basket_pointer_status: str,
    current_basket_pointer_basket_id: str,
    current_basket_pointer_updated_at: str,
    latest_basket_attempt_status: str,
    latest_basket_attempt_generated_at: str,
    latest_basket_attempt_blocking_reason: str,
    observation_wait_status: dict[str, Any],
    storage_dir: str | os.PathLike[str] | None = None,
    provider: StockAIExplainerProvider | None = None,
) -> dict[str, Any]:
    flags = resolve_stock_ai_explainer_flags()
    if not is_stock_ai_explainer_scope(base_path) or not flags["enabled"]:
        return dict(_FALSE_VISIBLE_RESULT)

    input_payload = build_stock_ai_explainer_input(
        primary_result=primary_result,
        primary_result_query=primary_result_query,
        decision_semantics=decision_semantics,
        blocker_semantics=blocker_semantics,
        execution_semantics=execution_semantics,
        evidence_semantics=evidence_semantics,
        governance_semantics=governance_semantics,
        current_basket_pointer_status=current_basket_pointer_status,
        current_basket_pointer_basket_id=current_basket_pointer_basket_id,
        current_basket_pointer_updated_at=current_basket_pointer_updated_at,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_generated_at=latest_basket_attempt_generated_at,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
        observation_wait_status=observation_wait_status,
    )
    input_errors = validate_stock_ai_explainer_input(input_payload)
    if input_errors:
        return {
            "enabled": True,
            "visible": False,
            "status": "blocked",
            "degraded": False,
            "reason": ";".join(input_errors),
            "trace_id": "",
            "display_label": "AI 解释",
            "summary_title": "",
            "summary_text": "",
            "risk_flags": [],
            "supporting_facts": [],
            "source_field_refs": [],
            "input_payload": input_payload,
        }

    try:
        provider_name, selected_provider = resolve_stock_ai_explainer_provider(provider)
    except Exception as exc:
        return {
            "enabled": True,
            "visible": False,
            "status": "blocked",
            "degraded": False,
            "reason": f"provider_resolution_error:{exc}",
            "trace_id": "",
            "display_label": "AI 解释",
            "summary_title": "",
            "summary_text": "",
            "risk_flags": [],
            "supporting_facts": [],
            "source_field_refs": [],
            "input_payload": input_payload,
        }
    try:
        if provider_name == "injected":
            output = selected_provider(input_payload)
            provider_attempt_ledger = [
                {"state": "ready", "request_id": "", "provider_name": "injected", "reason": ""},
                {"state": "running", "request_id": "", "provider_name": "injected", "reason": ""},
                {"state": "ok", "request_id": "", "provider_name": "injected", "reason": ""},
            ]
            provider_telemetry_buffer = []
        else:
            output, provider_telemetry, provider_attempt_ledger, provider_telemetry_buffer = run_stock_ai_provider_attempt(
                provider_name=provider_name,
                payload=input_payload,
                providers={
                    "local_preview": _local_stock_ai_provider,
                    "echo_summary": _echo_stock_ai_provider,
                    "timeout_stub": _timeout_stock_ai_provider,
                    "empty_response_stub": _empty_response_stock_ai_provider,
                    "non_dict_response_stub": _non_dict_response_stock_ai_provider,
                    "missing_payload_stub": _missing_payload_stock_ai_provider,
                    "payload_not_dict_stub": _payload_not_dict_stock_ai_provider,
                    "missing_required_fields_stub": _missing_required_fields_stock_ai_provider,
                    "invalid_schema_stub": _invalid_schema_stock_ai_provider,
                    "forbidden_output_field_stub": _forbidden_output_field_stock_ai_provider,
                    "status_code_error_stub": _status_code_error_stock_ai_provider,
                    "oversized_response_stub": _oversized_response_stock_ai_provider,
                },
            )
        if provider_name == "injected":
            provider_telemetry = {
                "provider_name": "injected",
                "request_id": "",
                "timeout_ms": 0,
                "retry_count": 0,
                "status": "ok",
                "response_schema_version": "",
            }
    except StockAIProviderAttemptError as exc:
        result = {
            "enabled": True,
            "visible": False,
            "status": "degraded",
            "degraded": True,
            "reason": f"provider_error:{exc}",
            "trace_id": "",
            "display_label": "AI 解释",
            "summary_title": "",
            "summary_text": "",
            "risk_flags": [],
            "supporting_facts": [],
            "source_field_refs": [],
            "input_payload": input_payload,
            "provider_name": provider_name,
            "provider_telemetry": exc.telemetry,
            "provider_attempt_ledger": exc.ledger,
            "provider_telemetry_buffer": exc.telemetry_buffer,
            "provider_telemetry_summary": build_stock_ai_provider_telemetry_summary(exc.telemetry_buffer),
        }
        return _persist_stock_ai_runner_result(
            result=result,
            storage_dir=storage_dir,
            result_id=_text(primary_result.get("result_id"), "primary:unavailable"),
        )
    except Exception as exc:
        result = {
            "enabled": True,
            "visible": False,
            "status": "degraded",
            "degraded": True,
            "reason": f"provider_error:{exc}",
            "trace_id": "",
            "display_label": "AI 解释",
            "summary_title": "",
            "summary_text": "",
            "risk_flags": [],
            "supporting_facts": [],
            "source_field_refs": [],
            "input_payload": input_payload,
            "provider_name": provider_name,
            "provider_telemetry": {
                "provider_name": provider_name,
                "request_id": "",
                "timeout_ms": 0,
                "retry_count": 0,
                "status": "error",
                "status_code": 0,
                "response_bytes": 0,
                "elapsed_ms": 0,
                "network_mode": "offline_only",
                "response_schema_version": "",
            },
            "provider_attempt_ledger": [],
            "provider_telemetry_buffer": [],
            "provider_telemetry_summary": build_stock_ai_provider_telemetry_summary([]),
        }
        return _persist_stock_ai_runner_result(
            result=result,
            storage_dir=storage_dir,
            result_id=_text(primary_result.get("result_id"), "primary:unavailable"),
        )

    output_errors = validate_stock_ai_explainer_output(output)
    if output_errors:
        result = {
            "enabled": True,
            "visible": False,
            "status": "blocked",
            "degraded": False,
            "reason": ";".join(output_errors),
            "trace_id": _text(output.get("trace_id")),
            "display_label": "AI 解释",
            "summary_title": "",
            "summary_text": "",
            "risk_flags": [],
            "supporting_facts": [],
            "source_field_refs": [],
            "input_payload": input_payload,
            "provider_name": provider_name,
            "provider_telemetry": provider_telemetry,
            "provider_attempt_ledger": provider_attempt_ledger,
            "provider_telemetry_buffer": provider_telemetry_buffer,
            "provider_telemetry_summary": build_stock_ai_provider_telemetry_summary(provider_telemetry_buffer),
        }
        return _persist_stock_ai_runner_result(
            result=result,
            storage_dir=storage_dir,
            result_id=_text(primary_result.get("result_id"), "primary:unavailable"),
        )

    result = {
        "enabled": True,
        "visible": True,
        "status": "ready",
        "degraded": False,
        "reason": "",
        "trace_id": _text(output.get("trace_id")),
        "display_label": _text(output.get("display_label"), "AI 解释"),
        "summary_title": _text(output.get("summary_title")),
        "summary_text": _text(output.get("summary_text")),
        "risk_flags": [str(item) for item in output.get("risk_flags", []) if str(item).strip()],
        "supporting_facts": [str(item) for item in output.get("supporting_facts", []) if str(item).strip()],
        "source_field_refs": [str(item) for item in output.get("source_field_refs", []) if str(item).strip()],
        "input_payload": input_payload,
        "output_payload": output,
        "provider_name": provider_name,
        "provider_schema_version": STOCK_AI_EXPLAINER_PROVIDER_STUB_VERSION,
        "provider_telemetry": provider_telemetry,
        "provider_attempt_ledger": provider_attempt_ledger,
        "provider_telemetry_buffer": provider_telemetry_buffer,
        "provider_telemetry_summary": build_stock_ai_provider_telemetry_summary(provider_telemetry_buffer),
    }
    return _persist_stock_ai_runner_result(
        result=result,
        storage_dir=storage_dir,
        result_id=_text(primary_result.get("result_id"), "primary:unavailable"),
    )


def _persist_stock_ai_runner_result(
    *,
    result: dict[str, Any],
    storage_dir: str | os.PathLike[str] | None,
    result_id: str,
) -> dict[str, Any]:
    if not storage_dir:
        return result
    try:
        storage = StockAIRunnerStorage.from_path(storage_dir)
        persisted = storage.persist(
            result_id=result_id,
            provider_name=str(result.get("provider_name", "") or "unknown"),
            final_status=str(result.get("status", "") or "unknown"),
            attempt_ledger=[dict(item) for item in (result.get("provider_attempt_ledger") or []) if isinstance(item, dict)],
            telemetry_buffer=[dict(item) for item in (result.get("provider_telemetry_buffer") or []) if isinstance(item, dict)],
        )
        next_result = dict(result)
        next_result["provider_storage"] = persisted
        return next_result
    except Exception as exc:
        next_result = dict(result)
        next_result["provider_storage_error"] = str(exc)
        return next_result
