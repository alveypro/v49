from src.stock_ai_explainer import (
    build_stock_ai_explainer_result,
    validate_stock_ai_explainer_input,
)


def _base_kwargs() -> dict[str, object]:
    return {
        "base_path": "/stock",
        "primary_result": {
            "result_id": "primary:000001.SZ",
            "run_id": "run-001",
            "lifecycle_id": "lifecycle-001",
            "as_of_date": "2026-04-28",
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L4",
            "disabled_reason": "",
            "invalid_reason": "",
        },
        "primary_result_query": {
            "headline_tone": "继续观察",
            "headline_detail": "主结果 primary:000001.SZ 保持锁定",
            "summary_lines": ["先看 pointer", "继续观察"],
        },
        "decision_semantics": {"headline": "primary:000001.SZ 是当前唯一主结论对象"},
        "blocker_semantics": {"has_blocker": False},
        "execution_semantics": {"decision_action": "进入复核"},
        "evidence_semantics": {"score_gap": "12.0"},
        "governance_semantics": {"gate_overall_status": "通过"},
        "current_basket_pointer_status": "approved",
        "current_basket_pointer_basket_id": "basket-001",
        "current_basket_pointer_updated_at": "2026-04-28T10:00:00",
        "latest_basket_attempt_status": "-",
        "latest_basket_attempt_generated_at": "-",
        "latest_basket_attempt_blocking_reason": "-",
        "observation_wait_status": {},
    }


def test_stock_ai_explainer_disabled_by_default():
    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["enabled"] is False
    assert result["visible"] is False
    assert result["status"] == "disabled"


def test_stock_ai_explainer_ready_for_stock_scope_when_flags_enabled(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["enabled"] is True
    assert result["visible"] is True
    assert result["status"] == "ready"
    assert result["display_label"] == "AI 解释"
    assert result["provider_name"] == "local_preview"
    assert result["provider_telemetry"]["provider_name"] == "local_preview"
    assert result["provider_telemetry"]["request_id"].startswith("stock-ai-req-")
    assert result["provider_telemetry"]["timeout_ms"] == 1200
    assert result["provider_telemetry"]["retry_count"] == 0
    assert result["provider_telemetry"]["status"] == "ok"
    assert result["provider_telemetry"]["network_mode"] == "offline_only"
    assert [item["state"] for item in result["provider_attempt_ledger"]] == ["ready", "running", "ok"]
    assert len(result["provider_telemetry_buffer"]) == 1
    assert result["provider_telemetry_summary"]["total_calls"] == 1
    assert result["provider_telemetry_summary"]["error_calls"] == 0
    assert "唯一 pointer 主结果为准" in str(result["summary_text"])


def test_stock_ai_explainer_stays_hidden_outside_stock_scope(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    kwargs = _base_kwargs()
    kwargs["base_path"] = "/t12"

    result = build_stock_ai_explainer_result(**kwargs)

    assert result["enabled"] is False
    assert result["visible"] is False
    assert result["status"] == "disabled"


def test_stock_ai_explainer_can_switch_to_echo_provider_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "echo_summary")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is True
    assert result["provider_name"] == "echo_summary"
    assert result["provider_telemetry"]["provider_name"] == "echo_summary"
    assert result["summary_title"] == "AI 解释受控回声预览"
    assert "先看 pointer" in str(result["summary_text"])


def test_stock_ai_explainer_blocks_unknown_provider_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "bad_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["enabled"] is True
    assert result["visible"] is False
    assert result["status"] == "blocked"
    assert "provider_resolution_error" in str(result["reason"])


def test_stock_ai_explainer_degrades_closed_on_timeout_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "timeout_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "degraded"
    assert result["degraded"] is True
    assert result["provider_name"] == "timeout_stub"
    assert result["provider_telemetry"]["status"] == "timeout"
    assert [item["state"] for item in result["provider_attempt_ledger"]] == ["ready", "running", "timeout"]


def test_stock_ai_explainer_degrades_closed_on_status_code_error_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "status_code_error_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "degraded"
    assert result["degraded"] is True
    assert result["provider_telemetry"]["status"] == "blocked"
    assert [item["state"] for item in result["provider_attempt_ledger"]] == ["ready", "running", "blocked"]


def test_stock_ai_explainer_degrades_closed_on_oversized_response_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "oversized_response_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "degraded"
    assert result["degraded"] is True


def test_stock_ai_explainer_degrades_closed_on_empty_response_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "empty_response_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "degraded"
    assert result["degraded"] is True


def test_stock_ai_explainer_degrades_closed_on_non_dict_response_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "non_dict_response_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "degraded"
    assert result["degraded"] is True


def test_stock_ai_explainer_degrades_closed_on_missing_payload_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "missing_payload_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "degraded"
    assert result["degraded"] is True


def test_stock_ai_explainer_degrades_closed_on_payload_not_dict_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "payload_not_dict_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "degraded"
    assert result["degraded"] is True


def test_stock_ai_explainer_degrades_closed_on_provider_error(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")

    def broken_provider(_: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("provider down")

    result = build_stock_ai_explainer_result(**_base_kwargs(), provider=broken_provider)

    assert result["enabled"] is True
    assert result["visible"] is False
    assert result["status"] == "degraded"
    assert result["degraded"] is True


def test_stock_ai_explainer_input_whitelist_gate_blocks_forbidden_l1_fields():
    kwargs = _base_kwargs()
    payload = dict(kwargs["primary_result"])
    payload["current_result_pointer"] = "pointer-001"

    errors = validate_stock_ai_explainer_input(
        {
            **kwargs["primary_result_query"],
            **payload,
            "schema_version": "stock_ai_explainer_input.v1",
            "decision_semantics": kwargs["decision_semantics"],
            "blocker_semantics": kwargs["blocker_semantics"],
            "execution_semantics": kwargs["execution_semantics"],
            "evidence_semantics": kwargs["evidence_semantics"],
            "governance_semantics": kwargs["governance_semantics"],
            "current_basket_pointer_status": kwargs["current_basket_pointer_status"],
            "current_basket_pointer_basket_id": kwargs["current_basket_pointer_basket_id"],
            "current_basket_pointer_updated_at": kwargs["current_basket_pointer_updated_at"],
            "latest_basket_attempt_status": kwargs["latest_basket_attempt_status"],
            "latest_basket_attempt_generated_at": kwargs["latest_basket_attempt_generated_at"],
            "latest_basket_attempt_blocking_reason": kwargs["latest_basket_attempt_blocking_reason"],
            "observation_wait_status": kwargs["observation_wait_status"],
            "source_field_whitelist": [
                "result_id",
            ],
        }
    )

    assert any("forbidden_input_fields" in item for item in errors)


def test_stock_ai_explainer_input_whitelist_gate_blocks_mismatched_source_refs():
    kwargs = _base_kwargs()

    errors = validate_stock_ai_explainer_input(
        {
            "schema_version": "stock_ai_explainer_input.v1",
            "result_id": "primary:000001.SZ",
            "run_id": "run-001",
            "lifecycle_id": "lifecycle-001",
            "as_of_date": "2026-04-28",
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L4",
            "headline_tone": "继续观察",
            "headline_detail": "主结果 primary:000001.SZ 保持锁定",
            "summary_lines": ["先看 pointer"],
            "source_field_whitelist": ["result_id", "ts_code"],
        }
    )

    assert "source_field_whitelist_mismatch" in errors


def test_stock_ai_explainer_blocks_forbidden_output_fields(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")

    def bad_provider(_: dict[str, object]) -> dict[str, object]:
        return {
            "schema_version": "stock_ai_explainer_output.v1",
            "trace_id": "stock-ai-test",
            "output_level": "A1",
            "output_role": "R-A1",
            "display_label": "AI 解释",
            "summary_title": "bad",
            "summary_text": "bad",
            "supporting_facts": [],
            "risk_flags": [],
            "source_field_refs": [],
            "release_decision": "approve",
        }

    result = build_stock_ai_explainer_result(**_base_kwargs(), provider=bad_provider)

    assert result["enabled"] is True
    assert result["visible"] is False
    assert result["status"] == "blocked"
    assert "unexpected_output_fields" in str(result["reason"]) or "forbidden_output_fields" in str(result["reason"])


def test_stock_ai_explainer_blocks_missing_required_output_fields(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "missing_required_fields_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "blocked"
    assert "missing_output_fields" in str(result["reason"])


def test_stock_ai_explainer_blocks_invalid_output_schema_version(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "invalid_schema_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "blocked"
    assert "invalid_output_schema_version" in str(result["reason"])


def test_stock_ai_explainer_blocks_forbidden_output_field_stub(monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "forbidden_output_field_stub")

    result = build_stock_ai_explainer_result(**_base_kwargs())

    assert result["visible"] is False
    assert result["status"] == "blocked"
    assert "unexpected_output_fields" in str(result["reason"])


def test_stock_ai_explainer_persists_runner_storage_when_storage_dir_provided(tmp_path, monkeypatch):
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "echo_summary")

    result = build_stock_ai_explainer_result(**_base_kwargs(), storage_dir=tmp_path / "artifacts" / "stock_ai_runner")

    assert result["visible"] is True
    assert "provider_storage" in result
    assert result["provider_storage"]["ledger_entries"] >= 3
    assert result["provider_storage"]["telemetry_entries"] >= 1
