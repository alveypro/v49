from src.t12_governance_summary import (
    GOVERNANCE_ALLOWED_PLACEHOLDER_VALUES,
    GOVERNANCE_ALLOWED_PROGRESS_VALUES,
    build_current_governance_note,
    build_current_primary_blocker,
    build_current_progress_status,
    build_t12_governance_summary_view_model,
    extract_governance_summary_facts,
    normalize_governance_placeholder,
    render_t12_governance_summary_template,
)


def test_governance_summary_view_model_structure():
    vm = build_t12_governance_summary_view_model(
        {
            "audit_status": "passed",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
        }
    )
    assert set(vm.as_dict().keys()) == {
        "current_progress_status",
        "current_primary_blocker",
        "current_governance_note",
    }


def test_terminal_outcome_maps_to_closed_status():
    assert build_current_progress_status({"terminal_outcome": "expired"}) == "已终局"


def test_disabled_reason_maps_to_blocked_status():
    assert build_current_progress_status({"disabled_reason": "当前进入冻结窗口。"}) == "不可推进"


def test_audit_in_review_maps_to_pending_review():
    assert build_current_progress_status({"audit_status": "in_review"}) == "待审核"


def test_governance_progress_can_move_forward_when_facts_are_sufficient():
    assert build_current_progress_status(
        {
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "audit_status": "passed",
        }
    ) == "可推进"


def test_primary_blocker_priority_prefers_disabled_reason():
    blocker = build_current_primary_blocker(
        {
            "disabled_reason": "当前进入冻结窗口。",
            "terminal_outcome": "expired",
            "audit_status": "failed",
            "promotion_status": "rejected",
        }
    )
    assert blocker == "当前进入冻结窗口。"


def test_data_sync_note_is_used_directly():
    assert build_current_governance_note({"data_sync_note": "制度字段已对齐。"}) == "制度字段已对齐。"


def test_governance_placeholders_are_centralized():
    assert normalize_governance_placeholder("missing") == "暂缺"
    assert normalize_governance_placeholder("pending") == "待补充"
    assert normalize_governance_placeholder("degraded") == "降级说明"
    assert GOVERNANCE_ALLOWED_PLACEHOLDER_VALUES == {"暂缺", "待补充", "降级说明"}


def test_governance_template_is_read_only_without_buttons_or_bridge():
    html_text = render_t12_governance_summary_template(
        build_t12_governance_summary_view_model(
            {
                "result_lifecycle_stage": "L2",
                "candidate_status": "shortlisted",
                "risk_level": "low",
                "data_sync_note": "制度字段已对齐。",
            }
        )
    )
    assert 'id="t12-governance-summary"' in html_text
    assert "<button" not in html_text
    assert "<form" not in html_text
    assert "bridge" not in html_text.lower()
    assert "data-toggle" not in html_text
    assert "onclick=" not in html_text
    assert "fetch(" not in html_text
    assert "XMLHttpRequest" not in html_text
    assert "hx-" not in html_text
    assert 'data-action="' not in html_text
    assert 'role="button"' not in html_text


def test_extract_governance_summary_facts_does_not_write_back_source():
    source = {
        "disabled_reason": "当前进入冻结窗口。",
        "audit_status": "failed",
        "extra_field": "must_ignore",
    }
    extracted = extract_governance_summary_facts(source)
    extracted["disabled_reason"] = "changed"
    assert source["disabled_reason"] == "当前进入冻结窗口。"
    assert "extra_field" not in extracted


def test_irrelevant_fields_do_not_change_governance_summary_output():
    base = {
        "result_lifecycle_stage": "L2",
        "candidate_status": "shortlisted",
        "risk_level": "low",
        "data_sync_note": "制度字段已对齐。",
    }
    with_noise = dict(base)
    with_noise.update(
        {
            "top1_signal": "strong_buy",
            "stock_name": "平安银行",
            "unexpected": "noise",
        }
    )
    assert build_t12_governance_summary_view_model(base) == build_t12_governance_summary_view_model(with_noise)


def test_high_risk_without_existing_blocker_support_must_not_become_blocked():
    facts = {
        "result_lifecycle_stage": "L2",
        "candidate_status": "shortlisted",
        "risk_level": "high",
    }
    assert build_current_progress_status(facts) == "可推进"


def test_progress_output_is_limited_to_allowed_contract_values():
    values = {
        build_current_progress_status({"terminal_outcome": "expired"}),
        build_current_progress_status({"disabled_reason": "x"}),
        build_current_progress_status({"audit_status": "in_review"}),
        build_current_progress_status({"result_lifecycle_stage": "L2", "candidate_status": "shortlisted", "risk_level": "low"}),
        build_current_progress_status({}),
    }
    assert values.issubset(GOVERNANCE_ALLOWED_PROGRESS_VALUES)
