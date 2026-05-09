from src.t12_overview_card import (
    build_t12_overview_card_view_model,
    render_t12_overview_card_template,
    select_t12_primary_source,
    t12_missing_text,
)


def test_t12_view_model_structure():
    vm = build_t12_overview_card_view_model(
        {
            "alert_status": "pass",
            "review_status": "pass",
            "threshold_applied": True,
            "threshold_recommendation": "tighten_thresholds",
            "rollback_triggered": False,
            "rollback_would_apply": False,
            "rollback_event_count": 0,
            "stable_threshold_count": 3,
            "source_timestamps": {
                "t12_threshold_control_latest.json": "2026-04-12 09:00:00",
            },
        }
    )
    data = vm.as_dict()
    assert set(data.keys()) == {
        "formal_stage_title",
        "result_type_label",
        "phase_detail",
        "governance_status_label",
        "governance_status_detail",
        "primary_source_label",
        "primary_source_detail",
        "note_label",
        "note_detail",
    }


def test_t12_formal_stage_title_uses_single_official_name():
    vm = build_t12_overview_card_view_model(
        {
            "alert_status": "fail",
            "review_status": "pass",
            "threshold_applied": True,
            "threshold_recommendation": "tighten_thresholds",
            "source_timestamps": {
                "t12_threshold_control_latest.json": "2026-04-12 09:00:00",
            },
        }
    )
    assert vm.formal_stage_title == "T12 阈值控制"
    assert vm.result_type_label == "阈值已应用"


def test_t12_status_stays_separate_from_main_stage():
    vm = build_t12_overview_card_view_model(
        {
            "alert_status": "fail",
            "review_status": "warn",
            "threshold_applied": True,
            "threshold_recommendation": "tighten_thresholds",
            "rollback_triggered": False,
            "rollback_event_count": 2,
            "stable_threshold_count": 3,
            "source_timestamps": {
                "t12_threshold_control_latest.json": "2026-04-12 09:00:00",
            },
        }
    )
    assert vm.formal_stage_title == "T12 阈值控制"
    assert vm.governance_status_label == "告警 失败 / 复核 告警 / 演练 未触发"


def test_t12_missing_values_use_central_mapping_only():
    vm = build_t12_overview_card_view_model({"source_timestamps": {}})
    html_text = render_t12_overview_card_template(vm)
    assert t12_missing_text("missing") == "暂缺"
    assert t12_missing_text("pending") == "待补充"
    assert t12_missing_text("degraded") == "降级说明"
    assert "同步信息暂缺" not in html_text
    assert "阶段信息暂缺" not in html_text
    assert "暂缺" in html_text
    assert "待补充" in html_text
    assert "降级说明" in html_text


def test_t12_template_renders_four_blocks():
    vm = build_t12_overview_card_view_model(
        {
            "alert_status": "pass",
            "review_status": "pass",
            "threshold_applied": True,
            "threshold_recommendation": "tighten_thresholds",
            "rollback_triggered": False,
            "rollback_would_apply": False,
            "rollback_event_count": 0,
            "stable_threshold_count": 3,
            "source_timestamps": {
                "t12_threshold_control_latest.json": "2026-04-12 09:00:00",
            },
        }
    )
    html_text = render_t12_overview_card_template(vm)
    assert 'id="t12-overview-card"' in html_text
    assert html_text.count('class="t12-overview-block') == 4
    assert "主阶段" in html_text
    assert "状态镜像" in html_text
    assert "主来源" in html_text
    assert "备注" in html_text


def test_t12_primary_source_selection_is_fixed():
    source_name, source_timestamp = select_t12_primary_source(
        {
            "source_timestamps": {
                "t12_alert_center_latest.json": "2026-04-12 09:01:00",
                "t12_review_checklist_latest.json": "2026-04-12 09:02:00",
                "t12_threshold_control_latest.json": "2026-04-12 09:00:00",
            }
        }
    )
    assert source_name == "t12_threshold_control_latest.json"
    assert source_timestamp == "2026-04-12 09:00:00"
