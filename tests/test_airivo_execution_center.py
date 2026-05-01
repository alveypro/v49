from __future__ import annotations

import importlib.util
from pathlib import Path


_STUB_PATH = Path(__file__).resolve().parents[1] / "openclaw" / "runtime" / "streamlit_stub.py"
_STUB_SPEC = importlib.util.spec_from_file_location("streamlit_stub_for_test", str(_STUB_PATH))
assert _STUB_SPEC is not None and _STUB_SPEC.loader is not None
streamlit_stub = importlib.util.module_from_spec(_STUB_SPEC)
_STUB_SPEC.loader.exec_module(streamlit_stub)
streamlit_stub.install_streamlit_stub()

_MODULE_PATH = Path(__file__).resolve().parents[1] / "openclaw" / "runtime" / "airivo_execution_center.py"
_SPEC = importlib.util.spec_from_file_location("airivo_execution_center_for_test", str(_MODULE_PATH))
assert _SPEC is not None and _SPEC.loader is not None
center = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(center)


def test_summarize_release_detail_builds_business_facing_sections():
    payload = center._build_release_detail_payload(
        selected_row={
            "decision_date": "2026-04-30",
            "release_status": "canary",
            "approved_by": "alice",
            "approved_at": "2026-04-30 09:40",
            "release_note": "先灰度验证盘中成交质量",
            "rollback_reason": "",
            "replaces_decision_date": "2026-04-29",
            "replaced_by_decision_date": "",
            "source_type": "overnight",
            "source_label": "v49-main",
            "trade_date": "2026-04-30",
            "selected_count": 6,
            "is_active": 0,
        },
        release_decision="canary",
        gate_review={
            "passed": True,
            "summary": "批次质量可用，但需要先做小范围灰度。",
            "blocking_gates": [],
            "review_gates": ["risk_review"],
            "canary_gates": ["execution_rate_low"],
            "validation_review": {
                "execution_rate": 62.5,
                "avg_realized_return_pct": 1.8,
                "win_rate": 54.0,
                "gates": ["execution_rate_low"],
            },
            "validation_streak": {"consecutive_severe_runs": 2},
            "governance_review": {"status": "tracked", "summary": "已记录本次灰度审批。", "gates": ["risk_canary"]},
        },
        canary_scope={
            "allowed_buckets": ["direct_execute", "observe"],
            "sample_limit": 2,
            "window_start": "09:35",
            "window_end": "10:15",
            "selected_codes": ["000001.SZ", "600000.SH"],
            "scope_note": "优先看高流动性样本",
        },
        outcome_review={"summary": "尚未形成完整盘后结果。"},
        override_audits=[
            {
                "operator_name": "alice",
                "created_at": "2026-04-30 09:41",
                "override_reason": "灰度验证后再决定是否全量",
                "requested_decision": "canary",
            }
        ],
        structured_count=6,
    )

    summary = center._summarize_release_detail(payload)

    assert summary["overview"]["decision"] == "灰度发布"
    assert summary["overview"]["source"] == "overnight/v49-main"
    assert any("小范围灰度" in line for line in summary["gate_lines"])
    assert any("执行率 62.5%" in line for line in summary["validation_lines"])
    assert any("连续触发 2 次" in line for line in summary["validation_lines"])
    assert any("治理复核" in line for line in summary["governance_lines"])
    assert any("样本上限 2" in line for line in summary["canary_lines"])
    assert any("例外批准" in line for line in summary["override_lines"])
    assert any("替换 2026-04-29" in line for line in summary["relation_lines"])
