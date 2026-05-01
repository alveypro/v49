from __future__ import annotations

from openclaw.runtime.v49_trading_assistant_entry import (
    ASSISTANT_TAB_LABELS,
    is_v49_trading_assistant_route,
    render_v49_trading_assistant_entry,
    render_v49_trading_assistant_shell,
)


def _recorder(calls, name, return_value=None):
    def _inner(*args, **kwargs):
        calls.append((name, args, kwargs))
        return return_value

    return _inner


class _Session(dict):
    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value


class _FakeTab:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeSt:
    def __init__(self):
        self.session_state = _Session(openclaw_qa_assistant=object())
        self.errors = []
        self.infos = []
        self.tab_labels = []

    def tabs(self, labels):
        self.tab_labels = list(labels)
        return [_FakeTab() for _ in labels]

    def error(self, text):
        self.errors.append(text)

    def info(self, text):
        self.infos.append(text)


class _Assistant:
    def __init__(self, db_path):
        self.db_path = db_path


class _QaAssistant:
    pass


def test_v49_trading_assistant_route_predicate_freezes_scope():
    assert is_v49_trading_assistant_route({"root": "研究后台", "research": "智能助手"})
    assert not is_v49_trading_assistant_route({"root": "研究后台", "research": "股票池分析"})


def test_render_v49_trading_assistant_shell_freezes_subentry_order():
    calls = []
    st = _FakeSt()
    st.session_state["desired_assistant_tab"] = "交易工作台"

    render_v49_trading_assistant_shell(
        st=st,
        permanent_db_path="/tmp/db.sqlite",
        assistant_cls=_Assistant,
        qa_assistant_cls=_QaAssistant,
        render_page_header=_recorder(calls, "header"),
        focus_tab_by_text=_recorder(calls, "focus_tab"),
        set_focus_once=_recorder(calls, "focus"),
        render_qa_chat_shell=_recorder(calls, "qa_chat"),
        render_qa_self_learning_panel=_recorder(calls, "qa_learning"),
        render_qa_submission_controller=_recorder(calls, "qa_submit"),
        render_assistant_ops_tabs=_recorder(calls, "ops"),
        render_result_overview=_recorder(calls, "overview"),
        render_single_stock_eval_tab=_recorder(calls, "single"),
        notification_service_cls=object,
        airivo_has_role=_recorder(calls, "role"),
        airivo_guard_action=_recorder(calls, "guard"),
        airivo_append_action_audit=_recorder(calls, "audit"),
    )

    assert st.tab_labels == ASSISTANT_TAB_LABELS
    assert st.session_state.trading_assistant.db_path == "/tmp/db.sqlite"
    assert [call[0] for call in calls[:6]] == ["header", "focus_tab", "qa_chat", "qa_learning", "qa_submit", "ops"]


def test_render_v49_trading_assistant_entry_skips_non_matching_route():
    calls = []
    render_v49_trading_assistant_entry(
        routes={"root": "研究后台", "research": "股票池分析"},
        st=_FakeSt(),
        permanent_db_path="/tmp/db.sqlite",
        render_page_header=_recorder(calls, "header"),
        focus_tab_by_text=_recorder(calls, "focus_tab"),
        set_focus_once=_recorder(calls, "focus"),
        render_qa_chat_shell=_recorder(calls, "qa"),
        render_qa_self_learning_panel=_recorder(calls, "learning"),
        render_qa_submission_controller=_recorder(calls, "submit"),
        render_assistant_ops_tabs=_recorder(calls, "ops"),
        render_result_overview=_recorder(calls, "overview"),
        render_single_stock_eval_tab=_recorder(calls, "single"),
        notification_service_cls=object,
        airivo_has_role=_recorder(calls, "role"),
        airivo_guard_action=_recorder(calls, "guard"),
        airivo_append_action_audit=_recorder(calls, "audit"),
    )

    assert calls == []
