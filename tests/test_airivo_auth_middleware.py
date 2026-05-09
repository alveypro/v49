from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from openclaw.services import airivo_auth_middleware as mw


class _FakeStreamlit:
    def __init__(self, path: str = "/app", query_params: dict | None = None):
        self.context = SimpleNamespace(path=path, headers={}, cookies={})
        self.session_state = {}
        self.query_params = query_params or {}
        self.switch_page_calls = []
        self.error_messages = []

    def switch_page(self, path: str) -> None:
        self.switch_page_calls.append(path)

    def error(self, message: str) -> None:
        self.error_messages.append(message)

    def stop(self) -> None:
        raise RuntimeError("streamlit_stop")


def test_resolve_auth_decision_redirects_without_user(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_st = _FakeStreamlit(path="/app")
    monkeypatch.setattr(mw, "st", fake_st)
    monkeypatch.setattr(mw, "AUTH_ENABLED", True)
    monkeypatch.setattr(mw, "get_current_user", lambda: None)

    decision = mw.resolve_auth_decision()

    assert decision["decision"] == "redirect_login"
    assert decision["reason"] == "no_valid_auth_context"
    assert decision["path"] == "/app"


def test_resolve_auth_decision_allows_whitelisted_path(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_st = _FakeStreamlit(path="/login")
    monkeypatch.setattr(mw, "st", fake_st)
    monkeypatch.setattr(mw, "AUTH_ENABLED", True)
    monkeypatch.setattr(
        mw,
        "get_current_user",
        lambda: {"sub": "admin", "role": "admin", "source": "gateway_session"},
    )

    decision = mw.resolve_auth_decision()

    assert decision["decision"] == "allow_whitelist"
    assert decision["reason"] == "whitelisted_path"
    assert decision["user_source"] == "gateway_session"


def test_check_auth_and_redirect_circuit_breaker_blocks_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_st = _FakeStreamlit(path="/app")
    now_ts = time.time()
    fake_st.session_state[mw._AUTH_REDIRECT_WINDOW_START_KEY] = now_ts
    fake_st.session_state[mw._AUTH_REDIRECT_COUNT_KEY] = 1

    monkeypatch.setattr(mw, "st", fake_st)
    monkeypatch.setattr(mw, "require_auth", lambda: False)
    monkeypatch.setattr(mw, "_AUTH_REDIRECT_MAX_COUNT", 2)
    monkeypatch.setattr(mw, "_AUTH_REDIRECT_WINDOW_SECONDS", 10.0)

    with pytest.raises(RuntimeError, match="streamlit_stop"):
        mw.check_auth_and_redirect()

    assert fake_st.switch_page_calls == []
    assert fake_st.error_messages
    assert "熔断" in fake_st.error_messages[-1]


def test_should_show_auth_debug_for_admin_with_query_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_st = _FakeStreamlit(path="/app", query_params={"auth_debug": "1"})
    monkeypatch.setattr(mw, "st", fake_st)
    monkeypatch.setenv("AIRIVO_AUTH_DEBUG", "0")

    assert mw.should_show_auth_debug_for_user({"sub": "admin", "role": "admin"})
    assert not mw.should_show_auth_debug_for_user({"sub": "demo", "role": "viewer"})
