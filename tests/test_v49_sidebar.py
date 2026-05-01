from __future__ import annotations

from openclaw.runtime.v49_sidebar import (
    build_sidebar_health_warning,
    build_sidebar_session_caption,
    render_v49_sidebar,
)


class FakeDbManager:
    db_path = "/tmp/airivo.db"

    def get_database_status(self):
        return {
            "active_stocks": 1234,
            "total_industries": 31,
            "total_records": 98765,
            "db_size_gb": 1.23,
            "max_date": "2026-05-01",
            "is_fresh": False,
            "days_old": 2,
        }


class FakeStreamlit:
    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def _record(*args, **kwargs):
            self.calls.append((name, args, kwargs))

        return _record


def test_build_sidebar_session_caption_freezes_role_label():
    assert build_sidebar_session_caption({"display_name": "Alice", "role": "admin"}) == "当前会话：Alice · Admin"
    assert build_sidebar_session_caption({"username": "bob", "role": "unknown"}) == "当前会话：bob · Viewer"


def test_build_sidebar_health_warning_filters_stale_risk_sentinel_for_same_db():
    report = {
        "ok": False,
        "warnings": ["risk sentinel=stale", "db lag", "quality low", "ignored fourth"],
        "stats": {"db_path": "/tmp/airivo.db", "risk_stale": True},
    }

    assert build_sidebar_health_warning(report, "/tmp/airivo.db") == "健康警报\ndb lag\nquality low\nignored fourth"


def test_build_sidebar_health_warning_ignores_other_database_report():
    report = {"ok": False, "warnings": ["db lag"], "stats": {"db_path": "/tmp/other.db"}}

    assert build_sidebar_health_warning(report, "/tmp/airivo.db") == ""


def test_render_v49_sidebar_returns_status_and_records_existing_status_facts():
    fake_st = FakeStreamlit()

    status = render_v49_sidebar(
        st=fake_st,
        db_manager=FakeDbManager(),
        permanent_db_path="/tmp/fallback.db",
        fingerprint={"pid": 123, "app_file": "v49_app.py"},
        session_meta={"username": "operator", "role": "operator"},
        app_dir="/tmp/does-not-exist",
    )

    assert status["active_stocks"] == 1234
    assert ("header", ("系统状态",), {}) in fake_st.calls
    assert any(call[0] == "metric" and call[1][0] == "活跃股票" for call in fake_st.calls)
    assert any(call[0] == "warning" and call[1][0] == "需更新（2天前）" for call in fake_st.calls)
    assert any(call[0] == "caption" and call[1][0] == "当前会话：operator · Operator" for call in fake_st.calls)
