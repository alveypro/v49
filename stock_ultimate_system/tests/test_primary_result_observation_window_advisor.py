from datetime import datetime
import json
from zoneinfo import ZoneInfo

from src.primary_result_observation_window_advisor import suggest_observation_window_start


def test_observation_window_advisor_uses_same_day_before_open():
    payload = suggest_observation_window_start(
        reference_time=datetime(2026, 4, 17, 8, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    )

    assert payload["suggested_window_start"] == "2026-04-17T01:30:00Z"
    assert payload["suggested_window_start_local"] == "2026-04-17T09:30:00+08:00"


def test_observation_window_advisor_uses_next_weekday_after_close_on_friday():
    payload = suggest_observation_window_start(
        reference_time=datetime(2026, 4, 17, 17, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    )

    assert payload["suggested_window_start"] == "2026-04-20T01:30:00Z"
    assert payload["suggested_window_start_local"] == "2026-04-20T09:30:00+08:00"
    assert "without_holiday_calendar" in payload["calendar_policy"]


def test_observation_window_advisor_skips_weekend():
    payload = suggest_observation_window_start(
        reference_time=datetime(2026, 4, 18, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    )

    assert payload["suggested_window_start"] == "2026-04-20T01:30:00Z"


def test_observation_window_advisor_uses_provided_trade_calendar():
    payload = suggest_observation_window_start(
        reference_time=datetime(2026, 4, 30, 17, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        trade_dates=["2026-05-06"],
    )

    assert payload["suggested_window_start"] == "2026-05-06T01:30:00Z"
    assert payload["suggested_window_start_local"] == "2026-05-06T09:30:00+08:00"
    assert payload["calendar_policy"] == "provided_trade_calendar_open_0930"


def test_observation_window_advisor_loads_trade_calendar_artifact(tmp_path):
    artifact = tmp_path / "calendar.json"
    artifact.write_text(json.dumps({"trade_dates": ["2026-05-06"]}), encoding="utf-8")

    payload = suggest_observation_window_start(
        reference_time=datetime(2026, 4, 30, 17, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        trade_calendar_path=artifact,
    )

    assert payload["suggested_window_start"] == "2026-05-06T01:30:00Z"
    assert payload["calendar_policy"] == "provided_trade_calendar_open_0930"
