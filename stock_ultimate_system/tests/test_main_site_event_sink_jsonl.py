import json

from src.main_site_conversion_events import build_main_site_conversion_event_payload
from src.main_site_event_bridge import emit_main_site_conversion_event, emit_main_site_runtime_event
from src.main_site_event_envelope import build_main_site_event_envelope
from src.main_site_event_sink import JsonlMainSiteEventSink, read_main_site_event_jsonl, replay_main_site_event_jsonl
from src.main_site_home import MAIN_SITE_HOME_PAGE_VERSION


def test_main_site_event_jsonl_sink_writes_one_event_per_line(tmp_path):
    sink_path = tmp_path / "main_site_events.jsonl"
    sink = JsonlMainSiteEventSink(sink_path)
    payload = build_main_site_conversion_event_payload("primary_cta_click")
    envelope = build_main_site_event_envelope(
        event_id="event-001",
        occurred_at="2026-04-14T09:00:00+00:00",
        session_id="session-001",
        page_version=MAIN_SITE_HOME_PAGE_VERSION,
        environment="test",
    )

    event = emit_main_site_runtime_event(payload=payload, envelope=envelope, sink=sink)

    lines = sink_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == event
    assert read_main_site_event_jsonl(sink_path) == [event]
    assert list(replay_main_site_event_jsonl(sink_path)) == [event]


def test_main_site_event_jsonl_sink_supports_bridge_event_key_emit(tmp_path):
    sink_path = tmp_path / "main_site_events.jsonl"
    sink = JsonlMainSiteEventSink(sink_path)

    event = emit_main_site_conversion_event(
        event_key="t12_card_click",
        sink=sink,
        session_id="session-002",
        page_version=MAIN_SITE_HOME_PAGE_VERSION,
        environment="test",
    )

    assert event["envelope"]["session_id"] == "session-002"
    assert event["payload"]["target_path"] == "/T12/"
    assert read_main_site_event_jsonl(sink_path) == [event]
