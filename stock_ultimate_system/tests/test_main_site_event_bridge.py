from src.main_site_conversion_events import build_main_site_conversion_event_payload
from src.main_site_event_bridge import build_main_site_runtime_event, emit_main_site_runtime_event
from src.main_site_event_envelope import build_main_site_event_envelope


class MemoryEventSink:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def write_event(self, event: dict[str, object]) -> None:
        self.events.append(event)


def test_main_site_event_bridge_builds_layered_event():
    payload = build_main_site_conversion_event_payload("primary_cta_click")
    envelope = build_main_site_event_envelope(
        event_id="event-001",
        occurred_at="2026-04-14T09:00:00+00:00",
        session_id="session-001",
        page_version="main_site_home.v1",
        environment="test",
    )

    event = build_main_site_runtime_event(payload=payload, envelope=envelope)

    assert tuple(event.keys()) == ("envelope", "payload")
    assert event["envelope"]["event_id"] == "event-001"
    assert event["payload"] == payload.as_dict()
    assert event["payload"]["event_name"] == "main_site_primary_cta_click"


def test_main_site_event_bridge_emits_to_sink():
    sink = MemoryEventSink()
    payload = build_main_site_conversion_event_payload("stock_card_click")
    envelope = build_main_site_event_envelope(
        event_id="event-002",
        occurred_at="2026-04-14T09:01:00+00:00",
        session_id="session-001",
        page_version="main_site_home.v1",
        environment="test",
    )

    event = emit_main_site_runtime_event(payload=payload, envelope=envelope, sink=sink)

    assert sink.events == [event]
    assert sink.events[0]["payload"]["component_id"] == "main-site-stock-card-link"
