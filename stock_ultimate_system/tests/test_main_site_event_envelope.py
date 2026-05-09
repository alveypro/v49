from src.main_site_event_envelope import (
    MAIN_SITE_EVENT_ENVELOPE_FIELDS,
    MAIN_SITE_EVENT_SCHEMA_VERSION,
    build_main_site_event_envelope,
)


def test_main_site_event_envelope_shape_is_stable():
    envelope = build_main_site_event_envelope(
        event_id="event-001",
        occurred_at="2026-04-14T09:00:00+00:00",
        session_id="session-001",
        page_version="main_site_home.v1",
        environment="test",
    ).as_dict()

    assert tuple(envelope.keys()) == MAIN_SITE_EVENT_ENVELOPE_FIELDS
    assert envelope == {
        "event_id": "event-001",
        "occurred_at": "2026-04-14T09:00:00+00:00",
        "session_id": "session-001",
        "page_version": "main_site_home.v1",
        "schema_version": MAIN_SITE_EVENT_SCHEMA_VERSION,
        "environment": "test",
    }


def test_main_site_event_envelope_generates_runtime_fields():
    envelope = build_main_site_event_envelope(
        session_id="session-001",
        page_version="main_site_home.v1",
    ).as_dict()

    assert envelope["event_id"].startswith("main-site-event-")
    assert envelope["occurred_at"].endswith("+00:00")
    assert envelope["schema_version"] == MAIN_SITE_EVENT_SCHEMA_VERSION
    assert envelope["environment"] == "local"
