from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from src.main_site_conversion_events import (
    MainSiteConversionEventPayload,
    build_main_site_conversion_event_payload,
)
from src.main_site_event_envelope import MainSiteEventEnvelope, build_main_site_event_envelope


class MainSiteEventSink(Protocol):
    def write_event(self, event: dict[str, object]) -> object:
        ...


def _payload_as_dict(payload: MainSiteConversionEventPayload | Mapping[str, object]) -> dict[str, object]:
    if isinstance(payload, MainSiteConversionEventPayload):
        return payload.as_dict()
    return dict(payload)


def _envelope_as_dict(envelope: MainSiteEventEnvelope | Mapping[str, object]) -> dict[str, object]:
    if isinstance(envelope, MainSiteEventEnvelope):
        return envelope.as_dict()
    return dict(envelope)


def build_main_site_runtime_event(
    *,
    payload: MainSiteConversionEventPayload | Mapping[str, object],
    envelope: MainSiteEventEnvelope | Mapping[str, object],
) -> dict[str, object]:
    return {
        "envelope": _envelope_as_dict(envelope),
        "payload": _payload_as_dict(payload),
    }


def emit_main_site_runtime_event(
    *,
    payload: MainSiteConversionEventPayload | Mapping[str, object],
    envelope: MainSiteEventEnvelope | Mapping[str, object],
    sink: MainSiteEventSink,
) -> dict[str, object]:
    event = build_main_site_runtime_event(payload=payload, envelope=envelope)
    sink.write_event(event)
    return event


def emit_main_site_conversion_event(
    *,
    event_key: str,
    sink: MainSiteEventSink,
    session_id: str,
    page_version: str,
    environment: str = "local",
) -> dict[str, object]:
    payload = build_main_site_conversion_event_payload(event_key)
    envelope = build_main_site_event_envelope(
        session_id=session_id,
        page_version=page_version,
        environment=environment,
    )
    return emit_main_site_runtime_event(payload=payload, envelope=envelope, sink=sink)
