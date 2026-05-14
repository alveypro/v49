from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from openclaw.services.top5_brief_manifest_freshness_service import (
    build_top5_manifest_health_payload,
    default_exports_dir_for_monorepo,
    evaluate_top5_brief_stale_banner,
    resolve_top5_brief_stale_alert_hours_threshold,
)


def test_default_exports_dir_for_monorepo_uses_dashboard_exports(tmp_path: Path) -> None:
    root = tmp_path / "stock_ultimate_system"

    assert default_exports_dir_for_monorepo(dashboard_root=root) == (root / "exports").resolve()


def test_missing_manifest_returns_stable_health_contract(tmp_path: Path) -> None:
    payload = build_top5_manifest_health_payload(exports_dir=tmp_path / "exports")

    assert payload["contract_version"] == "top5_trader_brief_health.v1"
    assert payload["manifest_found"] is False
    assert payload["reference_kind"] == "missing_manifest"
    assert payload["threshold_hours"] == 168.0
    assert payload["stale_banner_recommended"] is False
    assert payload["message_zh"] == ""


def test_stale_banner_uses_fallback_artifact_when_manifest_is_missing(tmp_path: Path) -> None:
    exports_dir = tmp_path / "exports"
    exports_dir.mkdir()
    artifact = exports_dir / "top5_trader_brief_20260514.md"
    artifact.write_text("# brief\n", encoding="utf-8")
    os.utime(artifact, (1_700_000_000, 1_700_000_000))

    payload, message = evaluate_top5_brief_stale_banner(
        exports_dir=exports_dir,
        manifest_fallback_paths=[artifact],
        secondary_config={"top5_brief_stale_alert_hours": 1},
        now=datetime.fromtimestamp(1_700_000_000 + 7200, tz=timezone.utc),
    )

    assert payload["reference_kind"] == "fallback_artifact"
    assert payload["stale_banner_recommended"] is True
    assert "Top5 交易员清单可能已过期" in message


def test_threshold_prefers_env_then_secondary_config(monkeypatch) -> None:
    monkeypatch.setenv("TOP5_BRIEF_STALE_ALERT_HOURS", "12")
    assert resolve_top5_brief_stale_alert_hours_threshold(secondary_config={"stale_alert_hours": 24}) == 12.0

    monkeypatch.delenv("TOP5_BRIEF_STALE_ALERT_HOURS")
    assert resolve_top5_brief_stale_alert_hours_threshold(secondary_config={"stale_alert_hours": 24}) == 24.0
