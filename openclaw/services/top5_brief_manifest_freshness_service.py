from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Tuple


DEFAULT_STALE_ALERT_HOURS = 168.0
MANIFEST_NAME = "top5_trader_brief_latest_manifest.json"


def default_exports_dir_for_monorepo(*, dashboard_root: Path | str) -> Path:
    """Resolve the dashboard-local exports directory used by /stock health checks."""
    return Path(dashboard_root).expanduser().resolve() / "exports"


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _config_lookup(config: Mapping[str, Any] | None, keys: Iterable[str]) -> Optional[float]:
    if not isinstance(config, Mapping):
        return None
    for key in keys:
        value = _safe_float(config.get(key))
        if value is not None:
            return value
    nested = config.get("top5") or config.get("top5_trader_brief") or config.get("top5_brief")
    if isinstance(nested, Mapping):
        for key in keys:
            value = _safe_float(nested.get(key))
            if value is not None:
                return value
    return None


def resolve_top5_brief_stale_alert_hours_threshold(
    *,
    secondary_config: Mapping[str, Any] | None = None,
) -> float:
    env_value = _safe_float(os.getenv("TOP5_BRIEF_STALE_ALERT_HOURS"))
    if env_value is not None:
        return env_value
    config_value = _config_lookup(
        secondary_config,
        (
            "TOP5_BRIEF_STALE_ALERT_HOURS",
            "top5_brief_stale_alert_hours",
            "top5_trader_brief_stale_alert_hours",
            "stale_alert_hours",
        ),
    )
    if config_value is not None:
        return config_value
    return DEFAULT_STALE_ALERT_HOURS


def _load_manifest(manifest_path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _resolve_manifest_artifact_paths(manifest: Mapping[str, Any], exports_dir: Path) -> list[Path]:
    paths: list[Path] = []
    for key in ("markdown", "csv", "manifest", "audit", "court_record"):
        raw = str(manifest.get(key) or "").strip()
        if not raw:
            continue
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = exports_dir / path
        if path.exists():
            paths.append(path.resolve())
    return paths


def _latest_existing_path(paths: Iterable[Path]) -> Optional[Path]:
    existing = [Path(p).expanduser().resolve() for p in paths if Path(p).expanduser().exists()]
    if not existing:
        return None
    return max(existing, key=lambda p: p.stat().st_mtime)


def _age_hours(path: Path, *, now: datetime | None = None) -> float:
    now_dt = now or datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return max(0.0, (now_dt - mtime).total_seconds() / 3600.0)


def build_top5_manifest_health_payload(
    *,
    exports_dir: Path | str,
    manifest_fallback_paths: Iterable[Path | str] | None = None,
    secondary_config: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    resolved_exports = Path(exports_dir).expanduser().resolve()
    manifest_path = resolved_exports / MANIFEST_NAME
    threshold_hours = resolve_top5_brief_stale_alert_hours_threshold(secondary_config=secondary_config)
    eval_enabled = threshold_hours > 0

    manifest_found = manifest_path.is_file()
    manifest = _load_manifest(manifest_path) if manifest_found else {}
    reference_kind = "manifest"
    reference_path: Optional[Path] = manifest_path if manifest_found else None

    if manifest_found:
        artifact_paths = _resolve_manifest_artifact_paths(manifest, resolved_exports)
        latest_artifact = _latest_existing_path(artifact_paths)
        if latest_artifact is not None:
            reference_kind = "manifest_artifact"
            reference_path = latest_artifact
    elif manifest_fallback_paths:
        latest_fallback = _latest_existing_path(Path(p) for p in manifest_fallback_paths)
        if latest_fallback is not None:
            reference_kind = "fallback_artifact"
            reference_path = latest_fallback
        else:
            reference_kind = "missing_manifest"
    else:
        reference_kind = "missing_manifest"

    age = _age_hours(reference_path, now=now) if reference_path is not None else None
    stale = bool(eval_enabled and age is not None and age > threshold_hours)
    message = ""
    if stale:
        message = (
            f"Top5 交易员清单可能已过期：最新产物约 {age:.1f} 小时前更新，"
            f"超过 {threshold_hours:.0f} 小时阈值；请先重建证据流水线再按今日清单执行。"
        )

    return {
        "contract_version": "top5_trader_brief_health.v1",
        "eval_enabled": bool(eval_enabled),
        "manifest_found": bool(manifest_found),
        "manifest_path": str(manifest_path),
        "reference_kind": reference_kind,
        "reference_path": str(reference_path) if reference_path is not None else "",
        "age_hours": age,
        "threshold_hours": threshold_hours,
        "stale_banner_recommended": stale,
        "message_zh": message,
    }


def evaluate_top5_brief_stale_banner(
    *,
    exports_dir: Path | str,
    manifest_fallback_paths: Iterable[Path | str] | None = None,
    secondary_config: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> Tuple[dict[str, Any], str]:
    payload = build_top5_manifest_health_payload(
        exports_dir=exports_dir,
        manifest_fallback_paths=manifest_fallback_paths,
        secondary_config=secondary_config,
        now=now,
    )
    return payload, str(payload.get("message_zh") or "")
