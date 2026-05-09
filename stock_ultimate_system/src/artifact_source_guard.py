from __future__ import annotations

from pathlib import Path

from src.utils.project_paths import PROJECT_ROOT


TEMP_SOURCE_PATTERNS = (
    "pytest-",
    "pytest-of-",
    "/tmp/",
    "/private/var/folders/",
    "/var/folders/",
)


def is_rejected_temp_source_path(value: object) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if "pytest-of-" in text or "pytest-" in text:
        return True
    if "/tmp/" in text:
        return True
    if "/var/folders/" in text or "/private/var/folders/" in text:
        return True
    return False


def assert_not_temp_source_path(value: object, *, field_name: str) -> None:
    if is_rejected_temp_source_path(value):
        raise ValueError(f"{field_name} points to a temporary or pytest-derived path and cannot be published as production latest")


def assert_mapping_has_no_temp_sources(payload: dict[str, object], *, field_names: tuple[str, ...] | None = None) -> None:
    keys = field_names or tuple(payload.keys())
    for key in keys:
        if key not in payload:
            continue
        assert_not_temp_source_path(payload.get(key), field_name=key)


def assert_path_is_not_temp_source(path: str | Path, *, field_name: str) -> None:
    assert_not_temp_source_path(str(path), field_name=field_name)


def should_enforce_production_source_guard(output_path: str | Path | None) -> bool:
    if output_path is None:
        return False
    path = Path(output_path)
    resolved = path if path.is_absolute() else (PROJECT_ROOT / path)
    try:
        resolved.relative_to(PROJECT_ROOT / "artifacts")
        return True
    except ValueError:
        return False
