from __future__ import annotations

from urllib.parse import urlencode

from src.airivo_scope_registry import get_airivo_namespace, get_airivo_scope, resolve_airivo_namespace_scope


def normalize_base_path(base_path: str) -> str:
    raw = (base_path or "").strip()
    if not raw or raw == "/":
        return ""
    return "/" + raw.strip("/")


def base_href(base_path: str, suffix: str = "/") -> str:
    base = normalize_base_path(base_path)
    if not base:
        return suffix
    if suffix == "/":
        return f"{base}/"
    if suffix.startswith("/"):
        return f"{base}{suffix}"
    return f"{base}/{suffix}"


def view_href(view: str, candidate_index: int, base_path: str) -> str:
    query = {"view": view}
    if view == "candidates":
        query["candidate"] = str(candidate_index)
    return base_href(base_path, f"/?{urlencode(query)}")


def report_href(report_key: str, candidate_index: int, base_path: str) -> str:
    return base_href(base_path, f"/?{urlencode({'view': 'reports', 'candidate': candidate_index, 'report': report_key})}")


def resolve_namespace_scope_with_fallback(base_path: str):
    normalized = normalize_base_path(base_path) or "/"
    try:
        return resolve_airivo_namespace_scope(normalized), None
    except KeyError:
        return (get_airivo_namespace("production"), get_airivo_scope("main_site")), normalized


def resolve_namespace_id(base_path: str) -> str:
    (namespace, _scope), _fallback_route = resolve_namespace_scope_with_fallback(base_path)
    return namespace.namespace_id


def resolve_scope_id(base_path: str) -> str:
    (_namespace, scope), _fallback_route = resolve_namespace_scope_with_fallback(base_path)
    return scope.scope_id


def is_t12_scope(base_path: str) -> bool:
    return resolve_scope_id(base_path) == "t12"


def is_main_site_scope(base_path: str) -> bool:
    return resolve_scope_id(base_path) == "main_site"
