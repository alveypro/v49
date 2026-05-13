from __future__ import annotations

from src.stock_dashboard_constants import VIEW_LABELS, VIEW_SUBTITLES
from src.stock_dashboard_url import is_t12_scope


T12_VIEW_KEY = "t12"
T12_VIEW_LABEL = "T12"
T12_VIEW_SUBTITLE = "聚焦 T12 最小制度镜像与治理摘要，不扩成完整控制台。"


def view_labels(base_path: str) -> dict[str, str]:
    labels = dict(VIEW_LABELS)
    if is_t12_scope(base_path):
        labels[T12_VIEW_KEY] = T12_VIEW_LABEL
    return labels


def view_subtitles(base_path: str) -> dict[str, str]:
    subtitles = dict(VIEW_SUBTITLES)
    if is_t12_scope(base_path):
        subtitles[T12_VIEW_KEY] = T12_VIEW_SUBTITLE
    return subtitles
