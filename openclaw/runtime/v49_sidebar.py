from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


ROLE_LABELS = {"viewer": "Viewer", "operator": "Operator", "admin": "Admin"}


def build_sidebar_session_caption(session_meta: Dict[str, Any]) -> str:
    name = session_meta.get("display_name") or session_meta.get("username") or "unknown"
    role = ROLE_LABELS.get(session_meta.get("role", "viewer"), "Viewer")
    return f"当前会话：{name} · {role}"


def build_sidebar_health_warning(report: Optional[Dict[str, Any]], current_db_path: str) -> str:
    report = report or {}
    report_db_path = str(((report.get("stats") or {}).get("db_path") or "")).strip()
    same_db = (not report_db_path) or (report_db_path == str(current_db_path or "").strip())
    if not report or not same_db or report.get("ok", True):
        return ""

    warnings = [str(w) for w in (report.get("warnings", []) or [])]
    risk_stale = bool(((report.get("stats") or {}).get("risk_stale", False)))
    if risk_stale:
        warnings = [w for w in warnings if not w.startswith("risk sentinel=")]
    preview = "\n".join(warnings[:3]) if warnings else "存在异常"
    return f"健康警报\n{preview}" if warnings else ""


def load_sidebar_health_warning(app_dir: str, current_db_path: str) -> str:
    try:
        report_path = Path(app_dir) / "evolution" / "health_report.json"
        if not report_path.exists():
            return ""
        with report_path.open("r", encoding="utf-8") as file_obj:
            report = json.load(file_obj)
        return build_sidebar_health_warning(report, current_db_path)
    except Exception:
        return ""


def render_v49_sidebar(
    *,
    st: Any,
    db_manager: Any,
    permanent_db_path: str,
    fingerprint: Dict[str, Any],
    session_meta: Dict[str, Any],
    app_dir: str,
) -> Dict[str, Any]:
    st.header("系统状态")
    st.caption(f"pid {fingerprint['pid']} / {fingerprint['app_file']}")

    status = db_manager.get_database_status()
    current_db_path = str(getattr(db_manager, "db_path", permanent_db_path))
    st.caption(f"当前数据库：`{current_db_path}`")

    if "error" not in status:
        st.metric("活跃股票", f"{status.get('active_stocks', 0):,} 只")
        st.metric("行业板块", f"{status.get('total_industries', 0)} 个")
        st.metric("数据量", f"{status.get('total_records', 0):,} 条")
        st.metric("数据库", f"{status.get('db_size_gb', 0):.2f} GB")

        st.divider()
        st.markdown("**数据状态**")
        st.markdown(f"- 最新：{status.get('max_date', 'N/A')}")

        if status.get("is_fresh"):
            st.success(f"最新（{status.get('days_old', 0)}天前）")
        else:
            st.warning(f"需更新（{status.get('days_old', 999)}天前）")
    else:
        st.error(f"{status['error']}")

    health_warning = load_sidebar_health_warning(app_dir, current_db_path)
    if health_warning:
        st.warning(health_warning)

    st.divider()
    st.caption("生产策略：v9 / v8 / v5 / combo")
    st.caption("主流程：今日决策 -> 执行中心 -> 策略演进")
    st.caption(build_sidebar_session_caption(session_meta))
    return status
