from __future__ import annotations

import time
from typing import Any, Callable

import streamlit as st


def render_data_ops_update_page(
    *,
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
    db_manager: Any,
) -> None:
    st.markdown("---")

    update_mode = st.radio("更新模式", ["快速（5天）", "标准（30天）", "深度（90天）"], horizontal=True)
    if update_mode == "快速（5天）":
        days = 5
    elif update_mode == "标准（30天）":
        days = 30
    else:
        days = 90

    st.caption(f"更新窗口：最近 {days} 天")

    if st.button("开始更新数据", type="primary", use_container_width=True, disabled=not airivo_has_role("admin")):
        if not airivo_guard_action("admin", "start_data_update", target="stock_data", reason=f"days={days}"):
            st.stop()
        with st.spinner(f"正在更新{days}天数据..."):
            try:
                result = db_manager.update_stock_data_from_tushare(days=days)
                if result["success"]:
                    airivo_append_action_audit(
                        "start_data_update",
                        True,
                        target="stock_data",
                        detail=f"updated_days={result['updated_days']},total_records={result['total_records']}",
                        extra={"days": days},
                    )
                    st.success(
                        f"""
 更新成功！
- 更新天数：{result['updated_days']}天
- 失败天数：{result.get('failed_days', 0)}天
- 总记录数：{result['total_records']:,}条
"""
                    )
                    if result.get("calendar_warning"):
                        st.warning(result.get("calendar_warning"))
                    time.sleep(1)
                    st.rerun()
                else:
                    airivo_append_action_audit(
                        "start_data_update",
                        False,
                        target="stock_data",
                        detail=str(result.get("error")),
                        extra={"days": days},
                    )
                    st.error(f"更新失败：{result.get('error')}")
            except Exception as e:
                airivo_append_action_audit(
                    "start_data_update",
                    False,
                    target="stock_data",
                    detail=str(e),
                    extra={"days": days},
                )
                st.error(f"更新失败：{e}")
                import traceback

                st.code(traceback.format_exc())

    st.markdown("---")
    st.subheader("流通市值数据更新")
    st.caption("市值筛选异常时先执行本操作。")

    if st.button("更新流通市值数据", use_container_width=True, type="primary", disabled=not airivo_has_role("admin")):
        if not airivo_guard_action("admin", "update_market_cap", target="market_cap", reason="market_cap_refresh"):
            st.stop()
        with st.spinner("正在从Tushare获取最新市值数据..."):
            result = db_manager.update_market_cap()
            if result.get("success"):
                airivo_append_action_audit("update_market_cap", True, target="market_cap", detail=f"updated_count={result.get('updated_count', 0)}")
                stats = result.get("stats", {})
                st.success(
                    f"""
 市值数据更新成功！
- 更新股票数：{result.get('updated_count', 0):,}只
- 100-500亿：{stats.get('count_100_500', 0)}只 黄金区间
- 50-100亿：{stats.get('count_50_100', 0)}只
- <50亿：{stats.get('count_below_50', 0)}只
- >500亿：{stats.get('count_above_500', 0)}只
"""
                )
                time.sleep(1)
                st.rerun()
            else:
                airivo_append_action_audit("update_market_cap", False, target="market_cap", detail=str(result.get("error")))
                st.error(f"更新失败：{result.get('error')}")

    st.markdown("---")
    st.subheader("数据库优化与维护")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("数据库健康检查", use_container_width=True, disabled=not airivo_has_role("admin")):
            if not airivo_guard_action("admin", "database_health_check", target="database", reason="manual_database_health_check"):
                st.stop()
            with st.spinner("正在检查数据库健康状态..."):
                health = db_manager.check_database_health()
                airivo_append_action_audit("database_health_check", "error" not in health, target="database", detail=str(health.get("error") or "ok"))
                if "error" in health:
                    st.error(f"检查失败: {health['error']}")
                else:
                    if health.get("has_stock_basic") and health.get("has_daily_data"):
                        st.success("数据库结构正常")
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("股票数量", f"{health.get('stock_count', 0):,}")
                        with col_b:
                            st.metric("数据记录", f"{health.get('data_count', 0):,}")
                        with col_c:
                            days_old = health.get("days_since_update", 999)
                            is_fresh = health.get("is_fresh", False)
                            st.metric(
                                "数据新鲜度",
                                f"{days_old}天前" if days_old < 999 else "未知",
                                delta="新鲜" if is_fresh else "需更新",
                                delta_color="normal" if is_fresh else "inverse",
                            )
                    else:
                        st.warning("数据库结构不完整，建议重新初始化")

    with col2:
        if st.button("优化数据库", use_container_width=True, type="secondary", disabled=not airivo_has_role("admin")):
            if not airivo_guard_action("admin", "optimize_database", target="database", reason="manual_database_optimize"):
                st.stop()
            with st.spinner("正在优化数据库（清理重复数据、重建索引）..."):
                result = db_manager.optimize_database()
                if result.get("success"):
                    airivo_append_action_audit("optimize_database", True, target="database", detail=str(result.get("message") or "ok"))
                    st.success(f"{result.get('message')}")
                    time.sleep(1)
                    st.rerun()
                else:
                    airivo_append_action_audit("optimize_database", False, target="database", detail=str(result.get("error")))
                    st.error(f"优化失败: {result.get('error')}")

    with st.expander("数据库维护说明"):
        st.markdown("建议：每周检查健康、每月优化一次、更新前保留备份。")
