from __future__ import annotations

from typing import Any, Callable

import streamlit as st


def _render_sector_group(
    title: str,
    hint: str,
    tone: Callable[[str], Any],
    sectors: list[dict[str, Any]],
    limit: int,
    advice_tone: Callable[[str], Any],
    advice_text: str,
) -> None:
    if not sectors:
        return

    st.markdown("---")
    st.markdown(f"###  {title}")
    if hint:
        tone(hint)

    for i, sector in enumerate(sectors[:limit], 1):
        with st.expander(f"{i}. 【{sector['sector_name']}】 评分: {sector['score']}分"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**所处阶段**: {sector['stage']}")
                st.markdown(f"**综合评分**: {sector['score']}分")
            with col2:
                st.markdown(f"**关键信号**: {', '.join(sector['signals'])}")
            advice_tone(advice_text)


def render_sector_flow_page(
    *,
    render_page_header: Callable[..., None],
    market_scanner_cls: type[Any],
) -> None:
    render_page_header(
        " 板块热点分析",
        "快速识别热门板块 · 生命周期分析 · 萌芽期重点关注",
        tag="Sector Flow",
    )

    col1, col2 = st.columns([3, 1])
    with col1:
        scan_days = st.slider(
            "扫描周期（天）",
            30,
            120,
            60,
            5,
            key="sector_scan_days",
            help="扫描最近N天的板块数据，建议60天",
        )
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        start_scan = st.button("开始扫描", type="primary", use_container_width=True, key="start_sector_scan")

    if start_scan:
        with st.spinner("正在扫描全市场板块..."):
            try:
                if "scanner" not in st.session_state:
                    st.session_state.scanner = market_scanner_cls()

                scan_results = st.session_state.scanner.scan_all_sectors(days=scan_days)
                st.session_state["scan_results"] = scan_results
                st.success("扫描完成！")
                st.rerun()
            except Exception as e:
                import traceback

                st.error(f"扫描失败: {e}")
                st.code(traceback.format_exc())

    if "scan_results" not in st.session_state:
        st.info("点击“开始扫描”后查看板块阶段分布。")
        return

    results = st.session_state["scan_results"]
    st.markdown("---")
    st.subheader("板块生命周期分布")

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("萌芽期", f"{len(results['emerging'])}个", help="成交量低迷但价格稳定，主力可能在布局")
    with col2:
        st.metric("启动期", f"{len(results['launching'])}个", help="量价齐升，板块开始启动")
    with col3:
        st.metric("加速期", f"{len(results['exploding'])}个", help="成交量趋势，价格大涨")
    with col4:
        st.metric("衰退期", f"{len(results['declining'])}个", help="量价齐跌，板块进入衰退")
    with col5:
        st.metric("过渡期", f"{len(results['transitioning'])}个", help="处于过渡阶段，观察为主")

    _render_sector_group(
        "萌芽期板块（重点关注 - 最佳布局时机）",
        "萌芽期特征：成交量低迷，价格稳定，主力可能在悄悄布局，是最佳介入时机！",
        st.info,
        results["emerging"],
        10,
        st.success,
        "建议：密切关注该板块龙头股，等待启动信号",
    )

    _render_sector_group(
        "启动期板块（关注 - 确认突破）",
        "",
        st.info,
        results["launching"],
        5,
        st.warning,
        "建议：关注龙头股突破，可考虑介入",
    )

    _render_sector_group(
        "加速期板块（谨慎 - 短线为主）",
        "",
        st.info,
        results["exploding"],
        5,
        st.error,
        "建议：高位追涨风险大，仅供短线高手参与",
    )

    st.markdown("---")
    with st.expander("操作提示"):
        st.markdown("优先看萌芽期与启动期；加速期仅短线，衰退期回避。")
