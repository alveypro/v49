from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import streamlit as st


def render_today_advanced_ops_panel(
    *,
    permanent_db_path: str,
    airivo_snapshot: dict[str, Any],
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
    render_airivo_batch_manager: Callable[[str], None],
    publish_manual_scan_to_execution_queue: Callable[..., tuple[bool, str]],
    production_baseline_params: Callable[..., dict[str, Any]],
    apply_production_baseline_to_session: Callable[[dict[str, Any]], None],
    save_production_unified_profile: Callable[..., tuple[bool, str]],
    build_unified_from_latest_evolve: Callable[..., tuple[dict[str, Any], list[str]]],
    get_production_compare_params: Callable[[], dict[str, dict[str, Any]]],
) -> None:
    with st.container(border=True):
        st.markdown("### 高级操作（非今日必做）")
        if airivo_has_role("admin"):
            st.caption("这里放手动发布、批次管理和统一口径设置。默认隐藏，避免干扰当天主路径。")
            render_airivo_batch_manager(permanent_db_path)
        else:
            st.warning("当前账号不是 admin。你可以查看执行队列，但不能做批次治理、例外批准或灰度发布。")

        candidate_raw = st.session_state.get("stock_pool_candidate")
        candidate = candidate_raw if isinstance(candidate_raw, dict) else {}
        candidate_df = candidate.get("df")
        if isinstance(candidate_df, pd.DataFrame) and not candidate_df.empty:
            st.markdown("### 当前手动扫描候选")
            st.caption(
                f"当前已缓存最近一次手动扫描结果：strategy={candidate.get('strategy') or 'unknown'}，"
                f"rows={len(candidate_df)}。可将其发布为今日结构化执行队列。"
            )
            auto_status = st.session_state.get("airivo_auto_publish_status") or {}
            auto_mode = str(auto_status.get("mode") or "")
            auto_message = str(auto_status.get("message") or "")
            if auto_message:
                if auto_mode == "auto":
                    st.success(f"自动发布：{auto_message}")
                elif auto_mode == "error":
                    st.error(f"自动发布失败：{auto_message}")
                else:
                    st.info(f"需人工确认：{auto_message}")
            pub_left, pub_right = st.columns([0.72, 0.28])
            with pub_left:
                st.caption("发布后会写入与 overnight decision 同一条执行队列产线，但来源标记为 manual_scan，不覆盖日报主链路。")
            with pub_right:
                manual_disabled = auto_mode == "auto" or (not airivo_has_role("admin"))
                if st.button("发布为今日执行队列", key="publish_manual_scan_queue", type="secondary", use_container_width=True, disabled=manual_disabled):
                    if not airivo_guard_action("admin", "publish_manual_scan_queue", target=str(candidate.get("strategy") or "manual_scan"), reason="manual_scan_publish"):
                        st.stop()
                    ok, msg = publish_manual_scan_to_execution_queue(candidate, permanent_db_path, airivo_snapshot)
                    if ok:
                        airivo_append_action_audit("publish_manual_scan_queue", True, target=str(candidate.get("strategy") or "manual_scan"), detail=msg)
                        st.session_state["airivo_auto_publish_status"] = {"mode": "auto", "message": msg}
                        st.success(msg)
                        st.rerun()
                    else:
                        airivo_append_action_audit("publish_manual_scan_queue", False, target=str(candidate.get("strategy") or "manual_scan"), detail=msg)
                        st.session_state["airivo_auto_publish_status"] = {"mode": "error", "message": msg}
                        st.error(msg)

        strict_full_market_mode = st.toggle(
            "全市场严格口径（扫描+回测）",
            value=bool(st.session_state.get("strict_full_market_mode", False)),
            key="strict_full_market_mode",
            help="开启后自动使用市值0~0、候选数上限，并将回测样本提升到全量档。",
        )
        if strict_full_market_mode:
            st.caption("严格口径已开启：扫描=全市场，回测=全量档。")

        col_u1, col_u2, col_u3 = st.columns(3)
        with col_u1:
            baseline_profile = st.selectbox("参数模板", ["稳健标准", "进攻增强"], index=0, key="prod_baseline_profile")
        with col_u2:
            unified_cap_min = st.number_input("统一最小市值（亿）", min_value=0, max_value=5000, value=100, step=10, key="prod_unified_cap_min")
        with col_u3:
            unified_cap_max = st.number_input("统一最大市值（亿）", min_value=0, max_value=50000, value=15000, step=50, key="prod_unified_cap_max")

        btn_sync_col, btn_evo_col = st.columns(2)
        with btn_sync_col:
            sync_clicked = st.button("一键同步扫描与回测口径", use_container_width=True, key="apply_prod_baseline", disabled=not airivo_has_role("admin"))
        with btn_evo_col:
            apply_evo_clicked = st.button("应用最新自动进化到统一口径", use_container_width=True, key="apply_prod_evolve", disabled=not airivo_has_role("admin"))

        if sync_clicked:
            if not airivo_guard_action("admin", "apply_prod_baseline", target=baseline_profile, reason="sync_production_baseline"):
                st.stop()
            baseline = production_baseline_params(baseline_profile, strict_full_market=bool(strict_full_market_mode))
            for sk in ("v5", "v8", "v9", "combo"):
                baseline[sk]["cap_min"] = float(unified_cap_min)
                baseline[sk]["cap_max"] = float(unified_cap_max)
            if strict_full_market_mode:
                for sk in ("v5", "v8", "v9", "combo"):
                    baseline[sk]["cap_min"] = 0.0
                    baseline[sk]["cap_max"] = 0.0
            apply_production_baseline_to_session(baseline)
            ok_save, save_msg = save_production_unified_profile(
                profile_name=baseline_profile,
                strict_full_market=bool(strict_full_market_mode),
                params=baseline,
            )
            if ok_save:
                airivo_append_action_audit("apply_prod_baseline", True, target=baseline_profile, detail=save_msg)
                st.success("已统一到生产参数口径：扫描与回测将使用同一套核心参数，并已写入自动进化口径文件。")
                st.caption(f"统一口径文件：{save_msg}")
            else:
                airivo_append_action_audit("apply_prod_baseline", False, target=baseline_profile, detail=save_msg)
                st.warning(f"已同步到会话参数，但写入统一口径文件失败：{save_msg}")
            st.rerun()

        if apply_evo_clicked:
            if not airivo_guard_action("admin", "apply_prod_evolve", target=baseline_profile, reason="apply_latest_evolve_profile"):
                st.stop()
            baseline, notes = build_unified_from_latest_evolve(
                profile=baseline_profile,
                strict_full_market=bool(strict_full_market_mode),
                unified_cap_min=float(unified_cap_min),
                unified_cap_max=float(unified_cap_max),
            )
            apply_production_baseline_to_session(baseline)
            ok_save, save_msg = save_production_unified_profile(
                profile_name=f"{baseline_profile}-自动进化",
                strict_full_market=bool(strict_full_market_mode),
                params=baseline,
            )
            if ok_save:
                airivo_append_action_audit("apply_prod_evolve", True, target=baseline_profile, detail=save_msg, extra={"notes": notes[:2]})
                st.success("已将最新自动进化参数写入统一口径，并同步到扫描与回测。")
                st.caption(f"统一口径文件：{save_msg}")
            else:
                airivo_append_action_audit("apply_prod_evolve", False, target=baseline_profile, detail=save_msg, extra={"notes": notes[:2]})
                st.warning(f"已同步到会话参数，但写入统一口径文件失败：{save_msg}")
            for n in notes[:2]:
                st.caption(f"提示：{n}")
            st.rerun()

        compare_params_preview = get_production_compare_params()
        preview_rows = [
            {
                "策略": "v5",
                "阈值": compare_params_preview["v5"]["score_threshold"],
                "持有天数": compare_params_preview["v5"]["holding_days"],
                "候选数": int(st.session_state.get("candidate_count_v5", 800)),
                "市值(亿)": f"{float(st.session_state.get('cap_min_v5', 100)):.0f}-{float(st.session_state.get('cap_max_v5', 15000)):.0f}",
            },
            {
                "策略": "v8",
                "阈值": compare_params_preview["v8"]["score_threshold"],
                "持有天数": compare_params_preview["v8"]["holding_days"],
                "候选数": int(st.session_state.get("candidate_count_v8", 800)),
                "市值(亿)": f"{float(st.session_state.get('cap_min_v8_tab1', 100)):.0f}-{float(st.session_state.get('cap_max_v8_tab1', 15000)):.0f}",
            },
            {
                "策略": "v9",
                "阈值": compare_params_preview["v9"]["score_threshold"],
                "持有天数": compare_params_preview["v9"]["holding_days"],
                "候选数": int(st.session_state.get("candidate_count_v9", 800)),
                "市值(亿)": f"{float(st.session_state.get('cap_min_v9', 100)):.0f}-{float(st.session_state.get('cap_max_v9', 15000)):.0f}",
            },
            {
                "策略": "combo",
                "阈值": compare_params_preview["combo"]["score_threshold"],
                "持有天数": compare_params_preview["combo"]["holding_days"],
                "候选数": int(st.session_state.get("combo_candidate_count", 800)),
                "市值(亿)": f"{float(st.session_state.get('combo_cap_min', 100)):.0f}-{float(st.session_state.get('combo_cap_max', 15000)):.0f}",
            },
        ]
        st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)
        caps = [r["市值(亿)"] for r in preview_rows]
        holds = [int(r["持有天数"]) for r in preview_rows]
        if len(set(caps)) > 1:
            st.warning("口径校验：各策略市值范围不一致，建议使用统一市值并再次同步。")
        if min(holds) < 3 or max(holds) > 30:
            st.warning("口径校验：存在异常持有天数，建议限定在 3-30 天。")
