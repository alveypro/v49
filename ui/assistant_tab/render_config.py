from __future__ import annotations

import json

import pandas as pd
import streamlit as st
from openclaw.runtime.root_dependency_bridge import load_notification_service_class

from .helpers import build_notification_config, load_notification_config


def render_assistant_config_tab(assistant) -> None:
    st.subheader("策略参数配置")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 选股参数")

        min_score = st.slider(
            "最低评分",
            50, 80, int(float(assistant.get_config('min_score'))),
            key="assistant_min_score_cfg",
            help="只推荐评分高于此值的股票",
        )

        market_cap_min = st.number_input(
            "最小市值（亿）",
            50, 500,
            int(float(assistant.get_config('market_cap_min')) / 100000000),
            key="assistant_mcap_min_cfg",
        )

        market_cap_max = st.number_input(
            "最大市值（亿）",
            100, 1000,
            int(float(assistant.get_config('market_cap_max')) / 100000000),
            key="assistant_mcap_max_cfg",
        )

        recommend_count = st.slider(
            "推荐数量",
            3, 10, int(assistant.get_config('recommend_count')),
            key="assistant_rec_count_cfg",
        )

    with col2:
        st.markdown("### 风控参数")

        take_profit = st.slider(
            "止盈比例（%）",
            3, 15, int(float(assistant.get_config('take_profit_pct')) * 100),
            help="达到此涨幅时提醒止盈",
        )

        stop_loss = st.slider(
            "止损比例（%）",
            2, 10, int(float(assistant.get_config('stop_loss_pct')) * 100),
            key="assistant_stop_loss_cfg",
            help="达到此跌幅时提醒止损",
        )

        single_position = st.slider(
            "单只仓位（%）",
            10, 30, int(float(assistant.get_config('single_position_pct')) * 100),
            key="assistant_single_pos_cfg",
            help="单只股票最大仓位比例",
        )

        max_position = st.slider(
            "最大仓位（%）",
            50, 100, int(float(assistant.get_config('max_position_pct')) * 100),
            key="assistant_max_pos_cfg",
            help="总仓位上限",
        )

    st.markdown("---")
    st.markdown("### 性能匹配参数")
    runtime_profile_cfg = assistant.get_config('runtime_profile') or 'unknown'
    st.caption(f"当前运行档位：`{runtime_profile_cfg}`（由系统自动识别，可手工覆盖关键参数）")
    col_p1, col_p2 = st.columns(2)
    with col_p1:
        scan_candidate_limit_cfg = int(float(assistant.get_config('scan_candidate_limit') or 1500))
        scan_candidate_limit = st.slider(
            "候选股票上限",
            200, 2500, max(200, min(2500, scan_candidate_limit_cfg)),
            step=50,
            key="assistant_scan_candidate_limit_cfg",
            help="越大越全面但越慢；交互场景建议 200-600。",
        )
    with col_p2:
        scan_history_bars_cfg = int(float(assistant.get_config('scan_history_bars') or 160))
        scan_history_bars = st.slider(
            "历史K线窗口",
            100, 260, max(100, min(260, scan_history_bars_cfg)),
            step=10,
            key="assistant_scan_history_bars_cfg",
            help="越大越稳健但越慢；交互场景建议 120-180。",
        )

    st.markdown("---")
    st.markdown("### 自学习自动调参")
    st.caption("基于近30天学习卡片结果（T+1/T+5/T+20）自动给出参数优化建议。")
    col_t1, col_t2 = st.columns([1, 1])
    with col_t1:
        if st.button("生成调参建议", key="assistant_gen_tuning", use_container_width=True):
            with st.spinner("正在分析最近学习结果..."):
                if hasattr(assistant, "get_auto_tuning_recommendation"):
                    tuning_rec = assistant.get_auto_tuning_recommendation(lookback_days=30, min_samples=8)
                else:
                    tuning_rec = {"ok": False, "reason": "当前部署版本不支持自动调参（缺少 get_auto_tuning_recommendation）"}
            st.session_state["assistant_tuning_rec"] = tuning_rec
    with col_t2:
        if st.button("应用自动调参", key="assistant_apply_tuning", use_container_width=True):
            with st.spinner("正在应用调参..."):
                base_rec = st.session_state.get("assistant_tuning_rec")
                if hasattr(assistant, "apply_auto_tuning"):
                    tune_result = assistant.apply_auto_tuning(base_rec if isinstance(base_rec, dict) else None)
                else:
                    tune_result = {"ok": False, "applied": False, "reason": "当前部署版本不支持自动调参（缺少 apply_auto_tuning）"}
            st.session_state["assistant_tuning_apply"] = tune_result
            if tune_result.get("ok") and tune_result.get("applied"):
                st.success("自动调参已应用")
                st.rerun()
            elif tune_result.get("ok"):
                st.info("当前参数无需变更")
            else:
                st.warning(f"自动调参未应用：{tune_result.get('reason', 'unknown')}")

    tuning_rec = st.session_state.get("assistant_tuning_rec")
    if isinstance(tuning_rec, dict):
        if tuning_rec.get("ok"):
            metrics = tuning_rec.get("metrics", {})
            st.caption(
                f"样本={metrics.get('sample_count', 0)} | "
                f"D5胜率={float(metrics.get('d5_win_rate', 0))*100:.1f}% | "
                f"D5均值={metrics.get('d5_avg_ret_pct', 0):.2f}% | "
                f"波动={metrics.get('d5_vol_pct', 0):.2f}"
            )
            changes = tuning_rec.get("changes", {})
            if changes:
                change_df = pd.DataFrame(
                    [{"参数": k, "当前": v.get("from"), "建议": v.get("to")} for k, v in changes.items()]
                )
                st.dataframe(change_df, use_container_width=True, hide_index=True)
            else:
                st.info("暂无建议变更，参数状态稳定。")
        else:
            st.info(f"暂无法生成建议：{tuning_rec.get('reason', '数据不足')}")

    tuning_apply = st.session_state.get("assistant_tuning_apply")
    if isinstance(tuning_apply, dict) and tuning_apply.get("applied"):
        st.caption("最近一次自动调参已完成。")

    st.markdown("---")
    st.markdown("### 策略级止损止盈参数")
    risk_map = assistant.get_strategy_risk_param_map() if hasattr(assistant, "get_strategy_risk_param_map") else {}
    strategy_keys = ["v4", "v5", "v6", "v7", "v8", "v9", "stable", "combo", "ai"]
    strategy_pick = st.selectbox("选择策略", strategy_keys, key="assistant_risk_strategy_pick")
    cur_cfg = risk_map.get(strategy_pick, {})
    c_r1, c_r2, c_r3 = st.columns(3)
    with c_r1:
        sl_pct = st.slider(
            "基础止损(%)",
            2.0, 20.0, float(cur_cfg.get("stop_loss_pct", 0.06)) * 100.0, 0.1,
            key="assistant_risk_sl_pct",
        )
    with c_r2:
        tp_pct = st.slider(
            "基础止盈(%)",
            4.0, 35.0, float(cur_cfg.get("take_profit_pct", 0.12)) * 100.0, 0.1,
            key="assistant_risk_tp_pct",
        )
    with c_r3:
        tp_sl_ratio = st.slider(
            "目标盈亏比",
            1.1, 3.5, float(cur_cfg.get("tp_sl_ratio", 1.8)), 0.05,
            key="assistant_risk_ratio",
        )
    c_r4, c_r5 = st.columns(2)
    with c_r4:
        min_sl_pct = st.slider(
            "最小止损(%)",
            2.0, 10.0, float(cur_cfg.get("min_stop_loss_pct", 0.04)) * 100.0, 0.1,
            key="assistant_risk_min_sl",
        )
    with c_r5:
        max_sl_pct = st.slider(
            "最大止损(%)",
            4.0, 20.0, float(cur_cfg.get("max_stop_loss_pct", 0.12)) * 100.0, 0.1,
            key="assistant_risk_max_sl",
        )

    c_ra, c_rb = st.columns([1, 1])
    with c_ra:
        if st.button("保存当前策略风险参数", key="assistant_save_strategy_risk", use_container_width=True):
            if hasattr(assistant, "update_strategy_risk_params"):
                out = assistant.update_strategy_risk_params(
                    strategy_pick,
                    {
                        "stop_loss_pct": sl_pct / 100.0,
                        "take_profit_pct": tp_pct / 100.0,
                        "min_stop_loss_pct": min_sl_pct / 100.0,
                        "max_stop_loss_pct": max_sl_pct / 100.0,
                        "tp_sl_ratio": tp_sl_ratio,
                    },
                )
                if out.get("ok"):
                    st.success(f"{strategy_pick.upper()} 风险参数已保存")
                    st.rerun()
                else:
                    st.error(f"保存失败: {out}")
            else:
                st.warning("当前部署版本不支持策略级参数保存。")
    with c_rb:
        if st.button("生成并应用分策略自动调参", key="assistant_apply_strategy_tune", use_container_width=True):
            if hasattr(assistant, "get_strategy_risk_tuning_recommendation") and hasattr(assistant, "apply_strategy_risk_tuning"):
                rec = assistant.get_strategy_risk_tuning_recommendation(lookback_days=60, min_samples=4)
                st.session_state["assistant_strategy_risk_rec"] = rec
                out = assistant.apply_strategy_risk_tuning(rec)
                st.session_state["assistant_strategy_risk_apply"] = out
                if out.get("ok") and out.get("applied"):
                    st.success(f"分策略自动调参已应用（{out.get('applied_count', 0)}个策略）")
                    st.rerun()
                elif out.get("ok"):
                    st.info("当前分策略参数无需调整。")
                else:
                    st.warning(f"分策略自动调参失败：{out.get('reason', 'unknown')}")
            else:
                st.warning("当前部署版本不支持分策略自动调参。")

    strategy_rec = st.session_state.get("assistant_strategy_risk_rec")
    if isinstance(strategy_rec, dict):
        changes = strategy_rec.get("changes", {})
        if changes:
            rows = []
            for k, v in changes.items():
                rows.append(
                    {
                        "策略": k.upper(),
                        "样本": int(v.get("samples", 0)),
                        "模式": v.get("reason", "neutral"),
                        "止损(当前->建议)": f"{float(v['from']['stop_loss_pct'])*100:.2f}% -> {float(v['to']['stop_loss_pct'])*100:.2f}%",
                        "止盈(当前->建议)": f"{float(v['from']['take_profit_pct'])*100:.2f}% -> {float(v['to']['take_profit_pct'])*100:.2f}%",
                    }
                )
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.caption("分策略自动调参：暂无变更建议。")

    st.markdown("###  通知设置")
    st.info(
        """
        **通知功能说明**
        -  支持邮件通知（推荐）
        -  支持企业微信通知
        -  支持钉钉通知
        -  每日推荐 + 止盈止损提醒
        """
    )
    _notif_cfg = load_notification_config("notification_config.json")
    _email_cfg = (_notif_cfg.get("email") or {}) if isinstance(_notif_cfg, dict) else {}
    _wechat_cfg = (_notif_cfg.get("wechat_work") or {}) if isinstance(_notif_cfg, dict) else {}
    _dingtalk_cfg = (_notif_cfg.get("dingtalk") or {}) if isinstance(_notif_cfg, dict) else {}

    col1, col2 = st.columns(2)

    with col1:
        enable_email = st.checkbox(
            " 启用邮件通知",
            value=bool(_email_cfg.get("enabled", False)),
            key="enable_email_notif",
        )
        email_address = ""
        smtp_server = str(_email_cfg.get("smtp_server") or "smtp.qq.com")
        smtp_user = ""
        smtp_password = ""

        if enable_email:
            email_address = st.text_input(
                "接收邮箱",
                placeholder="your@email.com",
                value=((_email_cfg.get("receiver_emails") or [""])[0] if isinstance(_email_cfg.get("receiver_emails"), list) else ""),
                key="email_addr",
            )
            smtp_server = st.text_input(
                "SMTP服务器",
                value=smtp_server,
                help="QQ邮箱: smtp.qq.com, 163邮箱: smtp.163.com",
                key="smtp_server",
            )
            smtp_user = st.text_input(
                "SMTP用户名",
                placeholder="your@email.com",
                value=str(_email_cfg.get("sender_email") or ""),
                key="smtp_user",
            )
            smtp_password = st.text_input(
                "SMTP密码/授权码",
                type="password",
                help="QQ/163邮箱需要使用授权码，不是登录密码",
                key="smtp_pwd",
            )

    with col2:
        wechat_webhook = ""
        dingtalk_webhook = ""
        enable_wechat = st.checkbox(
            " 启用企业微信通知",
            value=bool(_wechat_cfg.get("enabled", False)),
            key="enable_wechat_notif",
            help="需要企业微信群机器人Webhook",
        )
        if enable_wechat:
            wechat_webhook = st.text_input(
                "企业微信Webhook URL",
                placeholder="https://qyapi.weixin.qq.com/...",
                value=str(_wechat_cfg.get("webhook_url") or ""),
                key="wechat_webhook",
            )

        enable_dingtalk = st.checkbox(
            " 启用钉钉通知",
            value=bool(_dingtalk_cfg.get("enabled", False)),
            key="enable_dingtalk_notif",
            help="需要钉钉群机器人Webhook",
        )
        if enable_dingtalk:
            dingtalk_webhook = st.text_input(
                "钉钉Webhook URL",
                placeholder="https://oapi.dingtalk.com/...",
                value=str(_dingtalk_cfg.get("webhook_url") or ""),
                key="dingtalk_webhook",
            )

    if enable_email or enable_wechat or enable_dingtalk:
        st.markdown("---")
        st.markdown("####  通知内容设置")
        col1, col2 = st.columns(2)
        with col1:
            st.checkbox("每日选股推荐", value=True, key="notify_daily")
            st.checkbox("止损提醒", value=True, key="notify_stop")
        with col2:
            st.checkbox("止盈提醒", value=True, key="notify_profit")
            st.checkbox("持仓汇总（每周）", value=True, key="notify_hold")

    st.markdown("---")
    col1, col2, col3 = st.columns([2, 2, 1])

    with col1:
        if st.button("保存配置", type="primary"):
            assistant.update_config('min_score', str(min_score))
            assistant.update_config('market_cap_min', str(market_cap_min * 100000000))
            assistant.update_config('market_cap_max', str(market_cap_max * 100000000))
            assistant.update_config('recommend_count', str(recommend_count))
            assistant.update_config('take_profit_pct', str(take_profit / 100))
            assistant.update_config('stop_loss_pct', str(stop_loss / 100))
            assistant.update_config('single_position_pct', str(single_position / 100))
            assistant.update_config('max_position_pct', str(max_position / 100))
            assistant.update_config('scan_candidate_limit', str(scan_candidate_limit))
            assistant.update_config('scan_history_bars', str(scan_history_bars))

            if enable_email or enable_wechat or enable_dingtalk:
                try:
                    can_save_notify = True
                    if enable_email and (not email_address or not smtp_user or not smtp_password):
                        st.error("邮件通知已启用，但邮箱/SMTP账号/授权码未填写完整")
                        can_save_notify = False
                    if can_save_notify:
                        notification_config = build_notification_config(
                            enable_email=enable_email,
                            smtp_server=smtp_server,
                            smtp_user=smtp_user,
                            smtp_password=smtp_password,
                            email_address=email_address,
                            enable_wechat=enable_wechat,
                            wechat_webhook=wechat_webhook,
                            enable_dingtalk=enable_dingtalk,
                            dingtalk_webhook=dingtalk_webhook,
                        )
                        with open('notification_config.json', 'w', encoding='utf-8') as f:
                            json.dump(notification_config, f, indent=2, ensure_ascii=False)
                        st.success("配置已保存（包括通知设置）")
                except Exception as e:
                    st.error(f"保存通知配置失败: {e}")
            else:
                st.success("策略配置已保存")

            st.rerun()

    with col2:
        if (enable_email or enable_wechat or enable_dingtalk) and st.button("发送测试通知", type="secondary"):
            try:
                NotificationService = load_notification_service_class()
                notifier = NotificationService()
                test_message = """
                 智能交易助手测试通知

                如果您收到此消息，说明通知功能已正常配置！

                系统将自动发送：
                -  每日选股推荐
                -  止盈提醒
                -  止损提醒
                -  持仓汇总
                """
                success = notifier.send_notification("【测试】智能交易助手", test_message)
                if success:
                    st.success("测试通知已发送，请查收！")
                else:
                    st.error("发送失败，请检查配置")
            except Exception as e:
                st.error(f"发送测试失败: {e}")

    with col3:
        if st.button("帮助文档"):
            st.info(
                """
                **邮件配置帮助**

                QQ邮箱：
                1. 开启SMTP服务
                2. 生成授权码
                3. 使用授权码登录

                服务器：smtp.qq.com
                端口：465（SSL）

                163邮箱：
                服务器：smtp.163.com
                端口：465（SSL）

                Gmail：
                服务器：smtp.gmail.com
                端口：587（TLS）
                """
            )
