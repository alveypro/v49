from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import streamlit as st


def render_assistant_config_page(
    *,
    assistant: Any,
    notification_service_cls: Any,
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
) -> None:
    st.subheader("策略参数配置")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 选股参数")
        min_score = st.slider("最低评分", 50, 80, int(float(assistant.get_config("min_score"))), key="assistant_min_score_cfg", help="只推荐评分高于此值的股票")
        market_cap_min = st.number_input("最小市值（亿）", 50, 500, int(float(assistant.get_config("market_cap_min")) / 100000000), key="assistant_mcap_min_cfg")
        market_cap_max = st.number_input("最大市值（亿）", 100, 1000, int(float(assistant.get_config("market_cap_max")) / 100000000), key="assistant_mcap_max_cfg")
        recommend_count = st.slider("推荐数量", 3, 10, int(assistant.get_config("recommend_count")), key="assistant_rec_count_cfg")

    with col2:
        st.markdown("### 风控参数")
        take_profit = st.slider("止盈比例（%）", 3, 15, int(float(assistant.get_config("take_profit_pct")) * 100), help="达到此涨幅时提醒止盈")
        stop_loss = st.slider("止损比例（%）", 2, 10, int(float(assistant.get_config("stop_loss_pct")) * 100), key="assistant_stop_loss_cfg", help="达到此跌幅时提醒止损")
        single_position = st.slider("单只仓位（%）", 10, 30, int(float(assistant.get_config("single_position_pct")) * 100), key="assistant_single_pos_cfg", help="单只股票最大仓位比例")
        max_position = st.slider("最大仓位（%）", 50, 100, int(float(assistant.get_config("max_position_pct")) * 100), key="assistant_max_pos_cfg", help="总仓位上限")

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
                change_df = pd.DataFrame([{"参数": k, "当前": v.get("from"), "建议": v.get("to")} for k, v in changes.items()])
                st.dataframe(change_df, use_container_width=True, hide_index=True)
            else:
                st.info("暂无建议变更，参数状态稳定。")
        else:
            st.info(f"暂无法生成建议：{tuning_rec.get('reason', '数据不足')}")

    tuning_apply = st.session_state.get("assistant_tuning_apply")
    if isinstance(tuning_apply, dict) and tuning_apply.get("applied"):
        st.caption("最近一次自动调参已完成。")

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

    col1, col2 = st.columns(2)
    email_address = ""
    smtp_server = "smtp.qq.com"
    smtp_user = ""
    smtp_password = ""
    wechat_webhook = ""
    dingtalk_webhook = ""
    with col1:
        enable_email = st.checkbox(" 启用邮件通知", value=False, key="enable_email_notif")
        if enable_email:
            email_address = st.text_input("接收邮箱", placeholder="your@email.com", key="email_addr")
            smtp_server = st.text_input("SMTP服务器", value="smtp.qq.com", help="QQ邮箱: smtp.qq.com, 163邮箱: smtp.163.com", key="smtp_server")
            smtp_user = st.text_input("SMTP用户名", placeholder="your@email.com", key="smtp_user")
            smtp_password = st.text_input("SMTP密码/授权码", type="password", help="QQ/163邮箱需要使用授权码，不是登录密码", key="smtp_pwd")

    with col2:
        enable_wechat = st.checkbox(" 启用企业微信通知", value=False, key="enable_wechat_notif", help="需要企业微信群机器人Webhook")
        if enable_wechat:
            wechat_webhook = st.text_input("企业微信Webhook URL", placeholder="https://qyapi.weixin.qq.com/...", key="wechat_webhook")
        enable_dingtalk = st.checkbox(" 启用钉钉通知", value=False, key="enable_dingtalk_notif", help="需要钉钉群机器人Webhook")
        if enable_dingtalk:
            dingtalk_webhook = st.text_input("钉钉Webhook URL", placeholder="https://oapi.dingtalk.com/...", key="dingtalk_webhook")

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
        if st.button("保存配置", type="primary", disabled=not airivo_has_role("admin")):
            if not airivo_guard_action("admin", "assistant_save_config", target="assistant_config", reason="save_assistant_config"):
                st.stop()
            assistant.update_config("min_score", str(min_score))
            assistant.update_config("market_cap_min", str(market_cap_min * 100000000))
            assistant.update_config("market_cap_max", str(market_cap_max * 100000000))
            assistant.update_config("recommend_count", str(recommend_count))
            assistant.update_config("take_profit_pct", str(take_profit / 100))
            assistant.update_config("stop_loss_pct", str(stop_loss / 100))
            assistant.update_config("single_position_pct", str(single_position / 100))
            assistant.update_config("max_position_pct", str(max_position / 100))

            if enable_email and email_address and smtp_user and smtp_password:
                try:
                    notification_config = {
                        "email": {
                            "enabled": True,
                            "smtp_server": smtp_server,
                            "smtp_port": 465 if "qq.com" in smtp_server else 587,
                            "smtp_user": smtp_user,
                            "smtp_password": smtp_password,
                            "from_addr": smtp_user,
                            "to_addr": email_address,
                        },
                        "wechat_work": {"enabled": enable_wechat, "webhook_url": wechat_webhook if enable_wechat else ""},
                        "dingtalk": {"enabled": enable_dingtalk, "webhook_url": dingtalk_webhook if enable_dingtalk else ""},
                    }
                    config_path = Path("notification_config.json")
                    with config_path.open("w", encoding="utf-8") as f:
                        json.dump(notification_config, f, indent=2, ensure_ascii=False)
                    airivo_append_action_audit("assistant_save_config", True, target="assistant_config", detail="config_and_notifications_saved")
                    st.success("配置已保存（包括通知设置）")
                except Exception as e:
                    airivo_append_action_audit("assistant_save_config", False, target="assistant_config", detail=str(e))
                    st.error(f"保存通知配置失败: {e}")
            else:
                airivo_append_action_audit("assistant_save_config", True, target="assistant_config", detail="strategy_config_saved")
                st.success("策略配置已保存")
            st.rerun()

    with col2:
        if (enable_email or enable_wechat or enable_dingtalk) and st.button("发送测试通知", type="secondary", disabled=not airivo_has_role("admin")):
            if not airivo_guard_action("admin", "send_test_notification", target="notification_channels", reason="manual_notification_test"):
                st.stop()
            try:
                if notification_service_cls is None:
                    raise RuntimeError("NotificationService unavailable")
                notifier = notification_service_cls()
                success = notifier.send_notification(
                    "【测试】智能交易助手",
                    """
 智能交易助手测试通知

如果您收到此消息，说明通知功能已正常配置！

系统将自动发送：
-  每日选股推荐
-  止盈提醒
-  止损提醒
-  持仓汇总
""",
                )
                if success:
                    airivo_append_action_audit("send_test_notification", True, target="notification_channels", detail="notification_sent")
                    st.success("测试通知已发送，请查收！")
                else:
                    airivo_append_action_audit("send_test_notification", False, target="notification_channels", detail="notification_send_failed")
                    st.error("发送失败，请检查配置")
            except Exception as e:
                airivo_append_action_audit("send_test_notification", False, target="notification_channels", detail=str(e))
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
