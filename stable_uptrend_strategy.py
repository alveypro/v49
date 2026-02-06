"""
Stable Uptrend Strategy (extracted)

Usage (inside your system class):
    from stable_uptrend_strategy import render_stable_uptrend_strategy
    render_stable_uptrend_strategy(self, pro=pro)

Expected ctx (self) methods/attrs:
    - _permanent_db_available()
    - _get_global_filters()
    - _filter_summary_text(...)
    - get_real_stock_data_optimized()
    - _apply_global_filters(...)
    - _load_history_from_sqlite(...)
    - TUSHARE_AVAILABLE (bool) OR property/attr
"""

from __future__ import annotations

from datetime import datetime, timedelta
import os
import json
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st


def render_stable_uptrend_strategy(ctx, pro=None) -> None:
    """Render the Stable Uptrend Strategy UI and results."""
    st.header("📈 稳定上涨策略")
    st.caption("目标：筛选“底部启动 / 回撤企稳 / 二次启动”的稳定上涨候选股（非收益保证）")

    # 自动进化参数（可选）
    evolve_path = os.path.join(os.path.dirname(__file__), "evolution", "stable_uptrend_best.json")
    evolve = {}
    if os.path.exists(evolve_path):
        try:
            with open(evolve_path, "r", encoding="utf-8") as f:
                evolve = json.load(f) or {}
            params = evolve.get("params", {})
            st.success(f"🧬 已应用自动进化参数（{evolve.get('run_at', 'unknown')}）")
        except Exception:
            params = {}
    else:
        params = {}

    tushare_ok = bool(getattr(ctx, "TUSHARE_AVAILABLE", False))
    if not tushare_ok and not ctx._permanent_db_available():
        st.error("❌ Tushare 未配置且本地数据库不可用")
        return

    # 参数设置
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        lb_default = int(params.get("lookback_days", 120)) if isinstance(params.get("lookback_days"), (int, float)) else 120
        lookback_days = st.slider("历史窗口(天)", 60, 180, lb_default, step=10, key="stable_lookback_days")
    with col2:
        md_default = int(params.get("max_drawdown_pct", 15)) if isinstance(params.get("max_drawdown_pct"), (int, float)) else 15
        max_drawdown = st.slider("最大回撤(%)", 5, 30, md_default, key="stable_max_drawdown") / 100.0
    with col3:
        vol_default = float(params.get("vol_max_pct", 4.0)) if isinstance(params.get("vol_max_pct"), (int, float)) else 4.0
        vol_max = st.slider("波动上限(20日日波动%)", 2.0, 8.0, vol_default, key="stable_vol_max") / 100.0
    with col4:
        rb_default = int(params.get("rebound_min_pct", 10)) if isinstance(params.get("rebound_min_pct"), (int, float)) else 10
        rebound_min = st.slider("反弹幅度(%)", 5, 25, rb_default, key="stable_rebound_min") / 100.0

    col5, col6, col7 = st.columns(3)
    with col5:
        cc_default = int(params.get("candidate_count", 200)) if isinstance(params.get("candidate_count"), (int, float)) else 200
        candidate_count = st.slider("候选数量(按成交额)", 50, 2000, cc_default, step=50, key="stable_candidate_count")
    with col6:
        rc_default = int(params.get("result_count", 30)) if isinstance(params.get("result_count"), (int, float)) else 30
        result_count = st.slider("输出数量", 10, 100, rc_default, step=5, key="stable_result_count")
    with col7:
        mt_default = float(params.get("min_turnover", 5.0)) if isinstance(params.get("min_turnover"), (int, float)) else 5.0
        min_turnover = st.slider("最低成交额(亿)", 1.0, 50.0, mt_default, step=1.0, key="stable_min_turnover")

    gf = ctx._get_global_filters()
    evo_min_mv = params.get("min_mv")
    evo_max_mv = params.get("max_mv")
    if isinstance(evo_min_mv, (int, float)) and isinstance(evo_max_mv, (int, float)):
        mv_default = (int(evo_min_mv), int(evo_max_mv))
    else:
        mv_default = (gf.get("min_mv", 100), gf.get("max_mv", 5000))
    min_mv, max_mv = st.slider(
        "市值范围(亿)",
        min_value=10,
        max_value=10000,
        value=mv_default,
        step=10,
        key="stable_mv_range",
    )
    st.caption(ctx._filter_summary_text(min_mv=min_mv, max_mv=max_mv))

    st.info("⚠️ 该策略基于历史形态与风险约束，输出为概率评估，不保证未来收益。")

    data = ctx.get_real_stock_data_optimized()
    data = ctx._apply_global_filters(data, min_mv=min_mv, max_mv=max_mv, use_price=False, use_turnover=False)
    if data.empty:
        st.error("❌ 无法获取市场数据")
        return

    # 基础过滤：非ST、成交额、价格合理
    filtered = data.copy()
    filtered = filtered[filtered["成交额"] >= min_turnover * 1e8]
    filtered = filtered[(filtered["价格"] > 2) & (filtered["价格"] < 200)]
    filtered = filtered.sort_values("成交额", ascending=False).head(candidate_count)

    if filtered.empty:
        st.warning("未找到满足基础条件的股票")
        return

    start_date = (datetime.now() - timedelta(days=lookback_days * 2)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")

    progress = st.progress(0.0)
    status = st.empty()
    results = []

    for idx, row in enumerate(filtered.itertuples(index=False), 1):
        ts_code = getattr(row, "股票代码")
        name = getattr(row, "股票名称")
        status.text(f"正在分析 {name} ({idx}/{len(filtered)})")
        score_info = _score_stable_uptrend(
            ctx,
            ts_code,
            lookback_days,
            max_drawdown,
            vol_max,
            rebound_min,
            start_date,
            end_date,
            pro=pro,
        )
        if score_info:
            results.append(
                {
                    "股票代码": ts_code,
                    "股票名称": name,
                    "稳定上涨评分": round(score_info["score"], 1),
                    "最大回撤": f"{score_info['max_dd']*100:.1f}%",
                    "20日波动率": f"{score_info['vol']*100:.2f}%",
                    "反弹幅度": f"{score_info['rebound']*100:.1f}%",
                    "趋势": "✅" if score_info["trend_ok"] else "❌",
                    "二次启动": "✅" if score_info["breakout"] else "❌",
                    "建议持有周期": score_info["hold_days"],
                }
            )
        progress.progress(idx / len(filtered))

    progress.empty()
    status.empty()

    if not results:
        st.warning("未筛选到符合条件的股票，请放宽参数或减少约束")
        return

    result_df = pd.DataFrame(results).sort_values("稳定上涨评分", ascending=False).head(result_count)
    st.subheader(f"🎯 稳定上涨候选池（Top {len(result_df)}）")
    st.dataframe(result_df, use_container_width=True)


def _score_stable_uptrend(
    ctx,
    ts_code: str,
    lookback_days: int,
    max_drawdown: float,
    vol_max: float,
    rebound_min: float,
    start_date: str,
    end_date: str,
    pro=None,
) -> Optional[Dict[str, Any]]:
    """Calculate stable uptrend score (probability assessment, no return guarantee)."""
    try:
        # 优先使用本地数据库获取历史数据
        hist = ctx._load_history_from_sqlite(ts_code, start_date, end_date)
        if hist is None or hist.empty:
            if not bool(getattr(ctx, "TUSHARE_AVAILABLE", False)) or pro is None:
                return None
            hist = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if hist is None or hist.empty:
            return None

        hist = hist.sort_values("trade_date")
        if len(hist) < max(60, lookback_days // 2):
            return None

        close = hist["close"].astype(float).values
        if len(close) < 60:
            return None

        series = pd.Series(close)
        ma20 = series.rolling(20).mean()
        ma60 = series.rolling(60).mean()

        # 趋势条件
        trend_ok = ma20.iloc[-1] > ma60.iloc[-1] and ma20.iloc[-1] > ma20.iloc[-5]

        # 最大回撤
        rolling_peak = series.cummax()
        drawdown = (rolling_peak - series) / rolling_peak
        max_dd = float(drawdown.tail(60).max())
        if max_dd > max_drawdown:
            return None

        # 反弹幅度（20日低点到当前）
        recent_low = float(series.tail(20).min())
        rebound = (series.iloc[-1] / recent_low - 1.0) if recent_low > 0 else 0.0
        if rebound < rebound_min:
            return None

        # 波动率（20日收益）
        returns = series.pct_change().dropna()
        vol = float(returns.tail(20).std())
        if vol > vol_max:
            return None

        # 二次启动：突破近20日高点
        breakout = series.iloc[-1] > series.tail(21).iloc[:-1].max()

        # 评分
        score = 0.0
        score += 30.0 if trend_ok else 0.0
        score += max(0.0, 20.0 * (1.0 - max_dd / max_drawdown))
        score += min(20.0, 100.0 * rebound)
        score += max(0.0, 20.0 * (1.0 - vol / vol_max))
        score += 10.0 if breakout else 0.0

        hold_days = "20-30天" if breakout else "15-25天"

        return {
            "score": score,
            "max_dd": max_dd,
            "rebound": rebound,
            "vol": vol,
            "trend_ok": trend_ok,
            "breakout": breakout,
            "hold_days": hold_days,
        }
    except Exception:
        return None
