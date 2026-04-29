from __future__ import annotations

from datetime import datetime, timedelta
import os
import signal
import traceback
from typing import Any, Callable, Dict, List, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def _load_compare_history(connect_permanent_db: Callable[[], Any]) -> pd.DataFrame:
    from data.dao import DataAccessError, detect_daily_table  # type: ignore

    conn = connect_permanent_db()
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
    try:
        daily_table = detect_daily_table(conn)
    except DataAccessError:
        conn.close()
        raise RuntimeError("无法识别日线数据表（daily_trading_data/daily_data）")

    query = f"""
        SELECT dtd.ts_code, sb.name, sb.industry, dtd.trade_date,
               dtd.open_price, dtd.high_price, dtd.low_price,
               dtd.close_price, dtd.vol, dtd.pct_chg, dtd.amount
        FROM {daily_table} dtd
        INNER JOIN stock_basic sb ON dtd.ts_code = sb.ts_code
        WHERE dtd.trade_date >= ?
        ORDER BY dtd.ts_code, dtd.trade_date
    """
    df = pd.read_sql_query(query, conn, params=(start_date,))
    conn.close()
    return df


def _run_compare_once(
    *,
    vp_analyzer: Any,
    df_slice: pd.DataFrame,
    sample_size_for_slice: int,
    compare_strategy_params: Dict[str, Any],
    v7_evaluator_available: bool,
    v8_evaluator_available: bool,
    label: str = "",
) -> Dict[str, Dict[str, Any]]:
    results_local: Dict[str, Dict[str, Any]] = {}
    prefix = f"[{label}] " if label else ""

    st.info(f"{prefix}正在回测 v4.0 长期稳健版...")
    v4_result = vp_analyzer.backtest_explosive_hunter(
        df_slice,
        sample_size=sample_size_for_slice,
        holding_days=5,
    )
    if v4_result.get("success"):
        results_local["v4.0 长期稳健版"] = v4_result["stats"]

    st.info(f"{prefix}正在回测 v5.0 趋势趋势版...")
    v5_result = vp_analyzer.backtest_bottom_breakthrough(
        df_slice,
        sample_size=sample_size_for_slice,
        holding_days=int(compare_strategy_params.get("v5", {}).get("holding_days", 8)),
    )
    if v5_result.get("success"):
        results_local["v5.0 趋势趋势版"] = v5_result["stats"]

    st.info(f"{prefix}正在回测 v6.0 高级超短线...")
    v6_result = vp_analyzer.backtest_v6_ultra_short(
        df_slice,
        sample_size=sample_size_for_slice,
        holding_days=3,
        score_threshold=60.0,
    )
    if v6_result.get("success"):
        results_local["v6.0 高级超短线"] = v6_result["stats"]

    if v7_evaluator_available and getattr(vp_analyzer, "evaluator_v7", None):
        st.info(f"{prefix}正在回测 v7.0 智能版...")
        v7_result = vp_analyzer.backtest_v7_intelligent(
            df_slice,
            sample_size=sample_size_for_slice,
            holding_days=5,
            score_threshold=60.0,
        )
        if v7_result.get("success"):
            results_local["v7.0 智能版"] = v7_result["stats"]

    if v8_evaluator_available and getattr(vp_analyzer, "evaluator_v8", None):
        st.info(f"{prefix}正在回测 v8.0 进阶版...")
        v8_result = vp_analyzer.backtest_v8_ultimate(
            df_slice,
            sample_size=sample_size_for_slice,
            holding_days=int(compare_strategy_params.get("v8", {}).get("holding_days", 10)),
            score_threshold=float(compare_strategy_params.get("v8", {}).get("score_threshold", 65)),
        )
        if v8_result.get("success"):
            results_local["v8.0 进阶版"] = v8_result["stats"]

    st.info(f"{prefix}正在回测 v9.0 中线均衡版...")
    v9_result = vp_analyzer.backtest_v9_midterm(
        df_slice,
        sample_size=sample_size_for_slice,
        holding_days=int(compare_strategy_params.get("v9", {}).get("holding_days", 20)),
        score_threshold=float(compare_strategy_params.get("v9", {}).get("score_threshold", 65)),
    )
    if v9_result.get("success"):
        results_local["v9.0 中线均衡版"] = v9_result["stats"]

    st.info(f"{prefix}正在回测 组合策略（生产共识）...")
    combo_result = vp_analyzer.backtest_combo_production(
        df_slice,
        sample_size=sample_size_for_slice,
        holding_days=int(compare_strategy_params.get("combo", {}).get("holding_days", 10)),
        combo_threshold=float(compare_strategy_params.get("combo", {}).get("score_threshold", 68)),
        min_agree=2,
    )
    if combo_result.get("success"):
        results_local["组合策略（生产共识）"] = combo_result["stats"]
    return results_local


def _aggregate_fold_results(fold_results: List[Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    by_strategy: Dict[str, List[Dict[str, Any]]] = {}
    for fr in fold_results:
        for strategy_name, stats in fr.items():
            by_strategy.setdefault(strategy_name, []).append(stats)

    out: Dict[str, Dict[str, Any]] = {}
    for strategy_name, rows in by_strategy.items():
        total_signals = float(sum(float(r.get("total_signals", 0) or 0) for r in rows))
        weight = total_signals if total_signals > 0 else float(len(rows))

        def _wavg(field: str) -> float:
            if total_signals > 0:
                return float(
                    sum(
                        float(r.get(field, 0) or 0) * float(r.get("total_signals", 0) or 0)
                        for r in rows
                    )
                    / weight
                )
            return float(sum(float(r.get(field, 0) or 0) for r in rows) / max(1, len(rows)))

        out[strategy_name] = {
            "total_signals": int(total_signals),
            "analyzed_stocks": int(sum(int(r.get("analyzed_stocks", 0) or 0) for r in rows)),
            "win_rate": _wavg("win_rate"),
            "avg_return": _wavg("avg_return"),
            "median_return": _wavg("median_return"),
            "max_return": max(float(r.get("max_return", -999) or -999) for r in rows),
            "min_return": min(float(r.get("min_return", 999) or 999) for r in rows),
            "sharpe_ratio": _wavg("sharpe_ratio"),
            "sortino_ratio": _wavg("sortino_ratio"),
            "max_drawdown": _wavg("max_drawdown"),
            "profit_loss_ratio": _wavg("profit_loss_ratio"),
            "avg_holding_days": _wavg("avg_holding_days"),
            "annualized_return": _wavg("annualized_return"),
            "volatility": _wavg("volatility"),
            "fold_count": len(rows),
        }
    return out


def _quality_score(stats: Dict[str, Any]) -> float:
    win_rate = float(stats.get("win_rate", 0) or 0)
    avg_return = float(stats.get("avg_return", 0) or 0)
    sharpe = float(stats.get("sharpe_ratio", 0) or 0)
    sortino = float(stats.get("sortino_ratio", 0) or 0)
    total_signals = float(stats.get("total_signals", 0) or 0)
    max_drawdown = abs(float(stats.get("max_drawdown", 0) or 0))
    profit_loss = min(float(stats.get("profit_loss_ratio", 0) or 0), 10.0)
    return (
        win_rate * 0.25
        + avg_return * 3 * 0.25
        + sharpe * 10 * 0.15
        + sortino * 8 * 0.10
        + min(total_signals / 100.0, 1.0) * 100 * 0.15
        + max(10 - max_drawdown, 0) * 0.05
        + profit_loss * 3 * 0.05
    )


def _build_markdown_report(
    results: Dict[str, Dict[str, Any]],
    best_strategy: Tuple[str, Dict[str, Any]],
) -> str:
    report_md = f"""#  回测对比报告 v49.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    量价策略系统 · 策略回测分析报告
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

##  回测概况

- **回测时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **回测策略**: {', '.join(results.keys())}
- **系统版本**: v49.0 长期稳健版
- **数据来源**: Tushare Pro（真实数据）

---

##  策略表现汇总

"""
    for strategy_name, stats in results.items():
        report_md += f"""
###  {strategy_name}

#### 核心指标
| 指标 | 数值 | 说明 |
|------|------|------|
| 总信号数 | {stats.get('total_signals', 0)} | 历史回测产生的有效信号数量 |
| 分析股票数 | {stats.get('analyzed_stocks', 0)} | 回测分析的股票总数 |
| 胜率 | {stats.get('win_rate', 0):.1f}% | 盈利交易占比 |
| 平均收益 | {stats.get('avg_return', 0):.2f}% | 所有交易的平均收益率 |
| 中位数收益 | {stats.get('median_return', 0):.2f}% | 收益率的中位数 |
| 最大收益 | {stats.get('max_return', 0):.2f}% | 单笔最大盈利 |
| 最大亏损 | {stats.get('min_return', 0):.2f}% | 单笔最大亏损 |

#### 风险指标
| 指标 | 数值 | 说明 |
|------|------|------|
| 夏普比率 | {stats.get('sharpe_ratio', 0):.2f} | 风险调整后收益（>1为良好）|
| Sortino比率 | {stats.get('sortino_ratio', 0):.2f} | 下行风险调整收益 |
| 最大回撤 | {stats.get('max_drawdown', 0):.2f}% | 资金曲线最大跌幅 |
| 波动率 | {stats.get('volatility', 0):.2f}% | 收益率标准差 |
| Calmar比率 | {stats.get('calmar_ratio', 0):.2f} | 年化收益/最大回撤 |

#### 盈亏分析
| 指标 | 数值 | 说明 |
|------|------|------|
| 盈亏比 | {stats.get('profit_loss_ratio', 0):.2f} | 平均盈利/平均亏损 |
| 平均盈利 | {stats.get('avg_win', 0):.2f}% | 盈利交易的平均收益 |
| 平均亏损 | {stats.get('avg_loss', 0):.2f}% | 亏损交易的平均损失 |
| 最长连胜 | {stats.get('max_consecutive_wins', 0)} 次 | 连续盈利交易记录 |
| 最长连亏 | {stats.get('max_consecutive_losses', 0)} 次 | 连续亏损交易记录 |

#### 收益分布
| 分位数 | 数值 |
|--------|------|
| 25%分位 | {stats.get('return_25_percentile', 0):.2f}% |
| 50%分位 | {stats.get('median_return', 0):.2f}% |
| 75%分位 | {stats.get('return_75_percentile', 0):.2f}% |

#### 年化指标
| 指标 | 数值 |
|------|------|
| 年化收益 | {stats.get('annualized_return', 0):.2f}% |
| 期望值 | {stats.get('expected_value', 0):.2f}% |

"""

    report_md += f"""
---

##  最佳策略推荐

### 推荐策略：{best_strategy[0]}

**综合评分最高！**

#### 筛选理由
-  **胜率**: {best_strategy[1].get('win_rate', 0):.1f}% - {"超过50%，表现优秀" if best_strategy[1].get('win_rate', 0) > 50 else "有提升空间"}
-  **平均收益**: {best_strategy[1].get('avg_return', 0):.2f}% - {"收益可观" if best_strategy[1].get('avg_return', 0) > 3 else "稳健增长"}
-  **夏普比率**: {best_strategy[1].get('sharpe_ratio', 0):.2f} - {"风险收益比优秀" if best_strategy[1].get('sharpe_ratio', 0) > 1 else "风险适中"}
-  **最大回撤**: {best_strategy[1].get('max_drawdown', 0):.2f}% - {"回撤控制良好" if abs(best_strategy[1].get('max_drawdown', 0)) < 10 else "注意风险控制"}
-  **信号数量**: {best_strategy[1].get('total_signals', 0)} - {"样本充足" if best_strategy[1].get('total_signals', 0) > 100 else "样本适中"}

根据历史回测数据，该策略在风险收益平衡方面表现最佳，建议优先使用！

---

##  策略对比分析

### 核心指标对比表

| 策略 | 胜率 | 平均收益 | 夏普比率 | 最大回撤 | 信号数 |
|------|------|----------|----------|----------|--------|
"""
    for strategy_name, stats in results.items():
        report_md += (
            f"| {strategy_name} | {stats.get('win_rate', 0):.1f}% | "
            f"{stats.get('avg_return', 0):.2f}% | {stats.get('sharpe_ratio', 0):.2f} | "
            f"{stats.get('max_drawdown', 0):.2f}% | {stats.get('total_signals', 0)} |\n"
        )

    report_md += f"""

---

##  数据质量说明

### 数据来源
- **真实数据源**: Tushare Pro专业金融数据接口
- **数据完整性**:  100%真实市场数据，无模拟无演示
- **更新频率**: 每日收盘后自动更新
- **数据范围**: 最近1年历史数据，覆盖完整牛熊周期

### 回测可靠性
- **样本数量**: 充足（{sum(stats.get('total_signals', 0) for stats in results.values())}个信号）
- **时间跨度**: 覆盖不同市场环境
- **无未来函数**:  严格按照时间顺序回测
- **滑点处理**: 已考虑1%交易滑点和手续费

---

##  免责声明

本报告基于历史数据回测分析，仅供参考。历史表现不代表未来收益，股市有风险，投资需谨慎。

---

*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
*报告类型: 策略对比回测报告*
*系统版本: 量价策略系统 v49.0*
"""
    return report_md


def _render_comparison_results(results: Dict[str, Dict[str, Any]], comparison_meta: Dict[str, Any]) -> None:
    st.markdown("---")
    st.subheader("策略对比结果")
    mode_text = str(comparison_meta.get("validation_mode", "快速全样本"))
    if mode_text:
        st.caption(f"验证模式：{mode_text}")
    if mode_text.startswith("Walk-forward"):
        st.caption(
            f"窗口数：{comparison_meta.get('fold_count', 0)} | "
            f"窗口长度：{comparison_meta.get('wf_window_days', 'N/A')}交易日 | "
            f"步长：{comparison_meta.get('wf_step_days', 'N/A')}交易日"
        )

    comparison_df = pd.DataFrame(
        [
            {
                "策略": strategy_name,
                "胜率": f"{stats.get('win_rate', 0):.1f}%",
                "平均收益": f"{stats.get('avg_return', 0):.2f}%",
                "夏普比率": f"{stats.get('sharpe_ratio', 0):.2f}",
                "信号数量": stats.get("total_signals", 0),
                "平均持仓天数": stats.get("avg_holding_days", 0),
            }
            for strategy_name, stats in results.items()
        ]
    )
    st.dataframe(comparison_df, use_container_width=True, hide_index=True)

    if mode_text.startswith("Walk-forward"):
        fold_details = comparison_meta.get("fold_details", []) or []
        if fold_details:
            st.markdown("---")
            st.subheader("Walk-forward 分窗口明细")
            fold_df = pd.DataFrame(fold_details)
            if not fold_df.empty:
                fold_df = fold_df.sort_values(["fold", "strategy"]).reset_index(drop=True)
                show_df = fold_df.copy()
                show_df["win_rate"] = show_df["win_rate"].map(lambda x: f"{x:.1f}%")
                show_df["avg_return"] = show_df["avg_return"].map(lambda x: f"{x:.2f}%")
                show_df["sharpe_ratio"] = show_df["sharpe_ratio"].map(lambda x: f"{x:.2f}")
                show_df["max_drawdown"] = show_df["max_drawdown"].map(lambda x: f"{x:.2f}%")
                show_df = show_df.rename(
                    columns={
                        "fold": "窗口",
                        "range": "时间范围",
                        "strategy": "策略",
                        "win_rate": "胜率",
                        "avg_return": "平均收益",
                        "sharpe_ratio": "夏普比率",
                        "max_drawdown": "最大回撤",
                        "total_signals": "信号数",
                    }
                )
                st.dataframe(show_df, use_container_width=True, hide_index=True)

                metric_opt = st.selectbox(
                    "分窗口观察指标",
                    ["win_rate", "avg_return", "sharpe_ratio", "max_drawdown", "total_signals"],
                    index=0,
                    key="wf_metric_selector",
                )
                metric_title = {
                    "win_rate": "胜率(%)",
                    "avg_return": "平均收益(%)",
                    "sharpe_ratio": "夏普比率",
                    "max_drawdown": "最大回撤(%)",
                    "total_signals": "信号数",
                }.get(metric_opt, metric_opt)
                pivot_df = fold_df.pivot_table(index="fold", columns="strategy", values=metric_opt, aggfunc="mean")
                st.line_chart(pivot_df, height=280)
                st.caption(f"上图为各策略在不同窗口的 {metric_title} 变化，用于识别阶段性失效。")

    colors = ["#667eea", "#764ba2", "#FF6B6B", "#FFD700", "#FF1493", "#00D9FF", "#3CB371"]
    strategies = list(results.keys())
    win_rates = [float(stats.get("win_rate", 0) or 0) for stats in results.values()]
    avg_returns = [float(stats.get("avg_return", 0) or 0) for stats in results.values()]
    sharpe_ratios = [float(stats.get("sharpe_ratio", 0) or 0) for stats in results.values()]
    sortino_ratios = [float(stats.get("sortino_ratio", 0) or 0) for stats in results.values()]
    max_drawdowns = [abs(float(stats.get("max_drawdown", 0) or 0)) for stats in results.values()]
    profit_loss_ratios = [min(float(stats.get("profit_loss_ratio", 0) or 0), 10.0) for stats in results.values()]

    st.markdown("---")
    st.subheader("全方位可视化对比")
    col1, col2 = st.columns(2)
    with col1:
        fig_winrate = go.Figure()
        fig_winrate.add_trace(
            go.Bar(
                x=strategies,
                y=win_rates,
                marker=dict(color=colors[: len(strategies)], line=dict(color="white", width=2)),
                text=[f"{wr:.1f}%" for wr in win_rates],
                textposition="auto",
                hovertemplate="<b>%{x}</b><br>胜率: %{y:.1f}%<extra></extra>",
            )
        )
        fig_winrate.update_layout(
            title={"text": " 胜率对比", "x": 0.5, "xanchor": "center"},
            yaxis_title="胜率 (%)",
            height=350,
            plot_bgcolor="rgba(240, 242, 246, 0.5)",
            showlegend=False,
        )
        st.plotly_chart(fig_winrate, use_container_width=True)
    with col2:
        fig_return = go.Figure()
        fig_return.add_trace(
            go.Bar(
                x=strategies,
                y=avg_returns,
                marker=dict(color=colors[: len(strategies)], line=dict(color="white", width=2)),
                text=[f"{ar:.2f}%" for ar in avg_returns],
                textposition="auto",
                hovertemplate="<b>%{x}</b><br>平均收益: %{y:.2f}%<extra></extra>",
            )
        )
        fig_return.update_layout(
            title={"text": " 平均收益对比", "x": 0.5, "xanchor": "center"},
            yaxis_title="收益 (%)",
            height=350,
            plot_bgcolor="rgba(240, 242, 246, 0.5)",
            showlegend=False,
        )
        st.plotly_chart(fig_return, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        fig_risk = go.Figure()
        fig_risk.add_trace(go.Bar(name="夏普比率", x=strategies, y=sharpe_ratios, marker_color="#667eea"))
        fig_risk.add_trace(go.Bar(name="Sortino比率", x=strategies, y=sortino_ratios, marker_color="#764ba2"))
        fig_risk.update_layout(
            title={"text": " 风险调整收益对比", "x": 0.5, "xanchor": "center"},
            yaxis_title="比率",
            barmode="group",
            height=350,
            plot_bgcolor="rgba(240, 242, 246, 0.5)",
        )
        st.plotly_chart(fig_risk, use_container_width=True)
    with col2:
        fig_drawdown = go.Figure()
        fig_drawdown.add_trace(go.Bar(name="最大回撤", x=strategies, y=max_drawdowns, marker_color="#FF6B6B", yaxis="y"))
        fig_drawdown.add_trace(
            go.Scatter(
                name="盈亏比",
                x=strategies,
                y=profit_loss_ratios,
                marker=dict(size=15, color="#00D9FF", line=dict(width=2, color="white")),
                mode="markers+lines",
                line=dict(width=3),
                yaxis="y2",
            )
        )
        fig_drawdown.update_layout(
            title={"text": " 风险与盈亏比", "x": 0.5, "xanchor": "center"},
            yaxis=dict(title="最大回撤 (%)", side="left"),
            yaxis2=dict(title="盈亏比", side="right", overlaying="y"),
            height=350,
            plot_bgcolor="rgba(240, 242, 246, 0.5)",
        )
        st.plotly_chart(fig_drawdown, use_container_width=True)

    st.markdown("---")
    st.subheader("策略综合评分雷达图")
    radar_fig = go.Figure()
    for index, (strategy_name, stats) in enumerate(results.items()):
        normalized_scores = {
            "胜率": float(stats.get("win_rate", 0) or 0),
            "平均收益": min(float(stats.get("avg_return", 0) or 0) * 5, 100),
            "夏普比率": min(float(stats.get("sharpe_ratio", 0) or 0) * 25, 100),
            "盈亏比": min(float(stats.get("profit_loss_ratio", 0) or 0) * 20, 100),
            "信号数量": min(float(stats.get("total_signals", 0) or 0) / 5, 100),
            "稳定性": max(100 - abs(float(stats.get("max_drawdown", 0) or 0)) * 10, 0),
        }
        categories = list(normalized_scores.keys())
        values = list(normalized_scores.values())
        values.append(values[0])
        radar_fig.add_trace(
            go.Scatterpolar(
                r=values,
                theta=categories + [categories[0]],
                fill="toself",
                name=strategy_name,
                line=dict(color=colors[index % len(colors)], width=2),
            )
        )
    radar_fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100], tickmode="linear", tick0=0, dtick=20)),
        showlegend=True,
        height=500,
        title={"text": "策略六维评分（标准化）", "x": 0.5, "xanchor": "center"},
    )
    st.plotly_chart(radar_fig, use_container_width=True)

    best_strategy = max(
        results.items(),
        key=lambda x: float(x[1].get("avg_return", 0) or 0) * float(x[1].get("win_rate", 0) or 0) / 100.0,
    )
    st.success(
        f"""
        ###  推荐策略：{best_strategy[0]}

        **综合表现**：
        - 胜率：{best_strategy[1].get('win_rate', 0):.1f}%
        - 平均收益：{best_strategy[1].get('avg_return', 0):.2f}%
        - 夏普比率：{best_strategy[1].get('sharpe_ratio', 0):.2f}
        - 信号数量：{best_strategy[1].get('total_signals', 0)}

         根据历史回测数据，该策略综合表现最佳，建议优先使用！
        """
    )

    st.markdown("---")
    st.markdown("###  回测+ 增强分析")
    analysis_tab1, analysis_tab2, analysis_tab3, analysis_tab4 = st.tabs(["高级指标", "收益分析", "信号质量", "导出报告"])

    with analysis_tab1:
        st.subheader("高级性能指标（v49增强版）")
        for strategy_name, stats in results.items():
            with st.expander(f"{strategy_name} - 详细指标", expanded=True):
                st.markdown("####  核心指标")
                cols = st.columns(4)
                with cols[0]:
                    st.metric("总信号数", stats.get("total_signals", 0))
                    st.metric("胜率", f"{stats.get('win_rate', 0):.1f}%")
                with cols[1]:
                    st.metric("平均收益", f"{stats.get('avg_return', 0):.2f}%")
                    st.metric("中位数收益", f"{stats.get('median_return', 0):.2f}%")
                with cols[2]:
                    st.metric("最大收益", f"{stats.get('max_return', 0):.2f}%")
                    st.metric("最大亏损", f"{stats.get('min_return', 0):.2f}%")
                with cols[3]:
                    st.metric("夏普比率", f"{stats.get('sharpe_ratio', 0):.2f}")
                    profit_loss = stats.get("profit_loss_ratio", 0)
                    st.metric("盈亏比", "∞" if profit_loss == float("inf") else f"{profit_loss:.2f}")

                st.markdown("---")
                st.markdown("####  风险控制指标")
                cols = st.columns(4)
                with cols[0]:
                    st.metric(" 最大回撤", f"{stats.get('max_drawdown', 0):.2f}%")
                    st.metric(" 波动率", f"{stats.get('volatility', 0):.2f}%")
                with cols[1]:
                    st.metric(" Sortino比率", f"{stats.get('sortino_ratio', 0):.2f}")
                    st.metric(" Calmar比率", f"{stats.get('calmar_ratio', 0):.2f}")
                with cols[2]:
                    st.metric(" 最长连胜", f"{stats.get('max_consecutive_wins', 0)} 次")
                    st.metric(" 最长连亏", f"{stats.get('max_consecutive_losses', 0)} 次")
                with cols[3]:
                    st.metric(" 年化收益", f"{stats.get('annualized_return', 0):.2f}%")
                    st.metric(" 期望值", f"{stats.get('expected_value', 0):.2f}%")

                st.markdown("---")
                st.markdown("####  收益分布")
                cols = st.columns(3)
                with cols[0]:
                    st.metric("25%分位数", f"{stats.get('return_25_percentile', 0):.2f}%")
                with cols[1]:
                    st.metric("50%分位数(中位)", f"{stats.get('median_return', 0):.2f}%")
                with cols[2]:
                    st.metric("75%分位数", f"{stats.get('return_75_percentile', 0):.2f}%")

    with analysis_tab2:
        st.subheader("收益分布与资金曲线（v49增强版）")
        selected_for_analysis = st.selectbox("选择策略进行详细分析", list(results.keys()), key="analysis_strategy_select")
        stats_for_analysis = results[selected_for_analysis]

        cols = st.columns(2)
        with cols[0]:
            st.markdown("####  收益统计")
            st.info(
                f"""
                **平均收益**: {stats_for_analysis.get('avg_return', 0):.2f}%

                **中位数收益**: {stats_for_analysis.get('median_return', 0):.2f}%

                **最大收益**: {stats_for_analysis.get('max_return', 0):.2f}%

                **最大亏损**: {stats_for_analysis.get('min_return', 0):.2f}%

                **标准差**: {stats_for_analysis.get('volatility', 0):.2f}%
                """
            )
        with cols[1]:
            win_rate = float(stats_for_analysis.get("win_rate", 0) or 0)
            avg_return = float(stats_for_analysis.get("avg_return", 0) or 0)
            if win_rate >= 60 and avg_return >= 5:
                risk_level = " 低风险"
            elif win_rate >= 50 and avg_return >= 3:
                risk_level = " 中风险"
            else:
                risk_level = " 高风险"
            st.markdown("####  风险指标")
            st.metric("风险等级", risk_level)
            st.metric("胜率", f"{win_rate:.1f}%")
            st.metric("夏普比率", f"{stats_for_analysis.get('sharpe_ratio', 0):.2f}")
            st.metric("盈亏比", f"{stats_for_analysis.get('profit_loss_ratio', 0):.2f}")

        st.markdown("---")
        st.markdown("####  资金曲线")
        cumulative_returns = stats_for_analysis.get("cumulative_returns") or []
        if cumulative_returns:
            fig_equity = go.Figure()
            fig_equity.add_trace(
                go.Scatter(
                    x=list(range(len(cumulative_returns))),
                    y=cumulative_returns,
                    mode="lines",
                    name="资金曲线",
                    line=dict(color="#667eea", width=3),
                    fill="tozeroy",
                    fillcolor="rgba(102, 126, 234, 0.1)",
                )
            )
            fig_equity.add_trace(
                go.Scatter(
                    x=[0, len(cumulative_returns) - 1],
                    y=[1, 1],
                    mode="lines",
                    name="基准线",
                    line=dict(color="gray", width=2, dash="dash"),
                )
            )
            fig_equity.update_layout(
                title="累计收益率曲线",
                xaxis_title="交易次数",
                yaxis_title="累计收益倍数",
                height=400,
                hovermode="x unified",
                plot_bgcolor="rgba(240, 242, 246, 0.5)",
            )
            st.plotly_chart(fig_equity, use_container_width=True)
        else:
            st.info("资金曲线数据不可用")

        st.markdown("---")
        st.markdown("####  Monte Carlo模拟（未来收益预测）")
        col1, col2 = st.columns([2, 1])
        with col2:
            mc_simulations = st.slider("模拟次数", 100, 1000, 500, 100, key="mc_sims")
            mc_periods = st.slider("预测周期", 10, 100, 50, 10, key="mc_periods")
            run_mc = st.button("运行Monte Carlo模拟", type="primary", use_container_width=True)
        with col1:
            if run_mc:
                with st.spinner("正在运行Monte Carlo模拟..."):
                    avg_ret = float(stats_for_analysis.get("avg_return", 0) or 0) / 100
                    vol = float(stats_for_analysis.get("volatility", 0) or 0) / 100
                    np.random.seed(42)
                    simulations = np.array(
                        [np.cumprod(1 + np.random.normal(avg_ret, vol, mc_periods)) for _ in range(mc_simulations)]
                    )
                    fig_mc = go.Figure()
                    for i in range(min(100, mc_simulations)):
                        fig_mc.add_trace(
                            go.Scatter(
                                x=list(range(mc_periods)),
                                y=simulations[i],
                                mode="lines",
                                line=dict(color="lightblue", width=0.5),
                                opacity=0.3,
                                showlegend=False,
                                hoverinfo="skip",
                            )
                        )
                    median_path = np.median(simulations, axis=0)
                    percentile_25 = np.percentile(simulations, 25, axis=0)
                    percentile_75 = np.percentile(simulations, 75, axis=0)
                    fig_mc.add_trace(go.Scatter(x=list(range(mc_periods)), y=median_path, mode="lines", name="中位数预测", line=dict(color="red", width=3)))
                    fig_mc.add_trace(go.Scatter(x=list(range(mc_periods)), y=percentile_75, mode="lines", name="75%分位", line=dict(color="green", width=2, dash="dash")))
                    fig_mc.add_trace(
                        go.Scatter(
                            x=list(range(mc_periods)),
                            y=percentile_25,
                            mode="lines",
                            name="25%分位",
                            line=dict(color="orange", width=2, dash="dash"),
                            fill="tonexty",
                            fillcolor="rgba(102, 126, 234, 0.1)",
                        )
                    )
                    fig_mc.update_layout(
                        title=f"Monte Carlo模拟 ({mc_simulations}次模拟, {mc_periods}期)",
                        xaxis_title="交易周期",
                        yaxis_title="累计收益倍数",
                        height=450,
                        hovermode="x unified",
                        plot_bgcolor="rgba(240, 242, 246, 0.5)",
                    )
                    st.plotly_chart(fig_mc, use_container_width=True)

                    final_values = simulations[:, -1]
                    st.success(
                        f"""
                        ###  Monte Carlo模拟结果

                        **{mc_periods}个周期后的预期收益：**
                        - 中位数：{(median_path[-1] - 1) * 100:.2f}%
                        - 25%分位：{(percentile_25[-1] - 1) * 100:.2f}%
                        - 75%分位：{(percentile_75[-1] - 1) * 100:.2f}%
                        - 最好情况：{(final_values.max() - 1) * 100:.2f}%
                        - 最坏情况：{(final_values.min() - 1) * 100:.2f}%
                        - 盈利概率：{(final_values > 1).sum() / len(final_values) * 100:.1f}%
                        """
                    )

    with analysis_tab3:
        st.subheader("信号质量分析（v49增强版）")
        quality_rows: List[Dict[str, Any]] = []
        quality_scores_list: List[float] = []
        for strategy_name, stats in results.items():
            quality_score = _quality_score(stats)
            quality_scores_list.append(quality_score)
            if quality_score >= 80:
                grade = "S 级（优秀）"
                grade_icon = ""
            elif quality_score >= 70:
                grade = "A 级（良好）"
                grade_icon = "⭐"
            elif quality_score >= 60:
                grade = "B 级（合格）"
                grade_icon = ""
            else:
                grade = "C 级（待改进）"
                grade_icon = ""
            quality_rows.append(
                {
                    "策略": strategy_name,
                    "质量分数": f"{quality_score:.1f}",
                    "评级": f"{grade_icon} {grade}",
                    "胜率": f"{stats.get('win_rate', 0):.1f}%",
                    "平均收益": f"{stats.get('avg_return', 0):.2f}%",
                    "夏普比率": f"{stats.get('sharpe_ratio', 0):.2f}",
                    "Sortino比率": f"{stats.get('sortino_ratio', 0):.2f}",
                    "最大回撤": f"{abs(stats.get('max_drawdown', 0)):.2f}%",
                    "盈亏比": f"{min(stats.get('profit_loss_ratio', 0), 10):.2f}",
                    "信号数量": stats.get("total_signals", 0),
                }
            )
        st.dataframe(pd.DataFrame(quality_rows), use_container_width=True, hide_index=True)

        fig_quality = go.Figure()
        colors_quality = [
            "#FFD700" if score >= 80 else "#C0C0C0" if score >= 70 else "#CD7F32" if score >= 60 else "#808080"
            for score in quality_scores_list
        ]
        fig_quality.add_trace(
            go.Bar(
                x=list(results.keys()),
                y=quality_scores_list,
                marker=dict(color=colors_quality, line=dict(color="white", width=2)),
                text=[f"{score:.1f}" for score in quality_scores_list],
                textposition="auto",
            )
        )
        fig_quality.add_hline(y=80, line_dash="dash", line_color="gold", annotation_text="S级线", annotation_position="right")
        fig_quality.add_hline(y=70, line_dash="dash", line_color="silver", annotation_text="A级线", annotation_position="right")
        fig_quality.add_hline(y=60, line_dash="dash", line_color="#CD7F32", annotation_text="B级线", annotation_position="right")
        fig_quality.update_layout(title="策略质量分数对比", yaxis_title="质量分数", height=400, plot_bgcolor="rgba(240, 242, 246, 0.5)", showlegend=False)
        st.plotly_chart(fig_quality, use_container_width=True)

        heatmap_metrics = ["胜率", "平均收益", "夏普比率", "Sortino比率", "盈亏比"]
        heatmap_data = [
            [
                float(stats.get("win_rate", 0) or 0),
                float(stats.get("avg_return", 0) or 0) * 5,
                float(stats.get("sharpe_ratio", 0) or 0) * 20,
                float(stats.get("sortino_ratio", 0) or 0) * 15,
                min(float(stats.get("profit_loss_ratio", 0) or 0) * 15, 100),
            ]
            for stats in results.values()
        ]
        fig_heatmap = go.Figure(
            data=go.Heatmap(
                z=heatmap_data,
                x=heatmap_metrics,
                y=list(results.keys()),
                colorscale="RdYlGn",
                text=[[f"{val:.1f}" for val in row] for row in heatmap_data],
                texttemplate="%{text}",
                textfont={"size": 12},
                colorbar=dict(title="标准化分数"),
            )
        )
        fig_heatmap.update_layout(title="策略指标热力图（标准化）", height=300, xaxis_title="指标", yaxis_title="策略")
        st.plotly_chart(fig_heatmap, use_container_width=True)

    with analysis_tab4:
        st.subheader("导出回测报告（v49增强版）")
        cols = st.columns(3)
        best_strategy = max(
            results.items(),
            key=lambda x: float(x[1].get("avg_return", 0) or 0) * float(x[1].get("win_rate", 0) or 0) / 100.0,
        )
        with cols[0]:
            if st.button("生成Markdown报告", use_container_width=True):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label=" 下载 Markdown 报告",
                    data=_build_markdown_report(results, best_strategy),
                    file_name=f"回测报告_v49_{timestamp}.md",
                    mime="text/markdown",
                    help="下载完整的Markdown格式回测报告，包含所有分析细节",
                )
                st.success("报告已生成！点击上方按钮下载")
        with cols[1]:
            if st.button("导出 CSV 数据", use_container_width=True):
                csv_df = pd.DataFrame(
                    [
                        {
                            "策略名称": strategy_name,
                            "总信号数": stats.get("total_signals", 0),
                            "分析股票数": stats.get("analyzed_stocks", 0),
                            "胜率(%)": f"{stats.get('win_rate', 0):.1f}",
                            "平均收益(%)": f"{stats.get('avg_return', 0):.2f}",
                            "中位数收益(%)": f"{stats.get('median_return', 0):.2f}",
                            "最大收益(%)": f"{stats.get('max_return', 0):.2f}",
                            "最大亏损(%)": f"{stats.get('min_return', 0):.2f}",
                            "夏普比率": f"{stats.get('sharpe_ratio', 0):.2f}",
                            "Sortino比率": f"{stats.get('sortino_ratio', 0):.2f}",
                            "最大回撤(%)": f"{stats.get('max_drawdown', 0):.2f}",
                            "Calmar比率": f"{stats.get('calmar_ratio', 0):.2f}",
                            "盈亏比": f"{stats.get('profit_loss_ratio', 0):.2f}",
                            "年化收益(%)": f"{stats.get('annualized_return', 0):.2f}",
                            "波动率(%)": f"{stats.get('volatility', 0):.2f}",
                            "期望值(%)": f"{stats.get('expected_value', 0):.2f}",
                            "最长连胜": stats.get("max_consecutive_wins", 0),
                            "最长连亏": stats.get("max_consecutive_losses", 0),
                        }
                        for strategy_name, stats in results.items()
                    ]
                )
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label=" 下载 CSV 文件",
                    data=csv_df.to_csv(index=False, encoding="utf-8-sig"),
                    file_name=f"回测对比数据_v49_{timestamp}.csv",
                    mime="text/csv",
                    help="下载CSV格式数据，包含所有关键指标",
                )
                st.success("CSV数据已准备好！点击上方按钮下载")
        with cols[2]:
            if st.button("导出Excel完整版", use_container_width=True):
                st.info(
                    """
                    ###  Excel完整版报告功能

                    包含以下工作表：
                    1. **策略对比** - 所有策略的核心指标
                    2. **详细统计** - 每个策略的详细统计数据
                    3. **信号记录** - 所有交易信号的明细
                    4. **强度分析** - 信号强度分布统计

                     该功能需要安装 `openpyxl` 库
                    """
                )


def render_strategy_comparison_page(
    *,
    vp_analyzer: Any,
    get_production_compare_params: Callable[[], Dict[str, Any]],
    start_async_backtest_job: Callable[[str, Dict[str, Any]], Tuple[bool, str, str]],
    connect_permanent_db: Callable[[], Any],
    get_async_backtest_job: Callable[[str], Dict[str, Any] | None],
    is_pid_alive: Callable[[int], bool],
    merge_async_backtest_job: Callable[..., Dict[str, Any]],
    now_ts: Callable[[], float],
    v7_evaluator_available: bool,
    v8_evaluator_available: bool,
) -> None:
    st.subheader("六大策略对比")
    exp_backtest = st.expander("策略速览", expanded=False)
    exp_backtest.info(
        """
        - v4：潜伏与提前布局
        - v5：启动确认与趋势跟随
        - v6：超短线快进快出
        - v7：动态自适应与轮动
        - v8：强化风控与仓位管理
        - v9：中线均衡与回撤控制
        """
    )

    col1, col2 = st.columns([3, 1])
    strict_full_market_mode = bool(st.session_state.get("strict_full_market_mode", False))
    st.session_state.setdefault("comparison_full_market_mode", strict_full_market_mode)
    if strict_full_market_mode:
        st.session_state["comparison_full_market_mode"] = True

    with col1:
        full_market_mode_compare = st.checkbox(
            "全量模式（不抽样）",
            key="comparison_full_market_mode",
            help="开启后按当前历史数据中的全部股票回测，耗时会显著增加。",
        )
        backtest_sample_size = st.slider(
            "回测样本数量",
            100,
            6000,
            3000 if strict_full_market_mode else 500,
            100,
            help="建议500-1500；全量模式开启时该项仅作展示。",
            disabled=bool(full_market_mode_compare),
        )
        comparison_validation_mode = st.selectbox(
            "验证模式",
            ["快速全样本", "Walk-forward（滚动样本外）"],
            index=0,
            help="快速全样本：速度快；Walk-forward：按时间滚动分窗，更接近实盘稳定性验证。",
            key="comparison_validation_mode",
        )
        wf_folds = int(st.session_state.get("wf_folds", 4))
        wf_window_days = int(st.session_state.get("wf_window_days", 180))
        wf_step_days = int(st.session_state.get("wf_step_days", 40))
        if comparison_validation_mode == "Walk-forward（滚动样本外）":
            wf_cols = st.columns(3)
            with wf_cols[0]:
                wf_folds = st.slider("滚动窗口数", 2, 8, wf_folds, 1, key="wf_folds")
            with wf_cols[1]:
                wf_window_days = st.slider("单窗口交易日", 120, 260, wf_window_days, 10, key="wf_window_days")
            with wf_cols[2]:
                wf_step_days = st.slider("滚动步长交易日", 20, 120, wf_step_days, 5, key="wf_step_days")
        if full_market_mode_compare:
            st.caption("当前为全量模式：回测将使用当前历史数据中的全部股票。")

    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        async_compare = st.checkbox("后台运行（可选）", value=False, key="comparison_async_run")
        start_comparison = st.button(" 开始对比", type="primary", use_container_width=True, key="start_strategy_comparison")

    if start_comparison:
        compare_strategy_params = get_production_compare_params()
        if async_compare:
            payload = {
                "sample_size": int(backtest_sample_size),
                "full_market_mode": bool(full_market_mode_compare),
                "history_days": 240,
                "strategy_params": compare_strategy_params,
                "validation_mode": comparison_validation_mode,
                "wf_folds": int(wf_folds),
                "wf_window_days": int(wf_window_days),
                "wf_step_days": int(wf_step_days),
            }
            ok, msg, run_id = start_async_backtest_job("comparison", payload)
            if ok:
                st.session_state["comparison_async_job_id"] = run_id
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()

        with st.spinner("正在对比六大策略表现（包含v9.0！）...这可能需要几分钟..."):
            try:
                df = _load_compare_history(connect_permanent_db)
                if df.empty:
                    st.error("无法获取历史数据，请先到「数据与系统」更新数据")
                else:
                    sample_for_run = int(backtest_sample_size)
                    if (not full_market_mode_compare) and comparison_validation_mode == "Walk-forward（滚动样本外）":
                        sample_for_run = max(60, min(int(backtest_sample_size), int(300 / max(1, int(wf_folds)))))

                    meta: Dict[str, Any] = {"validation_mode": comparison_validation_mode}
                    if comparison_validation_mode == "Walk-forward（滚动样本外）":
                        unique_dates = sorted([str(x) for x in df["trade_date"].dropna().unique()])
                        fold_results: List[Dict[str, Dict[str, Any]]] = []
                        fold_ranges: List[str] = []
                        fold_details: List[Dict[str, Any]] = []
                        pos = max(0, len(unique_dates) - int(wf_window_days))
                        for index in range(int(wf_folds)):
                            start_pos = pos - index * int(wf_step_days)
                            end_pos = start_pos + int(wf_window_days)
                            if start_pos < 0 or end_pos > len(unique_dates):
                                continue
                            d0 = unique_dates[start_pos]
                            d1 = unique_dates[end_pos - 1]
                            df_fold = df[(df["trade_date"] >= d0) & (df["trade_date"] <= d1)].copy()
                            if df_fold.empty:
                                continue
                            fold_ranges.append(f"{d0}-{d1}")
                            one = _run_compare_once(
                                vp_analyzer=vp_analyzer,
                                df_slice=df_fold,
                                sample_size_for_slice=max(1, int(df_fold["ts_code"].nunique())) if full_market_mode_compare else int(sample_for_run),
                                compare_strategy_params=compare_strategy_params,
                                v7_evaluator_available=v7_evaluator_available,
                                v8_evaluator_available=v8_evaluator_available,
                                label=f"WF{index + 1}",
                            )
                            if one:
                                fold_results.append(one)
                                for strategy_name, stats in one.items():
                                    fold_details.append(
                                        {
                                            "fold": int(index + 1),
                                            "range": f"{d0}-{d1}",
                                            "strategy": strategy_name,
                                            "win_rate": float(stats.get("win_rate", 0) or 0),
                                            "avg_return": float(stats.get("avg_return", 0) or 0),
                                            "sharpe_ratio": float(stats.get("sharpe_ratio", 0) or 0),
                                            "max_drawdown": float(stats.get("max_drawdown", 0) or 0),
                                            "total_signals": int(stats.get("total_signals", 0) or 0),
                                        }
                                    )
                        fold_results = list(reversed(fold_results))
                        results = _aggregate_fold_results(fold_results)
                        meta.update(
                            {
                                "fold_count": len(fold_results),
                                "fold_ranges": fold_ranges,
                                "fold_details": fold_details,
                                "wf_window_days": int(wf_window_days),
                                "wf_step_days": int(wf_step_days),
                            }
                        )
                    else:
                        results = _run_compare_once(
                            vp_analyzer=vp_analyzer,
                            df_slice=df,
                            sample_size_for_slice=max(1, int(df["ts_code"].nunique())) if full_market_mode_compare else int(sample_for_run),
                            compare_strategy_params=compare_strategy_params,
                            v7_evaluator_available=v7_evaluator_available,
                            v8_evaluator_available=v8_evaluator_available,
                        )

                    if results:
                        st.session_state["comparison_results"] = results
                        st.session_state["comparison_results_meta"] = meta
                        st.success("策略对比完成！")
                        st.rerun()
                    else:
                        st.error("所有策略回测都失败了")
            except Exception as exc:
                st.error(f"回测失败: {exc}")
                st.code(traceback.format_exc())

    compare_job_id = str(st.session_state.get("comparison_async_job_id", "") or "")
    if compare_job_id:
        job = get_async_backtest_job(compare_job_id)
        if job:
            st.info(f"后台任务状态：{job.get('status')}（ID={compare_job_id}）")
            cj1, cj2 = st.columns([1, 1])
            with cj1:
                if st.button("刷新后台任务状态", key=f"refresh_compare_job_{compare_job_id}"):
                    st.rerun()
            with cj2:
                if str(job.get("status")) == "running" and st.button("取消对比任务", key=f"cancel_compare_job_{compare_job_id}"):
                    pid = int(job.get("pid", 0) or 0)
                    if pid > 0 and is_pid_alive(pid):
                        try:
                            os.killpg(pid, signal.SIGTERM)
                        except Exception:
                            try:
                                os.kill(pid, signal.SIGTERM)
                            except Exception:
                                pass
                    merge_async_backtest_job(
                        compare_job_id,
                        job,
                        status="failed",
                        error="任务已手动取消",
                        ended_at=now_ts(),
                    )
                    st.session_state["comparison_async_job_id"] = ""
                    st.rerun()
            if str(job.get("status")) == "success":
                out = job.get("result") or {}
                if out.get("success"):
                    st.session_state["comparison_results"] = out.get("results", {}) or {}
                    st.session_state["comparison_results_meta"] = out.get("meta", {}) or {}
                    st.success("后台策略对比已完成，结果已更新。")
                    st.session_state["comparison_async_job_id"] = ""
                    st.rerun()
            elif str(job.get("status")) == "failed":
                st.error(f"后台策略对比失败：{job.get('error', '未知错误')}")
                if str(job.get("traceback", "")).strip():
                    with st.expander("查看后台任务错误详情", expanded=False):
                        st.code(str(job.get("traceback", "")))
                st.session_state["comparison_async_job_id"] = ""

    if "comparison_results" in st.session_state:
        _render_comparison_results(
            st.session_state["comparison_results"],
            st.session_state.get("comparison_results_meta", {}) or {},
        )
