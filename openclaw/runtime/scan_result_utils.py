from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


def render_result_overview(df: pd.DataFrame, score_col: str = "综合评分", title: str = "结果概览") -> None:
    if df is None or df.empty:
        return
    st.markdown(f"### {title}")
    scores = None
    if score_col in df.columns:
        scores = pd.to_numeric(df[score_col], errors="coerce").dropna()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("标的数量", f"{len(df)}")
    if scores is not None and not scores.empty:
        with col2:
            st.metric("平均评分", f"{scores.mean():.1f}")
        with col3:
            st.metric("最高评分", f"{scores.max():.1f}")
        with col4:
            st.metric("最低评分", f"{scores.min():.1f}")
    else:
        with col2:
            st.metric("平均评分", "—")
        with col3:
            st.metric("最高评分", "—")
        with col4:
            st.metric("最低评分", "—")

    chart_cols = st.columns(2)
    with chart_cols[0]:
        if scores is not None and not scores.empty:
            bins = pd.cut(scores, bins=8)
            hist = bins.value_counts().sort_index()
            hist.index = hist.index.astype(str)
            st.bar_chart(hist, height=220)
            st.caption("评分分布")
        else:
            st.caption("评分分布（暂无数据）")

    with chart_cols[1]:
        if "行业" in df.columns:
            ind_counts = df["行业"].fillna("未知").value_counts().head(8)
            st.bar_chart(ind_counts, height=220)
            st.caption("行业分布 Top 8")
        else:
            st.caption("行业分布（暂无数据）")


def standardize_result_df(df: pd.DataFrame, score_col: str = "综合评分") -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "score_val" in out.columns:
        out = out.drop(columns=["score_val"])
    if score_col in out.columns:
        try:
            out[score_col] = pd.to_numeric(out[score_col], errors="ignore")
        except Exception:
            pass
    preferred = ["股票代码", "股票名称", "行业", score_col]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[cols]


def append_reason_col(display_cols: List[str], df: pd.DataFrame) -> List[str]:
    if df is None or df.empty:
        return display_cols
    if "核心理由" in df.columns and "核心理由" not in display_cols:
        return display_cols + ["核心理由"]
    return display_cols


def get_ts_code_col(df: pd.DataFrame) -> Optional[str]:
    for col in ("股票代码", "ts_code", "TS_CODE"):
        if col in df.columns:
            return col
    return None


def apply_multi_period_filter(
    df: pd.DataFrame,
    db_path: str,
    *,
    min_align: int = 2,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    ts_col = get_ts_code_col(df)
    if not ts_col:
        return df
    rows = []
    for _, row in df.iterrows():
        ts_code = row[ts_col]
        try:
            from data.history import load_stock_recent as load_stock_recent_v2  # type: ignore
            hist = load_stock_recent_v2(
                db_path=db_path,
                ts_code=ts_code,
                limit=61,
                columns="trade_date, close_price",
            )
        except Exception:
            hist = pd.DataFrame()
        if len(hist) < 21:
            continue
        hist = hist.sort_values("trade_date").reset_index(drop=True)
        closes = pd.to_numeric(hist["close_price"], errors="coerce").dropna()
        if len(closes) < 21:
            continue

        def _ret(n: int) -> float:
            if len(closes) <= n:
                return 0.0
            base = closes.iloc[-(n + 1)]
            return (closes.iloc[-1] / base - 1.0) if base else 0.0

        r5, r20, r60 = _ret(5), _ret(20), _ret(60)
        pos = sum(1 for value in (r5, r20, r60) if value >= 0)
        neg = 3 - pos
        align = max(pos, neg)
        if align >= min_align:
            row = row.copy()
            row["5日趋势"] = "上行" if r5 >= 0 else "下行"
            row["20日趋势"] = "上行" if r20 >= 0 else "下行"
            row["60日趋势"] = "上行" if r60 >= 0 else "下行"
            row["趋势一致数"] = align
            rows.append(row)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def add_reason_summary(df: pd.DataFrame, score_col: str = "综合评分") -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "理由摘要" in df.columns:
        return df
    out = df.copy()
    if "筛选理由" in out.columns:
        out["理由摘要"] = out["筛选理由"]
    elif "协同组合" in out.columns:
        out["理由摘要"] = out["协同组合"]
    else:
        out["理由摘要"] = ""
    return out


def signal_density_hint(results_count: int, candidate_count: int) -> Tuple[str, str]:
    if candidate_count <= 0:
        return ("候选池为空，请检查数据与筛选条件。", "warning")
    ratio = results_count / candidate_count
    if results_count == 0:
        return ("信号为空，建议放宽阈值或降低过滤强度。", "warning")
    if results_count < 5:
        return ("信号偏稀疏，当前市场或阈值偏严。", "info")
    if results_count > 200 or ratio > 0.3:
        return ("信号偏密集，建议提高阈值或收紧过滤。", "warning")
    return ("信号密度正常。", "info")


def apply_filter_mode(
    df: pd.DataFrame,
    score_col: str,
    mode: str,
    threshold: float,
    top_percent: int,
) -> pd.DataFrame:
    if df is None or df.empty or score_col not in df.columns:
        return df
    out = df.copy()
    out["score_val"] = pd.to_numeric(out[score_col], errors="coerce")
    out = out.dropna(subset=["score_val"])
    if out.empty:
        return out
    if mode == "阈值筛选":
        out = out[out["score_val"] >= threshold]
    elif mode == "双重筛选(阈值+Top%)":
        out = out[out["score_val"] >= threshold]
        if not out.empty:
            out = out.sort_values("score_val", ascending=False)
            keep_n = max(1, int(len(out) * top_percent / 100))
            out = out.head(keep_n)
    else:
        out = out.sort_values("score_val", ascending=False)
        keep_n = max(1, int(len(out) * top_percent / 100))
        out = out.head(keep_n)
    return out.drop(columns=["score_val"])


def apply_filter_mode_with_rescue(
    df: pd.DataFrame,
    score_col: str,
    mode: str,
    threshold: float,
    top_percent: int,
    *,
    threshold_floor: float = 45.0,
    rescue_top_percent: int = 6,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    primary = apply_filter_mode(df, score_col, mode, threshold, top_percent)
    if primary is not None and not primary.empty:
        return primary, {"used": False}
    if df is None or df.empty or score_col not in df.columns:
        return primary, {"used": False}

    relaxed_threshold = max(float(threshold_floor), float(threshold) - 8.0)
    relaxed_top = max(int(top_percent), int(rescue_top_percent))
    rescue = apply_filter_mode(df, score_col, mode, relaxed_threshold, relaxed_top)
    if rescue is None or rescue.empty:
        rescue = apply_filter_mode(df, score_col, "分位数筛选(Top%)", relaxed_threshold, relaxed_top)
    if rescue is None or rescue.empty:
        tmp = df.copy()
        tmp["score_val"] = pd.to_numeric(tmp[score_col], errors="coerce")
        tmp = tmp.dropna(subset=["score_val"]).sort_values("score_val", ascending=False)
        if not tmp.empty:
            keep_n = max(1, min(20, int(len(tmp) * max(relaxed_top, 10) / 100)))
            rescue = tmp.head(keep_n).drop(columns=["score_val"])

    if rescue is None or rescue.empty:
        return primary, {"used": False}
    return rescue, {
        "used": True,
        "threshold": float(relaxed_threshold),
        "top_percent": int(relaxed_top),
        "mode": str(mode),
    }
