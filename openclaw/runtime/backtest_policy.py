from __future__ import annotations

from datetime import datetime
import os
import re
from typing import Any, Callable, Dict, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st


def load_portfolio_risk_budget() -> Dict[str, Any]:
    enable_env = os.getenv("OPENCLAW_PROD_ENABLE_RISK_BUDGET", "1") == "1"
    max_positions_env = int(float(os.getenv("OPENCLAW_PROD_MAX_POSITIONS", "20") or 20))
    max_industry_ratio_env = float(os.getenv("OPENCLAW_PROD_MAX_INDUSTRY_RATIO", "0.35") or 0.35)
    enable = bool(st.session_state.get("prod_rb_enable", enable_env))
    max_positions = int(float(st.session_state.get("prod_rb_max_positions", max_positions_env) or max_positions_env))
    max_industry_ratio = float(st.session_state.get("prod_rb_max_ind_ratio", max_industry_ratio_env) or max_industry_ratio_env)
    max_positions = max(1, min(200, max_positions))
    max_industry_ratio = max(0.05, min(1.0, max_industry_ratio))
    return {
        "enabled": bool(enable),
        "max_positions": int(max_positions),
        "max_industry_ratio": float(max_industry_ratio),
    }


def parse_strength_range_label(label: str, *, safe_float: Callable[[Any, float], float]) -> Tuple[float, float]:
    text = str(label or "").strip().replace("分", "")
    if "+" in text:
        lo = safe_float(text.replace("+", ""), 0.0)
        return lo, 100.0
    if "-" in text:
        p = text.split("-", 1)
        return safe_float(p[0], 0.0), safe_float(p[1], 100.0)
    v = safe_float(text, 0.0)
    return v, v


def build_calibrated_strength_df(
    strength_perf: Dict[str, Dict[str, Any]],
    *,
    safe_float: Callable[[Any, float], float],
) -> pd.DataFrame:
    if not isinstance(strength_perf, dict) or not strength_perf:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for label, perf in strength_perf.items():
        lo, hi = parse_strength_range_label(str(label), safe_float=safe_float)
        rows.append(
            {
                "区间": str(label),
                "下界": float(lo),
                "上界": float(hi),
                "样本数": int(safe_float((perf or {}).get("count"), 0)),
                "原始胜率%": float(safe_float((perf or {}).get("win_rate"), 0.0)),
                "原始收益%": float(safe_float((perf or {}).get("avg_return"), 0.0)),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(["下界", "上界"]).reset_index(drop=True)
    df["校准胜率%"] = np.maximum.accumulate(df["原始胜率%"].to_numpy(dtype=float))
    df["校准收益%"] = np.maximum.accumulate(df["原始收益%"].to_numpy(dtype=float))
    df["校准综合分"] = df["校准胜率%"] * df["校准收益%"] / 100.0
    return df


def pick_tradable_segment_from_strength(
    strength_perf: Dict[str, Dict[str, Any]],
    *,
    safe_float: Callable[[Any, float], float],
) -> Dict[str, Any]:
    cal_df = build_calibrated_strength_df(strength_perf, safe_float=safe_float)
    if cal_df is None or cal_df.empty:
        return {}
    tradable = cal_df[(cal_df["样本数"] >= 20) & (cal_df["校准收益%"] > 0)].copy()
    src = tradable if not tradable.empty else cal_df
    best = src.sort_values(["校准综合分", "样本数"], ascending=[False, False]).iloc[0]
    lo = int(round(float(best.get("下界", 0.0))))
    hi = int(round(float(best.get("上界", lo))))
    lo = max(45, min(90, lo))
    hi = max(lo, min(90, hi))
    return {
        "segment": str(best.get("区间", "")),
        "low": lo,
        "high": hi,
        "samples": int(best.get("样本数", 0)),
        "cal_ret": float(best.get("校准收益%", 0.0)),
        "cal_win": float(best.get("校准胜率%", 0.0)),
    }


def apply_tradable_segment_to_strategy_session(
    strategy_name: str,
    seg: Dict[str, Any],
    *,
    top_percent: int = 1,
) -> str:
    if not isinstance(seg, dict) or not seg:
        return "无可用分段建议"
    lo = int(seg.get("low", 60))
    hi = int(seg.get("high", lo))
    tp = max(1, min(10, int(top_percent)))
    s = str(strategy_name or "")
    if "v9.0" in s:
        st.session_state["score_threshold_v9"] = int(lo)
        st.session_state["select_mode_v9"] = "分位数筛选(Top%)"
        st.session_state["top_percent_v9"] = int(tp)
        return f"已应用到v9：阈值>={lo}，模式=分位数Top{tp}%"
    if "v8.0" in s:
        st.session_state["score_threshold_v8_tab1"] = (int(lo), int(hi))
        st.session_state["v8_select_mode_tab1"] = "分位数筛选(Top%)"
        st.session_state["v8_top_percent_tab1"] = int(tp)
        return f"已应用到v8：阈值区间={lo}-{hi}，模式=分位数Top{tp}%"
    if "v5.0" in s:
        st.session_state["score_threshold_v5"] = int(lo)
        st.session_state["v5_select_mode"] = "分位数筛选(Top%)"
        st.session_state["v5_top_percent"] = int(tp)
        return f"已应用到v5：阈值>={lo}，模式=分位数Top{tp}%"
    if "组合策略" in s:
        st.session_state["combo_threshold"] = int(lo)
        st.session_state["combo_select_mode"] = "分位数筛选(Top%)"
        st.session_state["combo_top_percent"] = int(tp)
        return f"已应用到combo：阈值>={lo}，模式=分位数Top{tp}%"
    return "当前策略不支持自动应用"


def auto_backtest_scheduler_tick(
    *,
    get_sim_meta: Callable[[str], str],
    set_sim_meta: Callable[[str, str], None],
    start_async_backtest_job: Callable[[str, Dict[str, Any]], Tuple[bool, str, str]],
    now_text: Callable[[], str],
) -> Dict[str, Any]:
    enabled = bool(st.session_state.get("auto_backtest_enabled", False))
    hhmm = str(st.session_state.get("auto_backtest_time", "15:35") or "15:35").strip()
    if not enabled:
        return {"ok": False, "status": "disabled"}
    m = re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", hhmm)
    if not m:
        return {"ok": False, "status": "invalid_time", "time": hhmm}
    now = datetime.now()
    due = (now.hour, now.minute) >= (int(m.group(1)), int(m.group(2)))
    if not due:
        return {"ok": False, "status": "not_due", "time": hhmm}
    today = now.strftime("%Y%m%d")
    last_run_date = get_sim_meta("auto_backtest_last_run_date")
    if str(last_run_date or "") == today:
        return {"ok": False, "status": "already_ran_today", "date": today}

    payload = {
        "strategy": str(st.session_state.get("auto_backtest_strategy", "v9.0 中线均衡版（生产）")),
        "sample_size": int(st.session_state.get("auto_backtest_sample_size", 800)),
        "full_market_mode": bool(st.session_state.get("auto_backtest_full_market_mode", False)),
        "history_days": int(st.session_state.get("auto_backtest_history_days", 240)),
        "holding_days": int(st.session_state.get("auto_backtest_holding_days", 10)),
        "score_threshold": float(st.session_state.get("auto_backtest_score_threshold", 65.0)),
    }
    ok, msg, run_id = start_async_backtest_job("single", payload)
    if not ok:
        return {"ok": False, "status": "submit_failed", "error": msg}
    set_sim_meta("auto_backtest_last_run_date", today)
    set_sim_meta("auto_backtest_last_run_at", now_text())
    set_sim_meta("auto_backtest_last_job_id", run_id)
    st.session_state["single_backtest_async_job_id"] = run_id
    return {"ok": True, "status": "ran", "run_id": run_id}


def apply_portfolio_risk_budget(
    df: pd.DataFrame,
    score_col: str,
    *,
    industry_col: str = "行业",
    max_positions: int = 20,
    max_industry_ratio: float = 0.35,
) -> pd.DataFrame:
    if df is None or df.empty or score_col not in df.columns:
        return df
    out = df.copy()
    out["_score_num"] = pd.to_numeric(out[score_col], errors="coerce")
    out = out.dropna(subset=["_score_num"]).sort_values("_score_num", ascending=False).reset_index(drop=True)
    if out.empty:
        return out

    keep_limit = max(1, int(max_positions))
    if industry_col not in out.columns:
        return out.head(keep_limit).drop(columns=["_score_num"], errors="ignore")

    ind_cap = max(1, int(np.ceil(float(max_industry_ratio) * keep_limit)))
    chosen_idx: List[int] = []
    ind_cnt: Dict[str, int] = {}
    for idx, row in out.iterrows():
        if len(chosen_idx) >= keep_limit:
            break
        ind = str(row.get(industry_col, "未知") or "未知")
        if ind_cnt.get(ind, 0) >= ind_cap:
            continue
        chosen_idx.append(idx)
        ind_cnt[ind] = ind_cnt.get(ind, 0) + 1

    if not chosen_idx:
        return out.head(keep_limit).drop(columns=["_score_num"], errors="ignore")
    return out.iloc[chosen_idx].drop(columns=["_score_num"], errors="ignore").reset_index(drop=True)
