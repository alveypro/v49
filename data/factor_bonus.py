from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import pandas as pd


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _pick_col(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _read_trade_date(conn: sqlite3.Connection, preferred: Optional[str] = None) -> Optional[str]:
    if preferred:
        return str(preferred)
    try:
        df = pd.read_sql_query("SELECT MAX(trade_date) AS d FROM daily_trading_data", conn)
        if df is not None and not df.empty and df["d"].iloc[0]:
            return str(df["d"].iloc[0])
    except Exception:
        pass
    return None


def _read_table_latest_date(conn: sqlite3.Connection, table: str, date_col: str) -> Optional[str]:
    if not _table_exists(conn, table):
        return None
    try:
        df = pd.read_sql_query(f"SELECT MAX({date_col}) AS d FROM {table}", conn)
        if df is not None and not df.empty and df["d"].iloc[0]:
            return str(df["d"].iloc[0])
    except Exception:
        return None
    return None


def _shift_date_str(date_str: str, days: int) -> str:
    try:
        d = datetime.strptime(str(date_str), "%Y%m%d")
        return (d + timedelta(days=days)).strftime("%Y%m%d")
    except Exception:
        return str(date_str)


def load_factor_bonus_maps(
    conn: sqlite3.Connection,
    trade_date: Optional[str] = None,
) -> Tuple[float, Dict[str, float], Dict[str, float], Dict[str, Any]]:
    """
    Load factor-derived bonus maps.

    Returns:
      (global_bonus, stock_bonus_map, industry_bonus_map, meta)
    """
    enabled = os.getenv("OPENCLAW_FACTOR_BONUS_ENABLED", "1").strip() not in {"0", "false", "False"}
    if not enabled:
        return 0.0, {}, {}, {"enabled": False}

    td = _read_trade_date(conn, trade_date)
    if not td:
        return 0.0, {}, {}, {"enabled": True, "trade_date": None}

    global_bonus = 0.0
    stock_bonus: Dict[str, float] = {}
    industry_bonus: Dict[str, float] = {}
    meta: Dict[str, Any] = {"enabled": True, "trade_date": td, "sources": []}

    # 1) 大盘资金流（日）
    if _table_exists(conn, "moneyflow_mkt_dc_daily"):
        try:
            df = pd.read_sql_query(
                "SELECT * FROM moneyflow_mkt_dc_daily WHERE trade_date = ? LIMIT 1",
                conn,
                params=(td,),
            )
            if df is not None and not df.empty:
                col = _pick_col(df, ("net_amount_main", "net_main", "net_inflow_main", "net_amount"))
                if col:
                    v = _safe_float(df[col].iloc[0], 0.0)
                    if v > 0:
                        global_bonus += 0.6
                    elif v < 0:
                        global_bonus -= 0.6
                    meta["sources"].append("moneyflow_mkt_dc_daily")
        except Exception:
            pass

    # 2) 指数技术因子（日） - 优先上证指数
    if _table_exists(conn, "idx_factor_pro_daily"):
        try:
            df = pd.read_sql_query(
                "SELECT * FROM idx_factor_pro_daily WHERE trade_date = ?",
                conn,
                params=(td,),
            )
            if df is not None and not df.empty:
                if "ts_code" in df.columns:
                    p = df[df["ts_code"].astype(str) == "000001.SH"]
                    if p.empty:
                        p = df
                else:
                    p = df
                row = p.iloc[0]
                adx = _safe_float(row.get("adx"), 0.0)
                if adx >= 25:
                    global_bonus += 0.4
                elif adx <= 15:
                    global_bonus -= 0.4
                rsi = _safe_float(row.get("rsi"), 50.0)
                if 45 <= rsi <= 65:
                    global_bonus += 0.2
                elif rsi < 30 or rsi > 80:
                    global_bonus -= 0.2
                meta["sources"].append("idx_factor_pro_daily")
        except Exception:
            pass

    # 3) 行业资金流（日）
    if _table_exists(conn, "moneyflow_ind_dc_daily"):
        try:
            df = pd.read_sql_query(
                "SELECT * FROM moneyflow_ind_dc_daily WHERE trade_date = ?",
                conn,
                params=(td,),
            )
            if df is not None and not df.empty:
                ind_col = _pick_col(df, ("industry", "industry_name", "name"))
                flow_col = _pick_col(df, ("net_amount_main", "net_main", "net_inflow_main", "net_amount"))
                if ind_col and flow_col:
                    df[ind_col] = df[ind_col].astype(str)
                    flow = pd.to_numeric(df[flow_col], errors="coerce").fillna(0.0)
                    scale = float(max(1.0, flow.abs().quantile(0.85)))
                    for _, r in df.iterrows():
                        ind = str(r[ind_col])
                        v = _safe_float(r[flow_col], 0.0)
                        b = max(-0.6, min(0.6, v / scale))
                        if ind:
                            industry_bonus[ind] = b
                    meta["sources"].append("moneyflow_ind_dc_daily")
        except Exception:
            pass

    # 4) 个股技术因子（日）
    if _table_exists(conn, "stk_factor_pro_daily"):
        try:
            df = pd.read_sql_query(
                "SELECT * FROM stk_factor_pro_daily WHERE trade_date = ?",
                conn,
                params=(td,),
            )
            if df is not None and not df.empty and "ts_code" in df.columns:
                for _, r in df.iterrows():
                    ts_code = str(r.get("ts_code", "")).strip()
                    if not ts_code:
                        continue
                    bonus = 0.0
                    adx = _safe_float(r.get("adx"), 0.0)
                    rsi = _safe_float(r.get("rsi"), 50.0)
                    mfi = _safe_float(r.get("mfi"), 50.0)
                    atr = _safe_float(r.get("atr"), 0.0)
                    close = _safe_float(r.get("close"), _safe_float(r.get("close_price"), 0.0))
                    if adx >= 25:
                        bonus += 0.5
                    elif adx <= 15:
                        bonus -= 0.4
                    if 45 <= rsi <= 70:
                        bonus += 0.3
                    elif rsi < 25 or rsi > 85:
                        bonus -= 0.3
                    if 45 <= mfi <= 80:
                        bonus += 0.2
                    elif mfi < 20 or mfi > 90:
                        bonus -= 0.2
                    if close > 0 and atr > 0:
                        atr_pct = atr / close
                        if 0.01 <= atr_pct <= 0.07:
                            bonus += 0.2
                        elif atr_pct > 0.12:
                            bonus -= 0.3
                    stock_bonus[ts_code] = stock_bonus.get(ts_code, 0.0) + max(-1.5, min(1.5, bonus))
                meta["sources"].append("stk_factor_pro_daily")
        except Exception:
            pass

    # 5) 筹码因子（日）
    if _table_exists(conn, "cyq_perf_daily"):
        try:
            df = pd.read_sql_query(
                "SELECT * FROM cyq_perf_daily WHERE trade_date = ?",
                conn,
                params=(td,),
            )
            if df is not None and not df.empty and "ts_code" in df.columns:
                win_col = _pick_col(df, ("winner_rate", "win_rate", "profit_ratio"))
                c50_col = _pick_col(df, ("cost_50pct", "cost_50", "cost_avg"))
                c15_col = _pick_col(df, ("cost_15pct", "cost_15"))
                c85_col = _pick_col(df, ("cost_85pct", "cost_85"))
                for _, r in df.iterrows():
                    ts_code = str(r.get("ts_code", "")).strip()
                    if not ts_code:
                        continue
                    bonus = 0.0
                    if win_col:
                        w = _safe_float(r.get(win_col), 50.0)
                        if w >= 60:
                            bonus += 0.8
                        elif w <= 35:
                            bonus -= 0.8
                    if c50_col and c15_col and c85_col:
                        c50 = _safe_float(r.get(c50_col), 0.0)
                        c15 = _safe_float(r.get(c15_col), 0.0)
                        c85 = _safe_float(r.get(c85_col), 0.0)
                        if c50 > 0 and c85 > c15:
                            spread = (c85 - c15) / c50
                            if spread <= 0.12:
                                bonus += 0.4
                            elif spread >= 0.28:
                                bonus -= 0.4
                    stock_bonus[ts_code] = stock_bonus.get(ts_code, 0.0) + max(-1.2, min(1.2, bonus))
                meta["sources"].append("cyq_perf_daily")
        except Exception:
            pass

    # 6) 个股东财主力资金（日）
    if _table_exists(conn, "moneyflow_dc_daily"):
        try:
            df = pd.read_sql_query(
                "SELECT * FROM moneyflow_dc_daily WHERE trade_date = ?",
                conn,
                params=(td,),
            )
            if df is not None and not df.empty and "ts_code" in df.columns:
                flow_col = _pick_col(df, ("net_amount_main", "net_main", "net_inflow_main", "net_amount"))
                if flow_col:
                    flow = pd.to_numeric(df[flow_col], errors="coerce").fillna(0.0)
                    scale = float(max(1.0, flow.abs().quantile(0.85)))
                    for _, r in df.iterrows():
                        ts_code = str(r.get("ts_code", "")).strip()
                        if not ts_code:
                            continue
                        v = _safe_float(r.get(flow_col), 0.0)
                        b = max(-0.8, min(0.8, v / scale))
                        stock_bonus[ts_code] = stock_bonus.get(ts_code, 0.0) + b
                    meta["sources"].append("moneyflow_dc_daily")
        except Exception:
            pass

    # 7) 集合竞价（日）
    if _table_exists(conn, "stk_auction_daily"):
        try:
            df = pd.read_sql_query(
                "SELECT * FROM stk_auction_daily WHERE trade_date = ?",
                conn,
                params=(td,),
            )
            if df is not None and not df.empty and "ts_code" in df.columns:
                vol_ratio_col = _pick_col(df, ("vol_ratio", "ratio", "auction_vol_ratio"))
                amount_col = _pick_col(df, ("amount", "auction_amount", "turnover"))
                for _, r in df.iterrows():
                    ts_code = str(r.get("ts_code", "")).strip()
                    if not ts_code:
                        continue
                    bonus = 0.0
                    if vol_ratio_col:
                        vr = _safe_float(r.get(vol_ratio_col), 1.0)
                        if vr >= 2.0:
                            bonus += 0.5
                        elif vr < 0.6:
                            bonus -= 0.2
                    if amount_col:
                        amt = _safe_float(r.get(amount_col), 0.0)
                        if amt > 0:
                            bonus += 0.1
                    stock_bonus[ts_code] = stock_bonus.get(ts_code, 0.0) + max(-0.8, min(0.8, bonus))
                meta["sources"].append("stk_auction_daily")
        except Exception:
            pass

    # 8) 北向持股（日）
    if _table_exists(conn, "hk_hold_daily"):
        try:
            df = pd.read_sql_query(
                "SELECT * FROM hk_hold_daily WHERE trade_date = ?",
                conn,
                params=(td,),
            )
            if df is not None and not df.empty and "ts_code" in df.columns:
                ratio_col = _pick_col(df, ("ratio", "hold_ratio", "ratio_change"))
                change_col = _pick_col(df, ("ratio_change", "vol_change", "amount_change"))
                for _, r in df.iterrows():
                    ts_code = str(r.get("ts_code", "")).strip()
                    if not ts_code:
                        continue
                    bonus = 0.0
                    if ratio_col:
                        ratio_v = _safe_float(r.get(ratio_col), 0.0)
                        if ratio_v >= 2.0:
                            bonus += 0.2
                    if change_col:
                        chg = _safe_float(r.get(change_col), 0.0)
                        if chg > 0:
                            bonus += 0.5
                        elif chg < 0:
                            bonus -= 0.5
                    stock_bonus[ts_code] = stock_bonus.get(ts_code, 0.0) + max(-0.8, min(0.8, bonus))
                meta["sources"].append("hk_hold_daily")
        except Exception:
            pass

    # 9) 回购（事件）
    if _table_exists(conn, "repurchase_events"):
        try:
            latest_ann = _read_table_latest_date(conn, "repurchase_events", "ann_date")
            if latest_ann:
                df = pd.read_sql_query(
                    "SELECT * FROM repurchase_events WHERE ann_date >= ?",
                    conn,
                    params=(_shift_date_str(latest_ann, -90),),
                )
                if df is not None and not df.empty and "ts_code" in df.columns:
                    amount_col = _pick_col(df, ("amount", "exp_amount", "high_limit"))
                    for _, r in df.iterrows():
                        ts_code = str(r.get("ts_code", "")).strip()
                        if not ts_code:
                            continue
                        b = 0.5
                        if amount_col:
                            amt = _safe_float(r.get(amount_col), 0.0)
                            if amt > 0:
                                b += min(0.5, amt / 1e9)
                        stock_bonus[ts_code] = stock_bonus.get(ts_code, 0.0) + max(-0.2, min(1.0, b))
                    meta["sources"].append("repurchase_events")
        except Exception:
            pass

    # 10) 解禁（事件，近期负向）
    if _table_exists(conn, "share_float_events"):
        try:
            latest_float = _read_table_latest_date(conn, "share_float_events", "float_date")
            if latest_float:
                df = pd.read_sql_query(
                    "SELECT * FROM share_float_events WHERE float_date >= ?",
                    conn,
                    params=(_shift_date_str(latest_float, -60),),
                )
                if df is not None and not df.empty and "ts_code" in df.columns:
                    ratio_col = _pick_col(df, ("float_ratio", "ratio", "share_ratio"))
                    for _, r in df.iterrows():
                        ts_code = str(r.get("ts_code", "")).strip()
                        if not ts_code:
                            continue
                        ratio_v = _safe_float(r.get(ratio_col), 0.0) if ratio_col else 0.0
                        penalty = -0.3
                        if ratio_v >= 5:
                            penalty = -0.8
                        elif ratio_v >= 2:
                            penalty = -0.5
                        stock_bonus[ts_code] = stock_bonus.get(ts_code, 0.0) + penalty
                    meta["sources"].append("share_float_events")
        except Exception:
            pass

    # 11) 券商研报（事件）
    if _table_exists(conn, "broker_recommend_events"):
        try:
            latest_rec = _read_table_latest_date(conn, "broker_recommend_events", "trade_date")
            if latest_rec:
                df = pd.read_sql_query(
                    "SELECT * FROM broker_recommend_events WHERE trade_date >= ?",
                    conn,
                    params=(_shift_date_str(latest_rec, -60),),
                )
                if df is not None and not df.empty and "ts_code" in df.columns:
                    rec_col = _pick_col(df, ("rating", "recommend", "opinion"))
                    for _, r in df.iterrows():
                        ts_code = str(r.get("ts_code", "")).strip()
                        if not ts_code:
                            continue
                        txt = str(r.get(rec_col, "")).lower() if rec_col else ""
                        b = 0.0
                        if any(k in txt for k in ("buy", "增持", "强烈推荐", "outperform")):
                            b += 0.5
                        if any(k in txt for k in ("sell", "减持", "underperform")):
                            b -= 0.5
                        stock_bonus[ts_code] = stock_bonus.get(ts_code, 0.0) + b
                    meta["sources"].append("broker_recommend_events")
        except Exception:
            pass

    # Final cap
    global_bonus = max(-2.0, min(2.0, global_bonus))
    for k, v in list(stock_bonus.items()):
        stock_bonus[k] = max(-3.0, min(3.0, float(v)))
    for k, v in list(industry_bonus.items()):
        industry_bonus[k] = max(-1.0, min(1.0, float(v)))

    return global_bonus, stock_bonus, industry_bonus, meta
