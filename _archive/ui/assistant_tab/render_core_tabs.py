from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from risk_params import get_strategy_risk_params, normalize_strategy_name

from .cache import (
    cached_assistant_daily_recs,
    cached_assistant_holdings,
    cached_assistant_trades,
    clear_assistant_ui_cache,
)
from .helpers import summarize_holdings, summarize_trade_periods


def _load_single_stock_context(db_path: str, ts_code: str, bars: int = 220) -> Tuple[pd.DataFrame, str, str]:
    conn = sqlite3.connect(db_path)
    try:
        meta_df = pd.read_sql_query(
            "SELECT name, industry FROM stock_basic WHERE ts_code = ? LIMIT 1",
            conn,
            params=(ts_code,),
        )
        stock_name = str(meta_df.iloc[0]["name"]) if not meta_df.empty else ts_code
        industry = str(meta_df.iloc[0]["industry"]) if not meta_df.empty else "未知行业"

        history_sql = """
            SELECT trade_date, close_price, high_price, low_price, vol, amount, pct_chg, turnover_rate
            FROM {table}
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
        """
        history_df = pd.DataFrame()
        for table in ("daily_trading_data", "daily_data"):
            try:
                history_df = pd.read_sql_query(history_sql.format(table=table), conn, params=(ts_code, int(bars)))
                if not history_df.empty:
                    break
            except Exception:
                continue
        if history_df.empty:
            return pd.DataFrame(), stock_name, industry
        history_df = history_df.sort_values("trade_date").reset_index(drop=True)
        history_df["name"] = stock_name
        return history_df, stock_name, industry
    finally:
        conn.close()


def _load_index_context(db_path: str, bars: int = 220) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT trade_date, close_price, vol
            FROM daily_trading_data
            WHERE ts_code = '000001.SH'
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            conn,
            params=(int(bars),),
        )
        if df is None or df.empty:
            return pd.DataFrame()
        return df.sort_values("trade_date").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def _result_score(result: Dict[str, Any]) -> float:
    for key in ("final_score", "comprehensive_score", "score"):
        if key in result:
            try:
                return float(result.get(key, 0) or 0)
            except Exception:
                continue
    return 0.0


def _latest_price_from_history(stock_data: pd.DataFrame) -> float:
    if stock_data is None or stock_data.empty:
        return 0.0
    for col in ("close_price", "close"):
        if col in stock_data.columns:
            s = pd.to_numeric(stock_data[col], errors="coerce").dropna()
            if not s.empty:
                return float(s.iloc[-1])
    return 0.0


def _volatility_pct_from_history(stock_data: pd.DataFrame, window: int = 20) -> float:
    if stock_data is None or stock_data.empty:
        return 0.04
    if "pct_chg" in stock_data.columns:
        r = pd.to_numeric(stock_data["pct_chg"], errors="coerce").dropna() / 100.0
    else:
        px = pd.to_numeric(stock_data.get("close_price", stock_data.get("close")), errors="coerce").dropna()
        r = px.pct_change().dropna()
    if r is None or r.empty:
        return 0.04
    vol = float(r.tail(max(5, int(window))).std())
    if np.isnan(vol) or vol <= 0:
        return 0.04
    return float(min(0.12, max(0.015, vol)))


def _risk_band_by_strategy(strategy: str, score: float, vol_pct: float) -> Tuple[float, float]:
    s = normalize_strategy_name(strategy)
    cfg = get_strategy_risk_params(s)
    sl_base = float(cfg.get("stop_loss_pct", 0.06))
    tp_base = float(cfg.get("take_profit_pct", 0.12))

    score_adj = (float(score) - 60.0) / 200.0
    sl = sl_base + score_adj
    tp = tp_base + score_adj * 2.0
    sl += max(0.0, vol_pct - 0.03) * 1.2
    tp += max(0.0, vol_pct - 0.03) * 1.8
    sl = float(min(float(cfg.get("max_stop_loss_pct", 0.18)), max(float(cfg.get("min_stop_loss_pct", 0.03)), sl)))
    tp = float(min(0.35, max(0.05, tp)))
    tp = max(tp, sl * float(cfg.get("tp_sl_ratio", 1.4)))
    return sl, tp


def _normalize_stop_take(
    strategy: str,
    score: float,
    current_price: float,
    raw_stop: Any,
    raw_take: Any,
    stock_data: pd.DataFrame,
) -> Tuple[float, float, str]:
    cp = float(current_price or 0.0)
    if cp <= 0:
        return 0.0, 0.0, "价格缺失，无法计算风控价"

    vol_pct = _volatility_pct_from_history(stock_data)
    sl_pct, tp_pct = _risk_band_by_strategy(strategy, score, vol_pct)

    stop = float(raw_stop or 0.0) if raw_stop is not None else 0.0
    take = float(raw_take or 0.0) if raw_take is not None else 0.0
    valid_stop = np.isfinite(stop) and stop > 0 and stop < cp * 0.995 and stop > cp * 0.5
    valid_take = np.isfinite(take) and take > cp * 1.005 and take < cp * 2.0

    if not valid_stop:
        stop = cp * (1.0 - sl_pct)
    if not valid_take:
        take = cp * (1.0 + tp_pct)
    if stop >= cp:
        stop = cp * (1.0 - max(0.03, sl_pct))
    if take <= cp:
        take = cp * (1.0 + max(0.05, tp_pct))
    if take <= stop:
        take = cp * (1.0 + max(0.08, tp_pct))

    basis = f"现价{cp:.2f}；波动{vol_pct*100:.2f}%；止损{(1-stop/cp)*100:.2f}% 止盈{(take/cp-1)*100:.2f}%"
    return round(float(stop), 2), round(float(take), 2), basis


def _top_dimension_text(result: Dict[str, Any], limit: int = 3) -> str:
    dim = result.get("dimension_scores") or result.get("dim_scores") or {}
    if not isinstance(dim, dict) or not dim:
        return ""
    items: List[Tuple[str, float]] = []
    for k, v in dim.items():
        try:
            items.append((str(k), float(v)))
        except Exception:
            continue
    if not items:
        return ""
    items.sort(key=lambda x: x[1], reverse=True)
    top = items[: max(1, int(limit))]
    return "；".join([f"{k}:{v:.1f}" for k, v in top])


def _grade_from_score(score: float) -> str:
    if score >= 85:
        return "S"
    if score >= 75:
        return "A"
    if score >= 65:
        return "B"
    if score >= 55:
        return "C"
    return "D"


def _safe_float_cell(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        if isinstance(v, str):
            s = v.strip().replace("%", "").replace(",", "")
            if not s:
                return float(default)
            return float(s)
        return float(v)
    except Exception:
        return float(default)


def _latest_scan_df_for_strategy(strategy: str) -> Optional[pd.DataFrame]:
    key_map = {
        "v4": "v4_scan_results",
        "v5": "v5_scan_results",
        "v6": "v6_scan_results_tab1",
        "v7": "v7_scan_results_tab1",
        "v8": "v8_scan_results_tab1",
        "v9": "v9_scan_results_tab1",
        "combo": "combo_scan_results",
    }
    key = key_map.get(str(strategy).lower().strip())
    if not key:
        return None
    df = st.session_state.get(key)
    return df if isinstance(df, pd.DataFrame) and not df.empty else None


def _load_strategy_score_from_latest_scan(strategy: str, ts_code: str) -> Optional[Dict[str, float]]:
    """
    Align single-stock score with latest strategy-center table score in current session.
    """
    df = _latest_scan_df_for_strategy(strategy)
    if df is None:
        return None
    code_col = "股票代码" if "股票代码" in df.columns else ("ts_code" if "ts_code" in df.columns else None)
    if not code_col:
        return None
    score_col = "共识评分" if str(strategy).lower().strip() == "combo" else "综合评分"
    if score_col not in df.columns:
        return None
    hit = df[df[code_col].astype(str).str.upper() == str(ts_code).upper()]
    if hit.empty:
        return None
    row = hit.iloc[0]
    final_score = _safe_float_cell(row.get(score_col), default=0.0)
    extra_score = _safe_float_cell(row.get("资金加分"), default=0.0) if "资金加分" in row.index else 0.0
    base_score = final_score - extra_score
    return {"final_score": final_score, "base_score": base_score, "extra_score": extra_score}


def _load_v9_score_from_latest_scan(ts_code: str) -> Optional[Dict[str, float]]:
    """
    Prefer exact consistency with strategy-center v9 scan results when available in session.
    """
    try:
        return _load_strategy_score_from_latest_scan("v9", ts_code)
    except Exception:
        return None


def _estimate_industry_strength(assistant, ts_code: str, industry: str, bars: int = 80, limit: int = 240) -> float:
    if not industry:
        return 0.0
    conn = sqlite3.connect(assistant.db_path)
    try:
        peers = pd.read_sql_query(
            """
            SELECT ts_code
            FROM stock_basic
            WHERE industry = ?
            ORDER BY circ_mv DESC
            LIMIT ?
            """,
            conn,
            params=(industry, int(limit)),
        )
        if peers is None or peers.empty:
            return 0.0
        ts_codes = [str(x) for x in peers["ts_code"].astype(str).tolist()]
        if ts_code not in ts_codes:
            ts_codes.append(str(ts_code))
        history_cache = assistant._load_history_cache_bulk(conn, ts_codes, bars=bars)
        vals: List[float] = []
        for code in ts_codes:
            hist = history_cache.get(code)
            if hist is None or hist.empty:
                continue
            close = pd.to_numeric(hist["close_price"], errors="coerce").ffill().dropna()
            if len(close) > 21:
                vals.append((float(close.iloc[-1]) / float(close.iloc[-21]) - 1.0) * 100.0)
        if not vals:
            return 0.0
        return float(np.mean(vals))
    except Exception:
        return 0.0
    finally:
        conn.close()


def _evaluate_v9_with_assistant(assistant, stock_data: pd.DataFrame, ts_code: str, industry: str) -> Dict[str, Any]:
    # 1) If this stock exists in latest v9 scan table, use that exact score for strict consistency.
    from_scan = _load_v9_score_from_latest_scan(ts_code)
    if from_scan is not None:
        score = float(from_scan["final_score"])
        return {
            "success": True,
            "final_score": score,
            "grade": _grade_from_score(score),
            "description": "v9中线均衡：与策略中心v9列表综合评分口径一致（会话内对齐）",
            "dimension_scores": {"base_score": from_scan["base_score"], "资金加分": from_scan["extra_score"]},
            "stop_loss": 0.0,
            "take_profit": 0.0,
        }

    # 2) Fallback: same formula as strategy-center v9 (industry strength + external bonus).
    ind_strength = _estimate_industry_strength(assistant, ts_code=ts_code, industry=industry, bars=80, limit=240)
    info = assistant._calc_v9_score_from_hist(stock_data, industry_strength=ind_strength)
    base_score = float((info or {}).get("score", 0.0) or 0.0)
    extra_score = 0.0
    try:
        with sqlite3.connect(assistant.db_path) as conn:
            bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = assistant._load_external_bonus_maps(conn)
        extra_score = float(
            assistant._calc_external_bonus(
                ts_code, industry, bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map
            )
        )
    except Exception:
        extra_score = 0.0
    score = float(base_score + extra_score)
    return {
        "success": True,
        "final_score": score,
        "grade": _grade_from_score(score),
        "description": "v9中线均衡：策略中心同口径（行业强度+资金加分）",
        "dimension_scores": {**((info or {}).get("details", {}) or {}), "base_score": base_score, "资金加分": extra_score},
        "stop_loss": float((info or {}).get("stop_loss", 0.0) or 0.0),
        "take_profit": float((info or {}).get("take_profit", 0.0) or 0.0),
    }


def _evaluate_stable_from_history(stock_data: pd.DataFrame) -> Dict[str, Any]:
    if stock_data is None or stock_data.empty or len(stock_data) < 80:
        return {"success": False, "final_score": 0.0, "description": "数据不足"}
    close = pd.to_numeric(stock_data["close_price"], errors="coerce").ffill()
    close = close.dropna()
    if len(close) < 80:
        return {"success": False, "final_score": 0.0, "description": "数据不足"}

    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    trend_ok = bool(ma20.iloc[-1] > ma60.iloc[-1] and ma20.iloc[-1] > ma20.iloc[-5])
    rolling_peak = close.cummax()
    drawdown = (rolling_peak - close) / rolling_peak
    max_dd = float(drawdown.tail(60).max())
    recent_low = float(close.tail(20).min())
    rebound = (float(close.iloc[-1]) / recent_low - 1.0) if recent_low > 0 else 0.0
    returns = close.pct_change().dropna()
    vol = float(returns.tail(20).std()) if not returns.empty else 0.0
    breakout = bool(float(close.iloc[-1]) > float(close.tail(21).iloc[:-1].max()))

    score = 0.0
    score += 30.0 if trend_ok else 0.0
    score += max(0.0, 20.0 * (1.0 - max_dd / 0.15))
    score += min(20.0, 100.0 * rebound)
    score += max(0.0, 20.0 * (1.0 - vol / 0.06))
    score += 10.0 if breakout else 0.0
    score = max(0.0, min(100.0, score))

    current = float(close.iloc[-1])
    cfg = get_strategy_risk_params("stable")
    sl_base = float(cfg.get("stop_loss_pct", 0.06))
    sl_pct = sl_base if trend_ok else sl_base * 1.15
    sl_pct += max(0.0, vol - 0.03) * 0.8
    sl_pct = min(float(cfg.get("max_stop_loss_pct", 0.12)), max(float(cfg.get("min_stop_loss_pct", 0.04)), sl_pct))
    tp_base = float(cfg.get("take_profit_pct", 0.14))
    tp_ratio = float(cfg.get("tp_sl_ratio", 2.0))
    tp_pct = (tp_base if breakout else tp_base * 0.8) + max(0.0, score - 60.0) / 500.0
    tp_pct = min(0.30, max(sl_pct * tp_ratio, tp_pct))
    stop_loss = current * (1.0 - sl_pct)
    take_profit = current * (1.0 + tp_pct)
    return {
        "success": True,
        "final_score": score,
        "grade": _grade_from_score(score),
        "description": "稳定上涨：趋势/回撤/反弹/波动/二次启动",
        "stop_loss": round(stop_loss, 2),
        "take_profit": round(take_profit, 2),
        "dimension_scores": {
            "趋势": 30.0 if trend_ok else 0.0,
            "回撤控制": max(0.0, 20.0 * (1.0 - max_dd / 0.15)),
            "反弹力度": min(20.0, 100.0 * rebound),
            "波动稳定": max(0.0, 20.0 * (1.0 - vol / 0.06)),
            "二次启动": 10.0 if breakout else 0.0,
        },
    }


def _decision_from_score(score: float, in_position: bool, profit_loss_pct: Optional[float]) -> Tuple[str, str]:
    pnl = float(profit_loss_pct or 0.0)
    if in_position:
        if score < 55 or pnl <= -0.08:
            return "卖出", "评分走弱或回撤过深，优先保护资金。"
        if score >= 75 and pnl < 0.12:
            return "持有", "评分仍强，可继续持有并跟踪止盈线。"
        if pnl >= 0.12 and score < 70:
            return "卖出", "已有盈利且评分回落，建议分批止盈。"
        return "持有", "信号中性，维持仓位并观察后续强度。"
    if score >= 75:
        return "买入", "评分较高，可考虑分批建仓。"
    if score >= 60:
        return "持有", "尚可观察，等待更强确认信号。"
    return "卖出", "当前不具备买点，避免介入。"


def _evaluate_single_stock_by_strategy(
    assistant,
    strategy: str,
    stock_data: pd.DataFrame,
    ts_code: str,
    industry: str,
    db_path: str,
    index_data: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    strategy = strategy.lower().strip()
    if strategy == "v4":
        from comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4

        result = ComprehensiveStockEvaluatorV4().evaluate_stock_v4(stock_data)
    elif strategy == "v5":
        from comprehensive_stock_evaluator_v5 import ComprehensiveStockEvaluatorV5

        result = ComprehensiveStockEvaluatorV5().evaluate_stock_v4(stock_data)
    elif strategy == "v6":
        from comprehensive_stock_evaluator_v6 import ComprehensiveStockEvaluatorV6

        result = ComprehensiveStockEvaluatorV6().evaluate_stock_v6(stock_data, ts_code)
    elif strategy == "v7":
        from comprehensive_stock_evaluator_v7_ultimate import ComprehensiveStockEvaluatorV7Ultimate

        result = ComprehensiveStockEvaluatorV7Ultimate(db_path).evaluate_stock_v7(stock_data, ts_code, industry)
    elif strategy == "v8":
        from comprehensive_stock_evaluator_v8_ultimate import ComprehensiveStockEvaluatorV8Ultimate

        result = ComprehensiveStockEvaluatorV8Ultimate(db_path).evaluate_stock_v8(
            stock_data, ts_code=ts_code, index_data=index_data, industry=industry
        )
    elif strategy == "v9":
        result = _evaluate_v9_with_assistant(assistant, stock_data, ts_code=ts_code, industry=industry)
    elif strategy == "stable":
        result = _evaluate_stable_from_history(stock_data)
    elif strategy == "combo":
        min_score = float(assistant.get_config("min_score") or 60)
        thr = {"v4": min_score, "v5": min_score, "v7": min_score + 2, "v8": min_score + 2, "v9": min_score}
        weights = {"v4": 0.15, "v5": 0.15, "v7": 0.30, "v8": 0.25, "v9": 0.15}
        sub_scores: Dict[str, Optional[float]] = {}
        for sub in ("v4", "v5", "v7", "v8", "v9"):
            try:
                sub_r = _evaluate_single_stock_by_strategy(
                    assistant=assistant,
                    strategy=sub,
                    stock_data=stock_data,
                    ts_code=ts_code,
                    industry=industry,
                    db_path=db_path,
                    index_data=index_data,
                )
                sub_scores[sub] = float(sub_r.get("score", 0))
            except Exception:
                sub_scores[sub] = None
        weight_sum = sum(weights[k] for k, v in sub_scores.items() if v is not None)
        weighted_score = (
            sum(float(sub_scores[k]) * weights[k] for k in sub_scores if sub_scores[k] is not None) / weight_sum
            if weight_sum > 0
            else 0.0
        )
        bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = assistant._load_external_bonus_maps()
        extra = assistant._calc_external_bonus(
            ts_code, industry, bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map
        )
        agree = sum(1 for k, v in sub_scores.items() if v is not None and float(v) >= thr.get(k, min_score))
        final_score = float(weighted_score + extra)
        result = {
            "success": True,
            "final_score": final_score,
            "grade": _grade_from_score(final_score),
            "description": f"组合共识：一致数{agree}/5，资金加分{extra:.1f}",
            "dimension_scores": {k: float(v) for k, v in sub_scores.items() if v is not None},
            "combo_weights": weights,
        }
    elif strategy == "ai":
        combo = _evaluate_single_stock_by_strategy(
            assistant=assistant,
            strategy="combo",
            stock_data=stock_data,
            ts_code=ts_code,
            industry=industry,
            db_path=db_path,
            index_data=index_data,
        )
        stable = _evaluate_single_stock_by_strategy(
            assistant=assistant,
            strategy="stable",
            stock_data=stock_data,
            ts_code=ts_code,
            industry=industry,
            db_path=db_path,
            index_data=index_data,
        )
        combo_score = float(combo.get("score", combo.get("final_score", 0)) or 0)
        stable_score = float(stable.get("score", stable.get("final_score", 0)) or 0)
        index_penalty = 0.0
        if index_data is not None and not index_data.empty and len(index_data) >= 30:
            idx_close = pd.to_numeric(index_data["close_price"], errors="coerce").ffill()
            idx_ma20 = idx_close.rolling(20).mean().iloc[-1]
            if float(idx_close.iloc[-1]) < float(idx_ma20):
                index_penalty = 4.0
        final_score = max(0.0, min(100.0, combo_score * 0.7 + stable_score * 0.3 - index_penalty))
        result = {
            "success": True,
            "final_score": final_score,
            "grade": _grade_from_score(final_score),
            "description": f"AI融合：组合{combo_score:.1f} + 稳定{stable_score:.1f} - 市场惩罚{index_penalty:.1f}",
            "dimension_scores": {"combo": combo_score, "stable": stable_score, "market_penalty": -index_penalty},
        }
    else:
        raise ValueError(f"不支持的策略: {strategy}")

    # Align with strategy-center table score when that strategy has been scanned in current session.
    from_scan = _load_strategy_score_from_latest_scan(strategy, ts_code)
    if from_scan is not None:
        result["final_score"] = float(from_scan["final_score"])
        dim = result.get("dimension_scores") if isinstance(result.get("dimension_scores"), dict) else {}
        dim = dict(dim or {})
        dim.setdefault("base_score", float(from_scan["base_score"]))
        dim.setdefault("资金加分", float(from_scan["extra_score"]))
        result["dimension_scores"] = dim
        result["description"] = f"{str(result.get('description', '')).strip()}（与策略中心列表口径一致）".strip()
    elif strategy in {"v4", "v5"}:
        # v4/v5 strategy-center "综合评分" includes external bonus, align fallback path accordingly.
        try:
            base_score = _result_score(result)
            with sqlite3.connect(assistant.db_path) as conn:
                bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = assistant._load_external_bonus_maps(conn)
            extra = float(
                assistant._calc_external_bonus(
                    ts_code, industry, bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map
                )
            )
            result["final_score"] = float(base_score + extra)
            dim = result.get("dimension_scores") if isinstance(result.get("dimension_scores"), dict) else {}
            dim = dict(dim or {})
            dim["base_score"] = float(base_score)
            dim["资金加分"] = float(extra)
            result["dimension_scores"] = dim
            result["description"] = f"{str(result.get('description', '')).strip()}（含资金加分口径）".strip()
        except Exception:
            pass

    score = _result_score(result)
    current_price = _latest_price_from_history(stock_data)
    stop_loss, take_profit, risk_basis = _normalize_stop_take(
        strategy=strategy,
        score=score,
        current_price=current_price,
        raw_stop=result.get("stop_loss"),
        raw_take=result.get("take_profit"),
        stock_data=stock_data,
    )
    basis = _top_dimension_text(result)
    if strategy == "combo":
        basis = f"{basis}；权重 v4/v5/v7/v8/v9=0.15/0.15/0.30/0.25/0.15"
    if strategy == "ai":
        basis = f"{basis}；融合权重 combo/stable=0.7/0.3"
    return {
        "strategy": strategy.upper(),
        "score": round(score, 1),
        "grade": str(result.get("grade", "-")),
        "description": str(result.get("description", "")),
        "current_price": round(float(current_price), 2) if current_price > 0 else 0.0,
        "stop_loss": float(stop_loss),
        "take_profit": float(take_profit),
        "score_basis": basis,
        "risk_basis": risk_basis,
        "ok": bool(result.get("success", True)),
    }


def _render_single_stock_decision_panel(assistant, holdings: pd.DataFrame) -> None:
    st.markdown("---")
    st.subheader("单股评分决策")
    st.caption("按策略对单只股票独立评分，并输出买入/持有/卖出建议。")

    options = ["手动输入"]
    holding_map: Dict[str, Dict[str, Any]] = {}
    if holdings is not None and not holdings.empty:
        for _, row in holdings.iterrows():
            label = f"{row.get('stock_name', '')} ({row.get('ts_code', '')})"
            options.append(label)
            holding_map[label] = row.to_dict()

    c1, c2 = st.columns([2, 2])
    with c1:
        quick_pick = st.selectbox("快速选择", options, key="assistant_single_eval_pick")
    with c2:
        manual_code = st.text_input("股票代码", value="", placeholder="如 001331.SZ", key="assistant_single_eval_code")

    selected_row = holding_map.get(quick_pick, {})
    ts_code = str((selected_row.get("ts_code") if selected_row else manual_code) or manual_code).strip().upper()
    if ts_code and "." not in ts_code and ts_code.isdigit() and len(ts_code) == 6:
        ts_code = f"{ts_code}.SZ" if ts_code.startswith(("0", "3")) else f"{ts_code}.SH"

    col_a, col_b, col_c = st.columns([2, 1, 1])
    with col_a:
        strategies = st.multiselect(
            "评分策略",
            ["v4", "v5", "v6", "v7", "v8", "v9", "stable", "ai", "combo"],
            default=["v9", "v8", "v5", "combo"],
            key="assistant_single_eval_strategies",
        )
    with col_b:
        in_position = st.checkbox("当前持仓", value=bool(selected_row), key="assistant_single_eval_in_position")
    with col_c:
        fallback_pnl_pct = float(selected_row.get("profit_loss_pct", 0.0) or 0.0)
        profit_loss_pct = st.number_input(
            "当前盈亏(%)",
            min_value=-99.0,
            max_value=999.0,
            value=round(fallback_pnl_pct * 100, 2),
            step=0.5,
            key="assistant_single_eval_pnl",
        ) / 100.0

    st.caption("默认组合：v9(方向) + v8(风控) + v5(启动确认) + combo(共识决策)")

    with st.expander("评分参数与动作依据", expanded=False):
        min_score = float(assistant.get_config("min_score") or 60)
        risk_map = assistant.get_strategy_risk_param_map() if hasattr(assistant, "get_strategy_risk_param_map") else {}
        risk_lines = []
        for s in ("v4", "v5", "v6", "v7", "v8", "v9", "stable", "combo", "ai"):
            cfg = risk_map.get(s, {})
            if not cfg:
                continue
            risk_lines.append(
                f"- `{s.upper()}` 风险参数: 止损{float(cfg.get('stop_loss_pct', 0))*100:.1f}% / 止盈{float(cfg.get('take_profit_pct', 0))*100:.1f}% / 目标盈亏比{float(cfg.get('tp_sl_ratio', 0)):.2f}"
            )
        st.markdown(
            f"- 自动基础阈值：`{min_score:.1f}`（来自助手配置 `min_score`）\n"
            f"- 共识阈值：`v4/v5/v9={min_score:.1f}`，`v7/v8={min_score+2:.1f}`\n"
            "- 组合权重：`v4 0.15 / v5 0.15 / v7 0.30 / v8 0.25 / v9 0.15`\n"
            "- 动作规则：空仓 `>=75买入`、`60-74持有观察`、`<60不买`；持仓 `评分<55或回撤<=-8%卖出`、`评分强则持有`。\n"
            + ("\n".join(risk_lines) if risk_lines else "")
        )

    if st.button("执行单股评分", type="primary", use_container_width=True, key="assistant_single_eval_run"):
        if not ts_code:
            st.error("请先输入或选择股票代码。")
            return
        if not strategies:
            st.error("请至少选择一个评分策略。")
            return

        with st.spinner(f"正在评估 {ts_code} ..."):
            stock_data, stock_name, industry = _load_single_stock_context(assistant.db_path, ts_code, bars=220)
            if stock_data.empty or len(stock_data) < 60:
                st.error("历史数据不足或未找到该股票，请先更新数据库。")
                return

            index_data = _load_index_context(assistant.db_path, bars=220)
            rows: List[Dict[str, Any]] = []
            failures: List[str] = []
            for strategy in strategies:
                try:
                    item = _evaluate_single_stock_by_strategy(
                        assistant=assistant,
                        strategy=strategy,
                        stock_data=stock_data,
                        ts_code=ts_code,
                        industry=industry,
                        db_path=assistant.db_path,
                        index_data=index_data,
                    )
                    action, action_reason = _decision_from_score(item["score"], in_position, profit_loss_pct)
                    item["action"] = action
                    item["action_reason"] = action_reason
                    rows.append(item)
                except Exception as e:
                    failures.append(f"{strategy.upper()}: {e}")

            if not rows:
                st.error("评分失败，未产出有效结果。")
                if failures:
                    with st.expander("错误详情"):
                        st.code("\n".join(failures))
                return

            df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
            st.success(f"{stock_name}（{ts_code}）评估完成")
            st.dataframe(
                df[["strategy", "score", "grade", "action", "current_price", "stop_loss", "take_profit", "score_basis", "risk_basis", "action_reason", "description"]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "score": st.column_config.NumberColumn("评分", format="%.1f"),
                    "current_price": st.column_config.NumberColumn("现价", format="%.2f"),
                    "stop_loss": st.column_config.NumberColumn("止损价", format="%.2f"),
                    "take_profit": st.column_config.NumberColumn("止盈价", format="%.2f"),
                    "score_basis": st.column_config.TextColumn("分数依据"),
                    "risk_basis": st.column_config.TextColumn("风控依据"),
                },
            )

            avg_score = float(df["score"].mean())
            buy_count = int((df["action"] == "买入").sum())
            sell_count = int((df["action"] == "卖出").sum())
            hold_count = int((df["action"] == "持有").sum())
            if sell_count > buy_count and sell_count >= hold_count:
                final_action = "卖出"
            elif buy_count > sell_count and buy_count >= hold_count:
                final_action = "买入"
            else:
                final_action = "持有"

            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("综合评分", f"{avg_score:.1f}")
            with c2:
                st.metric("策略一致性", f"{max(buy_count, hold_count, sell_count)}/{len(df)}")
            with c3:
                st.metric("综合建议", final_action)

            if failures:
                with st.expander("部分策略执行失败（不影响已完成结果）", expanded=False):
                    st.code("\n".join(failures))


def render_daily_scan_tab(assistant, render_result_overview: Callable[..., None]) -> None:
    st.subheader("每日智能选股")

    st.info(
        """
         **选股说明**
        - 基于**共识策略**（v4/v5/v7/v8/v9）
        - 自动扫描全市场股票
        - 推荐Top高分标的（一致性筛选）
        - 仅供参考，需人工决策
        """
    )

    col1, col2 = st.columns([3, 1])

    with col1:
        _cfg_top_n = int(float(assistant.get_config("recommend_count") or 5))
        _cfg_top_n = max(3, min(10, _cfg_top_n))
        top_n = st.slider("推荐数量", 3, 10, _cfg_top_n, key="assistant_daily_scan_top_n")

    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("开始选股", type="primary", use_container_width=True):
            with st.spinner("正在扫描全市场...（可能需要2-3分钟）"):
                recommendations = assistant.daily_stock_scan(top_n=top_n)
                st.session_state["daily_recommendations"] = recommendations
                clear_assistant_ui_cache()
                if recommendations:
                    st.success(f"选股完成！找到{len(recommendations)}只标的数量")
                else:
                    st.warning("本次未选出股票，已记录诊断信息")
                st.rerun()

    if ("daily_recommendations" not in st.session_state) or (not st.session_state.get("daily_recommendations")):
        try:
            _today = datetime.now().strftime("%Y-%m-%d")
            _today_df = cached_assistant_daily_recs(assistant.assistant_db, _today)
            if _today_df is not None and not _today_df.empty:
                st.session_state["daily_recommendations"] = _today_df.to_dict(orient="records")
        except Exception:
            pass

    if "daily_recommendations" in st.session_state and st.session_state["daily_recommendations"]:
        st.markdown("---")
        st.subheader("今日推荐")

        recs = st.session_state["daily_recommendations"]
        recs_df = pd.DataFrame(recs)
        if not recs_df.empty:
            render_result_overview(recs_df, score_col="score", title="今日推荐概览")

        for i, rec in enumerate(recs, 1):
            with st.expander(
                f"#{i} {rec['stock_name']} ({rec['ts_code']}) - ⭐ {rec['score']:.1f}分",
                expanded=(i == 1),
            ):
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("评分", f"{rec['score']:.1f}分")
                with col2:
                    st.metric("价格", f"¥{rec['price']:.2f}")
                with col3:
                    st.metric("市值", f"{rec['market_cap'] / 100000000:.1f}亿")

                st.markdown(f"** 行业**: {rec['industry']}")
                st.markdown(f"** 筛选理由**: {rec['reason'][:150]}...")

                st.markdown("---")
                col1, col2, col3 = st.columns([2, 2, 1])

                with col1:
                    buy_price = st.number_input(
                        "买入价格",
                        value=float(rec["price"]),
                        key=f"price_{rec['ts_code']}",
                    )

                with col2:
                    quantity = st.number_input(
                        "买入数量",
                        value=100,
                        step=100,
                        key=f"qty_{rec['ts_code']}",
                    )

                with col3:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("记录买入", key=f"buy_{rec['ts_code']}"):
                        assistant.add_holding(
                            ts_code=rec["ts_code"],
                            buy_price=buy_price,
                            quantity=quantity,
                            score=rec["score"],
                        )
                        clear_assistant_ui_cache()
                        st.success(f"已记录买入 {rec['stock_name']}")
                        st.rerun()
    elif "daily_recommendations" in st.session_state:
        st.warning("本次未选出股票，请查看诊断信息")
        debug_info = getattr(assistant, "last_scan_debug", None)
        if debug_info:
            st.code(json.dumps(debug_info, ensure_ascii=False, indent=2))

    scan_debug_info = getattr(assistant, "last_scan_debug", None)
    if isinstance(scan_debug_info, dict) and scan_debug_info:
        with st.expander("最近一次扫描诊断", expanded=False):
            st.json(scan_debug_info)


def render_watchlist_tab(assistant) -> None:
    st.subheader("观察池")
    st.caption("手动录入每日策略推荐，持续跟踪并统计胜率/盈亏。")

    with st.expander("新增观察标的", expanded=True):
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            recommend_date = st.date_input("推荐日期", value=datetime.now().date(), key="watch_add_date")
        with c2:
            strategy = st.selectbox(
                "策略",
                ["v4", "v5", "v6", "v7", "v8", "v9", "stable", "ai", "combo", "manual"],
                index=9,
                key="watch_add_strategy",
            )
        with c3:
            ts_code = st.text_input("股票代码", value="", placeholder="如 001331 或 001331.SZ", key="watch_add_code")
        c4, c5 = st.columns([1, 3])
        with c4:
            entry_score = st.number_input("入池评分", min_value=0.0, max_value=100.0, value=0.0, step=0.1, key="watch_add_score")
        with c5:
            notes = st.text_input("备注", value="", placeholder="入池原因/观察点", key="watch_add_notes")

        cc1, cc2 = st.columns([1, 1])
        with cc1:
            if st.button("加入观察池", type="primary", use_container_width=True, key="watch_add_btn"):
                out = assistant.add_watchlist_entry(
                    recommend_date=str(recommend_date),
                    ts_code=ts_code,
                    strategy=strategy,
                    entry_score=float(entry_score) if entry_score > 0 else None,
                    notes=notes,
                )
                if out.get("ok"):
                    st.success(f"已加入观察池：{out.get('stock_name', ts_code)}")
                    st.rerun()
                else:
                    st.error(f"加入失败：{out.get('error', 'unknown')}")
        with cc2:
            if st.button("导入当日每日选股到观察池", use_container_width=True, key="watch_import_daily_btn"):
                out = assistant.import_daily_recommendations_to_watchlist(str(recommend_date), strategy="daily_scan")
                st.success(f"导入完成：{int(out.get('imported', 0))} 条")
                st.rerun()

    st.markdown("---")
    st.markdown("### 观察池筛选与统计")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 1])
    with f1:
        date_from = st.date_input("开始日期", value=datetime.now().date().replace(day=1), key="watch_filter_date_from")
    with f2:
        date_to = st.date_input("结束日期", value=datetime.now().date(), key="watch_filter_date_to")
    with f3:
        strategy_filter = st.selectbox(
            "策略筛选",
            ["全部", "v4", "v5", "v6", "v7", "v8", "v9", "stable", "ai", "combo", "manual", "daily_scan"],
            index=0,
            key="watch_filter_strategy",
        )
    with f4:
        status_filter = st.selectbox("状态", ["active", "removed", "全部"], index=0, key="watch_filter_status")

    perf = assistant.get_watchlist_performance(
        date_from=str(date_from),
        date_to=str(date_to),
        strategy=strategy_filter,
        status=status_filter,
    )
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.metric("观察标的", f"{int(perf.get('total', 0))}")
    with m2:
        st.metric("有效样本", f"{int(perf.get('valid', 0))}")
    with m3:
        st.metric("胜率", f"{float(perf.get('win_rate', 0.0)):.1f}%")
    with m4:
        st.metric("平均收益", f"{float(perf.get('avg_return_pct', 0.0)):.2f}%")
    with m5:
        st.metric("盈亏比", f"{float(perf.get('profit_loss_ratio', 0.0)):.2f}")

    watch_df = assistant.get_watchlist_entries(
        date_from=str(date_from),
        date_to=str(date_to),
        strategy=strategy_filter,
        status=status_filter,
    )
    if watch_df is None or watch_df.empty:
        st.info("当前筛选条件下暂无观察标的。")
        return

    show_df = watch_df.copy()
    show_df["entry_price"] = pd.to_numeric(show_df["entry_price"], errors="coerce")
    show_df["current_price"] = pd.to_numeric(show_df["current_price"], errors="coerce")
    show_df["return_pct"] = pd.to_numeric(show_df["return_pct"], errors="coerce")

    st.markdown("---")
    st.markdown("### 入池后多维统计（交易参考）")
    valid = show_df.dropna(subset=["return_pct"]).copy()
    if valid.empty:
        st.info("当前无可计算收益率样本，等价格更新后自动出现统计。")
    else:
        # 1) 区间命中统计（从入池到当前）
        r = valid["return_pct"]
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        with c1:
            st.metric(">= +3%", f"{int((r >= 3).sum())}只")
        with c2:
            st.metric(">= +5%", f"{int((r >= 5).sum())}只")
        with c3:
            st.metric(">= +8%", f"{int((r >= 8).sum())}只")
        with c4:
            st.metric("<= -3%", f"{int((r <= -3).sum())}只")
        with c5:
            st.metric("<= -5%", f"{int((r <= -5).sum())}只")
        with c6:
            st.metric("中位收益", f"{float(r.median()):.2f}%")

        # 2) 按策略统计
        by_strategy = (
            valid.groupby("strategy", as_index=False)
            .agg(
                样本数=("id", "count"),
                胜率=("return_pct", lambda s: float((s > 0).mean() * 100)),
                平均收益=("return_pct", "mean"),
                中位收益=("return_pct", "median"),
                平均观察天数=("days_in_watch", "mean"),
            )
            .sort_values(["样本数", "平均收益"], ascending=[False, False])
        )
        st.markdown("**按策略表现**")
        st.dataframe(
            by_strategy,
            use_container_width=True,
            hide_index=True,
            column_config={
                "胜率": st.column_config.NumberColumn(format="%.1f%%"),
                "平均收益": st.column_config.NumberColumn(format="%.2f%%"),
                "中位收益": st.column_config.NumberColumn(format="%.2f%%"),
                "平均观察天数": st.column_config.NumberColumn(format="%.1f"),
            },
        )

        # 3) 按入池日期统计（看哪天信号质量更高）
        by_day = (
            valid.groupby("recommend_date", as_index=False)
            .agg(
                样本数=("id", "count"),
                胜率=("return_pct", lambda s: float((s > 0).mean() * 100)),
                平均收益=("return_pct", "mean"),
            )
            .sort_values("recommend_date", ascending=False)
            .head(30)
        )
        st.markdown("**按入池日期表现（最近30天）**")
        st.dataframe(
            by_day,
            use_container_width=True,
            hide_index=True,
            column_config={
                "胜率": st.column_config.NumberColumn(format="%.1f%%"),
                "平均收益": st.column_config.NumberColumn(format="%.2f%%"),
            },
        )

        # 4) 按入池评分分层统计
        bins = [0, 60, 70, 80, 1000]
        labels = ["<60", "60-69", "70-79", "80+"]
        score_series = pd.to_numeric(valid.get("entry_score"), errors="coerce")
        if score_series.notna().any():
            valid["_score_bucket"] = pd.cut(score_series, bins=bins, labels=labels, right=False)
            by_score = (
                valid.dropna(subset=["_score_bucket"])
                .groupby("_score_bucket", as_index=False)
                .agg(
                    样本数=("id", "count"),
                    胜率=("return_pct", lambda s: float((s > 0).mean() * 100)),
                    平均收益=("return_pct", "mean"),
                )
                .sort_values("_score_bucket")
            )
            st.markdown("**按入池评分分层**")
            st.dataframe(
                by_score.rename(columns={"_score_bucket": "评分区间"}),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "胜率": st.column_config.NumberColumn(format="%.1f%%"),
                    "平均收益": st.column_config.NumberColumn(format="%.2f%%"),
                },
            )

        # 5) 按观察天数分层统计（短中期）
        day_bins = [0, 3, 7, 15, 31, 9999]
        day_labels = ["0-2天", "3-6天", "7-14天", "15-30天", "31天+"]
        valid["_day_bucket"] = pd.cut(pd.to_numeric(valid["days_in_watch"], errors="coerce"), bins=day_bins, labels=day_labels, right=False)
        by_days = (
            valid.dropna(subset=["_day_bucket"])
            .groupby("_day_bucket", as_index=False)
            .agg(
                样本数=("id", "count"),
                胜率=("return_pct", lambda s: float((s > 0).mean() * 100)),
                平均收益=("return_pct", "mean"),
            )
        )
        st.markdown("**按观察天数分层**")
        st.dataframe(
            by_days.rename(columns={"_day_bucket": "观察区间"}),
            use_container_width=True,
            hide_index=True,
            column_config={
                "胜率": st.column_config.NumberColumn(format="%.1f%%"),
                "平均收益": st.column_config.NumberColumn(format="%.2f%%"),
            },
        )

        # 6) 行业分布统计（样本较多时）
        if "industry" in valid.columns:
            by_ind = (
                valid[valid["industry"].astype(str).str.strip() != ""]
                .groupby("industry", as_index=False)
                .agg(
                    样本数=("id", "count"),
                    胜率=("return_pct", lambda s: float((s > 0).mean() * 100)),
                    平均收益=("return_pct", "mean"),
                )
                .sort_values(["样本数", "平均收益"], ascending=[False, False])
                .head(15)
            )
            if not by_ind.empty:
                st.markdown("**按行业表现（Top15样本）**")
                st.dataframe(
                    by_ind,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "胜率": st.column_config.NumberColumn(format="%.1f%%"),
                        "平均收益": st.column_config.NumberColumn(format="%.2f%%"),
                    },
                )

    st.dataframe(
        show_df[
            [
                "id",
                "recommend_date",
                "strategy",
                "ts_code",
                "stock_name",
                "entry_score",
                "entry_price",
                "current_price",
                "return_pct",
                "days_in_watch",
                "industry",
                "notes",
                "status",
            ]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "entry_score": st.column_config.NumberColumn("入池评分", format="%.1f"),
            "entry_price": st.column_config.NumberColumn("入池价", format="%.2f"),
            "current_price": st.column_config.NumberColumn("当前价", format="%.2f"),
            "return_pct": st.column_config.NumberColumn("收益率(%)", format="%.2f"),
            "days_in_watch": st.column_config.NumberColumn("观察天数", format="%d"),
        },
    )

    active_df = show_df[show_df["status"] == "active"]
    if not active_df.empty:
        options = {
            f"{int(r.id)} | {r.stock_name}({r.ts_code}) | {r.strategy} | {r.recommend_date}": int(r.id)
            for r in active_df.itertuples(index=False)
        }
        if options:
            c_del_1, c_del_2 = st.columns([3, 1])
            with c_del_1:
                selected_label = st.selectbox("选择要删除的观察标的", list(options.keys()), key="watch_remove_select")
            with c_del_2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("删除选中", type="secondary", use_container_width=True, key="watch_remove_btn"):
                    rid = options.get(selected_label)
                    out = assistant.remove_watchlist_entry(int(rid))
                    if out.get("ok"):
                        st.success("已删除（标记为 removed）")
                        st.rerun()
                    else:
                        st.error("删除失败")


def render_holdings_tab(assistant) -> None:
    st.subheader("当前持仓管理")

    col1, col2, col3 = st.columns([3, 1, 1])

    with col2:
        if st.button("更新持仓", use_container_width=True):
            with st.spinner("更新中..."):
                assistant.update_holdings()
            clear_assistant_ui_cache()
            st.success("更新完成")
            st.rerun()
    with col3:
        if st.button("止盈止损检测", use_container_width=True):
            with st.spinner("检测中..."):
                st.session_state["assistant_stop_alerts"] = assistant.check_stop_conditions()

    holdings = cached_assistant_holdings(assistant.assistant_db)

    if holdings.empty:
        st.info("当前无持仓")
        _render_single_stock_decision_panel(assistant, holdings)
        return

    hold_summary = summarize_holdings(holdings)
    total_cost = hold_summary["total_cost"]
    total_value = hold_summary["total_value"]
    total_profit = hold_summary["total_profit"]
    total_profit_pct = hold_summary["total_profit_pct"]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("持仓数量", f"{len(holdings)}只")
    with col2:
        st.metric("总成本", f"¥{total_cost:,.2f}")
    with col3:
        st.metric("总市值", f"¥{total_value:,.2f}")
    with col4:
        st.metric("总盈亏", f"¥{total_profit:,.2f}", delta=f"{total_profit_pct * 100:.2f}%")

    st.markdown("---")
    alerts = st.session_state.get("assistant_stop_alerts", [])
    if alerts:
        st.warning("**止盈止损提醒**")
        for alert in alerts:
            if alert["type"] == "take_profit":
                st.success(alert["message"])
            else:
                st.error(alert["message"])

    _render_single_stock_decision_panel(assistant, holdings)

    for _, holding in holdings.iterrows():
        profit_loss = holding.get("profit_loss", 0) or 0
        profit_loss_pct = holding.get("profit_loss_pct", 0) or 0
        buy_price = holding.get("buy_price", 0) or 0
        current_price = holding.get("current_price", 0) or buy_price

        with st.expander(f" {holding['stock_name']} ({holding['ts_code']}) - {profit_loss_pct * 100:.2f}%"):
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("买入价", f"¥{buy_price:.2f}")
            with col2:
                st.metric("当前价", f"¥{current_price:.2f}")
            with col3:
                st.metric("数量", f"{holding['quantity']}股")
            with col4:
                st.metric("盈亏", f"¥{profit_loss:.2f}", delta=f"{profit_loss_pct * 100:.2f}%")

            st.markdown(f"**买入日期**: {holding.get('buy_date', 'N/A')}")
            cost_total = holding.get("cost_total", 0) or 0
            current_value = holding.get("current_value", 0) or 0
            st.markdown(f"**成本**: ¥{cost_total:.2f}")
            st.markdown(f"**市值**: ¥{current_value:.2f}")

            st.markdown("---")
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                sell_price = st.number_input(
                    "卖出价格",
                    value=float(holding["current_price"]),
                    key=f"sell_price_{holding['id']}",
                )

            with col2:
                sell_reason = st.selectbox(
                    "卖出原因",
                    ["止盈", "止损", "手动卖出", "其他"],
                    key=f"sell_reason_{holding['id']}",
                )

            with col3:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("卖出", key=f"sell_{holding['id']}"):
                    assistant.sell_holding(
                        ts_code=holding["ts_code"],
                        sell_price=sell_price,
                        reason=sell_reason,
                    )
                    clear_assistant_ui_cache()
                    st.success(f"已记录卖出 {holding['stock_name']}")
                    st.rerun()


def render_single_stock_eval_tab(assistant) -> None:
    st.subheader("单股评分决策")
    st.caption("对单只股票执行多策略评分，并给出买入/持有/卖出建议。")
    holdings = cached_assistant_holdings(assistant.assistant_db)
    _render_single_stock_decision_panel(assistant, holdings)


def render_trades_tab(assistant) -> None:
    st.subheader("交易历史记录")

    trades = cached_assistant_trades(assistant.assistant_db, limit=50)

    if trades.empty:
        st.info("暂无交易记录")
        return

    period_summary = summarize_trade_periods(trades)
    daily_profit = period_summary["daily"]["profit"]
    daily_pct = period_summary["daily"]["pct"]
    daily_amount = period_summary["daily"]["amount"]
    weekly_profit = period_summary["weekly"]["profit"]
    weekly_pct = period_summary["weekly"]["pct"]
    weekly_amount = period_summary["weekly"]["amount"]
    monthly_profit = period_summary["monthly"]["profit"]
    monthly_pct = period_summary["monthly"]["pct"]
    monthly_amount = period_summary["monthly"]["amount"]

    st.markdown("###  已实现盈亏统计（卖出记录）")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("今日盈亏", f"¥{daily_profit:,.2f}", delta=f"{daily_pct * 100:.2f}%")
        st.caption(f"成交额：¥{daily_amount:,.2f}")
    with col2:
        st.metric("本周盈亏", f"¥{weekly_profit:,.2f}", delta=f"{weekly_pct * 100:.2f}%")
        st.caption(f"成交额：¥{weekly_amount:,.2f}")
    with col3:
        st.metric("本月盈亏", f"¥{monthly_profit:,.2f}", delta=f"{monthly_pct * 100:.2f}%")
        st.caption(f"成交额：¥{monthly_amount:,.2f}")

    st.markdown("---")

    buy_trades = trades[trades["action"] == "buy"]
    sell_trades = trades[trades["action"] == "sell"]
    profit_trades = sell_trades[sell_trades["profit_loss"] > 0]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("总交易", f"{len(trades)}次")
    with col2:
        st.metric("买入", f"{len(buy_trades)}次")
    with col3:
        st.metric("卖出", f"{len(sell_trades)}次")
    with col4:
        win_rate = len(profit_trades) / len(sell_trades) if len(sell_trades) > 0 else 0
        st.metric("胜率", f"{win_rate * 100:.1f}%")

    st.markdown("---")
    for _, trade in trades.iterrows():
        col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 2])
        with col1:
            st.markdown(f" **{trade['stock_name']}** ({trade['ts_code']})")
        with col2:
            st.markdown(f"{trade['trade_date']}")
        with col3:
            st.markdown(f"¥{trade['price']:.2f}")
        with col4:
            st.markdown(f"{trade['quantity']}股")
        with col5:
            if trade["action"] == "sell" and trade["profit_loss"]:
                profit_text = f" ¥{trade['profit_loss']:.2f} ({trade['profit_loss_pct'] * 100:.2f}%)"
                st.markdown(profit_text)
            else:
                st.markdown(f"¥{trade['amount']:.2f}")
