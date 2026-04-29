from __future__ import annotations

from datetime import datetime
import glob
import json
import os
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def stock_pool_paths(stock_pool_dir: str, pool_id: str) -> Tuple[str, str]:
    os.makedirs(stock_pool_dir, exist_ok=True)
    return (
        os.path.join(stock_pool_dir, f"{pool_id}.csv"),
        os.path.join(stock_pool_dir, f"{pool_id}.meta.json"),
    )


def save_stock_pool_snapshot(
    *,
    stock_pool_dir: str,
    now_text: Callable[[], str],
    strategy: str,
    params: Dict[str, Any],
    score_col: str,
    df: pd.DataFrame,
    note: str = "",
) -> Tuple[bool, str]:
    try:
        if df is None or df.empty:
            return False, "当前结果为空，无法保存到股票池"
        pool_id = f"{(strategy or 'unknown').lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        csv_path, meta_path = stock_pool_paths(stock_pool_dir, pool_id)
        df.to_csv(csv_path, index=False)
        score_vals = pd.to_numeric(df.get(score_col, pd.Series(dtype=float)), errors="coerce").dropna()
        meta = {
            "pool_id": pool_id,
            "created_at": now_text(),
            "strategy": (strategy or "unknown").lower(),
            "score_col": score_col,
            "params": params or {},
            "row_count": int(len(df)),
            "avg_score": float(score_vals.mean()) if len(score_vals) > 0 else None,
            "max_score": float(score_vals.max()) if len(score_vals) > 0 else None,
            "min_score": float(score_vals.min()) if len(score_vals) > 0 else None,
            "note": (note or "").strip(),
            "csv_path": csv_path,
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        return True, pool_id
    except Exception as exc:
        return False, f"保存失败: {exc}"


def list_stock_pool_meta(stock_pool_dir: str) -> List[Dict[str, Any]]:
    try:
        os.makedirs(stock_pool_dir, exist_ok=True)
        out: List[Dict[str, Any]] = []
        for meta_path in sorted(glob.glob(os.path.join(stock_pool_dir, "*.meta.json")), reverse=True):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f) or {}
                meta["_meta_path"] = meta_path
                out.append(meta)
            except Exception:
                continue
        out.sort(key=lambda x: str(x.get("created_at", "")), reverse=True)
        return out
    except Exception:
        return []


def load_stock_pool_snapshot(stock_pool_dir: str, pool_id: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    try:
        csv_path, meta_path = stock_pool_paths(stock_pool_dir, pool_id)
        if not (os.path.exists(csv_path) and os.path.exists(meta_path)):
            return None, {}
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f) or {}
        return pd.read_csv(csv_path), meta
    except Exception:
        return None, {}


def delete_stock_pool_snapshot(stock_pool_dir: str, pool_id: str) -> Tuple[bool, str]:
    try:
        csv_path, meta_path = stock_pool_paths(stock_pool_dir, pool_id)
        for p in (csv_path, meta_path):
            if os.path.exists(p):
                os.remove(p)
        return True, "已删除"
    except Exception as exc:
        return False, f"删除失败: {exc}"


def parse_pool_base_date(meta: Dict[str, Any]) -> str:
    created_at = str(meta.get("created_at", "") or "").strip()
    if created_at:
        try:
            return datetime.strptime(created_at[:19], "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
        except Exception:
            pass
    finished_at = str(meta.get("finished_at", "") or "").strip()
    if finished_at:
        try:
            return datetime.strptime(finished_at[:19], "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
        except Exception:
            pass
    return datetime.now().strftime("%Y%m%d")


def summarize_stock_pool_signal_performance(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return pd.DataFrame()
    out_rows: List[Dict[str, Any]] = []
    for (strategy, horizon), grp in detail_df.groupby(["strategy", "horizon_days"]):
        vals = pd.to_numeric(grp["ret_pct"], errors="coerce").dropna()
        if vals.empty:
            continue
        ex_vals = pd.to_numeric(grp["excess_ret_pct"], errors="coerce").dropna()
        dd_vals = pd.to_numeric(grp["max_dd_pct"], errors="coerce").dropna()
        settled = pd.to_numeric(grp.get("is_settled", pd.Series([], dtype=float)), errors="coerce")
        settled_ratio = float(settled.fillna(0).mean() * 100.0) if len(settled) > 0 else np.nan
        out_rows.append(
            {
                "strategy": str(strategy),
                "horizon_days": int(horizon),
                "samples": int(len(vals)),
                "settled_ratio_pct": settled_ratio,
                "win_rate_pct": float((vals > 0).mean() * 100.0),
                "avg_ret_pct": float(vals.mean()),
                "median_ret_pct": float(vals.median()),
                "avg_excess_ret_pct": float(ex_vals.mean()) if len(ex_vals) > 0 else np.nan,
                "p25_ret_pct": float(vals.quantile(0.25)),
                "p75_ret_pct": float(vals.quantile(0.75)),
                "avg_max_dd_pct": float(dd_vals.mean()) if len(dd_vals) > 0 else np.nan,
                "tail_loss_p10_pct": float(vals.quantile(0.10)),
            }
        )
    return pd.DataFrame(out_rows)


def load_latest_stock_pool_snapshot(app_root: str, max_files: int = 300) -> pd.DataFrame:
    pool_dir = os.path.join(app_root, "logs", "openclaw", "stock_pool")
    if not os.path.isdir(pool_dir):
        return pd.DataFrame()
    meta_files = sorted(glob.glob(os.path.join(pool_dir, "*.meta.json")), reverse=True)[: max(1, int(max_files))]
    latest: Dict[str, Dict[str, Any]] = {}
    for mf in meta_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                meta = json.load(f) or {}
            strategy = str(meta.get("strategy", "")).strip().lower()
            if strategy not in {"v9", "v8", "v5", "combo"} or strategy in latest:
                continue
            params = meta.get("params") or {}
            latest[strategy] = {
                "策略": strategy,
                "最近池子时间": str(meta.get("created_at", "") or ""),
                "池子行数": int(meta.get("row_count", 0) or 0),
                "持有天数": int(float(params.get("holding_days", 0) or 0)),
                "meta": mf,
            }
        except Exception:
            continue
    if not latest:
        return pd.DataFrame()
    out = pd.DataFrame(list(latest.values()))
    return out.sort_values("策略") if not out.empty else out


def extract_numeric_price(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace("元", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.extract(r"([-+]?\d+(?:\.\d+)?)", expand=False)
        .astype(float)
    )


def pick_entry_price(df: pd.DataFrame) -> pd.Series:
    for col in ["入池价格", "建仓价", "买入价", "最新价格", "当前价", "现价", "价格", "close_price", "close"]:
        if col in df.columns:
            s = pd.to_numeric(extract_numeric_price(df[col]), errors="coerce")
            if s.notna().any():
                return s
    return pd.Series([np.nan] * len(df), index=df.index, dtype=float)


def get_entry_prices_by_base_date(
    *,
    ts_codes: List[str],
    base_date: str,
    connect_permanent_db: Callable[[], Any],
    safe_daily_table_name: Callable[[str, str], str],
    iter_sqlite_in_chunks: Callable[[List[str], int], List[List[str]]],
    canonical_ts_code: Callable[[Any], str],
    expand_ts_code_keys: Callable[[Any], List[str]],
    safe_float: Callable[[Any, float], float],
) -> Dict[str, float]:
    if not ts_codes or not base_date:
        return {}
    query_codes: List[str] = []
    for code in ts_codes:
        canonical = canonical_ts_code(code)
        if canonical and canonical not in query_codes:
            query_codes.append(canonical)
    if not query_codes:
        return {}
    conn = connect_permanent_db()
    try:
        from data.dao import detect_daily_table  # type: ignore
        table = safe_daily_table_name(detect_daily_table(conn), "daily_trading_data")
    except Exception:
        table = "daily_trading_data"
    dfs: List[pd.DataFrame] = []
    try:
        for chunk in iter_sqlite_in_chunks(query_codes, 900):
            placeholders = ",".join(["?"] * len(chunk))
            query = f"""
                WITH ranked AS (
                    SELECT ts_code, trade_date, close_price,
                           ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                    FROM {table}
                    WHERE ts_code IN ({placeholders}) AND trade_date <= ?
                )
                SELECT ts_code, close_price
                FROM ranked
                WHERE rn = 1
            """
            params: List[Any] = list(chunk) + [str(base_date)]
            df_chunk = pd.read_sql_query(query, conn, params=params)
            if df_chunk is not None and not df_chunk.empty:
                dfs.append(df_chunk)
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except Exception:
        conn.close()
        return {}
    conn.close()
    by_code: Dict[str, float] = {}
    if df is not None and not df.empty:
        for _, r in df.iterrows():
            c = str(r.get("ts_code", "") or "")
            p = safe_float(r.get("close_price"), np.nan)
            if c and not np.isnan(p):
                by_code[c] = p
    out: Dict[str, float] = {}
    for code in ts_codes:
        keys = expand_ts_code_keys(code)
        price = np.nan
        for k in keys:
            v = by_code.get(k)
            if v is not None and not np.isnan(safe_float(v, np.nan)):
                price = safe_float(v, np.nan)
                break
        if not np.isnan(price):
            for k in keys:
                out[k] = price
    return out


def compute_pool_performance(
    *,
    df: pd.DataFrame,
    base_date: str = "",
    pick_entry_price: Callable[[pd.DataFrame], pd.Series],
    get_entry_prices_by_base_date: Callable[[List[str], str], Dict[str, float]],
    safe_float: Callable[[Any, float], float],
    get_latest_prices: Callable[[List[str]], Dict[str, Dict[str, Any]]],
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    work = df.copy()
    code_col = "股票代码" if "股票代码" in work.columns else ("ts_code" if "ts_code" in work.columns else "")
    name_col = "股票名称" if "股票名称" in work.columns else ("name" if "name" in work.columns else "")
    if not code_col:
        return pd.DataFrame(), {}

    work["entry_price"] = pick_entry_price(work)
    if base_date:
        code_list = work[code_col].dropna().astype(str).tolist()
        entry_map = get_entry_prices_by_base_date(code_list, base_date)
        if entry_map:
            db_entry = work[code_col].astype(str).map(lambda c: safe_float(entry_map.get(c), np.nan))
            work["entry_price"] = db_entry.where(db_entry.notna(), work["entry_price"])
    codes = work[code_col].dropna().astype(str).tolist()
    latest_map = get_latest_prices(codes)
    latest_info = work[code_col].astype(str).map(lambda c: latest_map.get(c) or {})
    work["latest_price"] = latest_info.map(lambda item: safe_float(item.get("price"), np.nan))
    work["latest_trade_date"] = latest_info.map(lambda item: str(item.get("trade_date", "") or ""))
    work["ret_pct"] = (work["latest_price"] / work["entry_price"] - 1.0) * 100.0
    perf = work[[code_col] + ([name_col] if name_col else []) + ["entry_price", "latest_price", "latest_trade_date", "ret_pct"]].copy()
    perf = perf.dropna(subset=["ret_pct"])
    if perf.empty:
        return perf, {}

    rets = perf["ret_pct"].astype(float)
    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    win_rate = float((rets > 0).mean() * 100.0)
    avg_ret = float(rets.mean())
    avg_win = float(wins.mean()) if len(wins) > 0 else 0.0
    avg_loss = float(losses.mean()) if len(losses) > 0 else 0.0
    profit_factor = float(wins.sum() / abs(losses.sum())) if len(losses) > 0 and abs(losses.sum()) > 1e-9 else float("inf")
    expectancy = float((win_rate / 100.0) * avg_win + (1.0 - win_rate / 100.0) * avg_loss)
    latest_trade_dates = perf["latest_trade_date"].astype(str).str.strip()
    latest_trade_dates = latest_trade_dates[latest_trade_dates != ""]
    latest_trade_date_max = str(latest_trade_dates.max()) if len(latest_trade_dates) > 0 else ""
    stale_count = int((latest_trade_dates <= str(base_date)).sum()) if base_date and len(latest_trade_dates) > 0 else 0
    stale_ratio = float(stale_count / max(len(latest_trade_dates), 1) * 100.0) if len(latest_trade_dates) > 0 else 0.0

    summary = {
        "count": float(len(rets)),
        "win_count": float(len(wins)),
        "loss_count": float(len(losses)),
        "win_rate": win_rate,
        "avg_ret": avg_ret,
        "median_ret": float(rets.median()),
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "max_gain": float(rets.max()),
        "max_drawdown": float(rets.min()),
        "latest_trade_date": latest_trade_date_max,
        "stale_count": float(stale_count),
        "stale_ratio": stale_ratio,
    }
    return perf, summary


def compute_forward_return_buckets(
    *,
    ts_codes: List[str],
    base_date: str,
    horizons: List[int],
    connect_permanent_db: Callable[[], Any],
    safe_daily_table_name: Callable[[str, str], str],
    canonical_ts_code: Callable[[Any], str],
    iter_sqlite_in_chunks: Callable[[List[str], int], List[List[str]]],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not ts_codes:
        return pd.DataFrame(), pd.DataFrame()
    query_codes: List[str] = []
    for code in ts_codes:
        canonical = canonical_ts_code(code)
        if canonical and canonical not in query_codes:
            query_codes.append(canonical)
    if not query_codes:
        return pd.DataFrame(), pd.DataFrame()
    hs = sorted([int(h) for h in horizons if int(h) >= 1])
    if not hs:
        return pd.DataFrame(), pd.DataFrame()
    max_h = max(hs)

    conn = connect_permanent_db()
    try:
        from data.dao import detect_daily_table  # type: ignore
        table = safe_daily_table_name(detect_daily_table(conn), "daily_trading_data")
    except Exception:
        table = "daily_trading_data"

    case_cols = [
        "MAX(CASE WHEN rn=1 THEN close_price END) AS p0",
        "MAX(CASE WHEN rn=1 THEN trade_date END) AS d0",
    ]
    for h in hs:
        case_cols.append(f"MAX(CASE WHEN rn={h+1} THEN close_price END) AS p{h}")
        case_cols.append(f"MAX(CASE WHEN rn={h+1} THEN trade_date END) AS d{h}")
    case_sql = ",\n                ".join(case_cols)

    raw_parts: List[pd.DataFrame] = []
    for chunk in iter_sqlite_in_chunks(query_codes, 900):
        placeholders = ",".join(["?"] * len(chunk))
        query = f"""
            WITH ranked AS (
                SELECT ts_code, trade_date, close_price,
                       ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date ASC) AS rn
                FROM {table}
                WHERE ts_code IN ({placeholders}) AND trade_date >= ?
            )
            SELECT ts_code,
                   {case_sql}
            FROM ranked
            WHERE rn <= ?
            GROUP BY ts_code
        """
        params: List[Any] = list(chunk) + [str(base_date), int(max_h + 1)]
        df_chunk = pd.read_sql_query(query, conn, params=params)
        if df_chunk is not None and not df_chunk.empty:
            raw_parts.append(df_chunk)
    conn.close()
    raw = pd.concat(raw_parts, ignore_index=True) if raw_parts else pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    details = raw.copy()
    details["p0"] = pd.to_numeric(details["p0"], errors="coerce")
    for h in hs:
        pcol = f"p{h}"
        details[pcol] = pd.to_numeric(details[pcol], errors="coerce")
        details[f"ret_t{h}_pct"] = (details[pcol] / details["p0"] - 1.0) * 100.0

    summary_rows: List[Dict[str, Any]] = []
    total_codes = max(1, len(details))
    for h in hs:
        rcol = f"ret_t{h}_pct"
        vals = pd.to_numeric(details[rcol], errors="coerce").dropna()
        if len(vals) == 0:
            summary_rows.append({"周期": f"T+{h}", "覆盖数": 0, "覆盖率%": 0.0, "胜率%": 0.0, "平均收益%": np.nan, "中位收益%": np.nan, "P10收益%": np.nan, "P90收益%": np.nan})
            continue
        summary_rows.append(
            {
                "周期": f"T+{h}",
                "覆盖数": int(len(vals)),
                "覆盖率%": float(len(vals) / total_codes * 100.0),
                "胜率%": float((vals > 0).mean() * 100.0),
                "平均收益%": float(vals.mean()),
                "中位收益%": float(vals.median()),
                "P10收益%": float(vals.quantile(0.10)),
                "P90收益%": float(vals.quantile(0.90)),
            }
        )
    return pd.DataFrame(summary_rows), details


def load_stock_pool_performance_detail(
    *,
    app_root: str,
    max_files: int,
    safe_float: Callable[[Any, float], float],
    connect_permanent_db: Callable[[], Any],
) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    benchmark_code = "000001.SH"
    max_signals = int(os.getenv("OPENCLAW_STOCK_POOL_PERF_MAX_SIGNALS", "40000"))
    pool_dirs = [
        os.path.join(app_root, "logs", "openclaw", "stock_pool"),
        os.path.join(os.getcwd(), "logs", "openclaw", "stock_pool"),
    ]
    pool_dir = ""
    for d in pool_dirs:
        if os.path.isdir(d):
            pool_dir = d
            break
    if not pool_dir:
        return pd.DataFrame(), pd.DataFrame(), ""

    meta_files = sorted(glob.glob(os.path.join(pool_dir, "*.meta.json")), reverse=True)[: max(1, int(max_files))]
    signal_rows: List[Dict[str, Any]] = []
    source_meta = ""
    for mf in meta_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                meta = json.load(f) or {}
            strategy = str(meta.get("strategy", "")).strip().lower()
            if strategy not in {"v9", "v8", "v5", "combo"}:
                continue
            created_at = str(meta.get("created_at", "") or "")
            signal_date = created_at[:10].replace("-", "") if created_at else ""
            if len(signal_date) != 8:
                continue
            params = meta.get("params") or {}
            holding_days = int(float(params.get("holding_days", 10) or 10))
            csv_path = str(meta.get("csv_path", "")).strip() or mf.replace(".meta.json", ".csv")
            if not os.path.isabs(csv_path):
                csv_path = os.path.join(app_root, csv_path)
            if not os.path.exists(csv_path):
                continue
            df_pool = pd.read_csv(csv_path)
            if df_pool is None or df_pool.empty:
                continue
            code_col = "股票代码" if "股票代码" in df_pool.columns else ("ts_code" if "ts_code" in df_pool.columns else "")
            if not code_col:
                continue
            name_col = "股票名称" if "股票名称" in df_pool.columns else ("name" if "name" in df_pool.columns else "")
            industry_col = next((c for c in ["所属行业", "行业", "industry", "行业板块"] if c in df_pool.columns), "")
            market_cap_col = next((c for c in ["总市值(亿)", "市值(亿)", "流通市值(亿)", "总市值", "流通市值", "market_cap", "cap"] if c in df_pool.columns), "")
            score_col_meta = str(meta.get("score_col", "") or "").strip()
            score_col = score_col_meta if score_col_meta in df_pool.columns else next((c for c in ["综合评分", "共识评分", "score"] if c in df_pool.columns), "")
            rank_col = next((c for c in ["排名", "rank", "序号"] if c in df_pool.columns), "")
            for _, row in df_pool.iterrows():
                ts_code = str(row.get(code_col, "") or "").strip()
                if not ts_code or ts_code.lower() in {"nan", "none", ""}:
                    continue
                signal_rows.append(
                    {
                        "strategy": strategy,
                        "ts_code": ts_code,
                        "stock_name": str(row.get(name_col, "")).strip() if name_col else "",
                        "industry": str(row.get(industry_col, "")).strip() if industry_col else "",
                        "score": safe_float(row.get(score_col), np.nan) if score_col else np.nan,
                        "market_cap_yi": safe_float(row.get(market_cap_col), np.nan) if market_cap_col else np.nan,
                        "rank": int(safe_float(row.get(rank_col), 0)) if rank_col else 0,
                        "signal_date": signal_date,
                        "horizon_days": max(1, holding_days),
                        "meta_file": mf,
                    }
                )
                if len(signal_rows) >= max_signals:
                    break
            if len(signal_rows) >= max_signals:
                if not source_meta:
                    source_meta = mf
                break
            if not source_meta:
                source_meta = mf
        except Exception:
            continue

    if not signal_rows:
        return pd.DataFrame(), pd.DataFrame(), ""
    signals_df = pd.DataFrame(signal_rows).drop_duplicates(subset=["strategy", "ts_code", "signal_date", "horizon_days"])
    if signals_df.empty:
        return pd.DataFrame(), pd.DataFrame(), ""
    codes = sorted(signals_df["ts_code"].dropna().astype(str).unique().tolist())
    if not codes:
        return pd.DataFrame(), pd.DataFrame(), source_meta
    min_signal_date = str(signals_df["signal_date"].min())

    try:
        from data.dao import DataAccessError, detect_daily_table  # type: ignore
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), source_meta

    conn = connect_permanent_db()
    try:
        try:
            daily_table = detect_daily_table(conn)
        except DataAccessError:
            return pd.DataFrame(), pd.DataFrame(), source_meta
        placeholders = ",".join(["?"] * len(codes))
        query = f"""
            SELECT ts_code, trade_date, close_price
            FROM {daily_table}
            WHERE trade_date >= ?
              AND (ts_code IN ({placeholders}) OR ts_code = ?)
            ORDER BY ts_code, trade_date
        """
        params: List[Any] = [min_signal_date] + codes + [benchmark_code]
        px = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    if px is None or px.empty:
        return pd.DataFrame(), pd.DataFrame(), source_meta
    px["trade_date"] = px["trade_date"].astype(str)
    px["close_price"] = pd.to_numeric(px["close_price"], errors="coerce")
    px = px.dropna(subset=["close_price"])
    if px.empty:
        return pd.DataFrame(), pd.DataFrame(), source_meta

    by_code: Dict[str, pd.DataFrame] = {}
    for ts_code, grp in px.groupby("ts_code"):
        by_code[str(ts_code)] = grp.sort_values("trade_date").reset_index(drop=True)

    idx_df = by_code.get(benchmark_code)
    perf_rows: List[Dict[str, Any]] = []
    for r in signals_df.to_dict("records"):
        ts_code = str(r.get("ts_code", ""))
        signal_date = str(r.get("signal_date", ""))
        horizon = int(r.get("horizon_days", 10))
        g = by_code.get(ts_code)
        if g is None or g.empty:
            continue
        match = g.index[g["trade_date"] >= signal_date].tolist()
        if not match:
            continue
        i0 = int(match[0])
        i1 = i0 + horizon
        is_settled = True
        if i1 >= len(g):
            i1 = len(g) - 1
            is_settled = False
        actual_horizon = int(max(0, i1 - i0))
        if actual_horizon < 1:
            continue
        p0 = float(g.at[i0, "close_price"])
        p1 = float(g.at[i1, "close_price"])
        if p0 <= 0:
            continue
        ret_pct = (p1 / p0 - 1.0) * 100.0
        path = g.iloc[i0 : i1 + 1].copy()
        path_ret = (pd.to_numeric(path["close_price"], errors="coerce") / p0 - 1.0) * 100.0
        max_dd_pct = float(path_ret.min()) if len(path_ret) > 0 else np.nan
        max_up_pct = float(path_ret.max()) if len(path_ret) > 0 else np.nan
        excess_ret = None
        bench_ret = None
        if idx_df is not None and not idx_df.empty:
            idx_match = idx_df.index[idx_df["trade_date"] >= signal_date].tolist()
            if idx_match:
                j0 = int(idx_match[0])
                j1 = j0 + horizon
                if j1 < len(idx_df):
                    b0 = float(idx_df.at[j0, "close_price"])
                    b1 = float(idx_df.at[j1, "close_price"])
                    if b0 > 0:
                        bench_ret = (b1 / b0 - 1.0) * 100.0
                        excess_ret = ret_pct - bench_ret
        perf_rows.append(
            {
                "strategy": str(r.get("strategy", "")),
                "ts_code": ts_code,
                "stock_name": str(r.get("stock_name", "")),
                "industry": str(r.get("industry", "")),
                "signal_date": signal_date,
                "entry_trade_date": str(g.at[i0, "trade_date"]),
                "exit_trade_date": str(g.at[i1, "trade_date"]),
                "horizon_days": horizon,
                "actual_horizon_days": actual_horizon,
                "is_settled": bool(is_settled),
                "score": safe_float(r.get("score"), np.nan),
                "market_cap_yi": safe_float(r.get("market_cap_yi"), np.nan),
                "rank": int(r.get("rank", 0) or 0),
                "entry_price": p0,
                "exit_price": p1,
                "ret_pct": ret_pct,
                "bench_ret_pct": bench_ret,
                "excess_ret_pct": excess_ret,
                "max_dd_pct": max_dd_pct,
                "max_up_pct": max_up_pct,
                "meta_file": str(r.get("meta_file", "")),
            }
        )
    detail_df = pd.DataFrame(perf_rows)
    if detail_df.empty:
        return pd.DataFrame(), pd.DataFrame(), source_meta
    summary_df = summarize_stock_pool_signal_performance(detail_df)
    return summary_df, detail_df, source_meta


def load_stock_pool_performance(
    *,
    app_root: str,
    max_files: int,
    safe_float: Callable[[Any, float], float],
    connect_permanent_db: Callable[[], Any],
) -> Tuple[pd.DataFrame, str]:
    summary_df, _, source_meta = load_stock_pool_performance_detail(
        app_root=app_root,
        max_files=max_files,
        safe_float=safe_float,
        connect_permanent_db=connect_permanent_db,
    )
    return summary_df, source_meta
