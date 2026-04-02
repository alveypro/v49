from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import os
import sqlite3

import pandas as pd


def get_db_last_trade_date(db_path: str) -> str:
    try:
        from data.history import get_db_last_trade_date as _get_db_last_trade_date_v2  # type: ignore

        return _get_db_last_trade_date_v2(db_path)
    except Exception:
        pass
    try:
        from data.dao import latest_trade_date as _latest_trade_date_v2  # type: ignore

        conn = sqlite3.connect(db_path)
        max_date = _latest_trade_date_v2(conn)
        conn.close()
        if max_date:
            return str(max_date)
    except Exception:
        pass
    return ""


def get_index_daily_from_db(db_path: str, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        from data.history import get_index_daily_from_db as _get_index_daily_from_db_v2  # type: ignore

        return _get_index_daily_from_db_v2(
            db_path=db_path,
            start_date=start_date,
            end_date=end_date,
            index_code="000001.SH",
        )
    except Exception:
        return pd.DataFrame()


def offline_apply_limit(stocks_df: pd.DataFrame, limit: int) -> pd.DataFrame:
    if limit and limit > 0 and len(stocks_df) > limit:
        return stocks_df.head(limit)
    return stocks_df


def load_candidate_stocks(
    conn: sqlite3.Connection,
    *,
    scan_all: bool = True,
    cap_min_yi: float = 0.0,
    cap_max_yi: float = 0.0,
    require_industry: bool = False,
    distinct: bool = True,
    random_order: bool = False,
) -> pd.DataFrame:
    select_kw = "SELECT DISTINCT" if distinct else "SELECT"
    where_parts: List[str] = []
    params: List[Any] = []
    use_cap = not (scan_all and cap_min_yi == 0 and cap_max_yi == 0)
    if require_industry:
        where_parts.append("sb.industry IS NOT NULL")
    if use_cap:
        cap_min_wan = cap_min_yi * 10000 if cap_min_yi > 0 else 0
        cap_max_wan = cap_max_yi * 10000 if cap_max_yi > 0 else 999999999
        where_parts.append("sb.circ_mv >= ?")
        where_parts.append("sb.circ_mv <= ?")
        params.extend([cap_min_wan, cap_max_wan])
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    order_sql = "ORDER BY RANDOM()" if random_order else "ORDER BY sb.circ_mv DESC"
    query = f"""
        {select_kw} sb.ts_code, sb.name, sb.industry, sb.circ_mv
        FROM stock_basic sb
        {where_sql}
        {order_sql}
    """
    return pd.read_sql_query(query, conn, params=params)


def offline_log_progress(tag: str, idx: int, total: int, *, log_every: int, logger: Any) -> None:
    if log_every <= 0:
        return
    if (idx + 1) % log_every == 0:
        logger.info(f"[offline:{tag}] progress {idx+1}/{total}")


def offline_targets() -> Set[str]:
    targets = {"v4", "v5", "v6", "v7", "v8", "v9", "combo", "sector", "stable", "ai"}
    only = os.getenv("OFFLINE_ONLY", "").strip()
    exclude = os.getenv("OFFLINE_EXCLUDE", "").strip()
    if only:
        targets = {t.strip().lower() for t in only.split(",") if t.strip()}
    if exclude:
        targets = {t for t in targets if t not in {x.strip().lower() for x in exclude.split(",") if x.strip()}}
    return targets


def postprocess_scan_results(
    results_df: pd.DataFrame,
    *,
    score_col: str,
    select_mode: str,
    threshold: float,
    top_percent: int,
    enable_consistency: bool,
    min_align: int,
    candidate_count: int,
    db_path: str,
    apply_filter_mode: Callable[[pd.DataFrame, str, str, float, int], Optional[pd.DataFrame]],
    apply_multi_period_filter: Callable[..., pd.DataFrame],
    add_reason_summary: Callable[..., pd.DataFrame],
) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    results_df = apply_filter_mode(results_df, score_col, select_mode, threshold, top_percent)
    if results_df is None or results_df.empty:
        return None, {"candidate_count": candidate_count, "filter_failed": 0}
    if enable_consistency and not results_df.empty:
        results_df = apply_multi_period_filter(results_df, db_path, min_align=min_align)
    results_df = add_reason_summary(results_df, score_col=score_col)
    if results_df is None or results_df.empty:
        return None, {"candidate_count": candidate_count, "filter_failed": 0}
    return results_df.reset_index(drop=True), {"candidate_count": candidate_count, "filter_failed": 0}
