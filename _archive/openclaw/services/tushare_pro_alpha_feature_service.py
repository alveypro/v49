from __future__ import annotations

import math
import sqlite3
from typing import Any, Dict, Sequence


JsonDict = Dict[str, Any]


def build_tushare_pro_alpha_features(
    conn: sqlite3.Connection,
    *,
    ts_code: str,
    as_of_date: str,
) -> JsonDict:
    """Build point-in-time alpha sleeve features from local Tushare Pro tables.

    The service deliberately reads only persisted local facts.  Live Tushare
    calls would make backtests non-replayable and would turn API availability
    into a strategy variable.
    """

    code = str(ts_code or "").strip()
    date = _compact_date(as_of_date)
    blocking: list[str] = []
    if not code:
        blocking.append("missing_ts_code")
    if not date:
        blocking.append("missing_as_of_date")
    if blocking:
        return _empty(blocking)

    price_rows = _price_window(conn, ts_code=code, as_of_date=date, limit=60)
    latest_price = price_rows[-1] if price_rows else {}
    money_flow = _latest_row(
        conn,
        table="moneyflow_daily",
        date_col="trade_date",
        as_of_date=date,
        where="ts_code = ?",
        params=(code,),
    )
    stock_basic = _latest_stock_basic(conn, code)
    industry = str(stock_basic.get("industry") or "")
    sector = _latest_row(
        conn,
        table="moneyflow_ind_ths",
        date_col="trade_date",
        as_of_date=date,
        where="industry = ?",
        params=(industry,),
    ) if industry else {}
    top_list = _latest_row(
        conn,
        table="top_list",
        date_col="trade_date",
        as_of_date=date,
        where="ts_code = ?",
        params=(code,),
    )
    event_facts = _event_facts(conn, ts_code=code, as_of_date=date, latest_price=latest_price, top_list=top_list)
    hard_alpha = _hard_alpha_evidence(
        conn,
        ts_code=code,
        as_of_date=date,
        industry=industry,
        price_rows=price_rows,
        latest_price=latest_price,
        event_facts=event_facts,
    )
    factor = _latest_row(
        conn,
        table="stk_factor_pro_daily",
        date_col="trade_date",
        as_of_date=date,
        where="ts_code = ?",
        params=(code,),
    )
    cyq = _latest_row(
        conn,
        table="cyq_perf_daily",
        date_col="trade_date",
        as_of_date=date,
        where="ts_code = ?",
        params=(code,),
    )

    scores = {
        "momentum": _momentum_score(price_rows, factor),
        "reversal": _reversal_score(price_rows, cyq),
        "money_flow": _money_flow_score(money_flow, latest_price),
        "sector_rotation": _sector_score(sector),
        "quality_low_vol": _quality_low_vol_score(price_rows, latest_price, factor, stock_basic),
        "event_risk": _event_score(event_facts, latest_price),
        "hard_event_alpha": _hard_event_alpha_score(hard_alpha),
    }
    sources = _sources(price_rows, money_flow, sector, event_facts, factor, cyq, stock_basic, hard_alpha)
    missing = _missing_sources(sources)
    return {
        "feature_version": "tushare_pro_alpha_features.v1",
        "research_only": True,
        "ts_code": code,
        "as_of_date": date,
        "scores": {key: round(float(value or 0.0), 4) for key, value in scores.items()},
        "evidence": {
            "price_trade_date": str(latest_price.get("trade_date") or ""),
            "moneyflow_trade_date": str(money_flow.get("trade_date") or ""),
            "sector_trade_date": str(sector.get("trade_date") or ""),
            "top_list_trade_date": str(top_list.get("trade_date") or ""),
            "factor_trade_date": str(factor.get("trade_date") or ""),
            "cyq_trade_date": str(cyq.get("trade_date") or ""),
            "industry": industry,
            "price_window_count": len(price_rows),
            "latest_amount": _num(latest_price.get("amount")),
            "net_mf_amount": _num(money_flow.get("net_mf_amount")),
            "sector_net_amount": _num(sector.get("net_amount")),
            "top_list_net_amount": _num(top_list.get("net_amount")),
            "event_count": len(event_facts),
            "event_types": sorted({str(item.get("event_type") or "") for item in event_facts if item.get("event_type")}),
            "event_facts": event_facts[:20],
            "hard_alpha": hard_alpha,
        },
        "pit_inputs": sorted(sources),
        "blocking_reasons": missing,
        "hard_boundaries": [
            "do_not_fetch_live_tushare_inside_backtest",
            "do_not_promote_without_multi_horizon_decay",
            "do_not_use_single_day_event_presence_as_alpha_edge",
            "do_not_use_unscored_event_presence_as_trade_signal",
            "do_not_use_missing_announcement_tables_as_synthetic_event_alpha",
        ],
    }


def _empty(blocking: Sequence[str]) -> JsonDict:
    return {
        "feature_version": "tushare_pro_alpha_features.v1",
        "research_only": True,
        "scores": {},
        "evidence": {},
        "pit_inputs": [],
        "blocking_reasons": list(blocking),
        "hard_boundaries": ["do_not_fetch_live_tushare_inside_backtest"],
    }


def _price_window(conn: sqlite3.Connection, *, ts_code: str, as_of_date: str, limit: int) -> list[JsonDict]:
    if not _table_exists(conn, "daily_trading_data"):
        return []
    rows = conn.execute(
        """
        SELECT trade_date, close_price, pct_chg, amount, turnover_rate
        FROM daily_trading_data
        WHERE ts_code = ?
          AND REPLACE(trade_date, '-', '') <= ?
          AND close_price IS NOT NULL
        ORDER BY REPLACE(trade_date, '-', '') DESC
        LIMIT ?
        """,
        (ts_code, as_of_date, int(limit)),
    ).fetchall()
    out = [
        {
            "trade_date": str(row[0] or ""),
            "close": _num(row[1]),
            "pct_chg": _num(row[2]),
            "amount": _num(row[3]),
            "turnover_rate": _num(row[4]),
        }
        for row in rows
    ]
    return list(reversed(out))


def _latest_row(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    as_of_date: str,
    where: str,
    params: Sequence[Any],
) -> JsonDict:
    if not _table_exists(conn, table):
        return {}
    try:
        row = conn.execute(
            f"""
            SELECT *
            FROM {table}
            WHERE {where}
              AND REPLACE({date_col}, '-', '') <= ?
            ORDER BY REPLACE({date_col}, '-', '') DESC
            LIMIT 1
            """,
            tuple(params) + (as_of_date,),
        ).fetchone()
    except sqlite3.Error:
        return {}
    if not row:
        return {}
    columns = [str(item[1] or "") for item in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return {column: row[idx] for idx, column in enumerate(columns)}


def _latest_stock_basic(conn: sqlite3.Connection, ts_code: str) -> JsonDict:
    if not _table_exists(conn, "stock_basic"):
        return {}
    row = conn.execute("SELECT * FROM stock_basic WHERE ts_code = ? LIMIT 1", (ts_code,)).fetchone()
    if not row:
        return {}
    columns = [str(item[1] or "") for item in conn.execute("PRAGMA table_info(stock_basic)").fetchall()]
    return {column: row[idx] for idx, column in enumerate(columns)}


def _momentum_score(price_rows: list[JsonDict], factor: JsonDict) -> float:
    closes = [_num(row.get("close")) for row in price_rows if _num(row.get("close")) > 0.0]
    if len(closes) < 6:
        return 0.0
    ret_5 = closes[-1] / closes[-6] - 1.0
    ret_20 = closes[-1] / closes[-21] - 1.0 if len(closes) >= 21 else ret_5
    ma5 = _num(factor.get("ma_qfq_5") or factor.get("ma_bfq_5"))
    ma20 = _num(factor.get("ma_qfq_20") or factor.get("ma_bfq_20"))
    trend_bonus = 15.0 if ma5 > 0.0 and ma20 > 0.0 and ma5 >= ma20 else 0.0
    return _clamp(50.0 + ret_5 * 220.0 + ret_20 * 90.0 + trend_bonus)


def _reversal_score(price_rows: list[JsonDict], cyq: JsonDict) -> float:
    closes = [_num(row.get("close")) for row in price_rows if _num(row.get("close")) > 0.0]
    if len(closes) < 10:
        return 0.0
    high = max(closes[-20:]) if len(closes) >= 20 else max(closes)
    drawdown = closes[-1] / high - 1.0 if high > 0.0 else 0.0
    bounce = closes[-1] / closes[-4] - 1.0 if len(closes) >= 4 and closes[-4] > 0.0 else 0.0
    winner_rate = _num(cyq.get("winner_rate"))
    chip_bonus = 8.0 if 0.0 < winner_rate < 35.0 else 0.0
    return _clamp(max(0.0, -drawdown) * 260.0 + max(0.0, bounce) * 180.0 + chip_bonus)


def _money_flow_score(money_flow: JsonDict, latest_price: JsonDict) -> float:
    if not money_flow:
        return 0.0
    net = _num(money_flow.get("net_mf_amount"))
    amount = _num(latest_price.get("amount"))
    ratio = net / amount if amount > 0.0 else 0.0
    large = _num(money_flow.get("buy_lg_amount")) + _num(money_flow.get("buy_elg_amount"))
    sell_large = _num(money_flow.get("sell_lg_amount")) + _num(money_flow.get("sell_elg_amount"))
    large_ratio = (large - sell_large) / amount if amount > 0.0 else 0.0
    return _clamp(50.0 + ratio * 180.0 + large_ratio * 120.0)


def _sector_score(sector: JsonDict) -> float:
    if not sector:
        return 0.0
    pct = _num(sector.get("pct_change"))
    net = _num(sector.get("net_amount"))
    return _clamp(50.0 + pct * 6.0 + (15.0 if net > 0.0 else -10.0 if net < 0.0 else 0.0))


def _quality_low_vol_score(
    price_rows: list[JsonDict],
    latest_price: JsonDict,
    factor: JsonDict,
    stock_basic: JsonDict,
) -> float:
    returns = [_num(row.get("pct_chg")) for row in price_rows[-20:] if row.get("pct_chg") is not None]
    if len(returns) < 8:
        return 0.0
    vol = _stddev(returns)
    turnover = _num(latest_price.get("turnover_rate"))
    pe = _num(factor.get("pe_ttm") or factor.get("pe"))
    pb = _num(factor.get("pb"))
    circ_mv = _num(factor.get("circ_mv") or stock_basic.get("circ_mv"))
    score = 85.0 - vol * 8.0 - turnover * 1.5
    if 0.0 < pe < 35.0:
        score += 8.0
    if 0.0 < pb < 5.0:
        score += 6.0
    if circ_mv >= 1_000_000.0:
        score += 5.0
    return _clamp(score)


def _event_facts(
    conn: sqlite3.Connection,
    *,
    ts_code: str,
    as_of_date: str,
    latest_price: JsonDict,
    top_list: JsonDict,
) -> list[JsonDict]:
    facts: list[JsonDict] = []
    if top_list:
        facts.append(
            _event_fact(
                event_type="top_list",
                event_date=top_list.get("trade_date"),
                source_table="top_list",
                ts_code=ts_code,
                directional_prior="mixed_flow_sensitive",
                magnitude=_num(top_list.get("net_amount")),
                reason=top_list.get("reason"),
            )
        )
    facts.extend(
        _event_rows(
            conn,
            table="top_inst",
            date_col="trade_date",
            ts_code=ts_code,
            as_of_date=as_of_date,
            event_type="top_inst",
            directional_prior="institution_flow_sensitive",
            magnitude_col="net_buy",
            reason_col="reason",
            limit=3,
        )
    )
    facts.extend(
        _event_rows(
            conn,
            table="hm_detail_daily",
            date_col="trade_date",
            ts_code=ts_code,
            as_of_date=as_of_date,
            event_type="hot_money",
            directional_prior="high_turnover_event_risk",
            magnitude_col="net_amount",
            reason_col="hm_name",
            limit=3,
        )
    )
    facts.extend(
        _event_rows(
            conn,
            table="repurchase_events",
            date_col="ann_date",
            ts_code=ts_code,
            as_of_date=as_of_date,
            event_type="repurchase",
            directional_prior="capital_return_positive_but_event_driven",
            magnitude_col="amount",
            reason_col="proc",
            limit=2,
        )
    )
    facts.extend(
        _event_rows(
            conn,
            table="share_float_events",
            date_col="ann_date",
            ts_code=ts_code,
            as_of_date=as_of_date,
            event_type="share_float_unlock",
            directional_prior="supply_overhang_risk",
            magnitude_col="float_ratio",
            reason_col="share_type",
            limit=2,
        )
    )
    facts.extend(
        _event_rows(
            conn,
            table="stk_surv_events",
            date_col="surv_date",
            ts_code=ts_code,
            as_of_date=as_of_date,
            event_type="institution_survey",
            directional_prior="attention_event_not_alpha",
            magnitude_col="fund_visitors",
            reason_col="rece_mode",
            limit=2,
        )
    )
    pct = _num(latest_price.get("pct_chg"))
    if abs(pct) >= 9.5:
        facts.append(
            _event_fact(
                event_type="limit_like_price_move",
                event_date=latest_price.get("trade_date"),
                source_table="daily_trading_data",
                ts_code=ts_code,
                directional_prior="limit_or_near_limit_event_risk",
                magnitude=pct,
                reason="abs_pct_chg_gte_9_5",
            )
        )
    return _dedupe_event_facts(facts)


def _event_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    ts_code: str,
    as_of_date: str,
    event_type: str,
    directional_prior: str,
    magnitude_col: str,
    reason_col: str,
    limit: int,
) -> list[JsonDict]:
    if not _table_exists(conn, table):
        return []
    columns = _columns(conn, table)
    if "ts_code" not in columns or date_col not in columns:
        return []
    magnitude_expr = magnitude_col if magnitude_col in columns else "NULL"
    reason_expr = reason_col if reason_col in columns else "NULL"
    try:
        rows = conn.execute(
            f"""
            SELECT {date_col}, {magnitude_expr}, {reason_expr}
            FROM {table}
            WHERE ts_code = ?
              AND REPLACE({date_col}, '-', '') <= ?
            ORDER BY REPLACE({date_col}, '-', '') DESC
            LIMIT ?
            """,
            (ts_code, as_of_date, int(limit)),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [
        _event_fact(
            event_type=event_type,
            event_date=row[0],
            source_table=table,
            ts_code=ts_code,
            directional_prior=directional_prior,
            magnitude=_num(row[1]),
            reason=row[2],
        )
        for row in rows
    ]


def _event_fact(
    *,
    event_type: str,
    event_date: Any,
    source_table: str,
    ts_code: str,
    directional_prior: str,
    magnitude: float,
    reason: Any,
) -> JsonDict:
    date = _compact_date(event_date)
    return {
        "event_type": str(event_type),
        "event_date": date,
        "visible_as_of_date": date,
        "source_table": str(source_table),
        "ts_code": str(ts_code),
        "directional_prior": str(directional_prior),
        "magnitude": round(float(magnitude or 0.0), 4),
        "reason": str(reason or "")[:120],
    }


def _dedupe_event_facts(facts: Sequence[JsonDict]) -> list[JsonDict]:
    out: list[JsonDict] = []
    seen: set[tuple[str, str, str]] = set()
    for fact in facts:
        key = (
            str(fact.get("source_table") or ""),
            str(fact.get("event_type") or ""),
            str(fact.get("event_date") or ""),
        )
        if not key[0] or key in seen:
            continue
        seen.add(key)
        out.append(fact)
    return sorted(out, key=lambda item: (str(item.get("event_date") or ""), str(item.get("source_table") or "")), reverse=True)


def _event_score(event_facts: Sequence[JsonDict], latest_price: JsonDict) -> float:
    pct = abs(_num(latest_price.get("pct_chg")))
    if event_facts:
        positive = sum(1 for item in event_facts if _num(item.get("magnitude")) > 0.0)
        supply_risk = sum(1 for item in event_facts if item.get("event_type") == "share_float_unlock")
        flow_bonus = min(18.0, positive * 4.0)
        event_penalty = min(12.0, supply_risk * 6.0)
        return _clamp(48.0 + flow_bonus - event_penalty + min(18.0, pct * 1.2))
    if pct >= 9.5:
        return _clamp(50.0 + min(30.0, pct * 2.0))
    return 0.0


def _hard_alpha_evidence(
    conn: sqlite3.Connection,
    *,
    ts_code: str,
    as_of_date: str,
    industry: str,
    price_rows: Sequence[JsonDict],
    latest_price: JsonDict,
    event_facts: Sequence[JsonDict],
) -> JsonDict:
    money_rows = _recent_rows(
        conn,
        table="moneyflow_daily",
        date_col="trade_date",
        as_of_date=as_of_date,
        where="ts_code = ?",
        params=(ts_code,),
        limit=5,
    )
    top_inst_rows = _recent_rows(
        conn,
        table="top_inst",
        date_col="trade_date",
        as_of_date=as_of_date,
        where="ts_code = ?",
        params=(ts_code,),
        limit=8,
    )
    hot_money_rows = _recent_rows(
        conn,
        table="hm_detail_daily",
        date_col="trade_date",
        as_of_date=as_of_date,
        where="ts_code = ?",
        params=(ts_code,),
        limit=5,
    )
    sector_rows = _recent_rows(
        conn,
        table="moneyflow_ind_ths",
        date_col="trade_date",
        as_of_date=as_of_date,
        where="industry = ?",
        params=(industry,),
        limit=5,
    ) if industry else []
    margin_rows = _recent_rows(
        conn,
        table="margin_detail",
        date_col="trade_date",
        as_of_date=as_of_date,
        where="ts_code = ?",
        params=(ts_code,),
        limit=5,
    )
    auction = _latest_row(
        conn,
        table="stk_auction_daily",
        date_col="trade_date",
        as_of_date=as_of_date,
        where="ts_code = ?",
        params=(ts_code,),
    )
    return {
        "evidence_version": "hard_alpha_event_sources.v1",
        "research_only": True,
        "money_flow_persistence": _money_flow_persistence(money_rows, latest_price),
        "dragon_tiger_seat_quality": _dragon_tiger_seat_quality(top_inst_rows, hot_money_rows),
        "limit_break_structure": _limit_break_structure(price_rows),
        "industry_crowding": _industry_crowding(sector_rows),
        "capacity_liquidity": _capacity_liquidity(latest_price, auction),
        "margin_pressure": _margin_pressure(margin_rows),
        "announcement_availability": _announcement_availability(conn),
        "source_tables": sorted(
            table
            for table, rows in {
                "moneyflow_daily": money_rows,
                "top_inst": top_inst_rows,
                "hm_detail_daily": hot_money_rows,
                "moneyflow_ind_ths": sector_rows,
                "margin_detail": margin_rows,
                "stk_auction_daily": [auction] if auction else [],
                "event_facts": list(event_facts),
            }.items()
            if rows
        ),
    }


def _recent_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    as_of_date: str,
    where: str,
    params: Sequence[Any],
    limit: int,
) -> list[JsonDict]:
    if not _table_exists(conn, table):
        return []
    columns = list(_columns_ordered(conn, table))
    if date_col not in columns:
        return []
    try:
        rows = conn.execute(
            f"""
            SELECT *
            FROM {table}
            WHERE {where}
              AND REPLACE({date_col}, '-', '') <= ?
            ORDER BY REPLACE({date_col}, '-', '') DESC
            LIMIT ?
            """,
            tuple(params) + (as_of_date, int(limit)),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [{column: row[idx] for idx, column in enumerate(columns)} for row in rows]


def _money_flow_persistence(rows: Sequence[JsonDict], latest_price: JsonDict) -> JsonDict:
    amount = max(_num(latest_price.get("amount")), 1.0)
    ratios = [_num(row.get("net_mf_amount")) / amount for row in rows]
    positive_days = sum(1 for value in ratios if value > 0.0)
    large_ratios = []
    for row in rows:
        large = _num(row.get("buy_lg_amount")) + _num(row.get("buy_elg_amount"))
        sell_large = _num(row.get("sell_lg_amount")) + _num(row.get("sell_elg_amount"))
        large_ratios.append((large - sell_large) / amount)
    return {
        "lookback_days": len(rows),
        "positive_net_days": positive_days,
        "net_amount_ratio_sum": round(sum(ratios), 6),
        "large_order_ratio_sum": round(sum(large_ratios), 6),
        "score": _clamp(45.0 + positive_days * 8.0 + sum(ratios) * 80.0 + sum(large_ratios) * 60.0) if rows else 0.0,
    }


def _dragon_tiger_seat_quality(top_inst_rows: Sequence[JsonDict], hot_money_rows: Sequence[JsonDict]) -> JsonDict:
    inst_net = sum(_num(row.get("net_buy")) for row in top_inst_rows)
    inst_buy_rows = sum(1 for row in top_inst_rows if _num(row.get("net_buy")) > 0.0)
    hot_net = sum(_num(row.get("net_amount")) for row in hot_money_rows)
    hot_names = sorted({str(row.get("hm_name") or "") for row in hot_money_rows if str(row.get("hm_name") or "")})
    return {
        "top_inst_rows": len(top_inst_rows),
        "institution_positive_rows": inst_buy_rows,
        "institution_net_buy": round(inst_net, 4),
        "hot_money_rows": len(hot_money_rows),
        "hot_money_net_amount": round(hot_net, 4),
        "hot_money_names": hot_names[:5],
        "score": _clamp(45.0 + inst_buy_rows * 8.0 + (10.0 if inst_net > 0.0 else -10.0 if inst_net < 0.0 else 0.0) + min(12.0, max(-12.0, hot_net / 5000.0))),
    }


def _limit_break_structure(price_rows: Sequence[JsonDict]) -> JsonDict:
    recent = list(price_rows)[-8:]
    pct_values = [_num(row.get("pct_chg")) for row in recent]
    limit_up_days = sum(1 for value in pct_values if value >= 9.5)
    limit_down_days = sum(1 for value in pct_values if value <= -9.5)
    latest_pct = pct_values[-1] if pct_values else 0.0
    previous_limit = len(pct_values) >= 2 and pct_values[-2] >= 9.5
    break_after_limit = previous_limit and latest_pct < 3.0
    continuation = previous_limit and latest_pct > 0.0
    score = 0.0
    if pct_values:
        score = 45.0 + limit_up_days * 10.0 - limit_down_days * 15.0
        if continuation:
            score += 10.0
        if break_after_limit:
            score -= 18.0
    return {
        "lookback_days": len(recent),
        "limit_up_days": limit_up_days,
        "limit_down_days": limit_down_days,
        "latest_pct_chg": round(latest_pct, 4),
        "break_after_limit": bool(break_after_limit),
        "continuation_after_limit": bool(continuation),
        "score": _clamp(score),
    }


def _industry_crowding(rows: Sequence[JsonDict]) -> JsonDict:
    net_values = [_num(row.get("net_amount")) for row in rows]
    pct_values = [_num(row.get("pct_change")) for row in rows]
    positive_days = sum(1 for value in net_values if value > 0.0)
    crowding_penalty = 10.0 if len(pct_values) >= 3 and sum(1 for value in pct_values if value > 2.5) >= 3 else 0.0
    return {
        "lookback_days": len(rows),
        "positive_net_days": positive_days,
        "net_amount_sum": round(sum(net_values), 4),
        "avg_pct_change": round(sum(pct_values) / len(pct_values), 4) if pct_values else 0.0,
        "crowding_penalty": crowding_penalty,
        "score": _clamp(45.0 + positive_days * 7.0 + (8.0 if sum(net_values) > 0.0 else -8.0 if sum(net_values) < 0.0 else 0.0) - crowding_penalty) if rows else 0.0,
    }


def _capacity_liquidity(latest_price: JsonDict, auction: JsonDict) -> JsonDict:
    amount = _num(latest_price.get("amount"))
    turnover = _num(latest_price.get("turnover_rate"))
    auction_amount = _num(auction.get("amount"))
    auction_turnover = _num(auction.get("turnover_rate"))
    amount_score = min(35.0, amount / 20000.0)
    turnover_score = min(25.0, turnover * 3.0)
    auction_score = min(20.0, auction_amount / 5000.0) + min(10.0, auction_turnover * 2.0)
    return {
        "amount": round(amount, 4),
        "turnover_rate": round(turnover, 4),
        "auction_trade_date": str(auction.get("trade_date") or ""),
        "auction_amount": round(auction_amount, 4),
        "auction_turnover_rate": round(auction_turnover, 4),
        "score": _clamp(20.0 + amount_score + turnover_score + auction_score),
    }


def _margin_pressure(rows: Sequence[JsonDict]) -> JsonDict:
    rz_values = [_num(row.get("rzye")) for row in rows]
    rq_values = [_num(row.get("rqye")) for row in rows]
    rz_delta = rz_values[0] - rz_values[-1] if len(rz_values) >= 2 else 0.0
    rq_delta = rq_values[0] - rq_values[-1] if len(rq_values) >= 2 else 0.0
    return {
        "lookback_days": len(rows),
        "financing_balance_delta": round(rz_delta, 4),
        "short_balance_delta": round(rq_delta, 4),
        "score": _clamp(50.0 + (8.0 if rz_delta > 0.0 else -8.0 if rz_delta < 0.0 else 0.0) - (8.0 if rq_delta > 0.0 else 0.0)) if rows else 0.0,
    }


def _announcement_availability(conn: sqlite3.Connection) -> JsonDict:
    candidates = ("forecast", "express", "fina_indicator", "anns", "stock_notice")
    available = [table for table in candidates if _table_exists(conn, table)]
    return {
        "available_tables": available,
        "blocking_reasons": [] if available else ["missing_announcement_or_earnings_forecast_tables"],
    }


def _hard_event_alpha_score(evidence: JsonDict) -> float:
    if not evidence:
        return 0.0
    components = [
        _num((evidence.get("money_flow_persistence") or {}).get("score")),
        _num((evidence.get("dragon_tiger_seat_quality") or {}).get("score")),
        _num((evidence.get("limit_break_structure") or {}).get("score")),
        _num((evidence.get("industry_crowding") or {}).get("score")),
        _num((evidence.get("capacity_liquidity") or {}).get("score")),
        _num((evidence.get("margin_pressure") or {}).get("score")),
    ]
    active = [item for item in components if item > 0.0]
    if not active:
        return 0.0
    return _clamp(sum(active) / len(active))


def _sources(
    price_rows: list[JsonDict],
    money_flow: JsonDict,
    sector: JsonDict,
    event_facts: Sequence[JsonDict],
    factor: JsonDict,
    cyq: JsonDict,
    stock_basic: JsonDict,
    hard_alpha: JsonDict,
) -> set[str]:
    out: set[str] = set()
    if price_rows:
        out.add("price_volume")
        out.add("volume_capacity")
    if money_flow:
        out.add("money_flow")
    if sector:
        out.add("sector_heat")
    if event_facts:
        out.add("event_risk")
    if factor:
        out.add("technical_factor")
    if cyq:
        out.add("chip_distribution")
    if stock_basic:
        out.add("security_master")
    for table in hard_alpha.get("source_tables") or []:
        out.add(str(table))
    return out


def _missing_sources(sources: set[str]) -> list[str]:
    required = ("price_volume", "money_flow", "sector_heat", "volume_capacity")
    return [f"missing_tushare_pit_input:{item}" for item in required if item not in sources]


def _stddev(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / (len(values) - 1))


def _num(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, float(value or 0.0)))


def _compact_date(value: Any) -> str:
    return str(value or "").strip().replace("-", "")


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (str(table),),
        ).fetchone()
    except sqlite3.Error:
        return False
    return bool(row)


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return set()
    return {str(row[1] or "") for row in rows}


def _columns_ordered(conn: sqlite3.Connection, table: str) -> list[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return []
    return [str(row[1] or "") for row in rows]
