from __future__ import annotations

import json
import math
import sqlite3
from typing import Any, Dict, Iterable, Sequence

from openclaw.services.ensemble_core_contract_service import REQUIRED_ALPHA_SLEEVES
from openclaw.services.ensemble_sleeve_policy_audit_service import build_ensemble_sleeve_policy_audit
from openclaw.services.tushare_pro_alpha_feature_service import build_tushare_pro_alpha_features


JsonDict = Dict[str, Any]


SLEEVE_KEYWORDS = {
    "momentum": ("momentum", "strength", "trend", "breakout", "启动", "突破", "趋势", "强势"),
    "reversal": ("reversal", "bottom", "pullback", "潜伏", "底部", "回踩", "反转"),
    "money_flow": ("money", "flow", "main", "fund", "资金", "主力", "v6"),
    "sector_rotation": ("sector", "industry", "rotation", "行业", "板块", "v7", "v8"),
    "quality_low_vol": ("quality", "low_vol", "stable", "defensive", "低波", "质量", "稳健"),
    "event_risk": ("event", "limit", "龙虎榜", "涨停", "事件", "risk"),
}

DECAY_HORIZONS = (1, 3, 5, 10, 20)
MIN_EVENT_RISK_ACTIVE_SIGNALS = 10


def build_ensemble_alpha_sleeve_fact_chain(
    conn: sqlite3.Connection,
    *,
    as_of_date: str,
    strategies: Sequence[str],
    holding_days: int = 5,
    top_n_per_strategy: int = 20,
) -> JsonDict:
    """Build a point-in-time alpha sleeve fact chain from existing signals.

    The output is research evidence only.  It explains which sleeves can be
    reconstructed from current signal facts and whether simple forward-return
    attribution is replayable.  It does not produce portfolio weights.
    """

    normalized_date = _compact_date(as_of_date)
    strategy_list = _unique(strategies)
    blocking: list[str] = []
    if not normalized_date:
        blocking.append("missing_as_of_date")
    if not strategy_list:
        blocking.append("missing_source_strategies")
    if int(holding_days or 0) <= 0:
        blocking.append("invalid_holding_days")
    if blocking:
        return _blocked(normalized_date, strategy_list, int(holding_days or 0), blocking)
    if not _table_exists(conn, "signal_runs") or not _table_exists(conn, "signal_items"):
        return _blocked(
            normalized_date,
            strategy_list,
            int(holding_days or 0),
            ["missing_signal_lineage_tables"],
        )

    runs = [_latest_scan_run(conn, strategy=strategy, as_of_date=normalized_date) for strategy in strategy_list]
    runs = [run for run in runs if run]
    missing_runs = [strategy for strategy in strategy_list if strategy not in {run["strategy"] for run in runs}]
    if missing_runs:
        blocking.append("missing_source_scan_runs:" + ",".join(missing_runs))

    items: list[JsonDict] = []
    for run in runs:
        for item in _signal_items(conn, run_id=str(run["run_id"]), limit=int(top_n_per_strategy or 0)):
            forward = _forward_return_pct(
                conn,
                ts_code=str(item.get("ts_code") or ""),
                as_of_date=normalized_date,
                holding_days=int(holding_days),
            )
            forward_returns = _forward_returns_pct(
                conn,
                ts_code=str(item.get("ts_code") or ""),
                as_of_date=normalized_date,
                horizons=DECAY_HORIZONS,
            )
            pit_features = build_tushare_pro_alpha_features(
                conn,
                ts_code=str(item.get("ts_code") or ""),
                as_of_date=normalized_date,
            )
            sleeve_scores = _sleeve_scores(item, pit_features=pit_features)
            items.append(
                {
                    **item,
                    **run,
                    "forward_return": forward,
                    "forward_returns": forward_returns,
                    "tushare_pro_alpha_features": pit_features,
                    "sleeve_scores": sleeve_scores,
                }
            )

    if not items:
        blocking.append("missing_signal_items_for_alpha_sleeves")

    sleeve_reviews = {
        sleeve: _sleeve_review(sleeve=sleeve, items=items)
        for sleeve in REQUIRED_ALPHA_SLEEVES
    }
    missing_active = [sleeve for sleeve, review in sleeve_reviews.items() if int(review.get("active_signal_count", 0) or 0) <= 0]
    if missing_active:
        blocking.append("missing_active_alpha_sleeves:" + ",".join(missing_active))

    replayable = [item for item in items if (item.get("forward_return") or {}).get("available") is True]
    if not replayable:
        blocking.append("missing_replayable_forward_returns_for_alpha_attribution")

    payload = {
        "fact_chain_version": "ensemble_alpha_sleeve_fact_chain.v1",
        "research_only": True,
        "as_of_date": normalized_date,
        "holding_days": int(holding_days),
        "source_strategies": strategy_list,
        "run_count": len(runs),
        "signal_count": len(items),
        "replayable_signal_count": len(replayable),
        "sleeves": sleeve_reviews,
        "sleeve_correlation": _sleeve_correlation(items),
        "multi_horizon_decay": {
            sleeve: review.get("multi_horizon_attribution", {})
            for sleeve, review in sleeve_reviews.items()
        },
        "sleeve_use_policy": {
            sleeve: review.get("recommended_use", "research_blocked")
            for sleeve, review in sleeve_reviews.items()
        },
        "blocking_reasons": sorted(set(blocking)),
        "hard_boundaries": [
            "do_not_emit_portfolio_weights_from_fact_chain",
            "do_not_promote_without_walk_forward_portfolio_backtest",
            "do_not_treat_keyword_sleeve_mapping_as_final_alpha_model",
            "do_not_use_negative_ic_sleeves_as_positive_alpha",
        ],
        "sample_facts": items[:50],
    }
    payload["sleeve_policy_audit"] = build_ensemble_sleeve_policy_audit(payload)
    return payload


def _blocked(as_of_date: str, strategies: list[str], holding_days: int, blocking: Sequence[str]) -> JsonDict:
    return {
        "fact_chain_version": "ensemble_alpha_sleeve_fact_chain.v1",
        "research_only": True,
        "as_of_date": as_of_date,
        "holding_days": holding_days,
        "source_strategies": strategies,
        "run_count": 0,
        "signal_count": 0,
        "replayable_signal_count": 0,
        "sleeves": {},
        "sleeve_correlation": {},
        "multi_horizon_decay": {},
        "sleeve_use_policy": {},
        "blocking_reasons": sorted(set(str(item) for item in blocking if str(item or ""))),
        "hard_boundaries": [
            "do_not_emit_portfolio_weights_from_fact_chain",
            "do_not_promote_without_walk_forward_portfolio_backtest",
        ],
        "sample_facts": [],
    }


def _latest_scan_run(conn: sqlite3.Connection, *, strategy: str, as_of_date: str) -> JsonDict:
    columns = _columns(conn, "signal_runs")
    data_version_expr = "data_version" if "data_version" in columns else "'' AS data_version"
    try:
        rows = conn.execute(
            f"""
            SELECT run_id, strategy, trade_date, created_at, {data_version_expr}
            FROM signal_runs
            WHERE run_type IN ('scan', 'experiment')
              AND strategy = ?
              AND status = 'success'
              AND trade_date != ''
            ORDER BY REPLACE(trade_date, '-', '') DESC, created_at DESC
            """,
            (str(strategy),),
        ).fetchall()
    except sqlite3.Error:
        return {}
    rows = [row for row in rows if _run_visible_as_of(trade_date=row[2], data_version=row[4], as_of_date=as_of_date)]
    if not rows:
        return {}
    row = rows[0]
    return {
        "run_id": str(row[0] or ""),
        "strategy": str(row[1] or ""),
        "run_trade_date": str(row[2] or ""),
        "visible_as_of_date": _run_visible_date(trade_date=row[2], data_version=row[4]),
        "visibility_source": "data_version" if _data_version_trade_date(row[4]) else "run_trade_date",
    }


def _signal_items(conn: sqlite3.Connection, *, run_id: str, limit: int) -> list[JsonDict]:
    if limit <= 0:
        return []
    columns = _columns(conn, "signal_items")
    reason_expr = "reason_codes" if "reason_codes" in columns else "'' AS reason_codes"
    raw_expr = "raw_payload_json" if "raw_payload_json" in columns else "'{}' AS raw_payload_json"
    rows = conn.execute(
        f"""
        SELECT ts_code, score, rank_idx, {reason_expr}, {raw_expr}
        FROM signal_items
        WHERE run_id = ?
        ORDER BY COALESCE(rank_idx, 999999), score DESC, ts_code
        LIMIT ?
        """,
        (str(run_id), int(limit)),
    ).fetchall()
    return [
        {
            "ts_code": str(row[0] or ""),
            "score": float(row[1] or 0.0),
            "rank_idx": int(row[2] or 0),
            "reason_codes": _safe_json_loads(row[3], []),
            "raw_payload": _safe_json_loads(row[4], {}),
        }
        for row in rows
    ]


def _sleeve_scores(item: JsonDict, *, pit_features: JsonDict | None = None) -> JsonDict:
    text = _fact_text(item)
    base = max(0.0, min(100.0, float(item.get("score", 0.0) or 0.0)))
    explicit_scores = (pit_features or {}).get("scores") if isinstance(pit_features, dict) else {}
    explicit_scores = explicit_scores if isinstance(explicit_scores, dict) else {}
    out: JsonDict = {}
    for sleeve, keywords in SLEEVE_KEYWORDS.items():
        hits = [keyword for keyword in keywords if str(keyword).lower() in text]
        keyword_score = round(base * min(1.0, len(hits) / 2.0), 4) if hits else 0.0
        explicit_score = max(0.0, min(100.0, float(explicit_scores.get(sleeve, 0.0) or 0.0)))
        score = explicit_score if explicit_score > 0.0 else keyword_score
        source = "tushare_pro_pit_feature" if explicit_score > 0.0 else "reason_codes_raw_payload_keyword_proxy"
        out[sleeve] = {
            "score": round(score, 4),
            "keyword_hits": hits[:8],
            "mapping_source": source,
        }
    hard_event = max(0.0, min(100.0, float(explicit_scores.get("hard_event_alpha", 0.0) or 0.0)))
    if hard_event > 0.0:
        out["hard_event_alpha"] = {
            "score": round(hard_event, 4),
            "keyword_hits": [],
            "mapping_source": "tushare_pro_hard_alpha_event_sources",
        }
    return out


def _fact_text(item: JsonDict) -> str:
    parts = [
        str(item.get("strategy") or ""),
        json.dumps(item.get("reason_codes") or [], ensure_ascii=False),
        json.dumps(item.get("raw_payload") or {}, ensure_ascii=False),
    ]
    return " ".join(parts).lower()


def _sleeve_review(*, sleeve: str, items: list[JsonDict]) -> JsonDict:
    scores = [float((item.get("sleeve_scores") or {}).get(sleeve, {}).get("score", 0.0) or 0.0) for item in items]
    returns = [
        float((item.get("forward_return") or {}).get("return_pct", 0.0) or 0.0)
        for item in items
        if (item.get("forward_return") or {}).get("available") is True
    ]
    aligned_scores = [
        float((item.get("sleeve_scores") or {}).get(sleeve, {}).get("score", 0.0) or 0.0)
        for item in items
        if (item.get("forward_return") or {}).get("available") is True
    ]
    active = [score for score in scores if score > 0.0]
    ic = _pearson(aligned_scores, returns) if len(aligned_scores) >= 3 else None
    multi = _multi_horizon_attribution(sleeve=sleeve, items=items)
    recommended_use = _recommended_sleeve_use(
        sleeve=sleeve,
        active_signal_count=len(active),
        five_day_ic=ic,
        five_day_rank_ic=multi.get("horizons", {}).get(str(5), {}).get("rank_ic"),
        multi_horizon=multi,
    )
    blocking = []
    if not active:
        blocking.append("missing_active_sleeve_signals")
    if ic is None:
        blocking.append("insufficient_replayable_samples_for_ic")
    if not multi.get("decay_available"):
        blocking.append("alpha_decay_requires_multi_horizon_replay")
    if recommended_use != "positive_alpha_candidate":
        blocking.append(f"sleeve_not_positive_alpha:{recommended_use}")
    return {
        "sleeve": sleeve,
        "signal_count": len(scores),
        "active_signal_count": len(active),
        "avg_score": sum(active) / float(len(active)) if active else 0.0,
        "ic_available": ic is not None,
        "ic": ic,
        "rank_ic": multi.get("horizons", {}).get(str(5), {}).get("rank_ic"),
        "multi_horizon_attribution": multi,
        "recommended_use": recommended_use,
        "blocking_reasons": blocking,
    }


def _multi_horizon_attribution(*, sleeve: str, items: list[JsonDict]) -> JsonDict:
    horizons: JsonDict = {}
    for horizon in DECAY_HORIZONS:
        scores: list[float] = []
        returns: list[float] = []
        for item in items:
            forward = (item.get("forward_returns") or {}).get(str(horizon), {})
            if forward.get("available") is not True:
                continue
            scores.append(float((item.get("sleeve_scores") or {}).get(sleeve, {}).get("score", 0.0) or 0.0))
            returns.append(float(forward.get("return_pct", 0.0) or 0.0))
        ic = _pearson(scores, returns) if len(scores) >= 3 else None
        rank_ic = _spearman(scores, returns) if len(scores) >= 3 else None
        horizons[str(horizon)] = {
            "horizon_days": horizon,
            "sample_count": len(scores),
            "ic": ic,
            "rank_ic": rank_ic,
            "available": ic is not None or rank_ic is not None,
        }
    available = [item for item in horizons.values() if item.get("available") is True]
    ic_path = [item.get("ic") for item in available if item.get("ic") is not None]
    positive_path = [value for value in ic_path if float(value) > 0.0]
    best = max(ic_path, key=lambda value: abs(float(value))) if ic_path else None
    return {
        "horizons": horizons,
        "decay_available": len(available) >= 2,
        "positive_horizon_count": len(positive_path),
        "positive_rank_horizon_count": len(
            [
                item.get("rank_ic")
                for item in available
                if item.get("rank_ic") is not None and float(item.get("rank_ic")) > 0.0
            ]
        ),
        "negative_horizon_count": len([value for value in ic_path if float(value) < 0.0]),
        "best_abs_ic": best,
        "decay_profile": _decay_profile(ic_path),
    }


def _recommended_sleeve_use(
    *,
    sleeve: str,
    active_signal_count: int,
    five_day_ic: float | None,
    five_day_rank_ic: Any,
    multi_horizon: JsonDict,
) -> str:
    if active_signal_count <= 0 or five_day_ic is None:
        return "research_blocked"
    if sleeve == "event_risk" and active_signal_count < MIN_EVENT_RISK_ACTIVE_SIGNALS:
        return "research_blocked_insufficient_event_sample"
    if sleeve in {"quality_low_vol", "sector_rotation"}:
        return "risk_filter_candidate"
    if float(five_day_ic) <= 0.0:
        return "research_blocked_negative_ic"
    if five_day_rank_ic is None or float(five_day_rank_ic) <= 0.0:
        return "research_blocked_unstable_rank_ic"
    if int(multi_horizon.get("positive_horizon_count", 0) or 0) < 2:
        return "research_blocked_unstable_decay"
    if int(multi_horizon.get("positive_rank_horizon_count", 0) or 0) < 2:
        return "research_blocked_unstable_rank_ic"
    return "positive_alpha_candidate"


def _sleeve_correlation(items: list[JsonDict]) -> JsonDict:
    out: JsonDict = {}
    for left in REQUIRED_ALPHA_SLEEVES:
        out[left] = {}
        left_scores = [float((item.get("sleeve_scores") or {}).get(left, {}).get("score", 0.0) or 0.0) for item in items]
        for right in REQUIRED_ALPHA_SLEEVES:
            right_scores = [float((item.get("sleeve_scores") or {}).get(right, {}).get("score", 0.0) or 0.0) for item in items]
            out[left][right] = _pearson(left_scores, right_scores) if len(items) >= 3 else None
    return out


def _forward_return_pct(conn: sqlite3.Connection, *, ts_code: str, as_of_date: str, holding_days: int) -> JsonDict:
    if not _table_exists(conn, "daily_trading_data"):
        return {"available": False, "blocking_reason": "missing_daily_trading_data_table"}
    try:
        rows = conn.execute(
            """
            SELECT trade_date, close_price
            FROM daily_trading_data
            WHERE ts_code = ?
              AND trade_date >= ?
              AND close_price IS NOT NULL
            ORDER BY trade_date ASC
            LIMIT ?
            """,
            (str(ts_code), str(as_of_date), int(holding_days) + 1),
        ).fetchall()
    except sqlite3.Error:
        return {"available": False, "blocking_reason": "forward_return_query_failed"}
    if len(rows) < int(holding_days) + 1:
        return {"available": False, "blocking_reason": "insufficient_forward_price_window", "price_count": len(rows)}
    entry = float(rows[0][1] or 0.0)
    exit_price = float(rows[int(holding_days)][1] or 0.0)
    if entry <= 0.0 or exit_price <= 0.0:
        return {"available": False, "blocking_reason": "invalid_forward_price"}
    return {
        "available": True,
        "entry_trade_date": str(rows[0][0] or ""),
        "exit_trade_date": str(rows[int(holding_days)][0] or ""),
        "return_pct": (exit_price / entry - 1.0) * 100.0,
    }


def _forward_returns_pct(
    conn: sqlite3.Connection,
    *,
    ts_code: str,
    as_of_date: str,
    horizons: Sequence[int],
) -> JsonDict:
    if not _table_exists(conn, "daily_trading_data"):
        return {
            str(horizon): {"available": False, "blocking_reason": "missing_daily_trading_data_table"}
            for horizon in horizons
        }
    max_horizon = max(int(horizon) for horizon in horizons if int(horizon) > 0)
    try:
        rows = conn.execute(
            """
            SELECT trade_date, close_price
            FROM daily_trading_data
            WHERE ts_code = ?
              AND trade_date >= ?
              AND close_price IS NOT NULL
            ORDER BY trade_date ASC
            LIMIT ?
            """,
            (str(ts_code), str(as_of_date), max_horizon + 1),
        ).fetchall()
    except sqlite3.Error:
        return {
            str(horizon): {"available": False, "blocking_reason": "forward_return_query_failed"}
            for horizon in horizons
        }
    out: JsonDict = {}
    for horizon in horizons:
        h = int(horizon)
        if len(rows) < h + 1:
            out[str(h)] = {
                "available": False,
                "blocking_reason": "insufficient_forward_price_window",
                "price_count": len(rows),
            }
            continue
        entry = float(rows[0][1] or 0.0)
        exit_price = float(rows[h][1] or 0.0)
        if entry <= 0.0 or exit_price <= 0.0:
            out[str(h)] = {"available": False, "blocking_reason": "invalid_forward_price"}
            continue
        out[str(h)] = {
            "available": True,
            "entry_trade_date": str(rows[0][0] or ""),
            "exit_trade_date": str(rows[h][0] or ""),
            "return_pct": (exit_price / entry - 1.0) * 100.0,
        }
    return out


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    lx = [float(item) for item in left]
    ry = [float(item) for item in right]
    mean_l = sum(lx) / len(lx)
    mean_r = sum(ry) / len(ry)
    cov = sum((a - mean_l) * (b - mean_r) for a, b in zip(lx, ry))
    var_l = sum((a - mean_l) ** 2 for a in lx)
    var_r = sum((b - mean_r) ** 2 for b in ry)
    if var_l <= 0.0 or var_r <= 0.0:
        return None
    return round(cov / math.sqrt(var_l * var_r), 6)


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    return _pearson(_ranks(left), _ranks(right))


def _ranks(values: Sequence[float]) -> list[float]:
    indexed = sorted(enumerate(float(value) for value in values), key=lambda item: item[1])
    ranks = [0.0] * len(indexed)
    idx = 0
    while idx < len(indexed):
        end = idx
        while end + 1 < len(indexed) and indexed[end + 1][1] == indexed[idx][1]:
            end += 1
        rank = (idx + end + 2) / 2.0
        for pos in range(idx, end + 1):
            ranks[indexed[pos][0]] = rank
        idx = end + 1
    return ranks


def _decay_profile(ic_path: Sequence[float]) -> str:
    if not ic_path:
        return "unavailable"
    positives = len([value for value in ic_path if float(value) > 0.0])
    negatives = len([value for value in ic_path if float(value) < 0.0])
    if positives == len(ic_path):
        return "positive_all_available_horizons"
    if negatives == len(ic_path):
        return "negative_all_available_horizons"
    if positives > negatives:
        return "mixed_positive_bias"
    if negatives > positives:
        return "mixed_negative_bias"
    return "mixed_unstable"


def _safe_json_loads(value: Any, fallback: Any) -> Any:
    try:
        parsed = json.loads(str(value or ""))
    except Exception:
        return fallback
    return parsed


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return set()
    return {str(row[1] or "") for row in rows}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (str(table),),
        ).fetchone()
    except sqlite3.Error:
        return False
    return bool(row)


def _unique(values: Iterable[Any]) -> list[str]:
    out = []
    for value in values or []:
        text = str(value or "").strip().lower()
        if text and text not in out:
            out.append(text)
    return out


def _compact_date(value: Any) -> str:
    return str(value or "").strip().replace("-", "")


def _run_visible_as_of(*, trade_date: Any, data_version: Any, as_of_date: str) -> bool:
    visible = _run_visible_date(trade_date=trade_date, data_version=data_version)
    return bool(visible and visible <= str(as_of_date or ""))


def _run_visible_date(*, trade_date: Any, data_version: Any) -> str:
    return _data_version_trade_date(data_version) or _compact_date(trade_date)


def _data_version_trade_date(value: Any) -> str:
    text = str(value or "")
    marker = "trade_date:"
    if marker not in text:
        return ""
    tail = text.split(marker, 1)[1]
    raw = tail.split("|", 1)[0].strip()
    return _compact_date(raw)
