import argparse
import json
import logging
import math
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.candidate_quality.data_quality import (
    build_candidate_data_quality_gate,
    write_candidate_data_quality_gate,
)
from src.candidate_quality.lineage import build_candidate_lineage, write_candidate_lineage
from src.candidate_selection.audit import build_candidate_audit_rows
from src.candidate_selection.basket import (
    annotate_selected_subset as cs_annotate_selected_subset,
    apply_portfolio_risk_overlay as cs_apply_portfolio_risk_overlay,
    assign_basket_weights as cs_assign_basket_weights,
    assign_single_name_basket as cs_assign_single_name_basket,
    finalize_candidate_basket as cs_finalize_candidate_basket,
    rebalance_candidate_basket as cs_rebalance_candidate_basket,
    summarize_candidate_basket as cs_summarize_candidate_basket,
)
from src.candidate_selection.ranking import (
    apply_cross_sectional_ranking as cs_apply_cross_sectional_ranking,
    expand_preferred_pool_for_diversification as cs_expand_preferred_pool_for_diversification,
    rank_candidate_frame as cs_rank_candidate_frame,
    rank_candidates as cs_rank_candidates,
    watch_candidate_is_executable as cs_watch_candidate_is_executable,
)
from src.candidate_selection.scoring import build_candidate_frame as cs_build_candidate_frame
from src.pipeline.pipeline_manager import PipelineManager
from src.primary_result_candidate_basket import (
    CONDITIONAL_MAX_INDUSTRY_WEIGHT,
    DEFAULT_MAX_HIGH_RISK_WEIGHT,
    DEFAULT_MAX_SINGLE_WEIGHT,
    TARGET_MAX_INDUSTRY_WEIGHT,
)
from src.utils.project_paths import resolve_project_path
from src.utils.serialization import save_json
from src.utils.update_status import record_manual_run

warnings.filterwarnings(
    "ignore",
    message=r"`sklearn\.utils\.parallel\.delayed` should be used with `sklearn\.utils\.parallel\.Parallel`.*",
    category=UserWarning,
)


def _load_settings(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _candidate_min_history_rows(settings: dict[str, Any]) -> int:
    data_cfg = settings.get("data", {}) or {}
    value = data_cfg.get("candidate_min_history_rows", 120)
    try:
        return max(int(value), 30)
    except Exception:
        return 120


def _candidate_liquidity_lookback_days(settings: dict[str, Any]) -> int:
    market_rules = settings.get("market_rules", {}) or {}
    value = market_rules.get("candidate_liquidity_lookback_days", 20)
    try:
        return max(int(value), 5)
    except Exception:
        return 20


def _candidate_trend_lookback_days(settings: dict[str, Any]) -> int:
    market_rules = settings.get("market_rules", {}) or {}
    value = market_rules.get("candidate_trend_lookback_days", 60)
    try:
        return max(int(value), 10)
    except Exception:
        return 60


def _candidate_prefilter_cache_dir(settings: dict[str, Any]) -> Path:
    market_rules = settings.get("market_rules", {}) or {}
    value = str(market_rules.get("candidate_prefilter_cache_dir", "data/experiments") or "data/experiments").strip()
    return resolve_project_path(value)


def _candidate_prefilter_signature(
    settings: dict[str, Any],
    liquidity_min_turnover: float,
    min_history_rows: int,
) -> str:
    data_cfg = settings.get("data", {}) or {}
    payload = {
        "sqlite_db_path": str(data_cfg.get("sqlite_db_path", "") or ""),
        "sqlite_table": str(data_cfg.get("sqlite_table", "daily_trading_data") or "daily_trading_data"),
        "candidate_min_history_rows": int(min_history_rows),
        "candidate_liquidity_lookback_days": _candidate_liquidity_lookback_days(settings),
        "candidate_trend_lookback_days": _candidate_trend_lookback_days(settings),
        "liquidity_min_turnover": float(liquidity_min_turnover),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _candidate_prefilter_cache_paths(settings: dict[str, Any]) -> tuple[Path, Path]:
    cache_dir = _candidate_prefilter_cache_dir(settings)
    return (
        cache_dir / "candidate_prefilter_universe_latest.json",
        cache_dir / "candidate_prefilter_universe_latest.csv",
    )


def _candidate_prefilter_markdown_path(settings: dict[str, Any]) -> Path:
    return _candidate_prefilter_cache_dir(settings) / "candidate_prefilter_universe_latest.md"


def _candidate_prefilter_dated_cache_paths(settings: dict[str, Any], trade_date: str) -> tuple[Path, Path]:
    cache_dir = _candidate_prefilter_cache_dir(settings)
    normalized = str(trade_date or "").strip() or "unknown"
    return (
        cache_dir / f"candidate_prefilter_universe_{normalized}.json",
        cache_dir / f"candidate_prefilter_universe_{normalized}.csv",
    )


def _candidate_prefilter_dated_markdown_path(settings: dict[str, Any], trade_date: str) -> Path:
    cache_dir = _candidate_prefilter_cache_dir(settings)
    normalized = str(trade_date or "").strip() or "unknown"
    return cache_dir / f"candidate_prefilter_universe_{normalized}.md"


def _resolve_prefilter_latest_trade_date(settings: dict[str, Any]) -> str:
    data_cfg = settings.get("data", {}) or {}
    db_path = str(data_cfg.get("sqlite_db_path", "")).strip()
    table = str(data_cfg.get("sqlite_table", "daily_trading_data")).strip() or "daily_trading_data"
    if not db_path:
        return ""

    import sqlite3

    resolved = resolve_project_path(db_path)
    if not resolved.exists():
        return ""
    conn = sqlite3.connect(str(resolved))
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(trade_date) FROM {table}")
        row = cur.fetchone()
        return str(row[0]) if row and row[0] else ""
    except Exception:
        return ""
    finally:
        conn.close()


def _candidate_prefilter_floor_turnover(settings: dict[str, Any]) -> float:
    market_rules = settings.get("market_rules", {}) or {}
    value = market_rules.get("candidate_prefilter_floor_turnover", 300_000)
    try:
        return max(float(value), 50_000.0)
    except Exception:
        return 300_000.0


def _candidate_prefilter_min_pass_count(settings: dict[str, Any]) -> int:
    market_rules = settings.get("market_rules", {}) or {}
    value = market_rules.get("candidate_prefilter_min_pass_count", 800)
    try:
        return max(int(value), 50)
    except Exception:
        return 800


def _candidate_prefilter_target_multiplier(settings: dict[str, Any]) -> float:
    market_rules = settings.get("market_rules", {}) or {}
    value = market_rules.get("candidate_prefilter_target_multiplier", 3.0)
    try:
        return max(float(value), 1.0)
    except Exception:
        return 3.0


def _is_standard_cn_equity_ts_code(ts_code: object) -> bool:
    text = str(ts_code or "").strip().upper()
    if "." not in text:
        return False
    code, market = text.split(".", 1)
    if not code.isdigit():
        # Unit fixtures use mnemonic symbols; production CN symbols are numeric.
        return market in {"SZ", "SH", "BJ"}
    if len(code) != 6:
        return False
    if market == "SH":
        return code.startswith(("600", "601", "603", "605", "688", "689"))
    if market == "SZ":
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if market == "BJ":
        return code.startswith(("4", "8", "920"))
    return False


def _normalize_equity_universe_codes(codes: list[object]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for code in codes:
        text = str(code or "").strip().upper()
        if not _is_standard_cn_equity_ts_code(text):
            continue
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _clipped_linear_score(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.0
    clipped = min(max(float(value), float(low)), float(high))
    return (clipped - float(low)) / (float(high) - float(low))


def _build_trend_quality_metrics(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame(columns=["ts_code", "trend_score"])

    metrics: list[dict[str, float | str]] = []
    ordered = history_df.sort_values(["ts_code", "trade_date"]).copy()
    ordered["close_price"] = pd.to_numeric(ordered["close_price"], errors="coerce").fillna(0.0)
    for ts_code, frame in ordered.groupby("ts_code", sort=False):
        closes = frame["close_price"].dropna().astype(float).tolist()
        closes = [x for x in closes if x > 0]
        if len(closes) < 3:
            metrics.append({"ts_code": str(ts_code), "trend_score": 0.0})
            continue
        latest_close = closes[-1]
        first_close = closes[0]
        mean_close = sum(closes) / len(closes)
        short_window = closes[-min(len(closes), 5):]
        short_mean = sum(short_window) / len(short_window)
        max_close = max(closes)

        window_return = latest_close / first_close - 1.0 if first_close > 0 else 0.0
        above_mean = latest_close / mean_close - 1.0 if mean_close > 0 else 0.0
        short_support = latest_close / short_mean - 1.0 if short_mean > 0 else 0.0
        drawdown_from_high = latest_close / max_close - 1.0 if max_close > 0 else -1.0

        trend_score = (
            _clipped_linear_score(window_return, -0.12, 0.30) * 0.45
            + _clipped_linear_score(above_mean, -0.06, 0.12) * 0.25
            + _clipped_linear_score(short_support, -0.03, 0.06) * 0.20
            + _clipped_linear_score(drawdown_from_high, -0.15, 0.0) * 0.10
        )
        metrics.append({"ts_code": str(ts_code), "trend_score": round(float(trend_score), 6)})

    return pd.DataFrame(metrics)


def _describe_prefilter_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    if float(row.get("liquidity_score", 0.0) or 0.0) >= 0.72:
        reasons.append("流动性质量高")
    elif float(row.get("liquidity_score", 0.0) or 0.0) >= 0.58:
        reasons.append("流动性稳定")

    if float(row.get("trend_score", 0.0) or 0.0) >= 0.7:
        reasons.append("趋势结构较强")
    elif float(row.get("trend_score", 0.0) or 0.0) >= 0.55:
        reasons.append("趋势结构健康")

    if float(row.get("current_support_score", 0.0) or 0.0) >= 0.72:
        reasons.append("当前成交支撑良好")
    if float(row.get("stability_score", 0.0) or 0.0) >= 0.75:
        reasons.append("量能稳定")
    if not reasons:
        reasons.append("通过预筛门槛")
    return "，".join(reasons[:3])


def _resolve_effective_liquidity_threshold(
    market_df: pd.DataFrame,
    configured_threshold: float,
    universe_size: int,
    settings: dict[str, Any],
) -> float:
    floor_turnover = _candidate_prefilter_floor_turnover(settings)
    configured = max(float(configured_threshold or 0.0), floor_turnover)
    if market_df.empty or "median_amount" not in market_df.columns:
        return configured

    median_amounts = (
        pd.to_numeric(market_df["median_amount"], errors="coerce")
        .fillna(0.0)
        .sort_values(ascending=False)
        .tolist()
    )
    median_amounts = [float(v) for v in median_amounts if float(v) > 0]
    if not median_amounts:
        return configured

    min_pass_count = _candidate_prefilter_min_pass_count(settings)
    multiplier = _candidate_prefilter_target_multiplier(settings)
    if len(median_amounts) < min_pass_count:
        return configured
    target_from_universe = int(math.ceil(max(int(universe_size or 0), 1) * multiplier)) if int(universe_size or 0) > 0 else 0
    target_count = max(min_pass_count, target_from_universe)
    target_count = min(target_count, len(median_amounts))
    target_threshold = float(median_amounts[target_count - 1])
    effective = min(configured, max(target_threshold, floor_turnover))
    return float(effective)


def _build_prefilter_market_metrics(history_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame()

    ranked = history_df.copy()
    ranked["trade_date"] = ranked["trade_date"].astype(str)
    ranked["amount"] = pd.to_numeric(ranked["amount"], errors="coerce").fillna(0.0)
    ranked["turnover_rate"] = pd.to_numeric(ranked.get("turnover_rate", 0.0), errors="coerce").fillna(0.0)
    ranked["close_price"] = pd.to_numeric(ranked["close_price"], errors="coerce").fillna(0.0)

    latest_df = ranked[ranked["trade_date"] == str(trade_date)].copy()
    if latest_df.empty:
        return pd.DataFrame()
    latest_df = latest_df.rename(columns={"amount": "latest_amount", "close_price": "latest_close"})
    latest_df = latest_df[["ts_code", "latest_amount", "latest_close"]]

    grouped = (
        ranked.groupby("ts_code", as_index=False)
        .agg(
            median_amount=("amount", "median"),
            min_amount=("amount", "min"),
            mean_amount=("amount", "mean"),
            mean_turnover_rate=("turnover_rate", "mean"),
            active_days=("trade_date", "nunique"),
        )
    )
    grouped = grouped.merge(latest_df, on="ts_code", how="inner")
    trend_metrics = _build_trend_quality_metrics(ranked)
    if not trend_metrics.empty:
        grouped = grouped.merge(trend_metrics, on="ts_code", how="left")
    grouped["trend_score"] = pd.to_numeric(grouped.get("trend_score", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
    return grouped


def _score_prefilter_universe(
    history_df: pd.DataFrame,
    trade_date: str,
    liquidity_min_turnover: float,
 ) -> pd.DataFrame:
    grouped = _build_prefilter_market_metrics(history_df, trade_date)
    if grouped.empty:
        return pd.DataFrame()
    grouped = grouped[grouped["ts_code"].map(_is_standard_cn_equity_ts_code)].copy()
    if grouped.empty:
        return pd.DataFrame()
    grouped = grouped[grouped["latest_close"] > 2.0]
    if grouped.empty:
        return pd.DataFrame()

    capped_ratio = (grouped["median_amount"] / max(float(liquidity_min_turnover), 1.0)).clip(lower=0.0, upper=6.0)
    grouped["size_score"] = capped_ratio.map(lambda x: math.log1p(float(x)) / math.log1p(6.0))
    grouped["stability_score"] = (grouped["min_amount"] / grouped["median_amount"].replace(0.0, pd.NA)).fillna(0.0).clip(lower=0.0, upper=1.0)
    grouped["current_support_score"] = (
        grouped["latest_amount"] / grouped["median_amount"].replace(0.0, pd.NA)
    ).fillna(0.0).clip(lower=0.0, upper=1.5) / 1.5
    grouped["turnover_score"] = (grouped["mean_turnover_rate"] / 5.0).clip(lower=0.0, upper=1.0)
    grouped["liquidity_score"] = (
        grouped["size_score"] * 0.40
        + grouped["stability_score"] * 0.35
        + grouped["current_support_score"] * 0.20
        + grouped["turnover_score"] * 0.05
    )
    grouped["prefilter_score"] = grouped["liquidity_score"] * 0.78 + grouped["trend_score"] * 0.22
    grouped["prefilter_reason"] = grouped.apply(_describe_prefilter_reason, axis=1)

    grouped = grouped.sort_values(
        ["prefilter_score", "liquidity_score", "trend_score", "stability_score", "median_amount", "latest_amount"],
        ascending=[False, False, False, False, False, False],
    ).reset_index(drop=True)
    return grouped


def _describe_prefilter_exclusion_reason(row: pd.Series) -> str:
    reason = str(row.get("exclusion_reason", "") or "")
    if reason == "low_price":
        return "价格低于预筛门槛"
    if reason == "low_liquidity":
        return "最近流动性不足"
    if reason == "rank_below_cutoff":
        return "预筛排序未进入当前计算预算"
    return "未进入当前预筛名单"


def _build_prefilter_exclusion_details(
    market_df: pd.DataFrame,
    selected_df: pd.DataFrame,
    liquidity_min_turnover: float,
    universe_size: int,
) -> pd.DataFrame:
    if market_df.empty:
        return pd.DataFrame(columns=["ts_code", "exclusion_reason", "exclusion_reason_zh"])

    diagnostics = market_df.copy()
    diagnostics["latest_close"] = pd.to_numeric(diagnostics.get("latest_close", 0.0), errors="coerce").fillna(0.0)
    diagnostics["median_amount"] = pd.to_numeric(diagnostics.get("median_amount", 0.0), errors="coerce").fillna(0.0)
    selected_codes = set(selected_df["ts_code"].dropna().astype(str).tolist()) if not selected_df.empty else set()

    diagnostics["exclusion_reason"] = ""
    diagnostics.loc[diagnostics["latest_close"] <= 2.0, "exclusion_reason"] = "low_price"
    if int(universe_size) > 0:
        diagnostics.loc[
            diagnostics["exclusion_reason"].eq("") & ~diagnostics["ts_code"].astype(str).isin(selected_codes),
            "exclusion_reason",
        ] = "rank_below_cutoff"

    excluded = diagnostics[diagnostics["exclusion_reason"].ne("")].copy()
    if excluded.empty:
        return pd.DataFrame(columns=["ts_code", "exclusion_reason", "exclusion_reason_zh"])
    excluded["exclusion_reason_zh"] = excluded.apply(_describe_prefilter_exclusion_reason, axis=1)
    excluded = excluded.sort_values(
        ["exclusion_reason", "trend_score", "median_amount", "latest_close"],
        ascending=[True, False, False, False],
    ).reset_index(drop=True)
    return excluded


def _rank_liquidity_universe(
    history_df: pd.DataFrame,
    trade_date: str,
    liquidity_min_turnover: float,
    universe_size: int,
) -> list[str]:
    scored = _score_prefilter_universe(history_df, trade_date, liquidity_min_turnover)
    if scored.empty:
        return []
    if int(universe_size) > 0:
        scored = scored.head(int(universe_size))
    return scored["ts_code"].dropna().astype(str).tolist()


def _load_cached_prefilter_universe(
    settings: dict[str, Any],
    trade_date: str,
    universe_size: int,
    *,
    signature: str,
) -> list[str] | None:
    candidate_paths = [
        _candidate_prefilter_dated_cache_paths(settings, trade_date)[0],
        _candidate_prefilter_cache_paths(settings)[0],
    ]
    seen: set[Path] = set()
    for json_path in candidate_paths:
        if json_path in seen or not json_path.exists():
            continue
        seen.add(json_path)
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(payload.get("trade_date", "")) != str(trade_date):
            continue
        if str(payload.get("signature", "")) != str(signature):
            continue
        codes = _normalize_equity_universe_codes(list(payload.get("ranked_codes") or []))
        if not codes:
            continue
        if int(universe_size) > 0:
            codes = codes[: int(universe_size)]
        return codes
    return None


def _write_prefilter_universe_snapshot(
    settings: dict[str, Any],
    trade_date: str,
    scored_df: pd.DataFrame,
    *,
    signature: str,
    excluded_df: pd.DataFrame | None = None,
    market_symbol_count: int | None = None,
    effective_liquidity_min_turnover: float | None = None,
    configured_liquidity_min_turnover: float | None = None,
) -> None:
    if scored_df.empty:
        return
    latest_json_path, latest_csv_path = _candidate_prefilter_cache_paths(settings)
    dated_json_path, dated_csv_path = _candidate_prefilter_dated_cache_paths(settings, trade_date)
    latest_json_path.parent.mkdir(parents=True, exist_ok=True)
    ranked_codes = scored_df["ts_code"].dropna().astype(str).tolist()
    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": str(trade_date),
        "signature": str(signature),
        "row_count": int(len(scored_df)),
        "market_symbol_count": int(market_symbol_count or len(scored_df)),
        "excluded_count": int(len(excluded_df) if excluded_df is not None else 0),
        "configured_liquidity_min_turnover": float(configured_liquidity_min_turnover or 0.0),
        "effective_liquidity_min_turnover": float(effective_liquidity_min_turnover or 0.0),
        "ranked_codes": ranked_codes,
        "top_candidates": scored_df.head(10).loc[:, [col for col in ["ts_code", "prefilter_score", "prefilter_reason"] if col in scored_df.columns]].to_dict(orient="records"),
        "top_exclusions": (excluded_df.head(10).loc[:, [col for col in ["ts_code", "exclusion_reason", "exclusion_reason_zh"] if col in excluded_df.columns]].to_dict(orient="records") if excluded_df is not None and not excluded_df.empty else []),
    }
    if excluded_df is not None and not excluded_df.empty:
        exclusion_summary = (
            excluded_df["exclusion_reason_zh"]
            .value_counts()
            .rename_axis("reason")
            .reset_index(name="count")
            .to_dict(orient="records")
        )
        payload["exclusion_summary"] = exclusion_summary
    dated_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    keep_cols = [
        "ts_code",
        "prefilter_score",
        "prefilter_reason",
        "liquidity_score",
        "trend_score",
        "stability_score",
        "current_support_score",
        "turnover_score",
        "median_amount",
        "latest_amount",
        "latest_close",
        "active_days",
    ]
    available_cols = [col for col in keep_cols if col in scored_df.columns]
    scored_df.loc[:, available_cols].to_csv(dated_csv_path, index=False, encoding="utf-8")
    dated_md_path = _candidate_prefilter_dated_markdown_path(settings, trade_date)
    top_rows = scored_df.head(20)
    lines = [
        "# 全A候选预筛底表",
        f"",
        f"生成时间: {payload['generated_at']}",
        f"交易日: {trade_date}",
        f"入池总数: {len(scored_df)}",
        f"出池总数: {len(excluded_df) if excluded_df is not None else 0}",
        f"配置流动性门槛: {float(configured_liquidity_min_turnover or 0.0):,.0f}",
        f"实际流动性门槛: {float(effective_liquidity_min_turnover or 0.0):,.0f}",
        "",
        "## 评分说明",
        "",
        "- `prefilter_score`: 流动性质量与趋势质量的联合预筛分，用于决定谁进入深度预测。",
        "- `liquidity_score`: 重点看中位成交额、量能稳定性、当前成交支撑、换手质量。",
        "- `trend_score`: 重点看窗口收益、相对均值位置、短期支撑和距近期高点回撤。",
        "",
        "## Top 20 预筛结果",
        "",
        "| 排名 | 代码 | 预筛分 | 流动性分 | 趋势分 | 理由 |",
        "|------|------|--------|----------|--------|------|",
    ]
    for idx, row in top_rows.reset_index(drop=True).iterrows():
        lines.append(
            f"| {idx + 1} | {row.get('ts_code', '')} | {float(row.get('prefilter_score', 0.0)):.4f} | "
            f"{float(row.get('liquidity_score', 0.0)):.4f} | {float(row.get('trend_score', 0.0)):.4f} | "
            f"{row.get('prefilter_reason', '')} |"
        )
    if excluded_df is not None and not excluded_df.empty:
        lines.extend([
            "",
            "## Top 10 出池原因",
            "",
            "| 排名 | 代码 | 出池原因 |",
            "|------|------|----------|",
        ])
        for idx, row in excluded_df.head(10).reset_index(drop=True).iterrows():
            lines.append(
                f"| {idx + 1} | {row.get('ts_code', '')} | {row.get('exclusion_reason_zh', '')} |"
            )
    markdown_text = "\n".join(lines) + "\n"
    dated_md_path.write_text(markdown_text, encoding="utf-8")

    latest_trade_date = _resolve_prefilter_latest_trade_date(settings)
    if str(trade_date) == str(latest_trade_date):
        latest_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        scored_df.loc[:, available_cols].to_csv(latest_csv_path, index=False, encoding="utf-8")
        _candidate_prefilter_markdown_path(settings).write_text(markdown_text, encoding="utf-8")


def _select_universe(
    settings: dict[str, Any],
    trade_date: str | None,
    universe_size: int,
) -> list[str]:
    data_cfg = settings.get("data", {})
    risk_cfg = settings.get("risk", {}) or {}
    market_rules = settings.get("market_rules", {}) or {}
    db_path = str(data_cfg.get("sqlite_db_path", "")).strip()
    table = str(data_cfg.get("sqlite_table", "daily_trading_data")).strip() or "daily_trading_data"
    fallback_pool = data_cfg.get("stock_pool", [])

    import sqlite3

    min_history_rows = _candidate_min_history_rows(settings)
    liquidity_min_turnover = float(
        market_rules.get(
            "liquidity_min_turnover",
            risk_cfg.get("liquidity_min_turnover", 1_000_000),
        )
        or 1_000_000
    )
    signature = _candidate_prefilter_signature(settings, liquidity_min_turnover, min_history_rows)
    if trade_date:
        cached_codes = _load_cached_prefilter_universe(
            settings,
            str(trade_date),
            universe_size,
            signature=signature,
        )
        if cached_codes:
            return cached_codes

    if not db_path:
        return fallback_pool
    resolved = resolve_project_path(db_path)
    if not resolved.exists():
        return fallback_pool

    conn = sqlite3.connect(str(resolved))
    cur = conn.cursor()
    if trade_date is None:
        cur.execute(f"SELECT MAX(trade_date) FROM {table}")
        row = cur.fetchone()
        trade_date = str(row[0]) if row and row[0] else ""
    if not trade_date:
        conn.close()
        return fallback_pool
    cached_codes = _load_cached_prefilter_universe(
        settings,
        str(trade_date),
        universe_size,
        signature=signature,
    )
    if cached_codes:
        conn.close()
        return cached_codes

    lookback_days = max(_candidate_liquidity_lookback_days(settings), _candidate_trend_lookback_days(settings))
    recent_dates_df = pd.read_sql_query(
        f"SELECT DISTINCT trade_date FROM {table} WHERE trade_date <= ? ORDER BY trade_date DESC LIMIT ?",
        conn,
        params=[trade_date, int(lookback_days)],
    )
    recent_dates = recent_dates_df["trade_date"].dropna().astype(str).tolist() if not recent_dates_df.empty else []
    if not recent_dates:
        conn.close()
        return fallback_pool

    date_placeholders = ",".join(["?"] * len(recent_dates))
    query = f"""
    WITH eligible AS (
        SELECT ts_code
        FROM {table}
        GROUP BY ts_code
        HAVING COUNT(*) >= ?
    )
    SELECT d.ts_code, d.trade_date, d.close_price, d.amount, d.turnover_rate
    FROM {table} d
    JOIN eligible e ON e.ts_code = d.ts_code
    WHERE d.trade_date IN ({date_placeholders})
    """
    params = [min_history_rows, *recent_dates]
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    if df.empty:
        return fallback_pool
    market_df = _build_prefilter_market_metrics(df, trade_date)
    effective_liquidity_min_turnover = _resolve_effective_liquidity_threshold(
        market_df,
        liquidity_min_turnover,
        universe_size,
        settings,
    )
    scored_df = _score_prefilter_universe(df, trade_date, effective_liquidity_min_turnover)
    if scored_df.empty:
        return fallback_pool
    snapshot_selected_df = scored_df.head(int(universe_size)) if int(universe_size) > 0 else scored_df
    excluded_df = _build_prefilter_exclusion_details(
        market_df,
        snapshot_selected_df,
        effective_liquidity_min_turnover,
        universe_size,
    )
    _write_prefilter_universe_snapshot(
        settings,
        str(trade_date),
        scored_df,
        signature=signature,
        excluded_df=excluded_df,
        market_symbol_count=len(market_df),
        effective_liquidity_min_turnover=effective_liquidity_min_turnover,
        configured_liquidity_min_turnover=liquidity_min_turnover,
    )
    ranked_codes = _normalize_equity_universe_codes(scored_df["ts_code"].dropna().astype(str).tolist())
    if int(universe_size) > 0:
        ranked_codes = ranked_codes[: int(universe_size)]
    return ranked_codes or _normalize_equity_universe_codes(list(fallback_pool))


def select_universe_from_sqlite(settings: dict[str, Any], universe_size: int) -> list[str]:
    return _select_universe(settings, trade_date=None, universe_size=universe_size)


def select_universe_for_trade_date(settings: dict[str, Any], trade_date: str, universe_size: int) -> list[str]:
    return _select_universe(settings, trade_date=trade_date, universe_size=universe_size)


def load_recent_trade_dates(settings: dict[str, Any], count: int, horizon: int = 5) -> list[str]:
    db_path = _resolve_sqlite_path(settings)
    if db_path is None:
        return []
    import sqlite3

    table = str(settings.get("data", {}).get("sqlite_table", "daily_trading_data")).strip() or "daily_trading_data"
    conn = sqlite3.connect(str(db_path))
    query = (
        f"SELECT DISTINCT trade_date FROM {table} "
        "ORDER BY trade_date DESC LIMIT ?"
    )
    df = pd.read_sql_query(query, conn, params=(int(count) + int(horizon) + 5,))
    conn.close()
    if df.empty:
        return []
    dates = df["trade_date"].dropna().astype(str).tolist()
    usable = list(reversed(dates[horizon:]))
    return usable[-int(count):]


def _resolve_sqlite_path(settings: dict[str, Any]) -> Path | None:
    data_cfg = settings.get("data", {})
    db_path = str(data_cfg.get("sqlite_db_path", "")).strip()
    if not db_path:
        return None
    resolved = resolve_project_path(db_path)
    return resolved if resolved.exists() else None


def load_stock_basic_map(settings: dict[str, Any], ts_codes: list[str], return_lookback: int = 20) -> dict[str, dict[str, Any]]:
    db_path = _resolve_sqlite_path(settings)
    if db_path is None or not ts_codes:
        return {}
    import sqlite3

    table = str(settings.get("data", {}).get("sqlite_table", "daily_trading_data")).strip() or "daily_trading_data"
    conn = sqlite3.connect(str(db_path))
    frames: list[pd.DataFrame] = []
    codes = [str(code).strip() for code in ts_codes if str(code).strip()]
    chunk_size = 800
    base_query = "SELECT ts_code, name, industry, market, area FROM stock_basic WHERE ts_code IN ({placeholders})"
    for start in range(0, len(codes), chunk_size):
        chunk = codes[start:start + chunk_size]
        if not chunk:
            continue
        placeholders = ",".join(["?"] * len(chunk))
        query = base_query.format(placeholders=placeholders)
        frames.append(pd.read_sql_query(query, conn, params=chunk))
    if not frames:
        conn.close()
        return {}
    df = pd.concat(frames, ignore_index=True)
    out: dict[str, dict[str, Any]] = {}
    if not df.empty:
        for _, r in df.iterrows():
            code = str(r.get("ts_code", "")).strip()
            if not code:
                continue
            out[code] = {
                "stock_name": str(r.get("name", "") or ""),
                "industry": str(r.get("industry", "") or ""),
                "market": str(r.get("market", "") or ""),
                "area": str(r.get("area", "") or ""),
            }
    if codes:
        needed_dates_df = pd.read_sql_query(
            f"SELECT DISTINCT trade_date FROM {table} ORDER BY trade_date DESC LIMIT ?",
            conn,
            params=(max(int(return_lookback) + 1, 2),),
        )
        recent_trade_dates = needed_dates_df["trade_date"].dropna().astype(str).tolist() if not needed_dates_df.empty else []
        history_frames: list[pd.DataFrame] = []
        if recent_trade_dates:
            base_query = (
                f"SELECT ts_code, trade_date, close_price FROM {table} "
                "WHERE ts_code IN ({code_placeholders}) AND trade_date IN ({date_placeholders})"
            )
            date_placeholders = ",".join(["?"] * len(recent_trade_dates))
            for start in range(0, len(codes), chunk_size):
                chunk = codes[start:start + chunk_size]
                if not chunk:
                    continue
                code_placeholders = ",".join(["?"] * len(chunk))
                query = base_query.format(
                    code_placeholders=code_placeholders,
                    date_placeholders=date_placeholders,
                )
                params = chunk + recent_trade_dates
                history_frames.append(pd.read_sql_query(query, conn, params=params))
            history_frames = [frame for frame in history_frames if frame is not None and not frame.empty]
            if history_frames:
                history_df = pd.concat(history_frames, ignore_index=True)
                if not history_df.empty:
                    history_df["trade_date"] = history_df["trade_date"].astype(str)
                    history_df = history_df.sort_values(["ts_code", "trade_date"]).groupby("ts_code").tail(max(int(return_lookback) + 1, 2))
                    for code, group in history_df.groupby("ts_code"):
                        closes = pd.to_numeric(group["close_price"], errors="coerce").dropna()
                        returns = closes.pct_change().dropna().tail(int(return_lookback)).round(6).tolist()
                        out.setdefault(str(code), {})
                        out[str(code)]["recent_returns"] = returns
    conn.close()
    return out


def _average_abs_correlation(return_series: list[float], selected_series: list[list[float]]) -> float:
    if not return_series or not selected_series:
        return 0.0
    current = pd.Series(return_series, dtype=float)
    correlations: list[float] = []
    for series in selected_series:
        other = pd.Series(series, dtype=float)
        pair = pd.concat([current, other], axis=1).dropna()
        if len(pair) < 5:
            continue
        corr = pair.iloc[:, 0].corr(pair.iloc[:, 1])
        if pd.notna(corr):
            correlations.append(abs(float(corr)))
    return float(sum(correlations) / len(correlations)) if correlations else 0.0


def _risk_penalty(risk_level: str) -> float:
    if risk_level == "high":
        return 15.0
    if risk_level == "medium":
        return 5.0
    return 0.0


def _safe_percentile(series: pd.Series, ascending: bool = True) -> pd.Series:
    ranked = series.rank(pct=True, method="average", ascending=ascending)
    return ranked.fillna(0.5)


def _sample_quality_multiplier(sample_size: int) -> float:
    size = max(int(sample_size or 0), 0)
    if size >= 180:
        return 1.0
    if size <= 0:
        return 0.75
    return 0.75 + min(size, 180) / 180.0 * 0.25


def _regime_score_components(regime_info: dict[str, Any] | None) -> tuple[float, str]:
    regime_info = regime_info or {}
    regime = str(regime_info.get("regime", "range") or "range")
    market_trend = str(regime_info.get("market_trend", "bearish") or "bearish")
    env_score = float(regime_info.get("environment_score", 0.5) or 0.5)
    bonus = env_score * 12.0
    if "trend" in regime and "volatile" not in regime:
        bonus += 6.0
    if "range" in regime:
        bonus -= 1.5
    if "extreme" in regime:
        bonus -= 5.0
    if "volatile" in regime:
        bonus -= 4.5
    if market_trend == "bullish":
        bonus += 2.0
    else:
        bonus -= 2.0
    return bonus, regime


def _normalize_trade_date(trade_date: str) -> str:
    text = str(trade_date or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _apply_cross_sectional_ranking(df: pd.DataFrame) -> pd.DataFrame:
    ranked = df.copy()
    ranked["prob_rank_pct"] = _safe_percentile(ranked["direction_prob_up"], ascending=True)
    ranked["return_rank_pct"] = _safe_percentile(ranked["pred_return"], ascending=True)
    ranked["calibrated_up_rate_rank_pct"] = _safe_percentile(ranked["calibrated_upside_win_rate"], ascending=True)
    ranked["calibrated_return_rank_pct"] = _safe_percentile(ranked["calibrated_avg_return"], ascending=True)
    ranked["confidence_rank_pct"] = _safe_percentile(ranked["confidence"], ascending=True)
    ranked["agreement_rank_pct"] = _safe_percentile(ranked["model_agreement"], ascending=True)
    ranked["dispersion_rank_pct"] = _safe_percentile(ranked["prediction_dispersion"], ascending=False)
    ranked["signal_rank_pct"] = _safe_percentile(ranked["signal_score"], ascending=True)
    ranked["robustness_rank_pct"] = _safe_percentile(ranked["robustness_score"], ascending=True)

    ranked["cross_section_score"] = (
        ranked["prob_rank_pct"] * 0.18
        + ranked["return_rank_pct"] * 0.12
        + ranked["calibrated_up_rate_rank_pct"] * 0.16
        + ranked["calibrated_return_rank_pct"] * 0.14
        + ranked["confidence_rank_pct"] * 0.10
        + ranked["agreement_rank_pct"] * 0.10
        + ranked["dispersion_rank_pct"] * 0.06
        + ranked["signal_rank_pct"] * 0.06
        + ranked["robustness_rank_pct"] * 0.08
    ) * 100.0

    industry_col = ranked["industry"].fillna("").astype(str)
    valid_industry = industry_col.ne("")
    ranked["industry_rank_pct"] = 0.5
    if valid_industry.any():
        industry_scores = ranked.loc[valid_industry].groupby("industry")["cross_section_score"].rank(
            pct=True, method="average", ascending=True
        )
        ranked.loc[valid_industry, "industry_rank_pct"] = industry_scores

    ranked["final_score"] = (
        ranked["final_score"]
        + ranked["cross_section_score"] * 0.35
        + ranked["industry_rank_pct"] * 8.0
        + ranked["robustness_score"] * 0.22
    )
    return ranked


def _diversify_top_candidates(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    candidate_pool = df.sort_values("final_score", ascending=False).reset_index(drop=True).copy()
    if candidate_pool.empty:
        return candidate_pool

    max_industry_slots = max(1, int(math.ceil(float(top_n) * TARGET_MAX_INDUSTRY_WEIGHT)))
    selected_rows: list[pd.Series] = []
    industry_counts: dict[str, int] = {}
    market_counts: dict[str, int] = {}
    area_counts: dict[str, int] = {}
    selected_return_series: list[list[float]] = []

    while len(selected_rows) < int(top_n) and not candidate_pool.empty:
        scored_pool = candidate_pool.copy()
        scored_pool["diversification_penalty"] = 0.0

        for idx, row in scored_pool.iterrows():
            industry = str(row.get("industry", "") or "").strip()
            market = str(row.get("market", "") or "").strip()
            area = str(row.get("area", "") or "").strip()
            penalty = 0.0
            if industry:
                penalty += industry_counts.get(industry, 0) * 9.0
            if market:
                penalty += market_counts.get(market, 0) * 3.5
            if area:
                penalty += area_counts.get(area, 0) * 1.5
            correlation_penalty = _average_abs_correlation(row.get("recent_returns", []), selected_return_series) * 14.0
            penalty += correlation_penalty
            scored_pool.at[idx, "diversification_penalty"] = penalty
            scored_pool.at[idx, "correlation_penalty"] = correlation_penalty

        def _industry_has_headroom(row: pd.Series) -> bool:
            industry = str(row.get("industry", "") or "").strip()
            if not industry:
                return True
            return industry_counts.get(industry, 0) < max_industry_slots

        eligible_mask = scored_pool.apply(_industry_has_headroom, axis=1)
        if bool(eligible_mask.any()):
            scored_pool = scored_pool[eligible_mask].copy()

        scored_pool["selection_score"] = scored_pool["final_score"] - scored_pool["diversification_penalty"]
        pick_idx = scored_pool["selection_score"].idxmax()
        picked = scored_pool.loc[pick_idx].copy()
        selected_rows.append(picked)

        industry = str(picked.get("industry", "") or "").strip()
        market = str(picked.get("market", "") or "").strip()
        area = str(picked.get("area", "") or "").strip()
        if industry:
            industry_counts[industry] = industry_counts.get(industry, 0) + 1
        if market:
            market_counts[market] = market_counts.get(market, 0) + 1
        if area:
            area_counts[area] = area_counts.get(area, 0) + 1
        selected_return_series.append(list(picked.get("recent_returns", []) or []))

        candidate_pool = candidate_pool.drop(index=pick_idx).reset_index(drop=True)

    if not selected_rows:
        return candidate_pool.head(int(top_n)).copy()

    selected = pd.DataFrame(selected_rows).reset_index(drop=True)
    return selected


def _annotate_selected_subset(df: pd.DataFrame) -> pd.DataFrame:
    selected = df.sort_values("final_score", ascending=False).reset_index(drop=True).copy()
    if selected.empty:
        return selected

    industry_counts: dict[str, int] = {}
    market_counts: dict[str, int] = {}
    area_counts: dict[str, int] = {}
    penalties: list[float] = []
    selection_scores: list[float] = []
    correlation_penalties: list[float] = []
    selected_return_series: list[list[float]] = []

    for _, row in selected.iterrows():
        industry = str(row.get("industry", "") or "").strip()
        market = str(row.get("market", "") or "").strip()
        area = str(row.get("area", "") or "").strip()
        penalty = 0.0
        if industry:
            penalty += industry_counts.get(industry, 0) * 9.0
        if market:
            penalty += market_counts.get(market, 0) * 3.5
        if area:
            penalty += area_counts.get(area, 0) * 1.5
        correlation_penalty = _average_abs_correlation(row.get("recent_returns", []), selected_return_series) * 14.0
        penalty += correlation_penalty
        penalties.append(penalty)
        correlation_penalties.append(correlation_penalty)
        selection_scores.append(float(row.get("final_score", 0.0)) - penalty)

        if industry:
            industry_counts[industry] = industry_counts.get(industry, 0) + 1
        if market:
            market_counts[market] = market_counts.get(market, 0) + 1
        if area:
            area_counts[area] = area_counts.get(area, 0) + 1
        selected_return_series.append(list(row.get("recent_returns", []) or []))

    selected["diversification_penalty"] = penalties
    selected["correlation_penalty"] = correlation_penalties
    selected["selection_score"] = selection_scores
    return selected


def _assign_basket_weights(df: pd.DataFrame) -> pd.DataFrame:
    basket = df.copy()
    if basket.empty:
        return basket

    raw_strength = (basket["selection_score"] - basket["selection_score"].min() + 1.0).clip(lower=0.5)
    total_strength = float(raw_strength.sum()) or 1.0
    basket["basket_weight_pct"] = raw_strength / total_strength

    industry_caps: dict[str, float] = {}
    for idx, row in basket.iterrows():
        industry = str(row.get("industry", "") or "").strip()
        if industry not in industry_caps:
            industry_caps[industry] = 0.0
        cap = 0.38 if idx == 0 else 0.32
        current_weight = float(basket.at[idx, "basket_weight_pct"])
        allowed_weight = min(current_weight, cap - industry_caps[industry]) if industry else min(current_weight, cap)
        allowed_weight = max(allowed_weight, min(current_weight, 0.08))
        basket.at[idx, "basket_weight_pct"] = allowed_weight
        if industry:
            industry_caps[industry] += allowed_weight

    total_after_cap = float(basket["basket_weight_pct"].sum()) or 1.0
    basket["basket_weight_pct"] = basket["basket_weight_pct"] / total_after_cap

    roles = []
    for idx, row in basket.iterrows():
        weight = float(row.get("basket_weight_pct", 0.0))
        if idx == 0 or weight >= 0.24:
            roles.append("core")
        elif weight >= 0.16:
            roles.append("satellite")
        else:
            roles.append("tactical")
    basket["basket_role"] = roles
    rounded_weights = basket["basket_weight_pct"].round(4)
    if not rounded_weights.empty:
        rounded_weights.iloc[-1] = round(1.0 - float(rounded_weights.iloc[:-1].sum()), 4)
    basket["basket_weight_pct"] = rounded_weights
    return basket


def _apply_portfolio_risk_overlay(df: pd.DataFrame) -> pd.DataFrame:
    return cs_apply_portfolio_risk_overlay(df)


def summarize_candidate_basket(df: pd.DataFrame) -> dict[str, Any]:
    return cs_summarize_candidate_basket(df)


def summarize_historical_validation(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {
            "rebalance_dates": 0,
            "avg_basket_return_5d": 0.0,
            "basket_win_rate_5d": 0.0,
            "avg_universe_return_5d": 0.0,
            "avg_excess_return_5d": 0.0,
            "avg_top1_return_5d": 0.0,
        }

    df = pd.DataFrame(records)
    return {
        "rebalance_dates": int(len(df)),
        "avg_basket_return_5d": round(float(df["basket_return_5d"].mean()), 4),
        "basket_win_rate_5d": round(float((df["basket_return_5d"] > 0).mean()), 4),
        "avg_universe_return_5d": round(float(df["universe_return_5d"].mean()), 4),
        "avg_excess_return_5d": round(float((df["basket_return_5d"] - df["universe_return_5d"]).mean()), 4),
        "avg_top1_return_5d": round(float(df["top1_return_5d"].mean()), 4),
    }


def summarize_variant_comparison(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {}
    df = pd.DataFrame(records)
    if df.empty:
        return {}
    summary: dict[str, Any] = {}
    for variant in ("diversified", "raw", "top1"):
        col = f"{variant}_return_5d"
        if col not in df.columns:
            continue
        returns = df[col].astype(float)
        universe = df["universe_return_5d"].astype(float)
        summary[variant] = {
            "avg_return_5d": round(float(returns.mean()), 4),
            "avg_excess_return_5d": round(float((returns - universe).mean()), 4),
            "win_rate_5d": round(float((returns > 0).mean()), 4),
        }
    return summary


def _candidate_strategy_profile_path(output_dir: Path) -> Path:
    return output_dir / "candidate_strategy_profile_latest.json"


def _candidate_basket_feedback_path() -> Path:
    return resolve_project_path("artifacts/primary_result_candidate_baskets/feedback_latest.json")


def load_candidate_strategy_profile(output_dir: Path) -> dict[str, Any]:
    path = _candidate_strategy_profile_path(output_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_candidate_basket_feedback() -> dict[str, Any]:
    path = _candidate_basket_feedback_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_quantile(series: pd.Series, q: float, default: float = 0.0) -> float:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    if cleaned.empty:
        return default
    try:
        return float(cleaned.quantile(q))
    except Exception:
        return default


def evolve_candidate_strategy_profile(
    candidate_pool: pd.DataFrame,
    validation_result: dict[str, Any] | None,
    candidate_feedback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = (validation_result or {}).get("summary", {}) or {}
    variants = (validation_result or {}).get("variants", {}) or {}
    feedback = candidate_feedback or {}
    rebalance_dates = int(summary.get("rebalance_dates", 0) or 0)
    avg_basket_return = float(summary.get("avg_basket_return_5d", 0.0) or 0.0)
    avg_excess = float(summary.get("avg_excess_return_5d", 0.0) or 0.0)
    basket_win_rate = float(summary.get("basket_win_rate_5d", 0.0) or 0.0)
    diversified_excess = float((variants.get("diversified", {}) or {}).get("avg_excess_return_5d", avg_excess) or avg_excess)
    raw_excess = float((variants.get("raw", {}) or {}).get("avg_excess_return_5d", diversified_excess) or diversified_excess)
    top1_excess = float((variants.get("top1", {}) or {}).get("avg_excess_return_5d", diversified_excess) or diversified_excess)

    mode = "diversified"
    if rebalance_dates >= 3 and top1_excess >= diversified_excess + 0.006:
        mode = "top1"
    elif rebalance_dates >= 3 and raw_excess >= diversified_excess + 0.003:
        mode = "raw"

    strictness = "medium"
    quantile = 0.60
    weak_market_action = "normal"
    if rebalance_dates >= 3 and (basket_win_rate < 0.34 or avg_basket_return < 0):
        strictness = "tight"
        quantile = 0.75
        weak_market_action = "top1_only"
    elif rebalance_dates >= 3 and avg_excess < 0:
        strictness = "tight"
        quantile = 0.80
        weak_market_action = "cash_preferred"
    elif rebalance_dates >= 3 and avg_excess < 0.005:
        strictness = "medium"
        quantile = 0.65
        weak_market_action = "top3_only"
    else:
        strictness = "loose"
        quantile = 0.50

    feedback_level = str(feedback.get("feedback_level", "") or "").strip().lower()
    feedback_change_total = int(feedback.get("change_total", 0) or 0)
    if feedback_level == "tighten":
        strictness = "tight"
        quantile = max(quantile, 0.8)
        if feedback_change_total >= 2:
            weak_market_action = "cash_preferred"
            mode = "top1"
        else:
            weak_market_action = "top1_only"
    elif feedback_level == "review":
        if strictness == "loose":
            strictness = "medium"
        quantile = max(quantile, 0.7)
        if weak_market_action == "normal":
            weak_market_action = "top3_only"
    elif feedback_level == "reinforce":
        if strictness == "medium" and avg_excess >= 0.005:
            strictness = "loose"
            quantile = min(quantile, 0.55)

    if candidate_pool.empty:
        return {
            "selection_mode": mode,
            "min_confidence": 0.0,
            "min_pred_return": 0.0,
            "min_final_score": 0.0,
            "strictness": strictness,
            "weak_market_action": weak_market_action,
            "validation_rebalance_dates": rebalance_dates,
            "feedback_level": feedback_level or "-",
            "feedback_change_total": feedback_change_total,
            "feedback_window_label": str(feedback.get("window_label", "-") or "-"),
            "feedback_summary_note": str(feedback.get("summary_note", "-") or "-"),
        }

    return {
        "selection_mode": mode,
        "min_confidence": round(max(0.45, _safe_quantile(candidate_pool["confidence"], quantile, 0.60)), 4),
        "min_pred_return": round(max(0.0, _safe_quantile(candidate_pool["pred_return"], quantile, 0.01)), 4),
        "min_final_score": round(max(0.0, _safe_quantile(candidate_pool["final_score"], quantile, 120.0)), 2),
        "strictness": strictness,
        "weak_market_action": weak_market_action,
        "validation_rebalance_dates": rebalance_dates,
        "avg_excess_return_5d": round(avg_excess, 4),
        "avg_basket_return_5d": round(avg_basket_return, 4),
        "basket_win_rate_5d": round(basket_win_rate, 4),
        "feedback_level": feedback_level or "-",
        "feedback_change_total": feedback_change_total,
        "feedback_window_label": str(feedback.get("window_label", "-") or "-"),
        "feedback_summary_note": str(feedback.get("summary_note", "-") or "-"),
        "variant_excess": {
            "diversified": round(diversified_excess, 4),
            "raw": round(raw_excess, 4),
            "top1": round(top1_excess, 4),
        },
    }


def _apply_candidate_strategy_profile(candidate_pool: pd.DataFrame, profile: dict[str, Any] | None) -> pd.DataFrame:
    if candidate_pool.empty or not isinstance(profile, dict) or not profile:
        return candidate_pool
    filtered = candidate_pool.copy()
    if float(profile.get("min_confidence", 0.0) or 0.0) > 0:
        filtered = filtered[filtered["confidence"].astype(float) >= float(profile.get("min_confidence", 0.0) or 0.0)]
    if float(profile.get("min_pred_return", 0.0) or 0.0) > 0:
        filtered = filtered[filtered["pred_return"].astype(float) >= float(profile.get("min_pred_return", 0.0) or 0.0)]
    if float(profile.get("min_final_score", 0.0) or 0.0) > 0:
        filtered = filtered[filtered["final_score"].astype(float) >= float(profile.get("min_final_score", 0.0) or 0.0)]
    if filtered.empty:
        weak_market_action = str(profile.get("weak_market_action", "normal") or "normal")
        fallback_count = 1 if weak_market_action == "cash_preferred" else 3
        filtered = candidate_pool.sort_values("final_score", ascending=False).head(fallback_count).copy()
    return filtered.reset_index(drop=True)


def _slice_history_until(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    normalized_date = _normalize_trade_date(trade_date)
    sliced = df[df["date"].astype(str) <= normalized_date].copy()
    return sliced.reset_index(drop=True)


def _forward_return_from_history(df: pd.DataFrame, trade_date: str, horizon: int = 5) -> float | None:
    if df.empty:
        return None
    history = df.sort_values("date").reset_index(drop=True)
    normalized_date = _normalize_trade_date(trade_date)
    indices = history.index[history["date"].astype(str) == normalized_date].tolist()
    if not indices:
        return None
    idx = indices[-1]
    future_idx = idx + int(horizon)
    if future_idx >= len(history):
        return None
    entry = float(history.iloc[idx]["close"])
    future = float(history.iloc[future_idx]["close"])
    if entry == 0:
        return None
    return future / entry - 1.0


def validate_recent_candidate_strategy(
    pm: PipelineManager,
    settings: dict[str, Any],
    universe_size: int,
    top_n: int,
    *,
    rebalance_count: int = 20,
    horizon: int = 5,
    strategy_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    trade_dates = load_recent_trade_dates(settings, rebalance_count, horizon=horizon)
    if not trade_dates:
        return {"summary": summarize_historical_validation([]), "records": []}

    records: list[dict[str, Any]] = []
    for trade_date in trade_dates:
        pool = select_universe_for_trade_date(settings, trade_date, universe_size)
        if not pool:
            continue

        pooled_training = pm._build_pooled_training_frame(pool)
        if pooled_training is None:
            continue
        pooled_df, feature_cols, target_col = pooled_training
        pooled_df = pooled_df[pooled_df["date"].astype(str) <= str(trade_date)].copy() if "date" in pooled_df.columns else pooled_df
        if len(pooled_df) < 200:
            continue
        pm.forecast_agent.train_models(pooled_df, feature_cols, target_col)

        results: list[dict[str, Any]] = []
        full_histories: dict[str, pd.DataFrame] = {}
        for code in pool:
            full_df = pm.data_agent.prepare_dataset(code)
            full_histories[code] = full_df
            hist = _slice_history_until(full_df, trade_date)
            if len(hist) < 80:
                continue
            featured = pm.feature_agent.build_features(hist.copy())
            frame, _, _ = pm.feature_agent.prepare_training_frame(featured)
            if frame.empty:
                continue
            regime_info = pm.regime_agent.detect_market_regime(featured)
            forecast_result = pm.forecast_agent.predict(featured, feature_cols, regime_info=regime_info)
            risk_info = pm.risk_agent.evaluate_trade_risk(featured, forecast_result, regime_info)
            signal_result = pm.signal_agent.generate_signal(featured, forecast_result, regime_info, risk_info)
            position_result = pm.position_agent.calculate_position_size(signal_result, risk_info, {"cash": 1_000_000})
            results.append({
                "ts_code": code,
                "regime_info": regime_info,
                "forecast_result": forecast_result,
                "risk_info": risk_info,
                "signal_result": signal_result,
                "position_result": position_result,
            })

        if not results:
            continue

        meta = load_stock_basic_map(settings, [item["ts_code"] for item in results])
        candidate_frame = _build_candidate_frame(results, stock_basic_map=meta)
        candidate_frame = _apply_candidate_strategy_profile(candidate_frame, strategy_profile)
        ranked_div = _rank_candidate_frame(candidate_frame, top_n=top_n, selection_mode="diversified")
        ranked_raw = _rank_candidate_frame(candidate_frame, top_n=top_n, selection_mode="raw")
        ranked_top1 = _rank_candidate_frame(candidate_frame, top_n=1, selection_mode="top1")
        if ranked_div.empty:
            continue

        def _realized_basket_return(ranked_df: pd.DataFrame) -> float | None:
            realized_returns: list[float] = []
            for _, row in ranked_df.iterrows():
                code = str(row["ts_code"])
                forward_return = _forward_return_from_history(full_histories.get(code, pd.DataFrame()), trade_date, horizon=horizon)
                if forward_return is None:
                    continue
                weight = float(row.get("portfolio_weight_after_risk", row.get("basket_weight_pct", 0.0)) or 0.0)
                realized_returns.append(weight * forward_return)
            if not realized_returns:
                return None
            return float(sum(realized_returns))

        universe_returns = [
            value
            for code, history in full_histories.items()
            for value in [_forward_return_from_history(history, trade_date, horizon=horizon)]
            if value is not None
        ]
        diversified_return = _realized_basket_return(ranked_div)
        raw_return = _realized_basket_return(ranked_raw)
        top1_return = _realized_basket_return(ranked_top1)
        if diversified_return is None or raw_return is None or top1_return is None or not universe_returns:
            continue

        records.append({
            "trade_date": trade_date,
            "basket_return_5d": diversified_return,
            "diversified_return_5d": diversified_return,
            "raw_return_5d": raw_return,
            "top1_return_5d": top1_return,
            "universe_return_5d": float(sum(universe_returns) / len(universe_returns)),
        })

    return {
        "summary": summarize_historical_validation(records),
        "variants": summarize_variant_comparison(records),
        "records": records,
    }


def _finalize_candidate_basket(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    return cs_finalize_candidate_basket(df)


def _basket_registration_pressure(summary: dict[str, Any]) -> tuple[float, float, float, float]:
    hard_overflow = max(float(summary.get("top_industry_weight", 0.0) or 0.0) - CONDITIONAL_MAX_INDUSTRY_WEIGHT, 0.0)
    target_overflow = max(float(summary.get("top_industry_weight", 0.0) or 0.0) - TARGET_MAX_INDUSTRY_WEIGHT, 0.0)
    risk_pressure = float(summary.get("risk_pressure_score", 0.0) or 0.0)
    score = -float(summary.get("calibrated_basket_return", summary.get("expected_basket_return", 0.0)) or 0.0)
    return (round(hard_overflow, 6), round(target_overflow, 6), round(risk_pressure, 6), round(score, 6))


def _rebalance_candidate_basket(candidate_pool: pd.DataFrame, top_n: int) -> tuple[pd.DataFrame, dict[str, Any]]:
    return cs_rebalance_candidate_basket(candidate_pool, top_n)


def _apply_conservative_weights(df: pd.DataFrame) -> pd.DataFrame:
    basket = df.copy().reset_index(drop=True)
    if basket.empty:
        return basket

    quality = (
        basket["calibrated_upside_win_rate"].astype(float) * 0.35
        + basket["calibrated_avg_return"].astype(float) * 6.0
        + basket["confidence"].astype(float) * 0.20
        + basket["model_agreement"].astype(float) * 0.15
        - basket["prediction_dispersion"].astype(float) * 0.40
    )
    quality = (quality - float(quality.min()) + 0.05).clip(lower=0.05)

    raw_weight = quality / float(quality.sum() or 1.0)
    basket["basket_weight_pct"] = raw_weight
    basket = _apply_portfolio_risk_overlay(basket)

    capped = basket["portfolio_weight_after_risk"].astype(float).clip(upper=0.20)
    total = float(capped.sum()) or 1.0
    basket["portfolio_weight_after_risk"] = (capped / total).round(4)
    rounded = basket["portfolio_weight_after_risk"].copy()
    if not rounded.empty:
        rounded.iloc[-1] = round(1.0 - float(rounded.iloc[:-1].sum()), 4)
    basket["portfolio_weight_after_risk"] = rounded
    basket["basket_weight_pct"] = basket["portfolio_weight_after_risk"]

    roles = []
    for idx, weight in enumerate(basket["portfolio_weight_after_risk"].astype(float).tolist()):
        if idx == 0 or weight >= 0.18:
            roles.append("defensive_core")
        elif weight >= 0.10:
            roles.append("defensive_satellite")
        else:
            roles.append("reserve")
    basket["basket_role"] = roles
    return basket


def _assign_single_name_basket(df: pd.DataFrame) -> pd.DataFrame:
    return cs_assign_single_name_basket(df)


def evaluate_basket_capacity_pressure(df: pd.DataFrame) -> dict[str, Any]:
    basket = df.copy()
    if basket.empty:
        return {
            "capacity_state": "unknown",
            "recommended_scale_profile": "hold",
            "scenarios": [],
            "worst_stress_score": 0.0,
        }

    def _numeric_column(column: str, default: float = 0.0) -> pd.Series:
        if column not in basket.columns:
            return pd.Series(default, index=basket.index, dtype=float)
        return pd.to_numeric(basket[column], errors="coerce").fillna(default)

    if "portfolio_weight_after_risk" in basket.columns:
        weights = pd.to_numeric(basket["portfolio_weight_after_risk"], errors="coerce").fillna(0.0)
    else:
        weights = _numeric_column("basket_weight_pct", 0.0)
    liquidity_score = _numeric_column("liquidity_score", 0.0)
    latest_amount = _numeric_column("latest_amount", 0.0)
    median_amount = _numeric_column("median_amount", 0.0)
    support_ratio = (latest_amount / median_amount.mask(median_amount <= 0.0)).fillna(0.0)

    scenarios = [
        ("base", 1.0, 0.0000),
        ("scale_x2", 2.0, 0.0008),
        ("scale_x5", 5.0, 0.0020),
        ("stress_x10", 10.0, 0.0040),
    ]
    rows: list[dict[str, Any]] = []
    worst_stress_score = 0.0
    for label, scale_multiple, slippage_cost in scenarios:
        effective_weight = weights * scale_multiple
        weight_pressure = (effective_weight - 0.20).clip(lower=0.0) * 100.0
        liquidity_gap = (0.62 - liquidity_score).clip(lower=0.0) * 18.0
        support_gap = (0.70 - support_ratio).clip(lower=0.0) * 14.0
        score = float((weight_pressure + liquidity_gap + support_gap).sum() + slippage_cost * 1000.0)
        worst_stress_score = max(worst_stress_score, score)
        rows.append(
            {
                "scenario": label,
                "scale_multiple": scale_multiple,
                "estimated_slippage_cost": round(slippage_cost, 4),
                "stress_score": round(score, 2),
            }
        )

    capacity_state = "scalable"
    recommended_scale_profile = "normal"
    if worst_stress_score >= 45.0:
        capacity_state = "stretched"
        recommended_scale_profile = "top1_only"
    elif worst_stress_score >= 24.0:
        capacity_state = "watch"
        recommended_scale_profile = "defensive"

    return {
        "capacity_state": capacity_state,
        "recommended_scale_profile": recommended_scale_profile,
        "scenarios": rows,
        "worst_stress_score": round(worst_stress_score, 2),
    }


def apply_validation_guardrail(
    ranked: pd.DataFrame,
    validation_result: dict[str, Any] | None,
    top_n: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    validation_summary = (validation_result or {}).get("summary", {}) or {}
    basket = ranked.copy()
    if basket.empty:
        return basket, {"mode": "empty", "reasons": []}

    basket_summary = summarize_candidate_basket(basket)
    capacity_pressure = evaluate_basket_capacity_pressure(basket)
    reasons: list[str] = []
    rebalance_dates = int(validation_summary.get("rebalance_dates", 0) or 0)
    avg_excess = float(validation_summary.get("avg_excess_return_5d", 0.0) or 0.0)
    basket_win_rate_5d = float(validation_summary.get("basket_win_rate_5d", 0.0) or 0.0)
    risk_pressure = float(basket_summary.get("risk_pressure_score", 0.0) or 0.0)
    variant_summary = (validation_result or {}).get("variants", {}) or {}
    diversified_excess = float((variant_summary.get("diversified", {}) or {}).get("avg_excess_return_5d", avg_excess) or avg_excess)
    raw_excess = float((variant_summary.get("raw", {}) or {}).get("avg_excess_return_5d", diversified_excess) or diversified_excess)

    if rebalance_dates >= 3 and diversified_excess < 0:
        reasons.append("negative_validation_excess")
    if rebalance_dates >= 3 and basket_win_rate_5d < 0.34:
        reasons.append("low_validation_win_rate")
    if rebalance_dates >= 5 and diversified_excess < raw_excess:
        reasons.append("diversification_not_helping")
    if risk_pressure > 120.0:
        reasons.append("high_risk_pressure")
    if str(capacity_pressure.get("capacity_state", "unknown")) == "stretched":
        reasons.append("capacity_stretched")
    elif str(capacity_pressure.get("capacity_state", "unknown")) == "watch":
        reasons.append("capacity_watch")

    if not reasons:
        basket["basket_guardrail_mode"] = "normal"
        basket["basket_guardrail_reason"] = ""
        return basket, {"mode": "normal", "reasons": [], "capacity_pressure": capacity_pressure}

    defensive_pool = basket.copy()
    defensive_pool = defensive_pool.sort_values(
        ["risk_level", "calibrated_upside_win_rate", "calibrated_avg_return", "risk_adjusted_score"],
        ascending=[True, False, False, False],
    ).reset_index(drop=True)
    defensive_pool = _diversify_top_candidates(defensive_pool, max(3, min(int(top_n), 5)))
    defensive_pool = _annotate_selected_subset(defensive_pool)
    defensive_pool = _apply_conservative_weights(defensive_pool)
    defensive_pool = defensive_pool.sort_values("risk_adjusted_score", ascending=False).reset_index(drop=True)
    if "rank" in defensive_pool.columns:
        defensive_pool["rank"] = range(1, len(defensive_pool) + 1)
    else:
        defensive_pool.insert(0, "rank", range(1, len(defensive_pool) + 1))
    defensive_pool["basket_guardrail_mode"] = "defensive"
    defensive_pool["basket_guardrail_reason"] = ",".join(reasons)
    defensive_pool["basket_risk_pressure_score"] = summarize_candidate_basket(defensive_pool).get("risk_pressure_score", 0.0)
    return defensive_pool, {"mode": "defensive", "reasons": reasons, "capacity_pressure": capacity_pressure}


def _build_candidate_frame(
    results: list[dict[str, Any]],
    stock_basic_map: dict[str, dict[str, str]] | None = None,
) -> pd.DataFrame:
    df = cs_build_candidate_frame(results, stock_basic_map=stock_basic_map)
    if df.empty:
        return df
    return cs_apply_cross_sectional_ranking(df)


def rank_candidates(
    results: list[dict[str, Any]],
    top_n: int,
    stock_basic_map: dict[str, dict[str, str]] | None = None,
    *,
    selection_mode: str = "diversified",
) -> pd.DataFrame:
    return cs_rank_candidates(results, top_n, stock_basic_map=stock_basic_map, selection_mode=selection_mode)


def _watch_candidate_is_executable(row: pd.Series) -> bool:
    return cs_watch_candidate_is_executable(row)


def _expand_preferred_pool_for_diversification(
    df: pd.DataFrame,
    preferred: pd.DataFrame,
    top_n: int,
) -> pd.DataFrame:
    return cs_expand_preferred_pool_for_diversification(df, preferred, top_n)


def _rank_candidate_frame(
    df: pd.DataFrame,
    top_n: int,
    *,
    selection_mode: str = "diversified",
) -> pd.DataFrame:
    return cs_rank_candidate_frame(df, top_n, selection_mode=selection_mode)


def write_candidate_outputs(
    df: pd.DataFrame,
    output_dir: Path,
    validation_result: dict[str, Any] | None = None,
    *,
    skipped_count: int = 0,
    guardrail: dict[str, Any] | None = None,
    generation_meta: dict[str, Any] | None = None,
    persist_history: bool = True,
    latest_tag: str = "latest",
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"candidates_top_{run_id}.csv"
    md_path = output_dir / f"candidates_top_{run_id}.md"
    summary_path = output_dir / f"candidates_basket_summary_{run_id}.json"
    validation_path = output_dir / f"candidates_basket_validation_{run_id}.json"
    audit_path = output_dir / f"candidates_audit_{run_id}.json"
    latest_csv = output_dir / f"candidates_top_{latest_tag}.csv"
    latest_md = output_dir / f"candidates_top_{latest_tag}.md"
    latest_summary = output_dir / f"candidates_basket_summary_{latest_tag}.json"
    latest_validation = output_dir / f"candidates_basket_validation_{latest_tag}.json"
    latest_audit = output_dir / f"candidates_audit_{latest_tag}.json"
    latest_gate = output_dir / f"candidate_data_quality_gate_{latest_tag}.json"
    latest_lineage = output_dir / f"candidate_lineage_{latest_tag}.json"
    guardrail = guardrail or {"mode": "normal", "reasons": []}
    generation_meta = generation_meta or {}
    validation_result = validation_result or {"summary": summarize_historical_validation([]), "records": []}
    original_candidate_count = int(len(df))
    candidate_codes = [str(code).strip() for code in df.get("ts_code", pd.Series(dtype=object)).dropna().tolist()]
    data_quality_report_path = output_dir / "data_quality_report_latest.json"
    data_quality_report: dict[str, Any] | None = None
    if data_quality_report_path.exists():
        data_quality_report = json.loads(data_quality_report_path.read_text(encoding="utf-8"))
        data_quality_gate = build_candidate_data_quality_gate(
            report=data_quality_report,
            candidate_codes=candidate_codes,
        )
    else:
        data_quality_gate = {
            "schema_version": "candidate_data_quality_gate.v1",
            "status": "failed",
            "generated_at": datetime.now().isoformat(),
            "report_schema_version": None,
            "candidate_count": len(candidate_codes),
            "blocked_count": len(candidate_codes),
            "review_count": 0,
            "blocked_codes": candidate_codes,
            "review_codes": [],
            "blocking_reasons": ["missing_data_quality_report"],
            "candidates": [
                {
                    "ts_code": code,
                    "quality_level": "blocked",
                    "quality_score": 0.0,
                    "blocking_reasons": ["missing_data_quality_report"],
                }
                for code in candidate_codes
            ],
        }
    quality_by_code = {str(item.get("ts_code") or ""): item for item in data_quality_gate.get("candidates", []) or []}
    df = df.copy()
    df["data_quality_score"] = [
        float((quality_by_code.get(str(code).strip()) or {}).get("quality_score", 0.0) or 0.0)
        for code in df.get("ts_code", pd.Series(dtype=object)).tolist()
    ]
    df["data_quality_level"] = [
        str((quality_by_code.get(str(code).strip()) or {}).get("quality_level", "blocked") or "blocked")
        for code in df.get("ts_code", pd.Series(dtype=object)).tolist()
    ]
    df["data_quality_blocking_reasons"] = [
        "|".join((quality_by_code.get(str(code).strip()) or {}).get("blocking_reasons", []) or [])
        for code in df.get("ts_code", pd.Series(dtype=object)).tolist()
    ]
    data_quality_removed_codes: list[str] = []
    data_quality_review_codes = [str(code).strip() for code in data_quality_gate.get("review_codes", []) or []]
    data_quality_gate_enforced = latest_tag == "latest"
    if data_quality_gate_enforced:
        blocked_codes = {str(code).strip() for code in data_quality_gate.get("blocked_codes", []) or []}
        if blocked_codes:
            keep_mask = ~df["ts_code"].astype(str).str.strip().isin(blocked_codes)
            data_quality_removed_codes = [str(code) for code in df.loc[~keep_mask, "ts_code"].tolist()]
            df = df.loc[keep_mask].copy()
            if "rank" in df.columns:
                df["rank"] = range(1, len(df) + 1)
        if data_quality_review_codes and not df.empty:
            review_mask = df["ts_code"].astype(str).str.strip().isin(set(data_quality_review_codes))
            if "basket_risk_flag" not in df.columns:
                df["basket_risk_flag"] = "ok"
            df.loc[review_mask, "basket_risk_flag"] = (
                df.loc[review_mask, "basket_risk_flag"].astype(str).replace({"": "ok"}) + "|data_quality_review"
            )

    basket_summary = summarize_candidate_basket(df)
    audit_rows = build_candidate_audit_rows(df)
    basket_summary["guardrail_mode"] = str(guardrail.get("mode", "normal"))
    basket_summary["guardrail_reasons"] = list(guardrail.get("reasons", []) or [])
    basket_summary["skipped_count"] = int(skipped_count)
    basket_summary["generation_degraded"] = bool(generation_meta.get("degraded", False))
    basket_summary["generation_reason"] = str(generation_meta.get("reason", "") or "")
    basket_summary["effective_universe_size"] = int(generation_meta.get("effective_universe_size", len(df)) or len(df))
    basket_summary["champion_version"] = str(generation_meta.get("champion_version", "") or "")
    basket_summary["champion_weights_applied"] = bool(generation_meta.get("champion_weights_applied", False))
    basket_summary["strategy_mode"] = str((generation_meta.get("strategy_profile", {}) or {}).get("selection_mode", "diversified") or "diversified")
    basket_summary["strategy_strictness"] = str((generation_meta.get("strategy_profile", {}) or {}).get("strictness", "-") or "-")
    basket_summary["strategy_weak_market_action"] = str((generation_meta.get("strategy_profile", {}) or {}).get("weak_market_action", "normal") or "normal")
    basket_summary["strategy_feedback_level"] = str((generation_meta.get("strategy_profile", {}) or {}).get("feedback_level", "-") or "-")
    basket_summary["strategy_feedback_change_total"] = int((generation_meta.get("strategy_profile", {}) or {}).get("feedback_change_total", 0) or 0)
    basket_summary["strategy_feedback_window_label"] = str((generation_meta.get("strategy_profile", {}) or {}).get("feedback_window_label", "-") or "-")
    basket_summary["capacity_pressure"] = dict((guardrail.get("capacity_pressure", {}) or {}))
    basket_summary["data_quality_gate_status"] = str(data_quality_gate.get("status", "failed"))
    basket_summary["data_quality_blocked_count"] = int(data_quality_gate.get("blocked_count", 0) or 0)
    basket_summary["data_quality_review_count"] = int(data_quality_gate.get("review_count", 0) or 0)
    basket_summary["data_quality_gate_enforced"] = bool(data_quality_gate_enforced)
    basket_summary["data_quality_original_candidate_count"] = int(original_candidate_count)
    basket_summary["data_quality_removed_count"] = int(len(data_quality_removed_codes))
    basket_summary["data_quality_removed_codes"] = data_quality_removed_codes
    basket_summary["data_quality_review_codes"] = data_quality_review_codes

    if persist_history:
        df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_csv(latest_csv, index=False, encoding="utf-8")
    if persist_history:
        summary_path.write_text(json.dumps(basket_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_summary.write_text(json.dumps(basket_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if persist_history:
        validation_path.write_text(json.dumps(validation_result, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_validation.write_text(json.dumps(validation_result, ensure_ascii=False, indent=2), encoding="utf-8")
    if persist_history:
        audit_path.write_text(json.dumps(audit_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_audit.write_text(json.dumps(audit_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    write_candidate_data_quality_gate(data_quality_gate, output_dir=output_dir, gate_name=latest_gate.name)

    lines = [
        "# 今日候选股票 TopN",
        f"\n生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"\n候选数量: {len(df)}",
        "\n## 组合展望",
        f"\n- 组合预期收益: {basket_summary['expected_basket_return']:.2%}",
        f"- 校准后预期收益: {basket_summary['calibrated_basket_return']:.2%}",
        f"- 组合胜率: {basket_summary['basket_win_rate']:.2%}",
        f"- Guardrail 模式: {basket_summary['guardrail_mode']}",
        f"- Guardrail 原因: {', '.join(basket_summary['guardrail_reasons']) if basket_summary['guardrail_reasons'] else '无'}",
        f"- 跳过样本数: {basket_summary['skipped_count']}",
        f"- 生成降级: {'是' if basket_summary['generation_degraded'] else '否'}",
        f"- 生成原因: {basket_summary['generation_reason'] or '无'}",
        f"- 有效股票池: {basket_summary['effective_universe_size']}",
        f"- 冠军版本: {basket_summary['champion_version'] or '无'}",
        f"- 冠军权重已应用: {'是' if basket_summary['champion_weights_applied'] else '否'}",
        f"- 策略模式: {basket_summary['strategy_mode']}",
        f"- 策略强度: {basket_summary['strategy_strictness']}",
        f"- 弱市动作: {basket_summary['strategy_weak_market_action']}",
        f"- 回灌级别: {basket_summary['strategy_feedback_level']}",
        f"- 回灌窗口: {basket_summary['strategy_feedback_window_label']}",
        f"- 回灌建议数: {basket_summary['strategy_feedback_change_total']}",
        f"- 最大单票权重: {basket_summary['max_single_weight']:.2%}",
        f"- 最大行业暴露: {basket_summary.get('top_industry', '')} {basket_summary['top_industry_weight']:.2%}".rstrip(),
        f"- 高风险权重: {basket_summary['high_risk_weight']:.2%}",
        f"- 集中度 HHI: {basket_summary['concentration_hhi']:.4f}",
        f"- 风险压力分: {basket_summary['risk_pressure_score']:.2f}",
        f"- 放大量状态: {basket_summary['capacity_pressure'].get('capacity_state', 'unknown')}",
        f"- 建议放大档位: {basket_summary['capacity_pressure'].get('recommended_scale_profile', 'hold')}",
        f"- 最差压力分: {float((basket_summary['capacity_pressure'].get('worst_stress_score', 0.0) or 0.0)):.2f}",
        "\n## 历史验证",
        f"\n- 验证调仓次数: {validation_result['summary'].get('rebalance_dates', 0)}",
        f"- 5日平均篮子收益: {validation_result['summary'].get('avg_basket_return_5d', 0.0):.2%}",
        f"- 5日平均Universe收益: {validation_result['summary'].get('avg_universe_return_5d', 0.0):.2%}",
        f"- 5日平均超额收益: {validation_result['summary'].get('avg_excess_return_5d', 0.0):.2%}",
        f"- 5日篮子胜率: {validation_result['summary'].get('basket_win_rate_5d', 0.0):.2%}",
        f"- Top1平均5日收益: {validation_result['summary'].get('avg_top1_return_5d', 0.0):.2%}",
        "\n| 排名 | 代码 | 名称 | 行业 | 信号 | 组合角色 | 篮子权重 | 风控后权重 | 风控标记 | 数据质量 | 综合分 | 上涨概率 | 校准胜率 | 预测收益 | 校准收益 | 置信度 | 风险等级 | 建议仓位 | 止损 | 止盈 | 理由 |",
        "|------|------|------|------|------|----------|----------|------------|----------|----------|--------|----------|----------|----------|----------|--------|----------|----------|------|------|------|",
    ]
    for _, row in df.iterrows():
        lines.append(
            f"| {int(row['rank'])} | {row['ts_code']} | {row.get('stock_name', '')} | {row.get('industry', '')} | "
            f"{row['signal']} | {row.get('basket_role', '')} | {row.get('basket_weight_pct', 0.0):.1%} | "
            f"{row.get('portfolio_weight_after_risk', row.get('basket_weight_pct', 0.0)):.1%} | {row.get('basket_risk_flag', 'ok')} | "
            f"{row.get('data_quality_level', 'blocked')} {float(row.get('data_quality_score', 0.0)):.1f} | {row['final_score']:.2f} | "
            f"{row['direction_prob_up']:.2%} | {row.get('calibrated_upside_win_rate', row['direction_prob_up']):.2%} | "
            f"{row['pred_return']:.2%} | {row.get('calibrated_avg_return', row['pred_return']):.2%} | {row['confidence']:.2%} | "
            f"{row['risk_level']} | {float(row.get('position_pct', 0.0)):.1%} | "
            f"{float(row.get('stop_loss', 0.0)):.2f} | {float(row.get('take_profit', 0.0)):.2f} | {row['reason']} |"
        )
    text = "\n".join(lines) + "\n"
    if persist_history:
        md_path.write_text(text, encoding="utf-8")
    latest_md.write_text(text, encoding="utf-8")
    lineage_output_paths = {
        "latest_csv": str(latest_csv),
        "latest_md": str(latest_md),
        "latest_summary": str(latest_summary),
        "latest_validation": str(latest_validation),
        "latest_audit": str(latest_audit),
        "latest_data_quality_gate": str(latest_gate),
        "data_quality_report": str(data_quality_report_path) if data_quality_report_path.exists() else "",
    }
    candidate_lineage = build_candidate_lineage(
        candidate_frame=df,
        run_id=f"candidate-{run_id}",
        output_paths=lineage_output_paths,
        data_quality_report=data_quality_report,
        data_quality_gate=data_quality_gate,
        generation_meta={**generation_meta, "run_id": f"candidate-{run_id}"},
        guardrail=guardrail,
        validation_result=validation_result,
        latest_tag=latest_tag,
    )
    write_candidate_lineage(candidate_lineage, output_dir=output_dir, lineage_name=latest_lineage.name)
    return {
        "csv_path": str(csv_path if persist_history else latest_csv),
        "md_path": str(md_path if persist_history else latest_md),
        "summary_path": str(summary_path if persist_history else latest_summary),
        "validation_path": str(validation_path if persist_history else latest_validation),
        "audit_path": str(audit_path if persist_history else latest_audit),
        "latest_csv": str(latest_csv),
        "latest_md": str(latest_md),
        "latest_summary": str(latest_summary),
        "latest_validation": str(latest_validation),
        "latest_audit": str(latest_audit),
        "latest_data_quality_gate": str(latest_gate),
        "latest_lineage": str(latest_lineage),
    }


def write_candidate_strategy_profile(output_dir: Path, profile: dict[str, Any]) -> str:
    path = _candidate_strategy_profile_path(output_dir)
    payload = dict(profile)
    payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def candidate_run_status_path(output_dir: Path) -> Path:
    return output_dir / "candidates_run_status_latest.json"


def _write_candidate_run_status(output_dir: Path, payload: dict[str, Any]) -> str:
    target = candidate_run_status_path(output_dir)
    base = {
        "status": "running",
        "stage": "-",
        "stage_label": "-",
        "started_at": "-",
        "updated_at": datetime.now().isoformat(),
        "finished_at": None,
        "elapsed_sec": 0.0,
        "results_ready": 0,
        "skipped_count": 0,
        "requested_universe_size": 0,
        "effective_universe_size": 0,
        "top_n": 0,
        "quick_mode": False,
        "skip_validation": False,
        "degraded": False,
        "generation_reason": "",
        "detail": "",
        "latest_summary_path": str(output_dir / "candidates_basket_summary_latest.json"),
        "latest_csv_path": str(output_dir / "candidates_top_latest.csv"),
        "latest_interim_summary_path": str(output_dir / "candidates_basket_summary_interim_latest.json"),
        "latest_interim_csv_path": str(output_dir / "candidates_top_interim_latest.csv"),
    }
    base.update(payload)
    base["updated_at"] = datetime.now().isoformat()
    save_json(base, str(target))
    return str(target)


def _apply_quick_mode(pm: PipelineManager, settings: dict[str, Any]) -> None:
    runtime_cfg = settings.get("runtime", {}) or {}
    trainer_params = pm.forecast_agent.trainer.params
    enabled_models = runtime_cfg.get("candidate_quick_enabled_models", ["logistic", "random_forest"])
    if isinstance(enabled_models, (list, tuple, set)):
        trainer_params["enabled_models"] = [str(name).strip() for name in enabled_models if str(name).strip()]
    logistic_cap = int(runtime_cfg.get("candidate_quick_logistic_max_iter", 200) or 200)
    rf_estimators_cap = int(runtime_cfg.get("candidate_quick_rf_estimators", 80) or 80)
    rf_depth_cap = int(runtime_cfg.get("candidate_quick_rf_max_depth", 4) or 4)

    logistic_params = dict(trainer_params.get("logistic", {}) or {})
    logistic_params["max_iter"] = min(int(logistic_params.get("max_iter", 500) or 500), logistic_cap)
    trainer_params["logistic"] = logistic_params

    rf_params = dict(trainer_params.get("random_forest", {}) or {})
    rf_params["n_estimators"] = min(int(rf_params.get("n_estimators", 200) or 200), rf_estimators_cap)
    rf_params["max_depth"] = min(int(rf_params.get("max_depth", 6) or 6), rf_depth_cap)
    trainer_params["random_forest"] = rf_params


def main() -> None:
    parser = argparse.ArgumentParser(description="生成今日可关注买入候选 TopN")
    parser.add_argument("--universe-size", type=int, default=100, help="从数据库抽取的流动性股票池数量，0表示全市场")
    parser.add_argument("--top-n", type=int, default=10, help="输出候选数量")
    parser.add_argument("--config-dir", default="config", help="配置目录")
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="跳过历史验证与 guardrail 回放，优先保证候选生成时效",
    )
    parser.add_argument(
        "--quick-mode",
        action="store_true",
        help="启用轻量候选生成模式，压缩模型训练成本，适合自动任务",
    )
    parser.add_argument(
        "--validation-rebalance-count",
        type=int,
        default=20,
        help="历史篮子验证的滚动回放次数",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    config_dir = resolve_project_path(args.config_dir)
    config_path = config_dir / "settings.yaml"
    settings = _load_settings(config_path)
    pool = select_universe_from_sqlite(settings, args.universe_size)
    if not pool:
        pool = settings.get("data", {}).get("stock_pool", ["000001.SZ"])

    print(f"Universe size: {len(pool)}")
    pm = PipelineManager(str(config_dir))
    if args.quick_mode:
        _apply_quick_mode(pm, settings)
    output_dir = resolve_project_path("data/experiments")
    strategy_profile = load_candidate_strategy_profile(output_dir)
    candidate_basket_feedback = load_candidate_basket_feedback()
    runtime_cfg = settings.get("runtime", {}) or {}
    interim_every = max(int(runtime_cfg.get("candidate_interim_every", 5) or 5), 1)
    interim_written = {"done": False}
    started_wall = datetime.now().isoformat()
    started_mono = time.monotonic()

    def _record_stage(
        stage: str,
        stage_label: str,
        *,
        status: str = "running",
        detail: str = "",
        results_ready: int = 0,
        skipped_count: int = 0,
        effective_universe_size: int = 0,
        degraded: bool = False,
        generation_reason: str = "",
        finished: bool = False,
        extra: dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "status": status,
            "stage": stage,
            "stage_label": stage_label,
            "started_at": started_wall,
            "finished_at": datetime.now().isoformat() if finished else None,
            "elapsed_sec": round(time.monotonic() - started_mono, 2),
            "results_ready": int(results_ready),
            "skipped_count": int(skipped_count),
            "requested_universe_size": int(args.universe_size),
            "effective_universe_size": int(effective_universe_size),
            "top_n": int(args.top_n),
            "quick_mode": bool(args.quick_mode),
            "skip_validation": bool(args.skip_validation),
            "degraded": bool(degraded),
            "generation_reason": str(generation_reason or ""),
            "detail": str(detail or ""),
        }
        if extra:
            payload.update(extra)
        _write_candidate_run_status(output_dir, payload)

    _record_stage(
        "initializing",
        "初始化候选主链",
        detail="读取配置、候选策略 profile 和回灌状态",
    )

    def _maybe_write_interim(results: list[dict[str, Any]], skipped: list[dict[str, str]]) -> None:
        if not results:
            return
        _record_stage(
            "batch_prediction_running",
            "批量预测运行中",
            detail="正在生成中间候选结果",
            results_ready=len(results),
            skipped_count=len(skipped),
            effective_universe_size=len(results) + len(skipped),
        )
        if len(results) % interim_every != 0 and not skipped:
            return
        if interim_written["done"] and len(results) < max(interim_every * 2, 10):
            return
        interim_meta = load_stock_basic_map(settings, [r.get("ts_code", "") for r in results if r.get("ts_code")])
        interim_ranked = rank_candidates(results, min(args.top_n, len(results)), stock_basic_map=interim_meta)
        if interim_ranked.empty:
            return
        write_candidate_outputs(
            interim_ranked,
            output_dir,
            validation_result={"summary": summarize_historical_validation([]), "records": []},
            skipped_count=len(skipped),
            guardrail={"mode": "interim", "reasons": ["partial_generation"]},
            generation_meta={
                "degraded": True,
                "reason": "interim_partial_generation",
                "effective_universe_size": len(results) + len(skipped),
            },
            persist_history=False,
            latest_tag="interim_latest",
        )
        interim_written["done"] = True
        _record_stage(
            "interim_outputs_written",
            "中间产物已写盘",
            detail="interim_latest 候选结果已刷新",
            results_ready=len(results),
            skipped_count=len(skipped),
            effective_universe_size=len(results) + len(skipped),
            degraded=True,
            generation_reason="interim_partial_generation",
        )

    out = {}
    try:
        _record_stage(
            "universe_ready",
            "股票池已确定",
            detail=f"候选股票池 {len(pool)} 只",
            effective_universe_size=len(pool),
        )
        batch_result = pm.run_batch_prediction(pool, progress_callback=_maybe_write_interim)
        results = batch_result.get("results", [])
        skipped = batch_result.get("skipped", [])
        generation_meta = {
            "degraded": bool(batch_result.get("degraded", False)),
            "reason": str(batch_result.get("degradation_reason", "") or ""),
            "effective_universe_size": len(results) + len(skipped),
            "champion_version": str((batch_result.get("champion_profile", {}) or {}).get("champion_version", "") or ""),
            "champion_weights_applied": bool((batch_result.get("champion_profile", {}) or {}).get("champion_weights_applied", False)),
        }
        _record_stage(
            "batch_prediction_completed",
            "批量预测完成",
            detail="开始构建候选打分与排序",
            results_ready=len(results),
            skipped_count=len(skipped),
            effective_universe_size=int(generation_meta["effective_universe_size"]),
            degraded=bool(generation_meta["degraded"]),
            generation_reason=str(generation_meta["reason"]),
        )
        meta = load_stock_basic_map(settings, [r.get("ts_code", "") for r in results if r.get("ts_code")])
        candidate_frame = _build_candidate_frame(results, stock_basic_map=meta)
        candidate_frame = _apply_candidate_strategy_profile(candidate_frame, strategy_profile)
        _record_stage(
            "candidate_frame_ready",
            "候选打分完成",
            detail=f"进入排序的候选数 {len(candidate_frame)}",
            results_ready=len(results),
            skipped_count=len(skipped),
            effective_universe_size=int(generation_meta["effective_universe_size"]),
            degraded=bool(generation_meta["degraded"]),
            generation_reason=str(generation_meta["reason"]),
        )
        preferred_mode = str(strategy_profile.get("selection_mode", "diversified") or "diversified")
        weak_market_action = str(strategy_profile.get("weak_market_action", "normal") or "normal")
        if weak_market_action == "top1_only":
            preferred_mode = "top1"
            effective_top_n = 1
        elif weak_market_action == "cash_preferred":
            preferred_mode = "top1"
            effective_top_n = 1
            candidate_frame = candidate_frame.sort_values("final_score", ascending=False).head(1).copy()
        elif weak_market_action == "top3_only" and preferred_mode == "diversified":
            effective_top_n = min(int(args.top_n), 3)
        else:
            effective_top_n = int(args.top_n)
        ranked = _rank_candidate_frame(candidate_frame, effective_top_n, selection_mode=preferred_mode)
        _record_stage(
            "ranking_completed",
            "候选排序完成",
            detail=f"输出候选数 {len(ranked)}，准备进入验证与 guardrail",
            results_ready=len(results),
            skipped_count=len(skipped),
            effective_universe_size=int(generation_meta["effective_universe_size"]),
            degraded=bool(generation_meta["degraded"]),
            generation_reason=str(generation_meta["reason"]),
            extra={"strategy_mode": preferred_mode},
        )
        if args.skip_validation:
            validation_result = {"summary": summarize_historical_validation([]), "records": []}
            guardrail = {"mode": "validation_skipped", "reasons": ["auto_runtime_budget"]}
        else:
            _record_stage(
                "validation_running",
                "历史验证运行中",
                detail="开始执行 validation 和 guardrail 回放",
                results_ready=len(results),
                skipped_count=len(skipped),
                effective_universe_size=int(generation_meta["effective_universe_size"]),
                degraded=bool(generation_meta["degraded"]),
                generation_reason=str(generation_meta["reason"]),
                extra={"strategy_mode": preferred_mode},
            )
            validation_result = validate_recent_candidate_strategy(
                pm,
                settings,
                args.universe_size,
                args.top_n,
                rebalance_count=max(int(args.validation_rebalance_count or 0), 1),
                strategy_profile=strategy_profile,
            )
            ranked, guardrail = apply_validation_guardrail(ranked, validation_result, args.top_n)
            evolved_profile = evolve_candidate_strategy_profile(
                candidate_frame,
                validation_result,
                candidate_feedback=candidate_basket_feedback,
            )
            strategy_profile_path = write_candidate_strategy_profile(output_dir, evolved_profile)
            generation_meta["strategy_profile"] = evolved_profile
            generation_meta["strategy_profile_path"] = strategy_profile_path
        _record_stage(
            "writing_outputs",
            "写入候选产物",
            detail="正在刷新 latest 候选、摘要、验证和 audit 文件",
            results_ready=len(results),
            skipped_count=len(skipped),
            effective_universe_size=int(generation_meta["effective_universe_size"]),
            degraded=bool(generation_meta["degraded"]),
            generation_reason=str(generation_meta["reason"]),
            extra={
                "strategy_mode": str((generation_meta.get("strategy_profile", {}) or {}).get("selection_mode", preferred_mode) or preferred_mode),
                "guardrail_mode": str(guardrail.get("mode", "") or ""),
            },
        )
        out = write_candidate_outputs(
            ranked,
            output_dir,
            validation_result=validation_result,
            skipped_count=len(skipped),
            guardrail=guardrail,
            generation_meta=generation_meta,
        )
        _record_stage(
            "completed",
            "候选主链完成",
            status="completed",
            detail="latest 候选与篮子摘要已刷新",
            results_ready=len(results),
            skipped_count=len(skipped),
            effective_universe_size=int(generation_meta["effective_universe_size"]),
            degraded=bool(generation_meta["degraded"]),
            generation_reason=str(generation_meta["reason"]),
            finished=True,
            extra={
                "strategy_mode": str((generation_meta.get("strategy_profile", {}) or {}).get("selection_mode", preferred_mode) or preferred_mode),
                "guardrail_mode": str(guardrail.get("mode", "") or ""),
                "latest_summary_path": str(out.get("latest_summary", "")),
                "latest_csv_path": str(out.get("latest_csv", "")),
                "latest_validation_path": str(out.get("latest_validation", "")),
                "latest_audit_path": str(out.get("latest_audit", "")),
                "latest_lineage_path": str(out.get("latest_lineage", "")),
            },
        )
        record_manual_run(
            run_type="candidates",
            ok=True,
            detail=f"候选股生成成功(top_n={args.top_n})",
            config_dir=args.config_dir,
            meta={
                "mode": "quick" if args.quick_mode else "full",
                "skip_validation": bool(args.skip_validation),
                "effective_universe_size": int(generation_meta["effective_universe_size"]),
                "elapsed_sec": round(time.monotonic() - started_mono, 2),
                "used_attempt": int(args.universe_size),
                "champion_version": str(generation_meta.get("champion_version", "") or ""),
                "champion_weights_applied": bool(generation_meta.get("champion_weights_applied", False)),
                "guardrail_mode": str(guardrail.get("mode", "") or ""),
                "strategy_mode": str((generation_meta.get("strategy_profile", {}) or {}).get("selection_mode", preferred_mode) or preferred_mode),
            },
        )
    except Exception as exc:
        record_manual_run(
            run_type="candidates",
            ok=False,
            detail=str(exc),
            config_dir=args.config_dir,
            meta={
                "mode": "quick" if args.quick_mode else "full",
                "skip_validation": bool(args.skip_validation),
                "elapsed_sec": round(time.monotonic() - started_mono, 2),
                "used_attempt": int(args.universe_size),
            },
        )
        _record_stage(
            "failed",
            "候选主链失败",
            status="failed",
            detail=str(exc),
            effective_universe_size=len(pool),
            finished=True,
        )
        raise
    print(f"Predictions succeeded: {len(results)}")
    print(f"Skipped due to insufficient/invalid samples: {len(skipped)}")
    if generation_meta["degraded"]:
        print(f"Generation degraded: {generation_meta['reason']}")
    print(f"Guardrail mode: {guardrail.get('mode', 'normal')}")
    if guardrail.get("reasons"):
        print(f"Guardrail reasons: {','.join(guardrail['reasons'])}")
    print(f"Strategy mode: {(generation_meta.get('strategy_profile', {}) or {}).get('selection_mode', strategy_profile.get('selection_mode', 'diversified'))}")
    if args.skip_validation:
        print("Historical validation: skipped")
    if args.quick_mode:
        enabled = pm.forecast_agent.trainer.params.get("enabled_models", [])
        print(f"Quick mode: {','.join(enabled) if enabled else 'enabled'}")
    print("Top candidates generated:")
    print(f"- CSV: {out['latest_csv']}")
    print(f"- Markdown: {out['latest_md']}")
    print(f"- Basket summary: {out['latest_summary']}")
    print(f"- Validation summary: {out['latest_validation']}")
    if not ranked.empty:
        print(ranked[["rank", "ts_code", "signal", "final_score"]].to_string(index=False))


if __name__ == "__main__":
    main()
