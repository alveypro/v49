"""Runtime handlers that connect OpenClaw adapter to existing local strategy modules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
import importlib.util
import logging
import os
import threading
from contextlib import contextmanager
from time import perf_counter
from types import ModuleType

import pandas as pd
import numpy as np
from data.dao import resolve_db_path as resolve_db_path_dao
from openclaw.runtime.async_env import temp_environ as runtime_temp_environ
from openclaw.services.strategy_governance_service import annotate_strategy_output, latest_daily_trade_date

from openclaw.runtime.streamlit_stub import install_streamlit_stub
from openclaw.runtime.v6_backtest_context import build_v6_runtime_diagnostics, v6_point_in_time_context
from openclaw.runtime.v8_signal_evaluator import build_v8_suppression_diagnostics
from openclaw.services.ai_runtime_backtest_contract_service import build_ai_runtime_backtest_contract_failure
from openclaw.services.stable_defensive_allocator_service import build_stable_defensive_allocator_review

JsonDict = Dict[str, Any]
_ENV_LOCK = threading.RLock()
_V6_DIMENSION_REFERENCE_SCORES: Dict[str, float] = {
    "资金流向": 22.0,
    "板块热度": 18.0,
    "短期动量": 18.0,
    "龙头属性": 10.0,
    "相对强度": 8.0,
    "技术突破": 5.0,
    "安全边际": 2.0,
}


def _optional_positive_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _run_engine_rolling(engine: Any, **kwargs: Any) -> Tuple[pd.DataFrame, int]:
    if kwargs.get("max_evaluations") is None:
        kwargs.pop("max_evaluations", None)
    try:
        return engine.run_rolling(**kwargs)
    except TypeError as exc:
        if "max_evaluations" not in str(exc):
            raise
        kwargs.pop("max_evaluations", None)
        return engine.run_rolling(**kwargs)


def _call_combo_backtest(analyzer: Any, df_bt: pd.DataFrame, **kwargs: Any) -> JsonDict:
    clean = {key: value for key, value in kwargs.items() if value is not None}
    try:
        return analyzer.backtest_combo_production(df_bt, **clean)
    except TypeError as exc:
        if "unexpected keyword argument" not in str(exc):
            raise
        clean.pop("combo_replay_step", None)
        clean.pop("max_evaluations", None)
        clean.pop("max_stop_loss_pct", None)
        clean.pop("max_take_profit_pct", None)
        return analyzer.backtest_combo_production(df_bt, **clean)


def execute_offline_scan_strategy(
    *,
    strategy: str,
    env_overrides: Dict[str, Any],
    analyzer_factory: Callable[[], Any],
    scan_handlers: Dict[str, Callable[[Any], Tuple[Optional[pd.DataFrame], Dict[str, Any]]]],
    v7_scan_handler: Callable[[], Tuple[Optional[pd.DataFrame], Dict[str, Any]]],
    temp_environ: Callable[[Dict[str, Any]], Any],
) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    normalized = str(strategy or "").strip().lower()
    with temp_environ(env_overrides):
        if normalized == "v7":
            df, meta = v7_scan_handler()
            return _attach_scan_governance(df, meta, normalized)
        handler = scan_handlers.get(normalized)
        if handler is None:
            raise ValueError(f"不支持的后台扫描策略: {normalized}")
        df, meta = handler(analyzer_factory())
        return _attach_scan_governance(df, meta, normalized)


def _attach_scan_governance(
    df: Optional[pd.DataFrame],
    meta: Dict[str, Any],
    strategy: str,
) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    meta_out = dict(meta or {})
    data_asof = str(
        meta_out.get("data_asof")
        or meta_out.get("db_last_trade_date")
        or latest_daily_trade_date(os.getenv("PERMANENT_DB_PATH", ""))
        or ""
    )
    run_id = str(os.getenv("OPENCLAW_ASYNC_SCAN_RUN_ID", "") or meta_out.get("run_id") or "")
    df_out = annotate_strategy_output(df, strategy, run_id=run_id, data_asof=data_asof)
    if isinstance(df_out, pd.DataFrame) and not df_out.empty:
        for key in ("strategy_version", "strategy_tier", "strategy_stage", "code_version", "data_asof", "run_id", "evidence_status"):
            if key in df_out.columns:
                meta_out[key] = str(df_out[key].iloc[0])
    return df_out, meta_out


@dataclass
class HandlerFactory:
    module_path: Path

    def create_scan_handler(self, strategy: str):
        strategy = strategy.lower()

        def _handler(params: Optional[JsonDict] = None) -> JsonDict:
            params = params or {}
            run_id = str(params.get("run_id") or "")
            try:
                np.random.seed(int(params.get("fixed_seed", os.getenv("OPENCLAW_WF_FIXED_SEED", "20260512")) or 20260512))
            except Exception:
                np.random.seed(20260512)
            db_path = _resolve_db_path(params.get("db_path"))
            offline_limit = int(params.get("offline_stock_limit", 300))
            temp_env = {
                "OFFLINE_ONLY": strategy,
                "OFFLINE_MODE": "1",
                "OFFLINE_STOCK_LIMIT": str(max(0, offline_limit)),
                "PERMANENT_DB_PATH": str(db_path),
                f"{strategy.upper()}_SELECT_MODE": "阈值筛选",
            }
            if "score_threshold" in params:
                temp_env[f"{strategy.upper()}_SCORE_THRESHOLD"] = str(params["score_threshold"])
                if strategy == "combo":
                    temp_env["COMBO_THRESHOLD"] = str(params["score_threshold"])
            if strategy == "combo":
                for component in ("v5", "v8", "v9"):
                    key = f"thr_{component}"
                    if key in params:
                        temp_env[f"COMBO_THR_{component.upper()}"] = str(params[key])

            with _temp_environ(temp_env):
                module = _load_module(self.module_path)
                if not hasattr(module, "run_offline_all"):
                    raise RuntimeError(f"run_offline_all not found in {self.module_path}")
                out = module.run_offline_all()

            raw = out.get(strategy)
            df, meta = _extract_df_and_meta(raw)
            meta = dict(meta or {})
            if run_id:
                meta["run_id"] = run_id
            df, meta = _attach_scan_governance(df, meta, strategy)

            picks = _df_to_picks(df, strategy, limit=int(params.get("limit", 30)))
            return {
                "picks": picks,
                "metrics": {
                    "count": len(picks),
                    "raw_rows": 0 if df is None else len(df),
                },
                "meta": meta,
            }

        return _handler

    def create_backtest_handler(self, strategy: str):
        strategy = strategy.lower()
        module_cache: Optional[ModuleType] = None
        analyzer_cache: Dict[str, Any] = {}
        frame_cache: Dict[Tuple[str, int, int], pd.DataFrame] = {}

        def _get_module() -> ModuleType:
            nonlocal module_cache
            if module_cache is None:
                module_cache = _load_module(self.module_path)
            return module_cache

        def _get_analyzer(db_path: Path) -> Any:
            key = str(db_path)
            analyzer = analyzer_cache.get(key)
            if analyzer is None:
                module = _get_module()
                analyzer_cls = getattr(module, "CompleteVolumePriceAnalyzer", None)
                if analyzer_cls is None:
                    raise RuntimeError("CompleteVolumePriceAnalyzer not found in module")
                analyzer = analyzer_cls(db_path=key)
                _patch_analyzer_stats_signal_strength(analyzer)
                analyzer_cache[key] = analyzer
            return analyzer

        def _get_backtest_frame(db_path: Path, lookback_days: int, sample_size: int) -> pd.DataFrame:
            key = (str(db_path), int(lookback_days), int(sample_size))
            df = frame_cache.get(key)
            if df is None:
                df = _load_backtest_frame(
                    db_path=db_path,
                    lookback_days=int(lookback_days),
                    sample_size=int(sample_size),
                )
                frame_cache[key] = df
            # Strategy methods may mutate columns; isolate each run from cache side effects.
            return df.copy()

        def _handler(params: Optional[JsonDict] = None) -> JsonDict:
            params = params or {}
            run_id = str(params.get("run_id") or "")
            try:
                np.random.seed(int(params.get("fixed_seed", os.getenv("OPENCLAW_WF_FIXED_SEED", "20260512")) or 20260512))
            except Exception:
                np.random.seed(20260512)

            if strategy in {"ai"}:
                raw = build_ai_runtime_backtest_contract_failure(strategy=strategy)
                return {
                    "status": "failed",
                    "summary": {"win_rate": 0.0, "max_drawdown": 1.0, "signal_density": 0.0},
                    "raw": raw,
                    "meta": {"run_id": run_id} if run_id else {},
                }

            db_path = _resolve_db_path(params.get("db_path"))

            if strategy == "v6":
                analyzer = _get_analyzer(db_path)
                module = _get_module()
                df_bt = _get_backtest_frame(
                    db_path=db_path,
                    lookback_days=int(params.get("lookback_days", 320)),
                    sample_size=int(params.get("sample_size", 500)),
                )
                if df_bt.empty:
                    return {
                        "status": "failed",
                        "summary": _default_skip_summary(strategy),
                        "raw": {"status": "failed", "reason": "empty_backtest_frame"},
                        "meta": {"run_id": run_id} if run_id else {},
                    }
                result = _run_v6_runtime_backtest(
                    module=module,
                    analyzer=analyzer,
                    df_bt=df_bt,
                    params=params,
                )
                status = "failed" if isinstance(result, dict) and result.get("success") is False else "success"
                stats_src = (result or {}).get("stats") if isinstance(result, dict) else None
                return {
                    "status": status,
                    "summary": _normalize_summary(
                        stats_src if isinstance(stats_src, dict) else result,
                        fallback_sample_size=int(params.get("sample_size", 400)),
                    ),
                    "raw": result,
                    "meta": {"run_id": run_id} if run_id else {},
                }

            if strategy == "v5":
                analyzer = _get_analyzer(db_path)
                module = _get_module()
                df_bt = _get_backtest_frame(
                    db_path=db_path,
                    lookback_days=int(params.get("lookback_days", 320)),
                    sample_size=int(params.get("sample_size", 500)),
                )
                if df_bt.empty:
                    return {
                        "status": "failed",
                        "summary": _default_skip_summary(strategy),
                        "raw": {"status": "failed", "reason": "empty_backtest_frame"},
                        "meta": {"run_id": run_id} if run_id else {},
                    }
                result = _run_v5_runtime_backtest(
                    module=module,
                    analyzer=analyzer,
                    df_bt=df_bt,
                    params=params,
                )
                status = "failed" if isinstance(result, dict) and result.get("success") is False else "success"
                stats_src = (result or {}).get("stats") if isinstance(result, dict) else None
                return {
                    "status": status,
                    "summary": _normalize_summary(
                        stats_src if isinstance(stats_src, dict) else result,
                        fallback_sample_size=int(params.get("sample_size", 500)),
                    ),
                    "raw": result,
                    "meta": {"run_id": run_id} if run_id else {},
                }

            if strategy in {"v4", "v7", "v8", "v9", "combo", "stable"}:
                analyzer = _get_analyzer(db_path)
                module = _get_module()
                df_bt = _get_backtest_frame(
                    db_path=db_path,
                    lookback_days=int(params.get("lookback_days", 320)),
                    sample_size=int(params.get("sample_size", 500)),
                )
                if df_bt.empty:
                    return {
                        "summary": _default_skip_summary(strategy),
                        "raw": {"status": "skipped", "reason": "empty_backtest_frame"},
                        "meta": {"run_id": run_id} if run_id else {},
                    }

                if strategy == "v4":
                    result = _run_v4_runtime_backtest(
                        module=module,
                        analyzer=analyzer,
                        df_bt=df_bt,
                        params=params,
                    )
                elif strategy == "stable":
                    result = _run_stable_uptrend_backtest(
                        module=module,
                        analyzer=analyzer,
                        df_bt=df_bt,
                        params=params,
                    )
                elif strategy == "v7":
                    result = _run_v7_runtime_backtest(
                        module=module,
                        analyzer=analyzer,
                        df_bt=df_bt,
                        params=params,
                    )
                elif strategy == "v8":
                    result = _run_v8_runtime_backtest(
                        module=module,
                        analyzer=analyzer,
                        df_bt=df_bt,
                        params=params,
                    )
                elif strategy == "v9":
                    result = analyzer.backtest_v9_midterm(
                        df_bt,
                        sample_size=int(params.get("sample_size", 500)),
                        holding_days=int(params.get("holding_days", 15)),
                        score_threshold=float(params.get("score_threshold", 60)),
                    )
                else:
                    combo_threshold = float(params.get("combo_threshold", params.get("score_threshold", 68)))
                    if hasattr(analyzer, "backtest_combo_production"):
                        result = _call_combo_backtest(
                            analyzer,
                            df_bt,
                            sample_size=int(params.get("sample_size", 500)),
                            holding_days=int(params.get("holding_days", 10)),
                            combo_threshold=combo_threshold,
                            min_agree=int(params.get("min_agree", 2)),
                            thr_v5=float(params.get("thr_v5", 60)),
                            thr_v8=float(params.get("thr_v8", 65)),
                            thr_v9=float(params.get("thr_v9", 60)),
                            combo_replay_step=_optional_positive_int(params.get("combo_replay_step")),
                            max_evaluations=_optional_positive_int(params.get("max_evaluations")),
                            max_stop_loss_pct=float(params.get("max_stop_loss_pct", 0.08)),
                            max_take_profit_pct=params.get("max_take_profit_pct"),
                        )
                    else:
                        # Legacy fallback: use an ensemble of available strategy backtests.
                        result = _run_combo_ensemble_backtest(analyzer, df_bt, params)

                stats_src = (result or {}).get("stats") if isinstance(result, dict) else None
                summary = _normalize_summary(
                    stats_src if isinstance(stats_src, dict) else result,
                    fallback_sample_size=int(params.get("sample_size", 500)),
                )
                status = "failed" if isinstance(result, dict) and result.get("success") is False else "success"
                return {"status": status, "summary": summary, "raw": result, "meta": {"run_id": run_id} if run_id else {}}

            raise RuntimeError(f"backtest handler not implemented for strategy={strategy}")

        return _handler


def _load_module(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"module path does not exist: {path}")
    try:
        import streamlit  # noqa: F401
    except Exception:
        # Offline OpenClaw scan should not hard depend on UI runtime.
        install_streamlit_stub()
    spec = importlib.util.spec_from_file_location("v49_runtime_module", str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to create import spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _import_backtest_module(module_name: str, fallback_rel_paths: list[str]) -> ModuleType:
    try:
        return __import__(module_name)
    except Exception:
        pass

    root = Path(__file__).resolve().parents[2]
    for rel in fallback_rel_paths:
        p = root / rel
        if not p.exists():
            continue
        spec = importlib.util.spec_from_file_location(f"legacy_{module_name}", str(p))
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    raise ModuleNotFoundError(
        f"No module named '{module_name}' (checked fallback paths: {fallback_rel_paths})"
    )


def _extract_df_and_meta(raw: Any) -> Tuple[Optional[pd.DataFrame], JsonDict]:
    if raw is None:
        return None, {}

    # run_offline_all usually returns (df, meta)
    if isinstance(raw, tuple) and len(raw) == 2:
        df = raw[0] if isinstance(raw[0], pd.DataFrame) else None
        meta = raw[1] if isinstance(raw[1], dict) else {}
        return df, meta

    if isinstance(raw, pd.DataFrame):
        return raw, {}

    if isinstance(raw, dict):
        if "df" in raw and isinstance(raw["df"], pd.DataFrame):
            return raw["df"], {k: v for k, v in raw.items() if k != "df"}
        # sector/other special outputs
        return None, raw

    return None, {}


def _df_to_picks(df: Optional[pd.DataFrame], strategy: str, limit: int = 30) -> List[JsonDict]:
    if df is None or df.empty:
        return []

    code_col = _find_col(df, ["股票代码", "ts_code", "code"])
    score_col = _find_col(df, ["综合评分", "最终评分", "总分", "评分", "final_score", "score"])
    reason_col = _find_col(df, ["推荐理由", "筛选理由", "reason"])

    work = df.copy()
    if score_col:
        work = work.sort_values(score_col, ascending=False)

    picks: List[JsonDict] = []
    for _, row in work.head(max(1, limit)).iterrows():
        ts_code = str(row[code_col]) if code_col else ""
        if not ts_code:
            continue
        score = float(row[score_col]) if score_col else 0.0
        reason = str(row[reason_col]) if reason_col else ""
        picks.append(
            {
                "ts_code": ts_code,
                "score": score,
                "strategy": strategy,
                "reason": reason,
            }
        )
    return picks


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _normalize_summary(result: Any, *, fallback_sample_size: Optional[int] = None) -> JsonDict:
    if not isinstance(result, dict):
        return {"win_rate": 0.0, "max_drawdown": 1.0, "signal_density": 0.0}

    # absorb different key styles from legacy scripts
    win_rate = result.get("win_rate", result.get("胜率", 0.0))
    max_drawdown = result.get(
        "max_drawdown",
        result.get("最大回撤", result.get("drawdown", result.get("最大回撤率", 0.0))),
    )
    total_trades = float(
        result.get(
            "total_trades",
            result.get("总交易次数", result.get("total_signals", result.get("信号总数", 0.0))),
        )
        or 0.0
    )
    sample_size_raw = result.get("sample_size", result.get("analyzed_stocks", result.get("分析股票数", None)))
    if sample_size_raw is None and fallback_sample_size is not None:
        sample_size_raw = fallback_sample_size
    sample_size = float(sample_size_raw or 500.0)

    # legacy scripts often return percent values for win_rate/max_drawdown
    win_rate = float(win_rate)
    if win_rate > 1.0:
        win_rate = win_rate / 100.0

    # legacy scripts may report drawdown as negative percentage, e.g. -21.9
    max_drawdown = abs(float(max_drawdown))
    if max_drawdown > 1.0:
        max_drawdown = max_drawdown / 100.0

    density = total_trades / sample_size if sample_size > 0 else 0.0

    out = {
        "win_rate": win_rate,
        "max_drawdown": max(0.0, max_drawdown),
        "signal_density": max(0.0, density),
    }
    for key in (
        "tradeability_filter_enabled",
        "volume_constraint_enabled",
        "skip_untradeable",
        "skip_volume",
        "skip_limit",
    ):
        if key in result:
            out[key] = result[key]
    if isinstance(result.get("trading_cost"), dict):
        out["trading_cost"] = result["trading_cost"]
    if isinstance(result.get("risk_diagnostics"), dict):
        out["risk_diagnostics"] = result["risk_diagnostics"]
    if isinstance(result.get("risk_control"), dict):
        out["risk_control"] = result["risk_control"]
    if isinstance(result.get("defensive_allocator"), dict):
        out["defensive_allocator"] = result["defensive_allocator"]
    return out


def _default_skip_summary(strategy: str) -> JsonDict:
    # Neutral summary to keep risk engine operational when backtest is intentionally skipped.
    if strategy == "stable":
        return {"win_rate": 0.58, "max_drawdown": 0.10, "signal_density": 0.02}
    if strategy == "combo":
        return {"win_rate": 0.57, "max_drawdown": 0.11, "signal_density": 0.02}
    return {"win_rate": 0.55, "max_drawdown": 0.12, "signal_density": 0.02}


def _load_backtest_frame(db_path: Path, lookback_days: int = 320, sample_size: int = 500) -> pd.DataFrame:
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=max(120, int(lookback_days)))
    start_s = start_dt.strftime("%Y%m%d")
    try:
        with pd.option_context("mode.chained_assignment", None):
            import sqlite3

            conn = sqlite3.connect(str(db_path), timeout=30)
            try:
                code_df = pd.read_sql_query(
                    """
                    SELECT d.ts_code
                    FROM daily_trading_data d
                    WHERE d.trade_date >= ?
                    GROUP BY d.ts_code
                    HAVING COUNT(*) >= 60
                    ORDER BY AVG(COALESCE(d.amount, 0)) DESC, MAX(d.trade_date) DESC, d.ts_code
                    LIMIT ?
                    """,
                    conn,
                    params=(start_s, int(sample_size)),
                )
                if code_df.empty:
                    code_df = pd.read_sql_query(
                        "SELECT ts_code FROM stock_basic ORDER BY ts_code LIMIT ?",
                        conn,
                        params=(int(sample_size),),
                    )
                codes = [str(x) for x in code_df["ts_code"].dropna().tolist()]
                if not codes:
                    return pd.DataFrame()
                ph = ",".join(["?"] * len(codes))
                params = [start_s] + codes
                df = pd.read_sql_query(
                    f"""
                    SELECT d.ts_code, d.trade_date, d.close_price, d.high_price, d.low_price,
                           d.vol, d.amount, d.pct_chg, d.turnover_rate,
                           COALESCE(b.name, d.ts_code) AS name,
                           COALESCE(b.industry, '') AS industry
                    FROM daily_trading_data d
                    LEFT JOIN stock_basic b ON d.ts_code = b.ts_code
                    WHERE d.trade_date >= ? AND d.ts_code IN ({ph})
                    ORDER BY d.trade_date, d.ts_code
                    """,
                    conn,
                    params=params,
                )
            finally:
                conn.close()
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df
    return df


def _patch_backtest_stock_pool(backtest_obj: Any) -> None:
    """Patch legacy backtest stock pool SQL for DBs without `is_active` column."""
    conn = getattr(backtest_obj, "conn", None)
    if conn is None:
        return

    try:
        cols_df = pd.read_sql_query("PRAGMA table_info(stock_basic)", conn)
        cols = set(cols_df["name"].astype(str).tolist())
    except Exception:
        return

    has_is_active = "is_active" in cols
    has_circ_mv = "circ_mv" in cols

    def _get_stock_pool(sample_size: int):
        where = []
        params: List[Any] = []
        if has_is_active:
            where.append("is_active = 1")
        if has_circ_mv:
            where.append("circ_mv >= ?")
            where.append("circ_mv <= ?")
            params.extend([1000000, 5000000])
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        query = f"""
            SELECT ts_code, name
            FROM stock_basic
            {where_sql}
            ORDER BY RANDOM()
            LIMIT ?
        """
        params.append(sample_size)
        df = pd.read_sql_query(query, conn, params=tuple(params))
        return list(zip(df["ts_code"], df["name"]))

    backtest_obj._get_stock_pool = _get_stock_pool


def _patch_analyzer_stats_signal_strength(analyzer: Any) -> None:
    """Legacy analyzers may miss `signal_strength` in backtest frames.

    Normalize before stats computation to avoid KeyError from older implementations.
    """
    fn = getattr(analyzer, "_calculate_backtest_stats", None)
    if fn is None or getattr(fn, "_openclaw_signal_strength_patched", False):
        return

    def _wrapped(backtest_df, *args, **kwargs):
        try:
            if isinstance(backtest_df, pd.DataFrame) and "signal_strength" not in backtest_df.columns:
                df2 = backtest_df.copy()
                if "score" in df2.columns:
                    df2["signal_strength"] = df2["score"]
                elif "final_score" in df2.columns:
                    df2["signal_strength"] = df2["final_score"]
                else:
                    df2["signal_strength"] = 0.0
                backtest_df = df2
        except Exception:
            pass
        return fn(backtest_df, *args, **kwargs)

    _wrapped._openclaw_signal_strength_patched = True  # type: ignore[attr-defined]
    setattr(analyzer, "_calculate_backtest_stats", _wrapped)


def _run_combo_ensemble_backtest(analyzer: Any, df_bt: pd.DataFrame, params: JsonDict) -> JsonDict:
    """Compose combo result from multiple strategy backtests for legacy modules."""
    sample_size = int(params.get("sample_size", 500))
    holding_days = int(params.get("holding_days", 10))
    combo_threshold = float(params.get("combo_threshold", params.get("score_threshold", 68)))
    components_raw = str(params.get("combo_components", "v8,v9,legacy")).strip().lower()
    enabled_components = {x.strip() for x in components_raw.split(",") if x.strip()}
    if "all" in enabled_components:
        enabled_components = {"v7", "v8", "v9", "legacy"}
    if not enabled_components:
        enabled_components = {"v8", "v9", "legacy"}
    # Keep combo ensemble responsive during parameter sweeps.
    sub_sample = max(30, min(sample_size, 60))

    components: Dict[str, JsonDict] = {}
    summaries: Dict[str, JsonDict] = {}

    def _run_component(name: str, fn):
        if name not in enabled_components:
            return
        try:
            out = fn()
            stats_src = out.get("stats") if isinstance(out, dict) else None
            summary = _normalize_summary(stats_src if isinstance(stats_src, dict) else out)
            ok = isinstance(out, dict) and bool(out.get("success", True))
            components[name] = {
                "status": "success" if ok else "failed",
                "summary": summary,
                "error": "" if ok else str(out.get("error", "unknown")) if isinstance(out, dict) else "unknown",
            }
            if ok:
                summaries[name] = summary
        except Exception as exc:
            components[name] = {"status": "failed", "summary": _default_skip_summary("combo"), "error": str(exc)}

    with _temp_log_level(logging.WARNING):
        _run_component(
            "v7",
            lambda: analyzer.backtest_v7_intelligent(
                df_bt,
                sample_size=sub_sample,
                holding_days=max(6, min(holding_days, 12)),
                score_threshold=max(55.0, min(75.0, combo_threshold)),
            ),
        )
        _run_component(
            "v8",
            lambda: analyzer.backtest_v8_ultimate(
                df_bt,
                sample_size=sub_sample,
                holding_days=max(6, min(holding_days, 10)),
                score_threshold=max(45.0, min(65.0, combo_threshold - 8.0)),
            ),
        )
        _run_component(
            "v9",
            lambda: analyzer.backtest_v9_midterm(
                df_bt,
                sample_size=sub_sample,
                holding_days=max(8, holding_days),
                score_threshold=max(55.0, min(75.0, combo_threshold + 2.0)),
            ),
        )
        _run_component(
            "legacy",
            lambda: analyzer.backtest_strategy_complete(
                df_bt,
                sample_size=sub_sample,
                signal_strength=(combo_threshold / 100.0 if combo_threshold > 1 else combo_threshold),
                holding_days=max(6, holding_days),
            ),
        )

    if not summaries:
        return {
            "success": False,
            "error": "combo ensemble has no successful component",
            "components": components,
            "stats": _default_skip_summary("combo"),
        }

    combo_summary = _weighted_combo_summary(summaries, sample_size=sub_sample)
    return {
        "success": True,
        "strategy": "combo_ensemble",
        "stats": {
            "win_rate": combo_summary["win_rate"],
            "max_drawdown": combo_summary["max_drawdown"],
            "total_signals": int(round(combo_summary["signal_density"] * sub_sample)),
            "sample_size": sub_sample,
        },
        "components": components,
    }


def _run_v4_runtime_backtest(*, module: ModuleType, analyzer: Any, df_bt: pd.DataFrame, params: JsonDict) -> JsonDict:
    engine_cls = getattr(module, "UnifiedBacktestEngine", None)
    evaluator = getattr(analyzer, "evaluator_v4", None)
    if engine_cls is None:
        return {"success": False, "error": "UnifiedBacktestEngine not found", "strategy": "v4"}
    if evaluator is None:
        return {"success": False, "error": "v4 evaluator not loaded", "strategy": "v4"}

    ensure_aliases = getattr(module, "_ensure_price_aliases", lambda x: x)
    sample_size = int(params.get("sample_size", 300))
    holding_days = int(params.get("holding_days", 5))
    score_threshold = float(params.get("score_threshold", 60))
    replay_step = int(params.get("replay_step", params.get("v4_replay_step", 20)))
    stop_loss = float(params.get("stop_loss", -0.04))
    take_profit = float(params.get("take_profit", 0.06))
    df_bt = ensure_aliases(df_bt.copy())
    engine = _make_unified_engine(engine_cls, df_bt, sample_size, holding_days, params)
    diagnostics = _new_score_distribution_diagnostics(strategy="v4", threshold=score_threshold)

    def _signal_v4(ts_code: str, g: pd.DataFrame, i: int, hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
        hist = ensure_aliases(hist)
        try:
            eval_result = evaluator.evaluate_stock_v4(hist)
        except Exception as exc:
            logging.getLogger(__name__).debug("v4 runtime replay failed for %s: %s", ts_code, exc)
            _record_score_diagnostic(diagnostics, score=None, reason="evaluator_exception")
            return None
        if not isinstance(eval_result, dict) or not eval_result.get("success"):
            _record_score_diagnostic(
                diagnostics,
                score=None,
                reason=_classify_unsuccessful_evaluation(eval_result, strategy="v4"),
            )
            return None
        final_score = float(eval_result.get("final_score", 0.0) or 0.0)
        _record_score_breakdown(
            diagnostics,
            eval_result,
            numeric_keys=("base_score", "synergy_bonus", "risk_penalty"),
            nested_numeric_keys=(("dim_scores", "dim"), ("dimension_scores", "dim")),
        )
        _record_score_diagnostic(diagnostics, score=final_score, reason="pass_threshold" if final_score >= score_threshold else "below_threshold")
        if final_score < score_threshold:
            return None
        close_col = "close" if "close" in g.columns else "close_price"
        buy_price = float(g.iloc[i][close_col])
        exit_day = int(holding_days)
        exit_reason = "holding_period"
        forced_exit_price: Optional[float] = None
        for day in range(1, int(holding_days) + 1):
            if i + day >= len(g):
                break
            current_price = float(g.iloc[i + day][close_col])
            ret = current_price / buy_price - 1.0 if buy_price > 0 else 0.0
            if ret <= stop_loss:
                exit_day = day
                exit_reason = "stop_loss"
                forced_exit_price = buy_price * (1.0 + stop_loss)
                break
            if ret >= take_profit:
                exit_day = day
                exit_reason = "take_profit"
                forced_exit_price = buy_price * (1.0 + take_profit)
                break
        signal = {
            "__exit_offset": int(exit_day),
            "score": final_score,
            "signal_strength": final_score,
            "stock_name": g.iloc[i].get("name", ts_code),
            "industry": g.iloc[i].get("industry", ""),
            "grade": eval_result.get("grade", ""),
            "exit_reason": exit_reason,
            "exit_day": int(exit_day),
        }
        if forced_exit_price is not None:
            signal["__exit_price"] = float(forced_exit_price)
            signal["execution_fill_model"] = "stop_take_price"
        return signal

    with _temp_log_level(logging.WARNING):
        backtest_df, analyzed = _run_engine_rolling(
            engine,
            min_rows=60 + holding_days,
            window=60,
            step=max(1, replay_step),
            signal_fn=_signal_v4,
            stop_on_first_signal=True,
            max_evaluations=_optional_positive_int(params.get("max_evaluations")),
        )
    if backtest_df.empty:
        return {
            "success": False,
            "error": "v4 runtime backtest produced no valid signals",
            "strategy": "v4",
            "stats": {"analyzed_stocks": analyzed},
            "backtest_diagnostics": _freeze_score_distribution_diagnostics(diagnostics),
        }
    stats = analyzer._calculate_backtest_stats(backtest_df.copy(), analyzed, holding_days)
    stats["risk_diagnostics"] = _build_v8_risk_diagnostics(backtest_df)
    stats["risk_control"] = {"stop_loss": float(stop_loss), "take_profit": float(take_profit)}
    if hasattr(engine, "execution_constraints_summary"):
        stats.update(engine.execution_constraints_summary())
    stats["replay_step"] = int(max(1, replay_step))
    return {
        "success": True,
        "strategy": "v4_runtime_replay",
        "stats": stats,
        "backtest_data": backtest_df,
        "backtest_diagnostics": _freeze_score_distribution_diagnostics(diagnostics),
    }


def _make_unified_engine(
    engine_cls: Any,
    df_bt: pd.DataFrame,
    sample_size: int,
    holding_days: int,
    params: JsonDict,
) -> Any:
    kwargs = {
        "sample_size": int(sample_size),
        "holding_days": int(holding_days),
    }
    if "fee_bps" in params:
        kwargs["fee_bps"] = float(params.get("fee_bps") or 0.0)
    if "slippage_bps" in params:
        kwargs["slippage_bps"] = float(params.get("slippage_bps") or 0.0)
    try:
        return engine_cls(df_bt, **kwargs)
    except TypeError:
        kwargs.pop("fee_bps", None)
        kwargs.pop("slippage_bps", None)
        return engine_cls(df_bt, **kwargs)


def _run_v5_runtime_backtest(*, module: ModuleType, analyzer: Any, df_bt: pd.DataFrame, params: JsonDict) -> JsonDict:
    engine_cls = getattr(module, "UnifiedBacktestEngine", None)
    evaluator = getattr(analyzer, "evaluator_v5", None)
    if engine_cls is None:
        return {"success": False, "error": "UnifiedBacktestEngine not found", "strategy": "v5"}
    if evaluator is None:
        return {"success": False, "error": "v5 evaluator not loaded", "strategy": "v5"}

    ensure_aliases = getattr(module, "_ensure_price_aliases", lambda x: x)
    sample_size = int(params.get("sample_size", 300))
    holding_days = int(params.get("holding_days", 5))
    score_threshold = float(params.get("score_threshold", 60))
    replay_step = int(params.get("replay_step", params.get("v5_replay_step", 20)))
    stop_loss = float(params.get("stop_loss", -0.03))
    take_profit = float(params.get("take_profit", 0.04))
    df_bt = ensure_aliases(df_bt.copy())
    engine = _make_unified_engine(engine_cls, df_bt, sample_size, holding_days, params)
    diagnostics = _new_score_distribution_diagnostics(strategy="v5", threshold=score_threshold)

    def _signal_v5(ts_code: str, g: pd.DataFrame, i: int, hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
        hist = ensure_aliases(hist)
        try:
            eval_result = evaluator.evaluate_stock_v4(hist)
        except Exception as exc:
            logging.getLogger(__name__).debug("v5 runtime replay failed for %s: %s", ts_code, exc)
            _record_score_diagnostic(diagnostics, score=None, reason="evaluator_exception")
            return None
        if not isinstance(eval_result, dict) or not eval_result.get("success"):
            _record_score_diagnostic(
                diagnostics,
                score=None,
                reason=_classify_unsuccessful_evaluation(eval_result, strategy="v5"),
            )
            return None
        final_score = float(eval_result.get("final_score", 0.0) or 0.0)
        _record_score_breakdown(
            diagnostics,
            eval_result,
            numeric_keys=("base_score", "synergy_bonus", "risk_penalty"),
            nested_numeric_keys=(("dim_scores", "dim"), ("dimension_scores", "dim")),
        )
        close_series = pd.to_numeric(hist.get("close_price", hist.get("close", pd.Series(dtype=float))), errors="coerce").dropna()
        pct_series = pd.to_numeric(hist.get("pct_chg", pd.Series(dtype=float)), errors="coerce").dropna()
        vol_series = pd.to_numeric(hist.get("vol", pd.Series(dtype=float)), errors="coerce").dropna()
        entry_features: JsonDict = {}
        if len(pct_series) >= 1:
            entry_features["entry_pct_chg_1d"] = float(pct_series.iloc[-1])
            _record_breakdown_value(diagnostics, "entry:pct_chg_1d", entry_features["entry_pct_chg_1d"])
        if len(pct_series) >= 3:
            entry_features["entry_pct_chg_3d"] = float(pct_series.tail(3).sum())
            _record_breakdown_value(diagnostics, "entry:pct_chg_3d", entry_features["entry_pct_chg_3d"])
        if len(pct_series) >= 5:
            entry_features["entry_pct_chg_5d"] = float(pct_series.tail(5).sum())
            _record_breakdown_value(diagnostics, "entry:pct_chg_5d", entry_features["entry_pct_chg_5d"])
        if len(vol_series) >= 20:
            base_vol = float(vol_series.tail(20).iloc[:-1].mean() or 0.0)
            if base_vol > 0:
                entry_features["entry_vol_ratio_20d"] = float(vol_series.iloc[-1] / base_vol)
                _record_breakdown_value(diagnostics, "entry:vol_ratio_20d", entry_features["entry_vol_ratio_20d"])
        if len(close_series) >= 20:
            recent_peak = float(close_series.tail(20).max() or 0.0)
            if recent_peak > 0:
                entry_features["entry_pullback_from_20d_high"] = float(close_series.iloc[-1] / recent_peak - 1.0)
                _record_breakdown_value(diagnostics, "entry:pullback_from_20d_high", entry_features["entry_pullback_from_20d_high"])
        _record_score_diagnostic(diagnostics, score=final_score, reason="pass_threshold" if final_score >= score_threshold else "below_threshold")
        if final_score < score_threshold:
            return None
        close_col = "close" if "close" in g.columns else "close_price"
        buy_price = float(g.iloc[i][close_col])
        exit_day = int(holding_days)
        exit_reason = "holding_period"
        forced_exit_price: Optional[float] = None
        for day in range(1, int(holding_days) + 1):
            if i + day >= len(g):
                break
            current_price = float(g.iloc[i + day][close_col])
            ret = current_price / buy_price - 1.0 if buy_price > 0 else 0.0
            if ret <= stop_loss:
                exit_day = day
                exit_reason = "stop_loss"
                forced_exit_price = buy_price * (1.0 + stop_loss)
                break
            if ret >= take_profit:
                exit_day = day
                exit_reason = "take_profit"
                forced_exit_price = buy_price * (1.0 + take_profit)
                break
        signal = {
            "__exit_offset": int(exit_day),
            "score": final_score,
            "signal_strength": final_score,
            "stock_name": g.iloc[i].get("name", ts_code),
            "industry": g.iloc[i].get("industry", ""),
            "grade": eval_result.get("grade", ""),
            "exit_reason": exit_reason,
            "exit_day": int(exit_day),
            **entry_features,
        }
        if forced_exit_price is not None:
            signal["__exit_price"] = float(forced_exit_price)
            signal["execution_fill_model"] = "stop_take_price"
        return signal

    with _temp_log_level(logging.WARNING):
        backtest_df, analyzed = _run_engine_rolling(
            engine,
            min_rows=60 + holding_days,
            window=60,
            step=max(1, replay_step),
            signal_fn=_signal_v5,
            stop_on_first_signal=True,
            max_evaluations=_optional_positive_int(params.get("max_evaluations")),
        )
    if backtest_df.empty:
        return {
            "success": False,
            "error": "v5 runtime backtest produced no valid signals",
            "strategy": "v5",
            "stats": {"analyzed_stocks": analyzed},
            "backtest_diagnostics": _freeze_score_distribution_diagnostics(diagnostics),
        }
    stats = analyzer._calculate_backtest_stats(backtest_df.copy(), analyzed, holding_days)
    stats["risk_diagnostics"] = _build_v8_risk_diagnostics(backtest_df)
    stats["risk_control"] = {"stop_loss": float(stop_loss), "take_profit": float(take_profit)}
    if hasattr(engine, "execution_constraints_summary"):
        stats.update(engine.execution_constraints_summary())
    stats["replay_step"] = int(max(1, replay_step))
    return {
        "success": True,
        "strategy": "v5_runtime_replay",
        "stats": stats,
        "backtest_data": backtest_df,
        "backtest_diagnostics": _freeze_score_distribution_diagnostics(diagnostics),
    }


def _run_v6_runtime_backtest(*, module: ModuleType, analyzer: Any, df_bt: pd.DataFrame, params: JsonDict) -> JsonDict:
    engine_cls = getattr(module, "UnifiedBacktestEngine", None)
    evaluator = getattr(analyzer, "evaluator_v6", None)
    if engine_cls is None:
        return {"success": False, "error": "UnifiedBacktestEngine not found", "strategy": "v6"}
    if evaluator is None:
        return {"success": False, "error": "v6 evaluator not loaded", "strategy": "v6"}

    ensure_aliases = getattr(module, "_ensure_price_aliases", lambda x: x)
    sample_size = int(params.get("sample_size", 300))
    holding_days = int(params.get("holding_days", 5))
    score_threshold = float(params.get("score_threshold", 75))
    replay_step = int(params.get("replay_step", params.get("v6_replay_step", 20)))
    stop_loss = float(params.get("stop_loss", -0.05))
    take_profit = float(params.get("take_profit", 0.08))
    candidate_filter_mode = str(params.get("candidate_filter_mode", "strict") or "strict")
    df_bt = ensure_aliases(df_bt.copy())
    engine = _make_unified_engine(engine_cls, df_bt, sample_size, holding_days, params)
    diagnostics = _new_score_distribution_diagnostics(strategy="v6", threshold=score_threshold)
    diagnostics["candidate_filter_mode"] = candidate_filter_mode
    point_in_time_cache: Dict[str, Any] = {}

    def _signal_v6(ts_code: str, g: pd.DataFrame, i: int, hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
        signal_start = perf_counter()
        hist = ensure_aliases(hist)
        try:
            as_of_date = str(hist["trade_date"].iloc[-1]) if "trade_date" in hist.columns and not hist.empty else str(g.iloc[i].get("trade_date", ""))
            context_start = perf_counter()
            with v6_point_in_time_context(evaluator, market_frame=df_bt, stock_hist=hist, as_of_date=as_of_date, cache=point_in_time_cache):
                _record_stage_timing(diagnostics, "point_in_time_context", (perf_counter() - context_start) * 1000.0)
                old_filter_mode = getattr(evaluator, "runtime_candidate_filter_mode", None)
                evaluator.runtime_candidate_filter_mode = candidate_filter_mode
                try:
                    eval_start = perf_counter()
                    eval_result = evaluator.evaluate_stock_v6(hist, ts_code)
                    _record_stage_timing(diagnostics, "evaluate_stock_v6", (perf_counter() - eval_start) * 1000.0)
                    runtime_diag = eval_result.get("runtime_diagnostics") if isinstance(eval_result, dict) else {}
                    _record_stage_timing_payload(
                        diagnostics,
                        "evaluate_stock_v6.",
                        runtime_diag.get("stage_timing_ms") if isinstance(runtime_diag, dict) else {},
                    )
                finally:
                    if old_filter_mode is None:
                        try:
                            delattr(evaluator, "runtime_candidate_filter_mode")
                        except AttributeError:
                            pass
                    else:
                        evaluator.runtime_candidate_filter_mode = old_filter_mode
        except Exception as exc:
            logging.getLogger(__name__).debug("v6 runtime replay failed for %s: %s", ts_code, exc)
            _record_score_diagnostic(diagnostics, score=None, reason="evaluator_exception")
            _record_stage_timing(diagnostics, "signal_total", (perf_counter() - signal_start) * 1000.0)
            return None
        if not isinstance(eval_result, dict) or not eval_result.get("success"):
            _record_score_diagnostic(
                diagnostics,
                score=None,
                reason=_classify_unsuccessful_evaluation(eval_result, strategy="v6"),
            )
            _record_stage_timing(diagnostics, "signal_total", (perf_counter() - signal_start) * 1000.0)
            return None
        final_score = float(eval_result.get("final_score", 0.0) or 0.0)
        if eval_result.get("candidate_filter_relaxed"):
            reason_counts = diagnostics.setdefault("reason_counts", {})
            reason_counts["candidate_filter_relaxed"] = int(reason_counts.get("candidate_filter_relaxed", 0) or 0) + 1
        _record_score_breakdown(
            diagnostics,
            eval_result,
            numeric_keys=("base_score", "synergy_bonus", "risk_penalty"),
            nested_numeric_keys=(("dimension_scores", "dim"),),
        )
        entry_gate = eval_result.get("entry_gate") if isinstance(eval_result.get("entry_gate"), dict) else {}
        entry_gate_passed = bool(entry_gate.get("passed", True))
        _record_entry_gate_diagnostic(diagnostics, entry_gate=entry_gate, final_score=final_score)
        _record_entry_gate_quality_gap(
            diagnostics,
            entry_gate=entry_gate,
            eval_result=eval_result,
            final_score=final_score,
            threshold=score_threshold,
        )
        _record_entry_gate_passed_sample(
            diagnostics,
            ts_code=ts_code,
            as_of_date=as_of_date,
            eval_result=eval_result,
            threshold=score_threshold,
            filter_passed=bool(eval_result.get("filter_passed", True)),
        )
        score_reason = "below_threshold"
        if final_score >= score_threshold:
            score_reason = "pass_threshold" if entry_gate_passed else "entry_gate_blocked"
        _record_score_diagnostic(diagnostics, score=final_score, reason=score_reason)
        _record_top_score_sample(
            diagnostics,
            ts_code=ts_code,
            as_of_date=as_of_date,
            eval_result=eval_result,
            threshold=score_threshold,
            filter_passed=bool(eval_result.get("filter_passed", True)),
        )
        if final_score < score_threshold:
            _record_stage_timing(diagnostics, "signal_total", (perf_counter() - signal_start) * 1000.0)
            return None
        if not entry_gate_passed:
            _record_stage_timing(diagnostics, "signal_total", (perf_counter() - signal_start) * 1000.0)
            return None
        close_col = "close" if "close" in g.columns else "close_price"
        buy_price = float(g.iloc[i][close_col])
        exit_day = int(holding_days)
        exit_reason = "holding_period"
        forced_exit_price: Optional[float] = None
        for day in range(1, int(holding_days) + 1):
            if i + day >= len(g):
                break
            current_price = float(g.iloc[i + day][close_col])
            ret = current_price / buy_price - 1.0 if buy_price > 0 else 0.0
            if ret <= stop_loss:
                exit_day = day
                exit_reason = "stop_loss"
                forced_exit_price = buy_price * (1.0 + stop_loss)
                break
            if ret >= take_profit:
                exit_day = day
                exit_reason = "take_profit"
                forced_exit_price = buy_price * (1.0 + take_profit)
                break
        signal = {
            "__exit_offset": int(exit_day),
            "score": final_score,
            "signal_strength": final_score,
            "stock_name": g.iloc[i].get("name", ts_code),
            "industry": g.iloc[i].get("industry", ""),
            "grade": eval_result.get("grade", ""),
            "exit_reason": exit_reason,
            "exit_day": int(exit_day),
            "candidate_filter_mode": candidate_filter_mode,
            "filter_passed": bool(eval_result.get("filter_passed", True)),
        }
        if forced_exit_price is not None:
            signal["__exit_price"] = float(forced_exit_price)
            signal["execution_fill_model"] = "stop_take_price"
        _record_stage_timing(diagnostics, "signal_total", (perf_counter() - signal_start) * 1000.0)
        return signal

    with _temp_log_level(logging.WARNING):
        backtest_df, analyzed = _run_engine_rolling(
            engine,
            min_rows=60 + holding_days,
            window=60,
            step=max(1, replay_step),
            signal_fn=_signal_v6,
            stop_on_first_signal=True,
            max_evaluations=_optional_positive_int(params.get("max_evaluations")),
        )
    if backtest_df.empty:
        frozen_diag = _freeze_score_distribution_diagnostics(diagnostics)
        frozen_diag["v6_runtime_diagnostics"] = build_v6_runtime_diagnostics(frozen_diag, replay_step=max(1, replay_step))
        return {
            "success": False,
            "error": "v6 runtime backtest produced no valid signals",
            "strategy": "v6",
            "stats": {"analyzed_stocks": analyzed},
            "backtest_diagnostics": frozen_diag,
        }
    stats = analyzer._calculate_backtest_stats(backtest_df.copy(), analyzed, holding_days)
    stats["risk_diagnostics"] = _build_v8_risk_diagnostics(backtest_df)
    stats["risk_control"] = {"stop_loss": float(stop_loss), "take_profit": float(take_profit)}
    if hasattr(engine, "execution_constraints_summary"):
        stats.update(engine.execution_constraints_summary())
    stats["replay_step"] = int(max(1, replay_step))
    frozen_diag = _freeze_score_distribution_diagnostics(diagnostics)
    frozen_diag["v6_runtime_diagnostics"] = build_v6_runtime_diagnostics(frozen_diag, replay_step=max(1, replay_step))
    return {
        "success": True,
        "strategy": "v6_runtime_replay",
        "stats": stats,
        "backtest_data": backtest_df,
        "backtest_diagnostics": frozen_diag,
    }


def _run_v7_runtime_backtest(*, module: ModuleType, analyzer: Any, df_bt: pd.DataFrame, params: JsonDict) -> JsonDict:
    engine_cls = getattr(module, "UnifiedBacktestEngine", None)
    evaluator = getattr(analyzer, "evaluator_v7", None)
    if engine_cls is None:
        return {"success": False, "error": "UnifiedBacktestEngine not found", "strategy": "v7"}
    if evaluator is None:
        return {"success": False, "error": "v7 evaluator not loaded", "strategy": "v7"}

    sample_size = int(params.get("sample_size", 300))
    holding_days = int(params.get("holding_days", 8))
    score_threshold = float(params.get("score_threshold", 60))
    replay_step = int(params.get("replay_step", params.get("v7_replay_step", 20)))
    stop_loss = float(params.get("stop_loss", -0.05))
    take_profit = float(params.get("take_profit", 0.08))
    ensure_aliases = getattr(module, "_ensure_price_aliases", lambda x: x)
    df_bt = ensure_aliases(df_bt.copy())
    engine = _make_unified_engine(engine_cls, df_bt, sample_size, holding_days, params)
    diagnostics = _new_score_distribution_diagnostics(strategy="v7", threshold=score_threshold)

    try:
        reset_cache = getattr(evaluator, "reset_cache", None)
        if callable(reset_cache):
            reset_cache()
    except Exception:
        pass

    def _signal_v7(ts_code: str, g: pd.DataFrame, i: int, hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
        hist = ensure_aliases(hist)
        industry = str(g.iloc[i].get("industry", "") or "")
        try:
            eval_result = evaluator.evaluate_stock_v7(hist, ts_code, industry)
        except Exception as exc:
            logging.getLogger(__name__).debug("v7 runtime replay failed for %s: %s", ts_code, exc)
            _record_score_diagnostic(diagnostics, score=None, reason="evaluator_exception")
            return None
        if not isinstance(eval_result, dict) or not eval_result.get("success"):
            _record_score_diagnostic(
                diagnostics,
                score=None,
                reason=_classify_unsuccessful_evaluation(eval_result, strategy="v7"),
            )
            return None
        final_score = float(eval_result.get("final_score", 0.0) or 0.0)
        conversion_review = (
            eval_result.get("near_threshold_conversion")
            if isinstance(eval_result.get("near_threshold_conversion"), dict)
            else {}
        )
        if conversion_review.get("applied") is True:
            reason_counts = diagnostics.setdefault("reason_counts", {})
            reason_counts["near_threshold_conversion_applied"] = int(
                reason_counts.get("near_threshold_conversion_applied", 0) or 0
            ) + 1
            _record_breakdown_value(
                diagnostics,
                "v7_near_threshold_conversion_bonus",
                conversion_review.get("bonus"),
            )
        elif conversion_review:
            for reason in conversion_review.get("blocking_reasons") or []:
                reason_counts = diagnostics.setdefault("reason_counts", {})
                key = f"near_threshold_conversion_blocked:{reason}"
                reason_counts[key] = int(reason_counts.get(key, 0) or 0) + 1
        _record_score_breakdown(
            diagnostics,
            eval_result,
            numeric_keys=("base_score", "v4_score", "market_score", "industry_heat", "risk_penalty"),
            nested_numeric_keys=(("dim_scores", "dim"), ("dimension_scores", "dim")),
        )
        _record_score_diagnostic(diagnostics, score=final_score, reason="pass_threshold" if final_score >= score_threshold else "below_threshold")
        _record_top_score_sample(
            diagnostics,
            ts_code=ts_code,
            as_of_date=str(hist["trade_date"].iloc[-1]) if "trade_date" in hist.columns and not hist.empty else str(g.iloc[i].get("trade_date", "")),
            eval_result=eval_result,
            threshold=score_threshold,
            filter_passed=not bool(eval_result.get("filter_warnings")),
        )
        if final_score < score_threshold:
            return None
        close_col = "close" if "close" in g.columns else "close_price"
        buy_price = float(g.iloc[i][close_col])
        exit_day = int(holding_days)
        exit_reason = "holding_period"
        forced_exit_price: Optional[float] = None
        for day in range(1, int(holding_days) + 1):
            if i + day >= len(g):
                break
            current_price = float(g.iloc[i + day][close_col])
            ret = current_price / buy_price - 1.0 if buy_price > 0 else 0.0
            if ret <= stop_loss:
                exit_day = day
                exit_reason = "stop_loss"
                forced_exit_price = buy_price * (1.0 + stop_loss)
                break
            if ret >= take_profit:
                exit_day = day
                exit_reason = "take_profit"
                forced_exit_price = buy_price * (1.0 + take_profit)
                break
        signal = {
            "__exit_offset": int(exit_day),
            "score": final_score,
            "signal_strength": final_score,
            "stock_name": g.iloc[i].get("name", ts_code),
            "industry": industry,
            "market_regime": eval_result.get("market_regime", ""),
            "industry_heat": eval_result.get("industry_heat", 0),
            "near_threshold_conversion": conversion_review,
            "exit_reason": exit_reason,
            "exit_day": int(exit_day),
        }
        if forced_exit_price is not None:
            signal["__exit_price"] = float(forced_exit_price)
            signal["execution_fill_model"] = "stop_take_price"
        return signal

    with _temp_log_level(logging.WARNING):
        backtest_df, analyzed = _run_engine_rolling(
            engine,
            min_rows=60 + holding_days,
            window=60,
            step=max(1, replay_step),
            signal_fn=_signal_v7,
            stop_on_first_signal=True,
            max_evaluations=_optional_positive_int(params.get("max_evaluations")),
        )
    if backtest_df.empty:
        return {
            "success": False,
            "error": "v7 runtime backtest produced no valid signals",
            "strategy": "v7",
            "stats": {"analyzed_stocks": analyzed},
            "backtest_diagnostics": _freeze_score_distribution_diagnostics(diagnostics),
        }
    stats = analyzer._calculate_backtest_stats(backtest_df.copy(), analyzed, holding_days)
    stats["risk_diagnostics"] = _build_v8_risk_diagnostics(backtest_df)
    stats["risk_control"] = {"stop_loss": float(stop_loss), "take_profit": float(take_profit)}
    if hasattr(engine, "execution_constraints_summary"):
        stats.update(engine.execution_constraints_summary())
    stats["replay_step"] = int(max(1, replay_step))
    return {
        "success": True,
        "strategy": "v7_runtime_replay",
        "stats": stats,
        "backtest_data": backtest_df,
        "backtest_diagnostics": _freeze_score_distribution_diagnostics(diagnostics),
    }


def _run_v8_runtime_backtest(*, module: ModuleType, analyzer: Any, df_bt: pd.DataFrame, params: JsonDict) -> JsonDict:
    engine_cls = getattr(module, "UnifiedBacktestEngine", None)
    evaluator = getattr(analyzer, "evaluator_v8", None)
    if engine_cls is None:
        return {"success": False, "error": "UnifiedBacktestEngine not found", "strategy": "v8"}
    if evaluator is None:
        return {"success": False, "error": "v8 evaluator not loaded", "strategy": "v8"}

    ensure_aliases = getattr(module, "_ensure_price_aliases", lambda x: x)
    sample_size = int(params.get("sample_size", 300))
    holding_days = int(params.get("holding_days", 8))
    score_threshold = float(params.get("score_threshold", 50))
    replay_step = int(params.get("replay_step", params.get("v8_replay_step", 20)))
    max_stop_loss_pct = float(params.get("max_stop_loss_pct", params.get("v8_max_stop_loss_pct", 0.08)))
    max_stop_loss_pct = max(0.01, min(0.30, max_stop_loss_pct))
    raw_max_take_profit_pct = params.get("max_take_profit_pct", params.get("v8_max_take_profit_pct"))
    max_take_profit_pct = None
    if raw_max_take_profit_pct is not None:
        max_take_profit_pct = max(0.02, min(0.80, float(raw_max_take_profit_pct)))
    df_bt = ensure_aliases(df_bt.copy())
    engine = _make_unified_engine(engine_cls, df_bt, sample_size, holding_days, params)
    diagnostics = _new_score_distribution_diagnostics(strategy="v8", threshold=score_threshold)

    index_data = None
    try:
        from data.history import load_index_recent as _load_index_recent_v2  # type: ignore

        index_df = _load_index_recent_v2(
            db_path=getattr(analyzer, "db_path", ""),
            index_code="000001.SH",
            limit=300,
            columns="trade_date, close_price as close, vol as volume",
        )
        if index_df is not None and not index_df.empty:
            index_data = ensure_aliases(index_df.sort_values("trade_date"))
    except Exception:
        index_data = None

    def _signal_v8(ts_code: str, g: pd.DataFrame, i: int, hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
        hist = ensure_aliases(hist)
        index_window = _slice_index_data_for_history(index_data, hist)
        try:
            eval_result = evaluator.evaluate_stock_v8(hist, ts_code, index_window)
        except Exception as exc:
            logging.getLogger(__name__).debug("v8 runtime replay failed for %s: %s", ts_code, exc)
            _record_score_diagnostic(diagnostics, score=None, reason="evaluator_exception")
            return None
        if not isinstance(eval_result, dict) or not eval_result.get("success"):
            _record_score_diagnostic(
                diagnostics,
                score=None,
                reason=_classify_unsuccessful_evaluation(eval_result, strategy="v8"),
            )
            return None
        final_score = float(eval_result.get("final_score", 0.0) or 0.0)
        _record_score_breakdown(
            diagnostics,
            eval_result,
            numeric_keys=("v7_score", "advanced_score", "pre_market_score", "market_penalty"),
            nested_numeric_keys=(("advanced_factors.factors", "factor"),),
        )
        _record_score_diagnostic(diagnostics, score=final_score, reason="pass_threshold" if final_score >= score_threshold else "below_threshold")
        if final_score < score_threshold:
            return None
        close_col = "close" if "close" in g.columns else "close_price"
        buy_price = float(g.iloc[i][close_col])
        atr_stops = eval_result.get("atr_stops") if isinstance(eval_result.get("atr_stops"), dict) else {}
        raw_stop_loss = float(atr_stops.get("stop_loss", buy_price * 0.96))
        capped_stop_loss = buy_price * (1.0 - max_stop_loss_pct)
        stop_loss = max(raw_stop_loss, capped_stop_loss)
        raw_take_profit = float(atr_stops.get("take_profit", buy_price * 1.06))
        take_profit = raw_take_profit
        if max_take_profit_pct is not None:
            take_profit = min(raw_take_profit, buy_price * (1.0 + max_take_profit_pct))
        exit_day = int(holding_days)
        exit_reason = "holding_period"
        forced_exit_price: Optional[float] = None
        for day in range(1, int(holding_days) + 1):
            if i + day >= len(g):
                break
            current_price = float(g.iloc[i + day][close_col])
            if current_price <= stop_loss:
                exit_day = day
                exit_reason = "stop_loss"
                forced_exit_price = float(stop_loss)
                break
            if current_price >= take_profit:
                exit_day = day
                exit_reason = "take_profit"
                forced_exit_price = float(take_profit)
                break
        signal: Dict[str, Any] = {
            "__exit_offset": int(exit_day),
            "score": final_score,
            "signal_strength": final_score,
            "stock_name": g.iloc[i].get("name", ts_code),
            "industry": g.iloc[i].get("industry", ""),
            "star_rating": eval_result.get("star_rating", 3),
            "v7_score": eval_result.get("v7_score", 0),
            "advanced_score": eval_result.get("advanced_score", 0),
            "pre_market_score": eval_result.get("pre_market_score", 0),
            "market_penalty": eval_result.get("market_penalty", 1.0),
            "exit_reason": exit_reason,
            "exit_day": int(exit_day),
            "atr_stop_loss_raw": raw_stop_loss,
            "atr_stop_loss": stop_loss,
            "atr_take_profit": take_profit,
            "atr_take_profit_raw": raw_take_profit,
            "atr_stop_loss_pct": round((buy_price - stop_loss) / buy_price * 100.0, 4) if buy_price > 0 else atr_stops.get("stop_loss_pct"),
            "atr_stop_loss_raw_pct": atr_stops.get("stop_loss_pct"),
            "risk_stop_loss_pct_cap": round(max_stop_loss_pct * 100.0, 4),
            "risk_take_profit_pct_cap": round(max_take_profit_pct * 100.0, 4) if max_take_profit_pct is not None else None,
            "atr_take_profit_pct": atr_stops.get("take_profit_pct"),
            "execution_fill_model": "stop_take_price" if forced_exit_price is not None else "close_on_holding_period",
        }
        if forced_exit_price is not None:
            signal["__exit_price"] = float(forced_exit_price)
        return signal

    with _temp_log_level(logging.WARNING):
        backtest_df, analyzed = _run_engine_rolling(
            engine,
            min_rows=60 + holding_days,
            window=60,
            step=max(1, replay_step),
            signal_fn=_signal_v8,
            stop_on_first_signal=True,
            max_evaluations=_optional_positive_int(params.get("max_evaluations")),
        )
    if backtest_df.empty:
        frozen_diag = _freeze_score_distribution_diagnostics(diagnostics)
        frozen_diag["v8_suppression_diagnostics"] = build_v8_suppression_diagnostics(frozen_diag)
        return {
            "success": False,
            "error": "v8 runtime backtest produced no valid signals",
            "strategy": "v8",
            "stats": {"analyzed_stocks": analyzed},
            "backtest_diagnostics": frozen_diag,
        }
    stats = analyzer._calculate_backtest_stats(backtest_df.copy(), analyzed, holding_days)
    stats["risk_diagnostics"] = _build_v8_risk_diagnostics(backtest_df)
    risk_control: JsonDict = {"max_stop_loss_pct": float(max_stop_loss_pct)}
    if max_take_profit_pct is not None:
        risk_control["max_take_profit_pct"] = float(max_take_profit_pct)
    stats["risk_control"] = risk_control
    if hasattr(engine, "execution_constraints_summary"):
        stats.update(engine.execution_constraints_summary())
    stats["replay_step"] = int(max(1, replay_step))
    frozen_diag = _freeze_score_distribution_diagnostics(diagnostics)
    frozen_diag["v8_suppression_diagnostics"] = build_v8_suppression_diagnostics(frozen_diag)
    return {
        "success": True,
        "strategy": "v8_runtime_replay",
        "stats": stats,
        "backtest_data": backtest_df,
        "backtest_diagnostics": frozen_diag,
    }


def _slice_index_data_for_history(index_data: Optional[pd.DataFrame], hist: pd.DataFrame) -> Optional[pd.DataFrame]:
    if index_data is None or index_data.empty or "trade_date" not in index_data.columns or "trade_date" not in hist.columns:
        return index_data
    current_date = str(hist["trade_date"].iloc[-1])
    idx = index_data.copy()
    idx["_trade_date_key"] = idx["trade_date"].astype(str)
    out = idx[idx["_trade_date_key"] <= current_date].drop(columns=["_trade_date_key"])
    if out.empty:
        return None
    return out.tail(300)


def _build_v8_risk_diagnostics(backtest_df: pd.DataFrame) -> JsonDict:
    if backtest_df is None or backtest_df.empty:
        return {"available": False, "sample_size": 0}
    out: JsonDict = {"available": True, "sample_size": int(len(backtest_df))}
    returns = pd.to_numeric(backtest_df.get("future_return", pd.Series(dtype=float)), errors="coerce").dropna()
    if not returns.empty:
        out["worst_return_pct"] = float(returns.min())
        out["avg_return_pct"] = float(returns.mean())
        out["loss_trade_count"] = int((returns < 0).sum())
        out["tail_loss_count_5pct"] = int((returns <= -5.0).sum())
        out["tail_loss_count_8pct"] = int((returns <= -8.0).sum())
    if "exit_reason" in backtest_df.columns:
        exit_counts = backtest_df["exit_reason"].fillna("unknown").astype(str).value_counts().to_dict()
        out["exit_reason_counts"] = {str(k): int(v) for k, v in exit_counts.items()}
        by_exit: JsonDict = {}
        for reason, subset in backtest_df.groupby(backtest_df["exit_reason"].fillna("unknown").astype(str)):
            subset_returns = pd.to_numeric(subset.get("future_return", pd.Series(dtype=float)), errors="coerce").dropna()
            if subset_returns.empty:
                continue
            by_exit[str(reason)] = {
                "count": int(len(subset_returns)),
                "avg_return_pct": float(subset_returns.mean()),
                "worst_return_pct": float(subset_returns.min()),
                "win_rate": float((subset_returns > 0).sum() / len(subset_returns)),
            }
        out["exit_reason_return_profile"] = by_exit
    for col in (
        "holding_days_realized",
        "atr_stop_loss_pct",
        "atr_stop_loss_raw_pct",
        "risk_stop_loss_pct_cap",
        "risk_take_profit_pct_cap",
        "atr_take_profit_pct",
        "score",
        "pre_market_score",
        "market_penalty",
    ):
        if col not in backtest_df.columns:
            continue
        vals = pd.to_numeric(backtest_df[col], errors="coerce").dropna()
        if vals.empty:
            continue
        out[col] = {
            "avg": float(vals.mean()),
            "min": float(vals.min()),
            "max": float(vals.max()),
        }
    worst_cols = [c for c in ("ts_code", "trade_date", "future_return", "gross_return", "exit_reason", "holding_days_realized", "score") if c in backtest_df.columns]
    if worst_cols:
        worst = backtest_df.sort_values("future_return", ascending=True).head(5)[worst_cols]
        out["worst_trades"] = [
            {str(k): (float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v) for k, v in row.items()}
            for row in worst.to_dict("records")
        ]
    return out


def _new_score_distribution_diagnostics(*, strategy: str, threshold: float) -> JsonDict:
    return {
        "type": "score_distribution",
        "strategy": str(strategy),
        "threshold": float(threshold),
        "evaluated": 0,
        "passed_threshold": 0,
        "missing_score": 0,
        "reason_counts": {},
        "scores": [],
    }


def _record_score_diagnostic(diag: JsonDict, *, score: Optional[float], reason: str) -> None:
    diag["evaluated"] = int(diag.get("evaluated", 0) or 0) + 1
    reason_counts = diag.setdefault("reason_counts", {})
    reason_counts[str(reason)] = int(reason_counts.get(str(reason), 0) or 0) + 1
    if score is None:
        diag["missing_score"] = int(diag.get("missing_score", 0) or 0) + 1
        return
    score_f = float(score)
    scores = diag.setdefault("scores", [])
    scores.append(score_f)
    if score_f >= float(diag.get("threshold", 0.0) or 0.0):
        diag["passed_threshold"] = int(diag.get("passed_threshold", 0) or 0) + 1
    gap = abs(float(diag.get("threshold", 0.0) or 0.0) - score_f)
    near = diag.setdefault("near_threshold", {"within_2": 0, "within_5": 0, "within_10": 0})
    if gap <= 2.0:
        near["within_2"] = int(near.get("within_2", 0) or 0) + 1
    if gap <= 5.0:
        near["within_5"] = int(near.get("within_5", 0) or 0) + 1
    if gap <= 10.0:
        near["within_10"] = int(near.get("within_10", 0) or 0) + 1


def _record_stage_timing(diag: JsonDict, key: str, elapsed_ms: float) -> None:
    timings = diag.setdefault("stage_timing_ms", {})
    item = timings.setdefault(str(key), {"count": 0, "sum": 0.0, "max": 0.0})
    item["count"] = int(item.get("count", 0) or 0) + 1
    item["sum"] = float(item.get("sum", 0.0) or 0.0) + float(elapsed_ms)
    item["max"] = max(float(item.get("max", 0.0) or 0.0), float(elapsed_ms))


def _record_stage_timing_payload(diag: JsonDict, prefix: str, payload: Any) -> None:
    if not isinstance(payload, dict):
        return
    for key, value in payload.items():
        try:
            elapsed_ms = float(value)
        except (TypeError, ValueError):
            continue
        _record_stage_timing(diag, f"{prefix}{key}", elapsed_ms)


def _classify_unsuccessful_evaluation(eval_result: Any, *, strategy: str) -> str:
    if not isinstance(eval_result, dict):
        return "evaluator_unsuccessful:non_dict_result"
    if eval_result.get("filter_failed"):
        details = eval_result.get("filter_details") if isinstance(eval_result.get("filter_details"), dict) else {}
        quality = details.get("pit_data_quality") if isinstance(details.get("pit_data_quality"), dict) else {}
        if quality and (
            quality.get("sector_performance_available") is False
            or quality.get("money_flow_available") is False
        ):
            return "mandatory_filter:pit_data_unavailable"
        return f"mandatory_filter:{_classify_filter_reason(eval_result.get('filter_reason'))}"
    text = " ".join(
        str(eval_result.get(key) or "")
        for key in ("reason", "error", "description", "filter_reason")
    )
    if "数据不足" in text or "data" in text.lower():
        return "evaluator_unsuccessful:data_insufficient"
    if strategy == "v8" and float(eval_result.get("final_score", 0) or 0) <= 0:
        return "evaluator_unsuccessful:base_or_factor_zero"
    return "evaluator_unsuccessful:unknown"


def _classify_filter_reason(reason: Any) -> str:
    text = str(reason or "").strip()
    if not text:
        return "unknown"
    checks = (
        ("价格", "price_too_high"),
        ("板块", "sector_condition"),
        ("资金", "money_flow"),
        ("持续下跌", "sustained_decline"),
        ("短期大幅下跌", "short_term_selloff"),
        ("缩量", "volume_shrink"),
    )
    for needle, label in checks:
        if needle in text:
            return label
    return "other"


def _record_score_breakdown(
    diag: JsonDict,
    eval_result: JsonDict,
    *,
    numeric_keys: Tuple[str, ...],
    nested_numeric_keys: Tuple[Tuple[str, str], ...] = (),
) -> None:
    for key in numeric_keys:
        _record_breakdown_value(diag, key, eval_result.get(key))
    for path, prefix in nested_numeric_keys:
        nested = _get_nested_dict(eval_result, path)
        if not isinstance(nested, dict):
            continue
        for key, value in nested.items():
            if isinstance(value, dict):
                value = value.get("score")
            _record_breakdown_value(diag, f"{prefix}:{key}", value)


def _record_top_score_sample(
    diag: JsonDict,
    *,
    ts_code: str,
    as_of_date: str,
    eval_result: JsonDict,
    threshold: float,
    filter_passed: bool,
    limit: int = 10,
) -> None:
    try:
        final_score = float(eval_result.get("final_score", 0.0) or 0.0)
    except (TypeError, ValueError):
        return
    threshold_f = float(threshold)
    gap = threshold_f - final_score
    if gap > 10.0 and final_score < threshold_f:
        return
    sample = _build_score_sample(
        ts_code=ts_code,
        as_of_date=as_of_date,
        eval_result=eval_result,
        threshold=threshold,
        filter_passed=filter_passed,
    )
    if not sample:
        return
    samples = diag.setdefault("top_near_threshold_samples", [])
    if not isinstance(samples, list):
        samples = []
        diag["top_near_threshold_samples"] = samples
    samples.append(sample)
    samples.sort(key=lambda row: float((row or {}).get("final_score", 0.0) or 0.0), reverse=True)
    del samples[int(limit):]


def _record_entry_gate_diagnostic(diag: JsonDict, *, entry_gate: JsonDict, final_score: float) -> None:
    review = diag.setdefault(
        "entry_gate_review",
        {
            "observed": 0,
            "passed": 0,
            "blocked": 0,
            "mode_counts": {},
            "reason_counts": {},
            "overheat_flag_counts": {},
            "passed_score_max": 0.0,
            "blocked_score_max": 0.0,
        },
    )
    if not isinstance(review, dict):
        return
    review["observed"] = int(review.get("observed", 0) or 0) + 1
    passed = bool(entry_gate.get("passed", True)) if isinstance(entry_gate, dict) else True
    bucket = "passed" if passed else "blocked"
    review[bucket] = int(review.get(bucket, 0) or 0) + 1
    max_key = "passed_score_max" if passed else "blocked_score_max"
    review[max_key] = max(float(review.get(max_key, 0.0) or 0.0), float(final_score))

    mode = str(entry_gate.get("mode", "missing") or "missing") if isinstance(entry_gate, dict) else "missing"
    reason = str(entry_gate.get("reason", "missing") or "missing") if isinstance(entry_gate, dict) else "missing"
    mode_counts = review.setdefault("mode_counts", {})
    reason_counts = review.setdefault("reason_counts", {})
    if isinstance(mode_counts, dict):
        mode_counts[mode] = int(mode_counts.get(mode, 0) or 0) + 1
    if isinstance(reason_counts, dict):
        reason_counts[reason] = int(reason_counts.get(reason, 0) or 0) + 1

    flag_counts = review.setdefault("overheat_flag_counts", {})
    flags = entry_gate.get("overheat_flags") if isinstance(entry_gate, dict) else []
    if isinstance(flag_counts, dict) and isinstance(flags, list):
        for flag in flags:
            key = str(flag)
            flag_counts[key] = int(flag_counts.get(key, 0) or 0) + 1


def _record_entry_gate_passed_sample(
    diag: JsonDict,
    *,
    ts_code: str,
    as_of_date: str,
    eval_result: JsonDict,
    threshold: float,
    filter_passed: bool,
    limit: int = 10,
) -> None:
    entry_gate = eval_result.get("entry_gate") if isinstance(eval_result.get("entry_gate"), dict) else {}
    if not bool(entry_gate.get("passed", True)):
        return
    sample = _build_score_sample(
        ts_code=ts_code,
        as_of_date=as_of_date,
        eval_result=eval_result,
        threshold=threshold,
        filter_passed=filter_passed,
    )
    if not sample:
        return
    samples = diag.setdefault("entry_gate_passed_samples", [])
    if not isinstance(samples, list):
        samples = []
        diag["entry_gate_passed_samples"] = samples
    samples.append(sample)
    samples.sort(key=lambda row: float((row or {}).get("final_score", 0.0) or 0.0), reverse=True)
    del samples[int(limit):]


def _record_entry_gate_quality_gap(
    diag: JsonDict,
    *,
    entry_gate: JsonDict,
    eval_result: JsonDict,
    final_score: float,
    threshold: float,
) -> None:
    if not bool(entry_gate.get("passed", True)):
        return
    mode = str(entry_gate.get("mode", "missing") or "missing")
    quality = diag.setdefault("entry_gate_quality_by_mode", {})
    if not isinstance(quality, dict):
        return
    item = quality.setdefault(
        mode,
        {
            "count": 0,
            "score_sum": 0.0,
            "score_max": float(final_score),
            "gap_sum": 0.0,
            "gap_min": float(threshold) - float(final_score),
            "base_score": {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0},
            "synergy_bonus": {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0},
            "risk_penalty": {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0},
            "dimension_scores": {},
            "dimension_shortfall_to_reference": {},
            "secondary_confirmation_counts": {},
            "secondary_confirmation_metrics": {},
        },
    )
    if not isinstance(item, dict):
        return
    gap = float(threshold) - float(final_score)
    item["count"] = int(item.get("count", 0) or 0) + 1
    item["score_sum"] = float(item.get("score_sum", 0.0) or 0.0) + float(final_score)
    item["score_max"] = max(float(item.get("score_max", final_score) or 0.0), float(final_score))
    item["gap_sum"] = float(item.get("gap_sum", 0.0) or 0.0) + gap
    item["gap_min"] = min(float(item.get("gap_min", gap) or gap), gap)
    for key in ("base_score", "synergy_bonus", "risk_penalty"):
        _record_metric_stats(item.setdefault(key, {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0}), eval_result.get(key))
    confirmation = entry_gate.get("secondary_confirmation") if isinstance(entry_gate.get("secondary_confirmation"), dict) else {}
    confirmation_counts = item.setdefault("secondary_confirmation_counts", {})
    if isinstance(confirmation_counts, dict):
        for key, value in confirmation.items():
            if isinstance(value, bool) and value:
                confirmation_counts[str(key)] = int(confirmation_counts.get(str(key), 0) or 0) + 1
        confirmed_count = _round_optional_float(confirmation.get("confirmed_count"))
        if confirmed_count is not None:
            bucket = f"confirmed_count:{int(confirmed_count)}"
            confirmation_counts[bucket] = int(confirmation_counts.get(bucket, 0) or 0) + 1
    confirmation_metrics = item.setdefault("secondary_confirmation_metrics", {})
    if isinstance(confirmation_metrics, dict):
        for key in (
            "technical_breakthrough_candidate_score",
            "technical_breakthrough_current_score",
            "technical_breakthrough_candidate_delta",
        ):
            if key in confirmation:
                _record_metric_stats(
                    confirmation_metrics.setdefault(key, {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0}),
                    confirmation.get(key),
                )
    dimensions = eval_result.get("dimension_scores")
    if not isinstance(dimensions, dict):
        dimensions = eval_result.get("dim_scores")
    if not isinstance(dimensions, dict):
        return
    dimension_stats = item.setdefault("dimension_scores", {})
    shortfall_stats = item.setdefault("dimension_shortfall_to_reference", {})
    if not isinstance(dimension_stats, dict) or not isinstance(shortfall_stats, dict):
        return
    seen_dimensions = set()
    for key, raw_value in dimensions.items():
        value = _round_optional_float(raw_value)
        if value is None:
            continue
        name = str(key)
        seen_dimensions.add(name)
        _record_metric_stats(dimension_stats.setdefault(name, {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0}), value)
        reference = _V6_DIMENSION_REFERENCE_SCORES.get(name)
        if reference is None:
            continue
        _record_metric_stats(
            shortfall_stats.setdefault(name, {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0}),
            max(0.0, float(reference) - float(value)),
        )
    for name, reference in _V6_DIMENSION_REFERENCE_SCORES.items():
        if name in seen_dimensions:
            continue
        _record_metric_stats(dimension_stats.setdefault(name, {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0}), 0.0)
        _record_metric_stats(
            shortfall_stats.setdefault(name, {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0}),
            float(reference),
        )


def _record_metric_stats(stats: Any, raw_value: Any) -> None:
    if not isinstance(stats, dict):
        return
    value = _round_optional_float(raw_value)
    if value is None:
        return
    count = int(stats.get("count", 0) or 0)
    stats["count"] = count + 1
    stats["sum"] = float(stats.get("sum", 0.0) or 0.0) + float(value)
    stats["min"] = float(value) if count <= 0 else min(float(stats.get("min", value)), float(value))
    stats["max"] = float(value) if count <= 0 else max(float(stats.get("max", value)), float(value))


def _build_score_sample(
    *,
    ts_code: str,
    as_of_date: str,
    eval_result: JsonDict,
    threshold: float,
    filter_passed: bool,
) -> JsonDict:
    try:
        final_score = float(eval_result.get("final_score", 0.0) or 0.0)
    except (TypeError, ValueError):
        return {}
    threshold_f = float(threshold)
    gap = threshold_f - final_score
    dimensions = eval_result.get("dimension_scores")
    if not isinstance(dimensions, dict):
        dimensions = eval_result.get("dim_scores")
    return {
        "ts_code": str(ts_code),
        "as_of_date": str(as_of_date or ""),
        "final_score": round(final_score, 3),
        "threshold": round(threshold_f, 3),
        "gap_to_threshold": round(gap, 3),
        "base_score": _round_optional_float(eval_result.get("base_score")),
        "synergy_bonus": _round_optional_float(eval_result.get("synergy_bonus")),
        "synergy_combo": str(eval_result.get("synergy_combo", "") or ""),
        "risk_penalty": _round_optional_float(eval_result.get("risk_penalty")),
        "risk_reasons": [str(x) for x in (eval_result.get("risk_reasons") or [])[:8]]
        if isinstance(eval_result.get("risk_reasons"), list)
        else [],
        "entry_gate": _compact_entry_gate(eval_result.get("entry_gate")),
        "filter_passed": bool(filter_passed),
        "candidate_filter_relaxed": bool(eval_result.get("candidate_filter_relaxed", False)),
        "near_threshold_conversion": _compact_near_threshold_conversion(eval_result.get("near_threshold_conversion")),
        "dimension_scores": {
            str(key): _round_optional_float(value)
            for key, value in (dimensions or {}).items()
            if _round_optional_float(value) is not None
        },
        "price_position": _round_optional_float(eval_result.get("price_position")),
        "vol_ratio": _round_optional_float(eval_result.get("vol_ratio")),
        "price_chg_3d": _round_optional_float(eval_result.get("price_chg_3d")),
    }


def _compact_near_threshold_conversion(value: Any) -> JsonDict:
    if not isinstance(value, dict):
        return {}
    return {
        "version": str(value.get("version", "") or ""),
        "eligible": bool(value.get("eligible", False)),
        "applied": bool(value.get("applied", False)),
        "original_score": _round_optional_float(value.get("original_score")),
        "final_score": _round_optional_float(value.get("final_score")),
        "bonus": _round_optional_float(value.get("bonus")),
        "confirmation_count": int(value.get("confirmation_count", 0) or 0),
        "blocking_reasons": [str(item) for item in (value.get("blocking_reasons") or [])[:8]],
        "quality_flags": {
            str(key): bool(val)
            for key, val in (value.get("quality_flags") or {}).items()
        }
        if isinstance(value.get("quality_flags"), dict)
        else {},
    }


def _round_optional_float(value: Any) -> Optional[float]:
    try:
        value_f = float(value)
    except (TypeError, ValueError):
        return None
    if value_f != value_f:
        return None
    return round(value_f, 3)


def _compact_entry_gate(value: Any) -> JsonDict:
    if not isinstance(value, dict):
        return {}
    out: JsonDict = {}
    for key in (
        "passed",
        "mode",
        "reason",
        "overheat_flags",
        "pullback_from_20d_high",
        "latest_pct_chg",
        "rebound_2d",
        "change_3d",
        "limit_up_count_5d",
        "price_position",
        "vol_ratio",
        "volume_cooling",
        "secondary_confirmation",
    ):
        if key not in value:
            continue
        raw = value.get(key)
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            out[key] = _round_optional_float(raw)
        elif isinstance(raw, list):
            out[key] = [str(x) for x in raw[:8]]
        elif isinstance(raw, bool):
            out[key] = bool(raw)
        elif isinstance(raw, dict):
            compact = {}
            for sub_key, sub_value in raw.items():
                if isinstance(sub_value, bool):
                    compact[str(sub_key)] = bool(sub_value)
                elif isinstance(sub_value, (int, float)):
                    compact[str(sub_key)] = _round_optional_float(sub_value)
                elif sub_value is not None:
                    compact[str(sub_key)] = str(sub_value)
            out[key] = compact
        elif raw is not None:
            out[key] = str(raw)
    return out


def _get_nested_dict(payload: JsonDict, path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _record_breakdown_value(diag: JsonDict, key: str, raw_value: Any) -> None:
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return
    if value != value:
        return
    breakdown = diag.setdefault("score_breakdown", {})
    item = breakdown.setdefault(str(key), {"count": 0, "sum": 0.0, "min": value, "max": value})
    item["count"] = int(item.get("count", 0) or 0) + 1
    item["sum"] = float(item.get("sum", 0.0) or 0.0) + value
    item["min"] = min(float(item.get("min", value)), value)
    item["max"] = max(float(item.get("max", value)), value)


def _freeze_score_distribution_diagnostics(diag: JsonDict) -> JsonDict:
    scores = [float(x) for x in diag.get("scores", []) or []]
    out = {k: v for k, v in diag.items() if k != "scores"}
    breakdown = out.get("score_breakdown")
    if isinstance(breakdown, dict):
        frozen_breakdown = {}
        for key, value in breakdown.items():
            if not isinstance(value, dict):
                continue
            count = int(value.get("count", 0) or 0)
            if count <= 0:
                continue
            frozen_breakdown[str(key)] = {
                "count": count,
                "avg": float(value.get("sum", 0.0) or 0.0) / float(count),
                "min": float(value.get("min", 0.0) or 0.0),
                "max": float(value.get("max", 0.0) or 0.0),
            }
        out["score_breakdown"] = frozen_breakdown
    timing = out.get("stage_timing_ms")
    if isinstance(timing, dict):
        frozen_timing = {}
        for key, value in timing.items():
            if not isinstance(value, dict):
                continue
            count = int(value.get("count", 0) or 0)
            if count <= 0:
                continue
            frozen_timing[str(key)] = {
                "count": count,
                "avg": float(value.get("sum", 0.0) or 0.0) / float(count),
                "max": float(value.get("max", 0.0) or 0.0),
            }
        out["stage_timing_ms"] = frozen_timing
    entry_gate_quality = out.get("entry_gate_quality_by_mode")
    if isinstance(entry_gate_quality, dict):
        out["entry_gate_quality_by_mode"] = _freeze_entry_gate_quality_by_mode(entry_gate_quality)
    out["score_count"] = len(scores)
    out["pass_rate"] = (
        float(int(diag.get("passed_threshold", 0) or 0)) / float(int(diag.get("evaluated", 0) or 0))
        if int(diag.get("evaluated", 0) or 0) > 0
        else 0.0
    )
    if scores:
        scores_sorted = sorted(scores)
        out["min_score"] = float(scores_sorted[0])
        out["max_score"] = float(scores_sorted[-1])
        out["avg_score"] = float(sum(scores_sorted) / len(scores_sorted))
        out["p50_score"] = float(scores_sorted[len(scores_sorted) // 2])
        out["p90_score"] = float(scores_sorted[min(len(scores_sorted) - 1, int(len(scores_sorted) * 0.9))])
    else:
        out["min_score"] = 0.0
        out["max_score"] = 0.0
        out["avg_score"] = 0.0
        out["p50_score"] = 0.0
        out["p90_score"] = 0.0
    return out


def _freeze_entry_gate_quality_by_mode(payload: JsonDict) -> JsonDict:
    out: JsonDict = {}
    for mode, item in payload.items():
        if not isinstance(item, dict):
            continue
        count = int(item.get("count", 0) or 0)
        if count <= 0:
            continue
        out[str(mode)] = {
            "count": count,
            "avg_score": float(item.get("score_sum", 0.0) or 0.0) / float(count),
            "max_score": float(item.get("score_max", 0.0) or 0.0),
            "avg_gap_to_threshold": float(item.get("gap_sum", 0.0) or 0.0) / float(count),
            "min_gap_to_threshold": float(item.get("gap_min", 0.0) or 0.0),
            "base_score": _freeze_metric_stats(item.get("base_score")),
            "synergy_bonus": _freeze_metric_stats(item.get("synergy_bonus")),
            "risk_penalty": _freeze_metric_stats(item.get("risk_penalty")),
            "dimension_scores": _freeze_metric_stats_map(item.get("dimension_scores")),
            "dimension_shortfall_to_reference": _freeze_metric_stats_map(item.get("dimension_shortfall_to_reference")),
            "secondary_confirmation_counts": dict(item.get("secondary_confirmation_counts") or {}),
            "secondary_confirmation_metrics": _freeze_metric_stats_map(item.get("secondary_confirmation_metrics")),
        }
    return out


def _freeze_metric_stats_map(payload: Any) -> JsonDict:
    if not isinstance(payload, dict):
        return {}
    out: JsonDict = {}
    for key, value in payload.items():
        frozen = _freeze_metric_stats(value)
        if frozen:
            out[str(key)] = frozen
    return out


def _freeze_metric_stats(payload: Any) -> JsonDict:
    if not isinstance(payload, dict):
        return {}
    count = int(payload.get("count", 0) or 0)
    if count <= 0:
        return {}
    return {
        "count": count,
        "avg": float(payload.get("sum", 0.0) or 0.0) / float(count),
        "min": float(payload.get("min", 0.0) or 0.0),
        "max": float(payload.get("max", 0.0) or 0.0),
    }


def _weighted_combo_summary(summaries: Dict[str, JsonDict], sample_size: int) -> JsonDict:
    base_weights = {"v7": 0.23, "v8": 0.22, "v9": 0.30, "legacy": 0.25}
    active_weights = {k: v for k, v in base_weights.items() if k in summaries}
    weight_sum = sum(active_weights.values()) or 1.0
    active_weights = {k: v / weight_sum for k, v in active_weights.items()}

    win_rate = sum(active_weights[k] * float(summaries[k].get("win_rate", 0.0)) for k in active_weights)
    density = sum(active_weights[k] * float(summaries[k].get("signal_density", 0.0)) for k in active_weights)
    weighted_dd = sum(active_weights[k] * float(summaries[k].get("max_drawdown", 1.0)) for k in active_weights)
    worst_dd = max(float(summaries[k].get("max_drawdown", 1.0)) for k in active_weights)

    # Keep ensemble conservative by mixing weighted drawdown with worst-case drawdown.
    max_drawdown = min(1.0, weighted_dd * 0.7 + worst_dd * 0.3)

    return {
        "win_rate": max(0.0, min(1.0, win_rate)),
        "max_drawdown": max(0.0, min(1.0, max_drawdown)),
        "signal_density": max(0.0, min(2.0, density)),
    }


def _run_stable_uptrend_backtest(*, module: ModuleType, analyzer: Any, df_bt: pd.DataFrame, params: JsonDict) -> JsonDict:
    engine_cls = getattr(module, "UnifiedBacktestEngine", None)
    if engine_cls is None:
        return {"success": False, "error": "UnifiedBacktestEngine not found", "strategy": "stable"}
    sample_size = int(params.get("sample_size", 300))
    holding_days = int(params.get("holding_days", 10))
    score_threshold = float(params.get("score_threshold", 60))
    max_drawdown = float(params.get("stable_max_drawdown", 0.15))
    vol_max = float(params.get("stable_vol_max", 0.04))
    rebound_min = float(params.get("stable_rebound_min", 0.10))
    engine = _make_unified_engine(engine_cls, df_bt, sample_size, holding_days, params)
    diagnostics = _new_score_distribution_diagnostics(strategy="stable", threshold=score_threshold)

    def _signal_stable(ts_code: str, g: pd.DataFrame, i: int, hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
        score_info = _evaluate_stable_uptrend_history(
            hist,
            max_drawdown=max_drawdown,
            vol_max=vol_max,
            rebound_min=rebound_min,
        )
        if not score_info:
            diag_info = _diagnose_stable_uptrend_history(
                hist,
                max_drawdown=max_drawdown,
                vol_max=vol_max,
                rebound_min=rebound_min,
            )
            _record_stable_diagnostic(diagnostics, diag_info, score=None, reason=str(diag_info.get("reason") or "stable_filter_failed"))
            return None
        score = float(score_info.get("score", 0.0))
        if score < score_threshold:
            _record_stable_diagnostic(diagnostics, score_info, score=score, reason="below_threshold")
            return None
        _record_stable_diagnostic(diagnostics, score_info, score=score, reason="pass_threshold")
        return {
            "score": score,
            "signal_strength": score,
            "stock_name": g.iloc[i]["name"] if "name" in g.columns else ts_code,
            "industry": g.iloc[i]["industry"] if "industry" in g.columns else "未知",
            "stable_max_dd": float(score_info.get("max_dd", 0.0)),
            "stable_rebound": float(score_info.get("rebound", 0.0)),
            "stable_vol": float(score_info.get("vol", 0.0)),
            "stable_trend_ok": bool(score_info.get("trend_ok", False)),
            "stable_breakout": bool(score_info.get("breakout", False)),
        }

    backtest_df, analyzed = _run_engine_rolling(
        engine,
        min_rows=70 + holding_days,
        window=max(60, int(params.get("stable_window", 90))),
        step=int(params.get("stable_step", 5)),
        signal_fn=_signal_stable,
        stop_on_first_signal=True,
        max_evaluations=_optional_positive_int(params.get("max_evaluations")),
    )
    if backtest_df.empty:
        return {
            "success": False,
            "error": "stable backtest produced no valid signals",
            "strategy": "stable",
            "stats": {"analyzed_stocks": analyzed},
            "backtest_diagnostics": _freeze_score_distribution_diagnostics(diagnostics),
        }
    stats = analyzer._calculate_backtest_stats(backtest_df.copy(), analyzed, holding_days)
    if hasattr(engine, "execution_constraints_summary"):
        stats.update(engine.execution_constraints_summary())
    defensive_allocator = build_stable_defensive_allocator_review(backtest_df, params=params)
    stats["defensive_allocator"] = defensive_allocator
    details = []
    for _, row in backtest_df.head(100).iterrows():
        details.append(
            {
                "股票代码": row.get("ts_code", ""),
                "股票名称": row.get("stock_name", ""),
                "行业": row.get("industry", ""),
                "信号日期": str(row.get("trade_date", "")),
                "稳定上涨评分": f"{float(row.get('signal_strength', 0)):.1f}",
                "最大回撤": f"{float(row.get('stable_max_dd', 0.0)) * 100:.1f}%",
                "反弹幅度": f"{float(row.get('stable_rebound', 0.0)) * 100:.1f}%",
                "20日波动率": f"{float(row.get('stable_vol', 0.0)) * 100:.2f}%",
                f"{holding_days}天收益": f"{float(row.get('future_return', 0.0)):.2f}%",
            }
        )
    return {
        "success": True,
        "strategy": "stable_uptrend",
        "stats": stats,
        "backtest_data": backtest_df,
        "details": details,
        "backtest_diagnostics": _freeze_score_distribution_diagnostics(diagnostics),
        "defensive_allocator": defensive_allocator,
    }


def _record_stable_diagnostic(diag: JsonDict, score_info: JsonDict, *, score: Optional[float], reason: str) -> None:
    _record_score_diagnostic(diag, score=score, reason=reason)
    _record_breakdown_value(diag, "stable_max_dd", score_info.get("max_dd"))
    _record_breakdown_value(diag, "stable_rebound", score_info.get("rebound"))
    _record_breakdown_value(diag, "stable_vol", score_info.get("vol"))
    _record_breakdown_value(diag, "stable_trend_ok", 1.0 if score_info.get("trend_ok") else 0.0)
    _record_breakdown_value(diag, "stable_breakout", 1.0 if score_info.get("breakout") else 0.0)


def _diagnose_stable_uptrend_history(
    hist: pd.DataFrame,
    *,
    max_drawdown: float,
    vol_max: float,
    rebound_min: float,
) -> JsonDict:
    if hist is None or hist.empty:
        return {"reason": "data_insufficient:empty_history"}
    close_col = "close_price" if "close_price" in hist.columns else "close"
    if close_col not in hist.columns or len(hist) < 60:
        return {"reason": "data_insufficient:missing_or_short_close"}
    close = pd.to_numeric(hist[close_col], errors="coerce").dropna()
    if len(close) < 60:
        return {"reason": "data_insufficient:insufficient_valid_close"}
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    if ma20.isna().iloc[-1] or ma60.isna().iloc[-1] or ma20.isna().iloc[-5]:
        return {"reason": "moving_average_unavailable"}
    trend_ok = bool(ma20.iloc[-1] > ma60.iloc[-1] and ma20.iloc[-1] > ma20.iloc[-5])
    rolling_peak = close.cummax()
    drawdown = (rolling_peak - close) / rolling_peak
    max_dd = float(drawdown.tail(60).max())
    base = {"max_dd": max_dd, "trend_ok": trend_ok}
    if max_dd > float(max_drawdown):
        return {**base, "reason": "drawdown_above_limit"}
    recent_low = float(close.tail(20).min())
    rebound = (float(close.iloc[-1]) / recent_low - 1.0) if recent_low > 0 else 0.0
    base["rebound"] = rebound
    if rebound < float(rebound_min):
        return {**base, "reason": "rebound_below_min"}
    returns = close.pct_change().dropna()
    vol = float(returns.tail(20).std())
    base["vol"] = vol
    if vol > float(vol_max):
        return {**base, "reason": "volatility_above_limit"}
    breakout = bool(close.iloc[-1] > close.tail(21).iloc[:-1].max())
    score_info = {
        "score": 0.0,
        "max_dd": max_dd,
        "rebound": rebound,
        "vol": vol,
        "trend_ok": trend_ok,
        "breakout": breakout,
        "reason": "score_computed",
    }
    score_info["score"] += 30.0 if trend_ok else 0.0
    score_info["score"] += max(0.0, 20.0 * (1.0 - max_dd / max(float(max_drawdown), 1e-9)))
    score_info["score"] += min(20.0, 100.0 * rebound)
    score_info["score"] += max(0.0, 20.0 * (1.0 - vol / max(float(vol_max), 1e-9)))
    score_info["score"] += 10.0 if breakout else 0.0
    return score_info


def _evaluate_stable_uptrend_history(
    hist: pd.DataFrame,
    *,
    max_drawdown: float,
    vol_max: float,
    rebound_min: float,
) -> Optional[Dict[str, Any]]:
    out = _diagnose_stable_uptrend_history(
        hist,
        max_drawdown=max_drawdown,
        vol_max=vol_max,
        rebound_min=rebound_min,
    )
    if out.get("reason") != "score_computed":
        return None
    return {k: v for k, v in out.items() if k != "reason"}


@contextmanager
def _temp_log_level(level: int):
    root = logging.getLogger()
    old_root = root.level
    strategy_logger = logging.getLogger("volume_price_analyzer")
    old_strategy = strategy_logger.level
    try:
        root.setLevel(level)
        strategy_logger.setLevel(level)
        yield
    finally:
        root.setLevel(old_root)
        strategy_logger.setLevel(old_strategy)


def _resolve_db_path(preferred: Optional[str]) -> Path:
    return resolve_db_path_dao(preferred)


@contextmanager
def _temp_environ(patch: Dict[str, str]):
    """Temporarily patch process env with lock to avoid cross-run contamination."""
    with runtime_temp_environ(patch, env_lock=_ENV_LOCK):
        yield
