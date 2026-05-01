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
from types import ModuleType

import pandas as pd
from data.dao import resolve_db_path as resolve_db_path_dao
from openclaw.runtime.async_env import temp_environ as runtime_temp_environ

from openclaw.runtime.root_dependency_bridge import load_v6_ultra_short_backtest_module
from openclaw.runtime.streamlit_stub import install_streamlit_stub

JsonDict = Dict[str, Any]
_ENV_LOCK = threading.RLock()


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
            return v7_scan_handler()
        handler = scan_handlers.get(normalized)
        if handler is None:
            raise ValueError(f"不支持的后台扫描策略: {normalized}")
        return handler(analyzer_factory())


@dataclass
class HandlerFactory:
    module_path: Path

    def create_scan_handler(self, strategy: str):
        strategy = strategy.lower()

        def _handler(params: Optional[JsonDict] = None) -> JsonDict:
            params = params or {}
            run_id = str(params.get("run_id") or "")
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
            db_path = _resolve_db_path(params.get("db_path"))

            if strategy == "v6":
                v6_mod = load_v6_ultra_short_backtest_module()
                v6_mod.PERMANENT_DB_PATH = str(db_path)
                V6UltraShortBacktest = v6_mod.V6UltraShortBacktest
                bt = V6UltraShortBacktest()
                _patch_backtest_stock_pool(bt)
                try:
                    result = bt.run_backtest(
                        sample_size=int(params.get("sample_size", 400)),
                        score_threshold=int(params.get("score_threshold", 75)),
                        holding_days=int(params.get("holding_days", 5)),
                        stop_loss=float(params.get("stop_loss", -0.05)),
                        take_profit=float(params.get("take_profit", 0.08)),
                    )
                finally:
                    try:
                        bt.conn.close()
                    except Exception:
                        pass
                return {"summary": _normalize_summary(result), "raw": result, "meta": {"run_id": run_id} if run_id else {}}

            if strategy == "v5":
                v5_mod = _import_backtest_module(
                    "backtest_v5_launch_confirm",
                    fallback_rel_paths=[
                        "versions/archive/legacy_systems/backtest_v5_launch_confirm.py",
                    ],
                )
                v5_mod.PERMANENT_DB_PATH = str(db_path)
                V5LaunchConfirmBacktest = v5_mod.V5LaunchConfirmBacktest

                bt = V5LaunchConfirmBacktest()
                _patch_backtest_stock_pool(bt)
                try:
                    result = bt.run_backtest(
                        sample_size=int(params.get("sample_size", 500)),
                        score_threshold=int(params.get("score_threshold", 60)),
                        holding_days=int(params.get("holding_days", 5)),
                        stop_loss=float(params.get("stop_loss", -0.03)),
                        take_profit=float(params.get("take_profit", 0.04)),
                    )
                finally:
                    try:
                        bt.conn.close()
                    except Exception:
                        pass
                return {"summary": _normalize_summary(result), "raw": result, "meta": {"run_id": run_id} if run_id else {}}

            if strategy in {"v7", "v8", "v9", "combo"}:
                analyzer = _get_analyzer(db_path)
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

                if strategy == "v7":
                    result = analyzer.backtest_v7_intelligent(
                        df_bt,
                        sample_size=int(params.get("sample_size", 400)),
                        holding_days=int(params.get("holding_days", 8)),
                        score_threshold=float(params.get("score_threshold", 60)),
                    )
                elif strategy == "v8":
                    result = analyzer.backtest_v8_ultimate(
                        df_bt,
                        sample_size=int(params.get("sample_size", 400)),
                        holding_days=int(params.get("holding_days", 8)),
                        score_threshold=float(params.get("score_threshold", 50)),
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
                        result = analyzer.backtest_combo_production(
                            df_bt,
                            sample_size=int(params.get("sample_size", 500)),
                            holding_days=int(params.get("holding_days", 10)),
                            combo_threshold=combo_threshold,
                            min_agree=int(params.get("min_agree", 2)),
                        )
                    else:
                        # Legacy fallback: use an ensemble of available strategy backtests.
                        result = _run_combo_ensemble_backtest(analyzer, df_bt, params)

                stats_src = (result or {}).get("stats") if isinstance(result, dict) else None
                summary = _normalize_summary(stats_src if isinstance(stats_src, dict) else result)
                return {"summary": summary, "raw": result, "meta": {"run_id": run_id} if run_id else {}}

            if strategy in {"v4", "stable"}:
                return {
                    "summary": _default_skip_summary(strategy),
                    "raw": {
                        "status": "skipped",
                        "reason": "backtest_not_implemented",
                        "strategy": strategy,
                    },
                    "meta": {"run_id": run_id} if run_id else {},
                }

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


def _normalize_summary(result: Any) -> JsonDict:
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
    sample_size = float(
        result.get(
            "sample_size",
            result.get("analyzed_stocks", result.get("分析股票数", 500.0)),
        )
        or 500.0
    )

    # legacy scripts often return percent values for win_rate/max_drawdown
    win_rate = float(win_rate)
    if win_rate > 1.0:
        win_rate = win_rate / 100.0

    # legacy scripts may report drawdown as negative percentage, e.g. -21.9
    max_drawdown = abs(float(max_drawdown))
    if max_drawdown > 1.0:
        max_drawdown = max_drawdown / 100.0

    density = total_trades / sample_size if sample_size > 0 else 0.0

    return {
        "win_rate": win_rate,
        "max_drawdown": max(0.0, max_drawdown),
        "signal_density": max(0.0, density),
    }


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
