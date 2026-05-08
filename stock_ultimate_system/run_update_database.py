from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.utils.update_status import load_update_status_payload, update_status_path, write_update_status_payload


logger = logging.getLogger(__name__)


def _load_settings(config_dir: Path) -> dict[str, Any]:
    settings_path = config_dir / "settings.yaml"
    with settings_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _resolve_path(project_root: Path, raw_path: str) -> Path:
    p = Path(raw_path).expanduser()
    if p.is_absolute():
        return p
    return (project_root / p).resolve()


def _candidate_retry_universe_sizes(requested_size: int, runtime_cfg: dict[str, Any]) -> list[int]:
    configured = runtime_cfg.get("candidate_retry_universe_sizes", [])
    sizes: list[int] = []
    fallback_sizes = [
        15,
        40,
        min(int(requested_size), 120) if int(requested_size) > 0 else 120,
        requested_size,
    ]
    raw_sizes = configured if isinstance(configured, list) and configured else fallback_sizes
    if int(requested_size) > 0:
        prioritized = [int(requested_size)]
        for raw in raw_sizes:
            try:
                size = int(raw)
            except Exception:
                continue
            if size <= 0 or size > int(requested_size):
                continue
            prioritized.append(size)
        raw_sizes = prioritized
    for raw in raw_sizes:
        try:
            size = int(raw)
        except Exception:
            continue
        if size <= 0:
            continue
        if size not in sizes:
            sizes.append(size)
    if int(requested_size) > 0 and int(requested_size) not in sizes:
        sizes.insert(0, int(requested_size))
    return sizes or [max(int(requested_size or 15), 15)]


def _formal_first_candidate_attempts(attempts: list[int], requested_size: int) -> list[int]:
    unique_attempts: list[int] = []
    for raw in attempts:
        try:
            size = int(raw)
        except Exception:
            continue
        if size > 0 and size not in unique_attempts:
            unique_attempts.append(size)
    if not unique_attempts:
        return [max(int(requested_size or 15), 15)]
    if int(requested_size) <= 0:
        return sorted(unique_attempts)
    smaller = sorted(size for size in unique_attempts if size < int(requested_size))
    larger_or_equal = [size for size in unique_attempts if size >= int(requested_size)]
    return smaller + larger_or_equal


def _resolve_online_post_universe_size(
    requested_size: int,
    runtime_cfg: dict[str, Any],
    *,
    allow_full_market: bool = False,
) -> tuple[int, str]:
    requested = int(requested_size or 0)
    if requested > 0:
        return requested, "requested"
    if allow_full_market:
        return 0, "full_market_allowed"
    configured = runtime_cfg.get("candidate_online_universe_size", 120)
    try:
        effective = max(int(configured), 15)
    except Exception:
        effective = 120
    return effective, "online_default"


def _resolve_latest_trade_date_from_settings(project_root: Path, settings: dict[str, Any]) -> str:
    data_cfg = settings.get("data", {}) if isinstance(settings, dict) else {}
    db_path = str(data_cfg.get("sqlite_db_path", "") or "").strip()
    table = str(data_cfg.get("sqlite_table", "daily_trading_data") or "daily_trading_data").strip()
    if not db_path:
        return ""
    resolved_db = _resolve_path(project_root, db_path)
    if not resolved_db.exists():
        return ""
    conn = sqlite3.connect(str(resolved_db))
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(trade_date) FROM {table}")
        row = cur.fetchone()
        return str(row[0]) if row and row[0] else ""
    except Exception:
        return ""
    finally:
        conn.close()


def _read_token_candidates(project_root: Path, settings: dict[str, Any]) -> list[str]:
    data_cfg = settings.get("data", {})
    candidates: list[Path] = []
    token_file = str(data_cfg.get("tushare_token_file", "")).strip()
    if token_file:
        candidates.append(_resolve_path(project_root, token_file))
    candidates.extend([
        project_root / ".tushare_token",
        project_root.parent / ".tushare_token",
        Path.home() / ".tushare_token",
        project_root / ".env",
        project_root.parent / ".env",
    ])
    tokens: list[str] = []
    env_token = os.getenv("TUSHARE_TOKEN", "").strip()
    if env_token:
        tokens.append(env_token)
    for p in candidates:
        if not p.exists() or not p.is_file():
            continue
        try:
            text = p.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if not text:
            continue
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                if k.strip().upper() in {"TUSHARE_TOKEN", "TS_TOKEN"}:
                    t = v.strip().strip('"').strip("'")
                    if t and t not in tokens:
                        tokens.append(t)
                continue
            t = line.strip().strip('"').strip("'")
            if t and t not in tokens:
                tokens.append(t)
    return tokens


def _classify_tushare_error(exc: Exception) -> str:
    msg = str(exc or "").strip()
    lowered = msg.lower()
    if "没有接口访问权限" in msg or "no permission" in lowered or "permission denied" in lowered:
        return "permission_denied"
    if "token不对" in msg or "token invalid" in lowered or "invalid token" in lowered:
        return "invalid_token"
    if "read timed out" in lowered or "connection" in lowered or "network" in lowered or "dns" in lowered:
        return "network_error"
    return "unknown"


def _format_tushare_init_error(exc: Exception) -> str:
    msg = str(exc or "").strip() or repr(exc)
    reason = _classify_tushare_error(exc)
    if reason == "permission_denied":
        return f"Tushare 账号无当前接口访问权限: {msg}"
    if reason == "invalid_token":
        return f"Tushare token 无效或未生效: {msg}"
    if reason == "network_error":
        return f"Tushare 网络访问失败: {msg}"
    return f"Tushare 初始化失败: {msg}"


def _tushare_error_priority(exc: Exception) -> int:
    reason = _classify_tushare_error(exc)
    if reason == "permission_denied":
        return 3
    if reason == "network_error":
        return 2
    if reason == "invalid_token":
        return 1
    return 0


def _init_tushare(project_root: Path, settings: dict[str, Any]):
    import tushare as ts

    tokens = _read_token_candidates(project_root, settings)
    if not tokens:
        raise RuntimeError("未找到 Tushare token")
    best_err = None
    for idx, token in enumerate(tokens, start=1):
        try:
            ts.set_token(token)
            pro = ts.pro_api(token)
            _ = pro.trade_cal(exchange="SSE", start_date="20260101", end_date="20260110")
            logger.info("Tushare 初始化成功（token #%d）", idx)
            return pro
        except Exception as e:
            if best_err is None or _tushare_error_priority(e) >= _tushare_error_priority(best_err):
                best_err = e
            logger.warning("Tushare token #%d 不可用: %s", idx, _format_tushare_init_error(e))
    raise RuntimeError(_format_tushare_init_error(best_err or RuntimeError("未知错误")))


def _ensure_schema(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_code_date ON {table}(ts_code, trade_date)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_update_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            update_type TEXT,
            start_date TEXT,
            end_date TEXT,
            stocks_count INTEGER,
            success_count INTEGER,
            error_count INTEGER,
            status TEXT,
            error_message TEXT,
            created_at TEXT
        )
        """
    )
    conn.commit()


def _db_latest_date(conn: sqlite3.Connection, table: str) -> str:
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(trade_date) FROM {table}")
    row = cur.fetchone()
    return str(row[0]) if row and row[0] else "20200101"


def _db_latest_date_for_code(conn: sqlite3.Connection, table: str, ts_code: str) -> str:
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(trade_date) FROM {table} WHERE ts_code=?", (ts_code,))
    row = cur.fetchone()
    return str(row[0]) if row and row[0] else "20200101"


def _open_trade_dates(pro, start_date: str, end_date: str) -> list[str]:
    cal = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date)
    if cal is None or cal.empty:
        return []
    cal = cal[cal["is_open"] == 1].copy()
    return sorted(cal["cal_date"].astype(str).tolist())


def _upsert_daily_rows(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for _, r in df.iterrows():
        rows.append(
            (
                str(r.get("ts_code", "")),
                str(r.get("trade_date", "")),
                float(r.get("open", 0) or 0),
                float(r.get("high", 0) or 0),
                float(r.get("low", 0) or 0),
                float(r.get("close", 0) or 0),
                float(r.get("pre_close", 0) or 0),
                float(r.get("change", 0) or 0),
                float(r.get("pct_chg", 0) or 0),
                float(r.get("vol", 0) or 0),
                float(r.get("amount", 0) or 0),
                float(r.get("turnover_rate", 0) or 0),
                now,
            )
        )
    sql = f"""
    INSERT INTO {table}
    (ts_code, trade_date, open_price, high_price, low_price, close_price, pre_close,
     change_amount, pct_chg, vol, amount, turnover_rate, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, trade_date) DO UPDATE SET
      open_price=excluded.open_price,
      high_price=excluded.high_price,
      low_price=excluded.low_price,
      close_price=excluded.close_price,
      pre_close=excluded.pre_close,
      change_amount=excluded.change_amount,
      pct_chg=excluded.pct_chg,
      vol=excluded.vol,
      amount=excluded.amount,
      turnover_rate=excluded.turnover_rate,
      created_at=excluded.created_at
    """
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def _upsert_index_rows(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for _, r in df.iterrows():
        rows.append(
            (
                str(r.get("ts_code", "")),
                str(r.get("trade_date", "")),
                float(r.get("open", 0) or 0),
                float(r.get("high", 0) or 0),
                float(r.get("low", 0) or 0),
                float(r.get("close", 0) or 0),
                float(r.get("pre_close", 0) or 0),
                float(r.get("change", 0) or 0),
                float(r.get("pct_chg", 0) or 0),
                float(r.get("vol", 0) or 0),
                float(r.get("amount", 0) or 0),
                0.0,
                now,
            )
        )
    sql = f"""
    INSERT INTO {table}
    (ts_code, trade_date, open_price, high_price, low_price, close_price, pre_close,
     change_amount, pct_chg, vol, amount, turnover_rate, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, trade_date) DO UPDATE SET
      open_price=excluded.open_price,
      high_price=excluded.high_price,
      low_price=excluded.low_price,
      close_price=excluded.close_price,
      pre_close=excluded.pre_close,
      change_amount=excluded.change_amount,
      pct_chg=excluded.pct_chg,
      vol=excluded.vol,
      amount=excluded.amount,
      turnover_rate=excluded.turnover_rate,
      created_at=excluded.created_at
    """
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def _benchmark_index_codes(data_cfg: dict[str, Any]) -> list[str]:
    raw = data_cfg.get("benchmark_indices", ["000001.SH"])
    if isinstance(raw, str):
        raw_codes = [raw]
    elif isinstance(raw, list):
        raw_codes = raw
    else:
        raw_codes = []
    codes: list[str] = []
    for value in raw_codes:
        code = str(value or "").strip()
        if code and code not in codes:
            codes.append(code)
    return codes or ["000001.SH"]


def _update_benchmark_indices(
    conn: sqlite3.Connection,
    table: str,
    pro,
    index_codes: list[str],
    today: str,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "requested_codes": list(index_codes),
        "processed_codes": [],
        "failed_codes": [],
        "written_rows": 0,
        "latest_before": {},
        "latest_after": {},
    }
    for code in index_codes:
        latest_before = _db_latest_date_for_code(conn, table, code)
        summary["latest_before"][code] = latest_before
        try:
            dates = _open_trade_dates(pro, latest_before, today)
            pending = [d for d in dates if d > latest_before]
            if not pending:
                summary["processed_codes"].append(code)
                summary["latest_after"][code] = latest_before
                continue
            frame = pro.index_daily(ts_code=code, start_date=pending[0], end_date=pending[-1])
            if frame is None or frame.empty:
                summary["failed_codes"].append(code)
                summary["latest_after"][code] = latest_before
                continue
            written = _upsert_index_rows(conn, table, frame)
            summary["written_rows"] = int(summary["written_rows"]) + written
            summary["processed_codes"].append(code)
            summary["latest_after"][code] = _db_latest_date_for_code(conn, table, code)
            logger.info("benchmark index=%s 更新完成，写入/更新 %d 行", code, written)
        except Exception as e:
            summary["failed_codes"].append(code)
            summary["latest_after"][code] = _db_latest_date_for_code(conn, table, code)
            logger.exception("benchmark index=%s 更新失败: %s", code, e)
    summary["status"] = "completed" if not summary["failed_codes"] else "partial_success"
    return summary


def _write_update_log(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    stocks_count: int,
    success_count: int,
    error_count: int,
    status: str,
    error_message: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO data_update_log
        (update_type, start_date, end_date, stocks_count, success_count, error_count, status, error_message, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "daily_trading_update",
            start_date,
            end_date,
            int(stocks_count),
            int(success_count),
            int(error_count),
            status,
            error_message,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    conn.commit()


def _status_path(project_root: Path) -> Path:
    return update_status_path(project_root)


def _write_status(project_root: Path, payload: dict[str, Any]) -> None:
    write_update_status_payload(payload, project_root)


def _sync_update_status(progress: dict[str, Any]) -> None:
    root = Path(__file__).resolve().parent
    p = _status_path(root)
    if not p.exists():
        return
    try:
        obj = load_update_status_payload(root)
    except Exception:
        return
    obj.setdefault("update_summary", {})
    obj["update_summary"].update(progress)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def run_update(config_dir: str) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parent
    settings = _load_settings(project_root / config_dir)
    data_cfg = settings.get("data", {})
    db_path = _resolve_path(project_root, str(data_cfg.get("sqlite_db_path", "")).strip())
    table = str(data_cfg.get("sqlite_table", "daily_trading_data")).strip() or "daily_trading_data"
    benchmark_indices = _benchmark_index_codes(data_cfg)
    if not db_path.exists():
        raise FileNotFoundError(f"数据库文件不存在: {db_path}")

    conn = sqlite3.connect(str(db_path))
    _ensure_schema(conn, table)
    db_max = _db_latest_date(conn, table)
    today = datetime.now().strftime("%Y%m%d")
    pro = _init_tushare(project_root, settings)
    dates = _open_trade_dates(pro, db_max, today)
    pending = [d for d in dates if d > db_max]
    summary: dict[str, Any] = {
        "db_path": str(db_path),
        "table": table,
        "db_latest_before": db_max,
        "pending_dates": pending,
        "processed_dates": [],
        "failed_dates": [],
        "status": "running",
        "written_rows": 0,
        "benchmark_indices": {
            "requested_codes": benchmark_indices,
            "processed_codes": [],
            "failed_codes": [],
            "written_rows": 0,
        },
        "progress_pct": 0.0,
    }

    if not pending:
        logger.info("无需更新，数据库已是最新交易日: %s", db_max)
        benchmark_summary = _update_benchmark_indices(conn, table, pro, benchmark_indices, today)
        status = "up_to_date" if benchmark_summary["status"] == "completed" else "partial_success"
        _write_update_log(
            conn,
            db_max,
            db_max,
            int(benchmark_summary["written_rows"]),
            len(benchmark_summary["processed_codes"]),
            len(benchmark_summary["failed_codes"]),
            status,
            "" if status == "up_to_date" else f"benchmark_failed_codes={benchmark_summary['failed_codes']}",
        )
        db_latest_after = _db_latest_date(conn, table)
        conn.close()
        summary.update({
            "status": status,
            "db_latest_after": db_latest_after,
            "written_rows": int(benchmark_summary["written_rows"]),
            "benchmark_indices": benchmark_summary,
        })
        return summary

    logger.info("准备更新交易日: %s", pending)
    total_rows = 0
    failed_days: list[str] = []
    for d in pending:
        try:
            daily = pro.daily(trade_date=d)
            if daily is None or daily.empty:
                logger.warning("trade_date=%s 无日线数据，跳过", d)
                failed_days.append(d)
                continue
            basic = pro.daily_basic(trade_date=d, fields="ts_code,trade_date,turnover_rate")
            if basic is not None and not basic.empty:
                daily = daily.merge(
                    basic[["ts_code", "trade_date", "turnover_rate"]],
                    on=["ts_code", "trade_date"],
                    how="left",
                )
            if "turnover_rate" not in daily.columns:
                daily["turnover_rate"] = 0.0
            written = _upsert_daily_rows(conn, table, daily)
            total_rows += written
            summary["processed_dates"].append(d)
            summary["written_rows"] = total_rows
            done = len(summary["processed_dates"]) + len(failed_days)
            total = len(pending)
            summary["progress_pct"] = round(done * 100.0 / total, 1) if total > 0 else 100.0
            _sync_update_status({
                "processed_dates": summary["processed_dates"],
                "failed_dates": failed_days,
                "written_rows": total_rows,
                "progress_pct": summary["progress_pct"],
            })
            logger.info("trade_date=%s 更新完成，写入/更新 %d 行", d, written)
        except Exception as e:
            failed_days.append(d)
            summary["failed_dates"] = failed_days
            done = len(summary["processed_dates"]) + len(failed_days)
            total = len(pending)
            summary["progress_pct"] = round(done * 100.0 / total, 1) if total > 0 else 100.0
            _sync_update_status({
                "processed_dates": summary["processed_dates"],
                "failed_dates": failed_days,
                "written_rows": total_rows,
                "progress_pct": summary["progress_pct"],
            })
            logger.exception("trade_date=%s 更新失败: %s", d, e)

    benchmark_summary = _update_benchmark_indices(conn, table, pro, benchmark_indices, today)
    total_written_rows = total_rows + int(benchmark_summary["written_rows"])
    status = "completed" if not failed_days and benchmark_summary["status"] == "completed" else "partial_success"
    err_parts = []
    if failed_days:
        err_parts.append(f"failed_days={failed_days}")
    if benchmark_summary["failed_codes"]:
        err_parts.append(f"benchmark_failed_codes={benchmark_summary['failed_codes']}")
    err_msg = "; ".join(err_parts)
    _write_update_log(
        conn,
        pending[0],
        pending[-1],
        stocks_count=total_written_rows,
        success_count=len(pending) - len(failed_days),
        error_count=len(failed_days),
        status=status,
        error_message=err_msg,
    )
    db_latest_after = _db_latest_date(conn, table)
    conn.close()
    logger.info("数据库更新完成：status=%s, total_rows=%d", status, total_rows)
    summary.update({
        "status": status,
        "db_latest_after": db_latest_after,
        "failed_dates": failed_days,
        "written_rows": total_written_rows,
        "stock_written_rows": total_rows,
        "benchmark_indices": benchmark_summary,
        "progress_pct": 100.0,
    })
    return summary


def run_post_candidates(
    project_root: Path,
    config_dir: str,
    universe_size: int,
    top_n: int,
    *,
    return_meta: bool = False,
) -> tuple[bool, str] | tuple[bool, str, dict[str, Any]]:
    settings = _load_settings(project_root / config_dir)
    runtime_cfg = settings.get("runtime", {}) if isinstance(settings, dict) else {}
    timeout_sec = float(runtime_cfg.get("candidate_timeout_sec", 90) or 90)
    latest_csv = project_root / "data" / "experiments" / "candidates_top_latest.csv"
    latest_md = project_root / "data" / "experiments" / "candidates_top_latest.md"
    latest_summary = project_root / "data" / "experiments" / "candidates_basket_summary_latest.json"
    attempts = _formal_first_candidate_attempts(
        _candidate_retry_universe_sizes(int(universe_size), runtime_cfg),
        int(universe_size),
    )
    notes: list[str] = []
    last_interim_meta: dict[str, Any] | None = None
    base_meta: dict[str, Any] = {
        "mode": "quick",
        "skip_validation": True,
        "timeout_sec": timeout_sec,
        "requested_universe_size": int(universe_size),
        "attempts": list(attempts),
        "attempt_strategy": "formal_first",
        "effective_universe_size": None,
        "elapsed_sec": None,
        "used_attempt": None,
    }

    def _fresh_output_exists(started_at: float) -> bool:
        if not (latest_csv.exists() and latest_md.exists()):
            return False
        try:
            return min(latest_csv.stat().st_mtime, latest_md.stat().st_mtime) >= started_at
        except Exception:
            return False

    def _latest_output_is_formal(started_at: float) -> bool:
        if not latest_summary.exists():
            return False
        try:
            if latest_summary.stat().st_mtime < started_at:
                return False
            payload = json.loads(latest_summary.read_text(encoding="utf-8"))
        except Exception:
            return False
        degraded = bool(payload.get("generation_degraded", False))
        guardrail_mode = str(payload.get("guardrail_mode", "") or "").strip()
        generation_reason = str(payload.get("generation_reason", "") or "").strip()
        return not degraded and guardrail_mode != "interim" and generation_reason != "interim_partial_generation"

    def _latest_candidate_count(started_at: float) -> int | None:
        if not latest_summary.exists():
            return None
        try:
            if latest_summary.stat().st_mtime < started_at:
                return None
            payload = json.loads(latest_summary.read_text(encoding="utf-8"))
            return int(payload.get("candidate_count", 0) or 0)
        except Exception:
            return None

    for attempt_size in attempts:
        cmd = [
            sys.executable,
            str(project_root / "run_top_candidates.py"),
            "--config-dir",
            str(config_dir),
            "--universe-size",
            str(int(attempt_size)),
            "--top-n",
            str(int(top_n)),
            "--skip-validation",
            "--quick-mode",
        ]
        attempt_started_at = time.time()
        try:
            logger.info(
                "开始生成候选股：universe_size=%d, top_n=%d, timeout=%.0fs",
                attempt_size, int(top_n), timeout_sec,
            )
            proc = subprocess.run(
                cmd,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_sec,
            )
            output_ok = _fresh_output_exists(attempt_started_at)
            if proc.returncode == 0 and output_ok:
                if not _latest_output_is_formal(attempt_started_at):
                    notes.append(f"{attempt_size}:interim_output")
                    last_interim_meta = dict(base_meta)
                    last_interim_meta["effective_universe_size"] = int(attempt_size)
                    last_interim_meta["elapsed_sec"] = round(time.time() - attempt_started_at, 2)
                    last_interim_meta["used_attempt"] = int(attempt_size)
                    last_interim_meta["degraded"] = True
                    last_interim_meta["interim_only"] = True
                    logger.warning("候选股生成仅产出 interim 结果：universe_size=%d", attempt_size)
                    continue
                candidate_count = _latest_candidate_count(attempt_started_at)
                if candidate_count is not None and candidate_count <= 0 and int(attempt_size) < max(attempts):
                    notes.append(f"{attempt_size}:zero_candidates")
                    logger.warning(
                        "候选股生成产出 0 个候选，继续尝试更大股票池：universe_size=%d",
                        attempt_size,
                    )
                    continue
                detail = f"候选股生成成功(universe_size={attempt_size})"
                stdout = (proc.stdout or "").strip()
                if stdout:
                    tail = "\n".join(stdout.splitlines()[-3:])
                    detail = f"{detail}｜{tail}"
                if notes:
                    detail = f"{detail}｜fallbacks={'; '.join(notes)}"
                meta = dict(base_meta)
                meta["effective_universe_size"] = int(attempt_size)
                meta["elapsed_sec"] = round(time.time() - attempt_started_at, 2)
                meta["used_attempt"] = int(attempt_size)
                return (True, detail[:500], meta) if return_meta else (True, detail[:500])
            stderr = (proc.stderr or proc.stdout or "").strip() or "候选股产物未生成"
            notes.append(f"{attempt_size}:rc={proc.returncode}")
            logger.warning("候选股生成失败：universe_size=%d detail=%s", attempt_size, stderr[:300])
        except subprocess.TimeoutExpired:
            if _fresh_output_exists(attempt_started_at):
                notes.append(f"{attempt_size}:timeout_with_interim")
                logger.warning(
                    "候选股生成超时但已写出中间产物：universe_size=%d timeout=%.0fs",
                    attempt_size,
                    timeout_sec,
                )
                last_interim_meta = dict(base_meta)
                last_interim_meta["effective_universe_size"] = int(attempt_size)
                last_interim_meta["elapsed_sec"] = round(time.time() - attempt_started_at, 2)
                last_interim_meta["used_attempt"] = int(attempt_size)
                last_interim_meta["degraded"] = True
                last_interim_meta["interim_only"] = True
                continue
            notes.append(f"{attempt_size}:timeout")
            logger.warning("候选股生成超时：universe_size=%d timeout=%.0fs", attempt_size, timeout_sec)
            continue
        except Exception as e:
            notes.append(f"{attempt_size}:{type(e).__name__}")
            logger.warning("候选股生成异常：universe_size=%d error=%s", attempt_size, e)
            continue

    detail = "候选股生成失败"
    if notes:
        detail = f"{detail}｜attempts={'; '.join(notes)}"
    if last_interim_meta is not None:
        detail = f"候选股仅生成 interim 结果，未形成正式 latest｜attempts={'; '.join(notes)}"
        return (False, detail[:500], last_interim_meta) if return_meta else (False, detail[:500])
    meta = dict(base_meta)
    meta["elapsed_sec"] = round(timeout_sec * max(len(attempts), 1), 2)
    return (False, detail[:500], meta) if return_meta else (False, detail[:500])


def run_post_candidate_data_quality_report(project_root: Path, config_dir: str) -> tuple[bool, str]:
    settings = _load_settings(project_root / config_dir)
    latest_trade_date = _resolve_latest_trade_date_from_settings(project_root, settings)
    cmd = [
        sys.executable,
        str(project_root / "scripts" / "build_candidate_data_quality_report.py"),
        "--config",
        str(Path(config_dir) / "settings.yaml"),
        "--output-dir",
        "data/experiments",
    ]
    if latest_trade_date:
        cmd.extend(["--expected-latest-trade-date", latest_trade_date])
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=False,
        )
        detail = (proc.stdout or proc.stderr or "").strip()
        if proc.returncode != 0:
            return False, detail[:500] or "candidate data quality report failed"
        suffix = f"data_as_of={latest_trade_date}" if latest_trade_date else "data_as_of=unknown"
        return True, f"candidate data quality report refreshed ({suffix})"
    except Exception as e:
        return False, str(e)


def run_post_daily_research(
    project_root: Path,
    config_dir: str,
    profiles: list[str],
) -> tuple[bool, str]:
    cmd = [
        sys.executable,
        str(project_root / "run_daily_research.py"),
        "--config-dir",
        str(config_dir),
        "--out",
        "data/experiments/daily_research_latest.md",
    ]
    if profiles:
        cmd.extend(["--profiles", *profiles])
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            return False, detail[:500]
        return True, "每日研究生成成功"
    except Exception as e:
        return False, str(e)


def run_post_buylist_snapshot(project_root: Path, target_count: int) -> tuple[bool, str]:
    cmd = [
        sys.executable,
        str(project_root / "run_buylist_snapshot.py"),
        "--input-csv",
        "data/experiments/candidates_top_latest.csv",
        "--output-dir",
        "data/experiments",
        "--target-count",
        str(int(target_count)),
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            return False, detail[:500]
        return True, "buylist snapshot refreshed"
    except Exception as e:
        return False, str(e)


def run_post_candidate_basket_snapshot(project_root: Path, top_n: int) -> tuple[bool, str, dict[str, object]]:
    summary_json_path = "data/experiments/candidates_basket_summary_latest.json"
    snapshot_output_path = "artifacts/primary_result_candidate_baskets/latest_attempt.json"
    cmd = [
        sys.executable,
        str(project_root / "scripts" / "register_primary_result_candidate_basket.py"),
        "--candidates-csv",
        "data/experiments/candidates_top_latest.csv",
        "--summary-json",
        summary_json_path,
        "--baskets-dir",
        "artifacts/primary_result_candidate_baskets",
        "--run-id",
        "daily-candidate-basket",
        "--top-n",
        str(int(top_n)),
        "--snapshot-output",
        snapshot_output_path,
        "--json",
    ]
    meta: dict[str, object] = {
        "summary_json_path": summary_json_path,
        "snapshot_output_path": snapshot_output_path,
    }
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            return False, detail[:500], meta
        try:
            payload = json.loads(proc.stdout or "{}")
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            meta["status"] = payload.get("status")
            meta["basket_id"] = payload.get("basket_id")
            meta["registered"] = payload.get("registered")
            pointer = payload.get("current_pointer")
            if isinstance(pointer, dict):
                meta["current_snapshot_path"] = pointer.get("snapshot_path")
                meta["current_snapshot_hash"] = pointer.get("snapshot_hash")
        return True, "candidate basket snapshot registered", meta
    except Exception as e:
        return False, str(e), meta


def run_post_candidate_artifacts(project_root: Path, top_n: int) -> tuple[int, dict[str, dict[str, object]]]:
    failures = 0
    artifacts: dict[str, dict[str, object]] = {}
    buylist_ok, buylist_detail = run_post_buylist_snapshot(project_root, top_n)
    artifacts["buylist_snapshot"] = {"ok": buylist_ok, "detail": buylist_detail}
    if not buylist_ok:
        failures += 1
    basket_ok, basket_detail, basket_meta = run_post_candidate_basket_snapshot(project_root, top_n)
    artifacts["candidate_basket_snapshot"] = {"ok": basket_ok, "detail": basket_detail, **basket_meta}
    if not basket_ok:
        failures += 1
    return failures, artifacts


def _can_use_local_db_after_update_failure(project_root: Path, config_dir: str) -> tuple[bool, str]:
    try:
        settings = _load_settings(project_root / config_dir)
        data_cfg = settings.get("data", {})
        db_path = _resolve_path(project_root, str(data_cfg.get("sqlite_db_path", "")).strip())
        table = str(data_cfg.get("sqlite_table", "daily_trading_data")).strip() or "daily_trading_data"
        if not db_path.exists():
            return False, f"数据库文件不存在: {db_path}"
        conn = sqlite3.connect(str(db_path))
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(trade_date), COUNT(*) FROM {table}")
            row = cur.fetchone()
        finally:
            conn.close()
        latest = str(row[0]) if row and row[0] else ""
        count = int(row[1]) if row and row[1] else 0
        if not latest or count <= 0:
            return False, "本地数据库无可用行情数据"
        return True, f"fallback_to_local_db(latest_trade_date={latest}, rows={count})"
    except Exception as e:
        return False, str(e)


def main() -> None:
    parser = argparse.ArgumentParser(description="按交易日历增量更新 SQLite 股票数据库")
    parser.add_argument("--config-dir", default="config", help="配置目录（包含 settings.yaml）")
    parser.add_argument(
        "--post-top-candidates",
        dest="post_top_candidates",
        action="store_true",
        default=True,
        help="更新后自动生成候选股票",
    )
    parser.add_argument(
        "--no-post-top-candidates",
        dest="post_top_candidates",
        action="store_false",
        help="更新后不生成候选股票",
    )
    parser.add_argument("--post-universe-size", type=int, default=300, help="自动候选股票池规模（0=全市场）")
    parser.add_argument(
        "--allow-full-market-post-candidates",
        action="store_true",
        help="允许更新后候选链直接使用全市场模式（线上默认建议关闭，仅离线链开启）",
    )
    parser.add_argument("--post-top-n", type=int, default=10, help="自动候选输出数量")
    parser.add_argument(
        "--post-daily-research",
        dest="post_daily_research",
        action="store_true",
        default=True,
        help="更新后自动运行每日研究并产出健康评分",
    )
    parser.add_argument(
        "--no-post-daily-research",
        dest="post_daily_research",
        action="store_false",
        help="更新后不运行每日研究",
    )
    parser.add_argument(
        "--post-research-profiles",
        nargs="*",
        default=["short", "medium"],
        choices=["short", "medium", "long"],
        help="自动每日研究的 profile 序列",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    root = Path(__file__).resolve().parent
    settings = _load_settings(root / args.config_dir)
    runtime_cfg = settings.get("runtime", {}) if isinstance(settings, dict) else {}
    effective_post_universe_size, post_universe_mode = _resolve_online_post_universe_size(
        args.post_universe_size,
        runtime_cfg if isinstance(runtime_cfg, dict) else {},
        allow_full_market=bool(args.allow_full_market_post_candidates),
    )
    started_at = datetime.now().isoformat()
    status_payload: dict[str, Any] = {
        "started_at": started_at,
        "ended_at": None,
        "stage": "updating_database",
        "status": "running",
        "config_dir": args.config_dir,
        "post_top_candidates": bool(args.post_top_candidates),
        "post_universe_size": int(args.post_universe_size),
        "post_effective_universe_size": int(effective_post_universe_size),
        "post_universe_mode": str(post_universe_mode),
        "allow_full_market_post_candidates": bool(args.allow_full_market_post_candidates),
        "post_top_n": int(args.post_top_n),
        "post_daily_research": bool(args.post_daily_research),
        "post_research_profiles": list(args.post_research_profiles),
    }
    _write_status(root, status_payload)
    try:
        update_summary = run_update(args.config_dir)
        status_payload["update_summary"] = update_summary
        post_failures = 0
        if args.post_top_candidates:
            dq_ok, dq_detail = run_post_candidate_data_quality_report(root, args.config_dir)
            status_payload["post_candidate_data_quality_report"] = {"ok": dq_ok, "detail": dq_detail}
            if dq_ok:
                logger.info("更新后候选数据质量报告已刷新：%s", dq_detail)
            else:
                post_failures += 1
                logger.warning("更新后候选数据质量报告刷新失败：%s", dq_detail)
            status_payload["stage"] = "generating_candidates"
            _write_status(root, status_payload)
            ok, detail, candidate_meta = run_post_candidates(
                root,
                args.config_dir,
                effective_post_universe_size,
                args.post_top_n,
                return_meta=True,
            )
            status_payload["post_candidates"] = {"ok": ok, "detail": detail}
            status_payload["post_candidates_meta"] = candidate_meta
            if ok:
                logger.info(
                    "更新后候选生成完成：Top%d，股票池规模=%d（mode=%s，请求=%d）",
                    args.post_top_n,
                    effective_post_universe_size,
                    post_universe_mode,
                    args.post_universe_size,
                )
                artifact_failures, artifacts = run_post_candidate_artifacts(root, args.post_top_n)
                status_payload["post_buylist_snapshot"] = artifacts["buylist_snapshot"]
                status_payload["post_candidate_basket_snapshot"] = artifacts["candidate_basket_snapshot"]
                post_failures += artifact_failures
                if artifacts["buylist_snapshot"]["ok"]:
                    logger.info("更新后 buylist snapshot 已刷新")
                else:
                    logger.warning("更新后 buylist snapshot 刷新失败：%s", artifacts["buylist_snapshot"]["detail"])
                if artifacts["candidate_basket_snapshot"]["ok"]:
                    logger.info("更新后 candidate basket snapshot 已注册")
                else:
                    logger.warning("更新后 candidate basket snapshot 注册失败：%s", artifacts["candidate_basket_snapshot"]["detail"])
            else:
                post_failures += 1
                logger.warning("更新后候选生成失败：%s", detail)
        if args.post_daily_research:
            status_payload["stage"] = "running_daily_research"
            _write_status(root, status_payload)
            ok, detail = run_post_daily_research(root, args.config_dir, args.post_research_profiles)
            status_payload["post_daily_research"] = {"ok": ok, "detail": detail}
            if ok:
                logger.info("更新后每日研究完成：profiles=%s", args.post_research_profiles)
            else:
                post_failures += 1
                logger.warning("更新后每日研究失败：%s", detail)
        status_payload["stage"] = "done"
        update_status = str(update_summary.get("status", "completed"))
        status_payload["status"] = "partial_success" if post_failures or update_status == "partial_success" else update_status
    except Exception as e:
        logger.exception("数据库更新任务失败: %s", e)
        can_fallback, detail = _can_use_local_db_after_update_failure(root, args.config_dir)
        status_payload["update_summary"] = {
            "status": "failed",
            "error": str(e),
            "local_fallback_ready": can_fallback,
            "local_fallback_detail": detail,
        }
        status_payload["error"] = str(e)
        if not can_fallback:
            status_payload["stage"] = "failed"
            status_payload["status"] = "failed"
            raise

        post_failures = 0
        status_payload["stage"] = "update_failed_using_local_db"
        status_payload["status"] = "partial_success"
        if args.post_top_candidates:
            dq_ok, dq_detail = run_post_candidate_data_quality_report(root, args.config_dir)
            status_payload["post_candidate_data_quality_report"] = {"ok": dq_ok, "detail": dq_detail}
            if dq_ok:
                logger.info("本地库兜底候选数据质量报告已刷新：%s", dq_detail)
            else:
                post_failures += 1
                logger.warning("本地库兜底候选数据质量报告刷新失败：%s", dq_detail)
            status_payload["stage"] = "generating_candidates"
            _write_status(root, status_payload)
            ok, detail, candidate_meta = run_post_candidates(
                root,
                args.config_dir,
                effective_post_universe_size,
                args.post_top_n,
                return_meta=True,
            )
            status_payload["post_candidates"] = {"ok": ok, "detail": detail}
            status_payload["post_candidates_meta"] = candidate_meta
            if ok:
                logger.info(
                    "本地库兜底候选生成完成：Top%d，股票池规模=%d（mode=%s，请求=%d）",
                    args.post_top_n,
                    effective_post_universe_size,
                    post_universe_mode,
                    args.post_universe_size,
                )
                artifact_failures, artifacts = run_post_candidate_artifacts(root, args.post_top_n)
                status_payload["post_buylist_snapshot"] = artifacts["buylist_snapshot"]
                status_payload["post_candidate_basket_snapshot"] = artifacts["candidate_basket_snapshot"]
                post_failures += artifact_failures
                if artifacts["buylist_snapshot"]["ok"]:
                    logger.info("本地库兜底 buylist snapshot 已刷新")
                else:
                    logger.warning("本地库兜底 buylist snapshot 刷新失败：%s", artifacts["buylist_snapshot"]["detail"])
                if artifacts["candidate_basket_snapshot"]["ok"]:
                    logger.info("本地库兜底 candidate basket snapshot 已注册")
                else:
                    logger.warning("本地库兜底 candidate basket snapshot 注册失败：%s", artifacts["candidate_basket_snapshot"]["detail"])
            else:
                post_failures += 1
                logger.warning("本地库兜底候选生成失败：%s", detail)
        if args.post_daily_research:
            status_payload["stage"] = "running_daily_research"
            _write_status(root, status_payload)
            ok, detail = run_post_daily_research(root, args.config_dir, args.post_research_profiles)
            status_payload["post_daily_research"] = {"ok": ok, "detail": detail}
            if ok:
                logger.info("本地库兜底每日研究完成：profiles=%s", args.post_research_profiles)
            else:
                post_failures += 1
                logger.warning("本地库兜底每日研究失败：%s", detail)
        status_payload["stage"] = "done"
        status_payload["status"] = "partial_success" if post_failures else "degraded_success"
    finally:
        status_payload["ended_at"] = datetime.now().isoformat()
        _write_status(root, status_payload)


if __name__ == "__main__":
    main()
