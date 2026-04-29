"""Scan cache with atomic file writes to prevent partial-read corruption."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import glob
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger("openclaw.scan_cache")


def cache_dir() -> str:
    try:
        from openclaw.paths import cache_dir as _cd
        return str(_cd())
    except Exception:
        default_dir = Path(__file__).resolve().parents[2] / "cache_v9"
        return os.getenv("AIRIVO_CACHE_DIR", str(default_dir))


def _atomic_write_text(path: str, content: str) -> None:
    """Write to a temp file then atomically rename to avoid partial reads."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, str(target))
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _atomic_write_csv(path: str, df: pd.DataFrame) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
    try:
        os.close(fd)
        df.to_csv(tmp, index=False)
        os.replace(tmp, str(target))
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ── v7-specific helpers (kept for backward compat) ──────────────────────

def v7_cache_key(params: Dict[str, Any], db_last: str) -> str:
    raw = json.dumps({"params": params, "db_last": db_last}, ensure_ascii=False, sort_keys=True)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def v7_cache_paths(params: Dict[str, Any], db_last: str) -> Tuple[str, str]:
    base = cache_dir()
    os.makedirs(base, exist_ok=True)
    key = v7_cache_key(params, db_last)
    csv_path = os.path.join(base, f"v7_scan_{key}.csv")
    meta_path = os.path.join(base, f"v7_scan_{key}.meta.json")
    return csv_path, meta_path


def load_v7_cache(params: Dict[str, Any], db_last: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    try:
        csv_path, meta_path = v7_cache_paths(params, db_last)
        if not (os.path.exists(csv_path) and os.path.exists(meta_path)):
            return None, {}
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f) or {}
        df = pd.read_csv(csv_path)
        return df, meta
    except Exception as exc:
        logger.warning("load_v7_cache failed: %s", exc)
        return None, {}


def save_v7_cache(params: Dict[str, Any], db_last: str, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
    try:
        csv_path, meta_path = v7_cache_paths(params, db_last)
        meta_out = {
            "params": params,
            "db_last": db_last,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        meta_out.update(meta or {})
        _atomic_write_csv(csv_path, df)
        _atomic_write_text(meta_path, json.dumps(meta_out, ensure_ascii=False, indent=2))
    except Exception as exc:
        logger.warning("save_v7_cache failed: %s", exc)


# ── generic strategy cache ──────────────────────────────────────────────

def scan_cache_key(strategy: str, params: Dict[str, Any], db_last: str) -> str:
    raw = json.dumps({"strategy": strategy, "params": params, "db_last": db_last}, ensure_ascii=False, sort_keys=True)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def scan_cache_paths(strategy: str, params: Dict[str, Any], db_last: str) -> Tuple[str, str]:
    base = cache_dir()
    os.makedirs(base, exist_ok=True)
    key = scan_cache_key(strategy, params, db_last)
    csv_path = os.path.join(base, f"{strategy}_{key}.csv")
    meta_path = os.path.join(base, f"{strategy}_{key}.meta.json")
    return csv_path, meta_path


def load_scan_cache(strategy: str, params: Dict[str, Any], db_last: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    try:
        csv_path, meta_path = scan_cache_paths(strategy, params, db_last)
        if not (os.path.exists(csv_path) and os.path.exists(meta_path)):
            return None, {}
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f) or {}
        df = pd.read_csv(csv_path)
        return df, meta
    except Exception as exc:
        logger.warning("load_scan_cache(%s) failed: %s", strategy, exc)
        return None, {}


def load_scan_cache_meta_from_paths(csv_path: str, meta_path: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    try:
        if not (os.path.exists(csv_path) and os.path.exists(meta_path)):
            return None, {}
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f) or {}
        df = pd.read_csv(csv_path)
        return df, meta if isinstance(meta, dict) else {}
    except Exception as exc:
        logger.warning("load_scan_cache_meta_from_paths failed: %s", exc)
        return None, {}


def find_recent_scan_cache(
    strategy: str,
    db_last: str,
    predicate: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    try:
        base = cache_dir()
        candidates = sorted(
            glob.glob(os.path.join(base, f"{strategy}_*.meta.json")),
            key=os.path.getmtime,
            reverse=True,
        )
        for meta_path in candidates:
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f) or {}
            except Exception:
                continue
            if not isinstance(meta, dict):
                continue
            if str(meta.get("db_last") or "") != str(db_last):
                continue
            if predicate and not predicate(meta):
                continue
            csv_path = meta_path.replace(".meta.json", ".csv")
            df, loaded_meta = load_scan_cache_meta_from_paths(csv_path, meta_path)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df, loaded_meta
        return None, {}
    except Exception as exc:
        logger.warning("find_recent_scan_cache(%s) failed: %s", strategy, exc)
        return None, {}


def save_scan_cache(strategy: str, params: Dict[str, Any], db_last: str, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
    try:
        csv_path, meta_path = scan_cache_paths(strategy, params, db_last)
        meta_out = {
            "strategy": strategy,
            "params": params,
            "db_last": db_last,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        meta_out.update(meta or {})
        _atomic_write_csv(csv_path, df)
        _atomic_write_text(meta_path, json.dumps(meta_out, ensure_ascii=False, indent=2))
    except Exception as exc:
        logger.warning("save_scan_cache(%s) failed: %s", strategy, exc)
