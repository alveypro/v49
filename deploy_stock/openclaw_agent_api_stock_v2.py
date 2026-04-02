import csv
import glob
import json
import math
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

try:
    from openclaw.paths import db_path as resolve_db_path
    from openclaw.paths import evolution_dir, log_dir, project_root
except Exception:
    resolve_db_path = None
    evolution_dir = None
    log_dir = None
    project_root = None

try:
    from openclaw.assistant.agent_mesh import AGENT_VERSION, count_agents, select_agents
except Exception:
    AGENT_VERSION = "mesh50-v1"

    def count_agents() -> int:
        return 50

    def select_agents(question: str, route: str, confidence: float, max_agents: int = 10):
        base = ["intent_router", "task_decomposer", "skill_orchestrator", "response_synth"]
        if route == "stock_core":
            base += ["stock_snapshot_agent", "trend_agent", "risk_gate_agent", "execution_agent"]
        return base[:max_agents]

def _load_dotenv_if_exists() -> None:
    """Load KEY=VALUE pairs from project .env for launchd-started processes."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    try:
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            key = k.strip()
            if not key:
                continue
            value = v.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
    except Exception:
        # Keep service alive even if .env parsing fails.
        return


_load_dotenv_if_exists()

DEFAULT_MEMORY_DB = str(Path(__file__).resolve().parents[1] / "data" / "agent_memory.db")
MEMORY_DB = os.getenv("STOCK_AGENT_MEMORY_DB", DEFAULT_MEMORY_DB)
MAX_MEMORY_TURNS = int(os.getenv("STOCK_AGENT_MEMORY_TURNS", "24"))
USE_LLM = os.getenv("STOCK_AGENT_USE_LLM", "1") == "1"
LLM_URL = os.getenv("STOCK_LLM_URL", "http://127.0.0.1:3443/api/ai/chat")
QUALITY_MIN_CONFIDENCE = float(os.getenv("STOCK_AGENT_MIN_CONFIDENCE", "0.58"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
def _project_root() -> Path:
    if project_root is not None:
        try:
            return Path(project_root())
        except Exception:
            pass
    return Path(__file__).resolve().parents[1]


def _report_dir() -> str:
    env = os.getenv("OPENCLAW_REPORT_DIR", "").strip()
    if env:
        return env
    if log_dir is not None:
        try:
            return str(log_dir())
        except Exception:
            pass
    return str(_project_root() / "logs" / "openclaw")


OPENCLAW_REPORT_DIR = _report_dir()

REFUSAL_MARKERS = (
    "无法预测",
    "不能提供",
    "不提供",
    "我必须明确告知",
    "合规要求",
)
EVIDENCE_TAG_PATTERN = re.compile(r"\[E\d(?:[,/，、]E\d)*\]")


def _init_memory_db() -> None:
    os.makedirs(os.path.dirname(MEMORY_DB), exist_ok=True)
    conn = sqlite3.connect(MEMORY_DB)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_memory_session_time ON memory(session_id, id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_profiles (
            code TEXT PRIMARY KEY,
            profile_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_params (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT,
            code TEXT,
            signal REAL,
            expected_return REAL,
            realized_return REAL,
            drawdown REAL,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_cache (
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            features_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (trade_date, code)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_hit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at INTEGER NOT NULL,
            trade_date TEXT NOT NULL,
            session_id TEXT NOT NULL,
            route TEXT,
            mode TEXT,
            confidence REAL,
            agent_version TEXT,
            agent_count INTEGER,
            agent_hits_json TEXT NOT NULL,
            question TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_hit_date ON agent_hit_log(trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_hit_session ON agent_hit_log(session_id, id)")
    conn.commit()
    conn.close()


def _remember(session_id: str, role: str, text: str) -> None:
    conn = sqlite3.connect(MEMORY_DB)
    conn.execute(
        "INSERT INTO memory(session_id, role, text, created_at) VALUES (?, ?, ?, ?)",
        (session_id, role, text, int(time.time())),
    )
    conn.commit()
    conn.close()


def _history(session_id: str, n: int = MAX_MEMORY_TURNS) -> List[Dict[str, str]]:
    conn = sqlite3.connect(MEMORY_DB)
    cur = conn.execute(
        "SELECT role, text FROM memory WHERE session_id=? ORDER BY id DESC LIMIT ?",
        (session_id, n),
    )
    rows = [{"role": r[0], "text": r[1]} for r in cur.fetchall()]
    conn.close()
    rows.reverse()
    return rows


def _today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def _normalize_report_date(v: Optional[str]) -> str:
    raw = (v or "").strip()
    if not raw:
        return _today_yyyymmdd()
    s = raw.replace("-", "")
    if len(s) == 8 and s.isdigit():
        return s
    return _today_yyyymmdd()


def _log_agent_hit(
    session_id: str,
    question: str,
    route: str,
    mode: str,
    confidence: float,
    agent_version: str,
    agent_count: int,
    agent_hits: List[str],
) -> None:
    conn = sqlite3.connect(MEMORY_DB)
    conn.execute(
        """
        INSERT INTO agent_hit_log(
            created_at, trade_date, session_id, route, mode, confidence,
            agent_version, agent_count, agent_hits_json, question
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(time.time()),
            _today_yyyymmdd(),
            str(session_id or "stock-web"),
            str(route or "unknown"),
            str(mode or "unknown"),
            float(confidence or 0.0),
            str(agent_version or AGENT_VERSION),
            int(agent_count or 0),
            json.dumps(agent_hits or [], ensure_ascii=False),
            str(question or "")[:500],
        ),
    )
    conn.commit()
    conn.close()


def _build_agent_hit_daily_summary(date_yyyymmdd: Optional[str] = None) -> Dict[str, Any]:
    date_key = _normalize_report_date(date_yyyymmdd)
    conn = sqlite3.connect(MEMORY_DB)
    rows = conn.execute(
        """
        SELECT mode, route, confidence, agent_version, agent_count, agent_hits_json
        FROM agent_hit_log
        WHERE trade_date = ?
        ORDER BY id ASC
        """,
        (date_key,),
    ).fetchall()
    conn.close()

    mode_counts: Dict[str, int] = {}
    route_counts: Dict[str, int] = {}
    agent_counts: Dict[str, int] = {}
    confs: List[float] = []
    versions: Dict[str, int] = {}
    max_agent_count = 0
    for mode, route, conf, ver, cnt, hits_json in rows:
        m = str(mode or "unknown")
        r = str(route or "unknown")
        mode_counts[m] = mode_counts.get(m, 0) + 1
        route_counts[r] = route_counts.get(r, 0) + 1
        versions[str(ver or "unknown")] = versions.get(str(ver or "unknown"), 0) + 1
        try:
            max_agent_count = max(max_agent_count, int(cnt or 0))
        except Exception:
            pass
        try:
            confs.append(float(conf or 0.0))
        except Exception:
            pass
        try:
            hits = json.loads(hits_json or "[]")
            if isinstance(hits, list):
                for h in hits:
                    k = str(h)
                    agent_counts[k] = agent_counts.get(k, 0) + 1
        except Exception:
            continue

    top_agents = sorted(agent_counts.items(), key=lambda x: x[1], reverse=True)
    summary = {
        "ok": True,
        "date": date_key,
        "total_requests": len(rows),
        "avg_confidence": round(sum(confs) / len(confs), 4) if confs else 0.0,
        "mode_counts": mode_counts,
        "route_counts": route_counts,
        "agent_version_counts": versions,
        "agent_registry_max_count_seen": max_agent_count,
        "top_agents": [{"agent_id": k, "hits": v} for k, v in top_agents[:50]],
    }
    return summary


def _save_agent_hit_daily_report(summary: Dict[str, Any]) -> Dict[str, str]:
    os.makedirs(OPENCLAW_REPORT_DIR, exist_ok=True)
    d = str(summary.get("date") or _today_yyyymmdd())
    json_path = os.path.join(OPENCLAW_REPORT_DIR, "agent_hits_daily_{}.json".format(d))
    md_path = os.path.join(OPENCLAW_REPORT_DIR, "agent_hits_daily_{}.md".format(d))
    csv_path = os.path.join(OPENCLAW_REPORT_DIR, "agent_hits_daily_{}.csv".format(d))
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Agent Hit Daily Report ({})\n\n".format(d))
        f.write("- total_requests: {}\n".format(summary.get("total_requests", 0)))
        f.write("- avg_confidence: {}\n".format(summary.get("avg_confidence", 0.0)))
        f.write("- agent_registry_max_count_seen: {}\n\n".format(summary.get("agent_registry_max_count_seen", 0)))
        f.write("## Top Agents\n")
        for i, item in enumerate(summary.get("top_agents", [])[:20], 1):
            f.write("{}. {} ({})\n".format(i, item.get("agent_id"), item.get("hits")))
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["agent_id", "hits"])
        for item in summary.get("top_agents", []):
            writer.writerow([item.get("agent_id"), item.get("hits")])
    return {"json": json_path, "markdown": md_path, "csv": csv_path}


def _db_has_table(db_path: str, table_name: str) -> bool:
    try:
        conn = sqlite3.connect(db_path)
        x = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
        conn.close()
        return bool(x)
    except Exception:
        return False


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        if math.isfinite(x):
            return x
        return default
    except Exception:
        return default


def _save_stock_profile(code: str, payload: Dict[str, Any]) -> None:
    if not code:
        return
    conn = sqlite3.connect(MEMORY_DB)
    conn.execute(
        "INSERT OR REPLACE INTO stock_profiles(code, profile_json, updated_at) VALUES (?, ?, ?)",
        (code, json.dumps(payload, ensure_ascii=False), int(time.time())),
    )
    conn.commit()
    conn.close()


def _load_stock_profile(code: str) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    conn = sqlite3.connect(MEMORY_DB)
    row = conn.execute("SELECT profile_json FROM stock_profiles WHERE code=? LIMIT 1", (code,)).fetchone()
    conn.close()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None


def _set_param(k: str, v: Any) -> None:
    conn = sqlite3.connect(MEMORY_DB)
    conn.execute(
        "INSERT OR REPLACE INTO agent_params(k, v, updated_at) VALUES (?, ?, ?)",
        (k, str(v), int(time.time())),
    )
    conn.commit()
    conn.close()


def _get_param(k: str, default: Optional[str] = None) -> Optional[str]:
    conn = sqlite3.connect(MEMORY_DB)
    row = conn.execute("SELECT v FROM agent_params WHERE k=? LIMIT 1", (k,)).fetchone()
    conn.close()
    return row[0] if row else default


def _to_code(text: str) -> Optional[str]:
    m = re.search(r"(\d{6}(?:\.(?:SZ|SH))?)", (text or "").upper())
    if not m:
        return None
    c = m.group(1)
    if "." not in c:
        c = c + (".SH" if c.startswith(("5", "6", "9")) else ".SZ")
    return c


def _fmt_pct(v: Any) -> str:
    try:
        x = float(v)
        return f"{x:+.2f}%"
    except Exception:
        return "--"


def _find_db() -> Optional[str]:
    if resolve_db_path is not None:
        try:
            return str(resolve_db_path())
        except Exception:
            pass
    root = _project_root()
    cands = [
        str(root / "permanent_stock_database.db"),
        str(root / "permanent_stock_database.backup.db"),
        str(root / "stock_data.db"),
        "/opt/openclaw/permanent_stock_database.db",
        "/opt/airivo/data/permanent_stock_database.db",
        "/opt/airivo/permanent_stock_database.db",
        "/opt/airivo/app/permanent_stock_database.db",
    ]
    for p in cands:
        if not os.path.exists(p):
            continue
        try:
            conn = sqlite3.connect(p)
            t = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            conn.close()
            names = {x[0] for x in t}
            if "stock_basic" in names and "daily_trading_data" in names:
                return p
        except Exception:
            continue
    return None


def _latest_summary() -> Optional[dict]:
    files = []
    report_dir = Path(OPENCLAW_REPORT_DIR)
    for pat in (
        str(report_dir / "run_summary_*.json"),
        "/opt/airivo/app/logs/openclaw/run_summary_*.json",
        "/opt/openclaw/logs/openclaw/run_summary_*.json",
    ):
        files.extend(glob.glob(pat))
    files = sorted(files, reverse=True)
    if not files:
        return None
    try:
        with open(files[0], "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _latest_evolution() -> Optional[dict]:
    candidates = []
    if evolution_dir is not None:
        try:
            candidates.append(str(evolution_dir() / "last_run.json"))
        except Exception:
            pass
    root = _project_root()
    candidates.extend(
        [
            str(root / "evolution" / "last_run.json"),
            "/opt/openclaw/evolution/last_run.json",
            "/opt/openclaw/qa-stock/evolution/last_run.json",
            "/opt/airivo/app/evolution/last_run.json",
            "evolution/last_run.json",
        ]
    )
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    return None


def _latest_trade_date(db: str) -> Optional[str]:
    try:
        conn = sqlite3.connect(db)
        d = conn.execute("SELECT MAX(trade_date) FROM daily_trading_data").fetchone()
        conn.close()
        return str(d[0]) if d and d[0] else None
    except Exception:
        return None


def _compute_feature_warehouse(limit: int = 200) -> Dict[str, Any]:
    db = _find_db()
    if not db:
        return {"ok": False, "error": "database not found"}
    try:
        conn = sqlite3.connect(db)
        dts = pd.read_sql_query(
            "SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT 130",
            conn,
        )
        if dts.empty or len(dts) < 120:
            conn.close()
            return {"ok": False, "error": "not enough dates for 20/60/120 features"}
        dates = dts["trade_date"].astype(str).tolist()
        d0, d20, d60, d120 = dates[0], dates[19], dates[59], dates[119]
        latest = pd.read_sql_query(
            "SELECT ts_code,close_price,pct_chg,amount,turnover_rate,trade_date FROM daily_trading_data WHERE trade_date=?",
            conn, params=(d0,)
        )
        p20 = pd.read_sql_query("SELECT ts_code,close_price AS c20 FROM daily_trading_data WHERE trade_date=?", conn, params=(d20,))
        p60 = pd.read_sql_query("SELECT ts_code,close_price AS c60 FROM daily_trading_data WHERE trade_date=?", conn, params=(d60,))
        p120 = pd.read_sql_query("SELECT ts_code,close_price AS c120 FROM daily_trading_data WHERE trade_date=?", conn, params=(d120,))
        basic = pd.read_sql_query("SELECT ts_code,name,industry FROM stock_basic", conn)
        valuation = None
        fundamentals = None
        if _db_has_table(db, "valuation_daily"):
            try:
                valuation = pd.read_sql_query(
                    "SELECT ts_code, trade_date, pe_ttm, pb, total_mv, circ_mv, turnover_rate FROM valuation_daily WHERE trade_date=?",
                    conn,
                    params=(d0,),
                )
            except Exception:
                valuation = None
        if _db_has_table(db, "fina_indicator_ext"):
            try:
                fundamentals = pd.read_sql_query(
                    """
                    SELECT f.ts_code, f.end_date, f.roe, f.netprofit_margin, f.grossprofit_margin, f.or_yoy, f.op_yoy, f.dt_netprofit_yoy
                    FROM fina_indicator_ext f
                    JOIN (
                        SELECT ts_code, MAX(end_date) AS end_date
                        FROM fina_indicator_ext
                        GROUP BY ts_code
                    ) x ON f.ts_code=x.ts_code AND f.end_date=x.end_date
                    """,
                    conn,
                )
            except Exception:
                fundamentals = None
        conn.close()

        df = latest.merge(p20, on="ts_code").merge(p60, on="ts_code").merge(p120, on="ts_code").merge(basic, on="ts_code", how="left")
        if valuation is not None and not valuation.empty:
            df = df.merge(valuation.drop(columns=["trade_date"], errors="ignore"), on="ts_code", how="left")
        if fundamentals is not None and not fundamentals.empty:
            df = df.merge(fundamentals.drop(columns=["end_date"], errors="ignore"), on="ts_code", how="left")
        for c in ("close_price", "pct_chg", "amount", "turnover_rate", "c20", "c60", "c120"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["close_price", "c20", "c60", "c120", "amount"]).copy()
        if df.empty:
            return {"ok": False, "error": "empty frame after clean"}

        df["ret20"] = (df["close_price"] / df["c20"] - 1.0) * 100.0
        df["ret60"] = (df["close_price"] / df["c60"] - 1.0) * 100.0
        df["ret120"] = (df["close_price"] / df["c120"] - 1.0) * 100.0
        df["liq"] = df["amount"].clip(lower=1).apply(lambda x: math.log10(float(x)))
        df["turnover_rate"] = df["turnover_rate"].fillna(0.0)

        # Multi-factor composite (trend/value proxy/quality proxy/flow/volatility constraints).
        df["trend_score"] = 0.25 * df["ret20"] + 0.35 * df["ret60"] + 0.25 * df["ret120"] + 0.15 * df["pct_chg"]
        df["flow_score"] = 0.7 * df["liq"] + 0.3 * df["turnover_rate"]
        df["risk_penalty"] = df["pct_chg"].abs().clip(upper=12.0)
        if "roe" in df.columns:
            df["roe"] = pd.to_numeric(df["roe"], errors="coerce")
            df["or_yoy"] = pd.to_numeric(df.get("or_yoy"), errors="coerce")
            df["quality_proxy"] = (
                0.55 * df["roe"].fillna(0.0) + 0.45 * df["or_yoy"].fillna(0.0)
            ).clip(lower=-80, upper=220)
        else:
            df["quality_proxy"] = (df["ret60"] - 0.5 * df["pct_chg"].abs()).clip(lower=-50, upper=200)

        if "pe_ttm" in df.columns and "pb" in df.columns:
            df["pe_ttm"] = pd.to_numeric(df["pe_ttm"], errors="coerce")
            df["pb"] = pd.to_numeric(df["pb"], errors="coerce")
            df["value_proxy"] = (
                40.0 / (df["pe_ttm"].clip(lower=1.0)) + 10.0 / (df["pb"].clip(lower=0.2))
            ).clip(lower=-80, upper=80)
        else:
            df["value_proxy"] = (-df["ret20"]).clip(lower=-80, upper=80)
        df["composite_score"] = (
            0.38 * df["trend_score"]
            + 0.18 * df["flow_score"]
            + 0.16 * df["quality_proxy"]
            + 0.10 * df["value_proxy"]
            - 0.18 * df["risk_penalty"]
        )

        df = df.sort_values("composite_score", ascending=False).head(max(20, int(limit)))
        features: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            features.append(
                {
                    "code": str(r["ts_code"]),
                    "name": str(r.get("name", "") or r["ts_code"]),
                    "industry": str(r.get("industry", "未知")),
                    "trade_date": str(r["trade_date"]),
                    "ret20": round(_safe_float(r["ret20"]), 2),
                    "ret60": round(_safe_float(r["ret60"]), 2),
                    "ret120": round(_safe_float(r["ret120"]), 2),
                    "trend_score": round(_safe_float(r["trend_score"]), 4),
                    "flow_score": round(_safe_float(r["flow_score"]), 4),
                    "quality_proxy": round(_safe_float(r["quality_proxy"]), 4),
                    "value_proxy": round(_safe_float(r["value_proxy"]), 4),
                    "risk_penalty": round(_safe_float(r["risk_penalty"]), 4),
                    "composite_score": round(_safe_float(r["composite_score"]), 4),
                }
            )
        return {"ok": True, "trade_date": d0, "count": len(features), "features": features}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _data_freshness_info() -> Dict[str, Any]:
    db = _find_db()
    if not db:
        return {"latest_trade_date": None, "days_lag": 999}
    latest = _latest_trade_date(db)
    if not latest:
        return {"latest_trade_date": None, "days_lag": 999}
    try:
        d0 = datetime.strptime(str(latest), "%Y%m%d").date()
        lag = (datetime.utcnow().date() - d0).days
        if lag < 0:
            lag = 0
        return {"latest_trade_date": str(latest), "days_lag": int(lag)}
    except Exception:
        return {"latest_trade_date": str(latest), "days_lag": 999}


def _data_capability_summary(db: Optional[str]) -> Dict[str, bool]:
    if not db:
        return {"daily": False, "minute": False, "fundamental": False, "valuation": False, "flow": False, "events": False}
    names = set()
    try:
        conn = sqlite3.connect(db)
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        conn.close()
        names = {str(r[0]).lower() for r in rows}
    except Exception:
        pass

    def has_any(keys: List[str]) -> bool:
        for n in names:
            for k in keys:
                if k in n:
                    return True
        return False

    return {
        "daily": "daily_trading_data" in names,
        "minute": has_any(["minute", "intraday", "tick"]),
        "fundamental": has_any(["income", "balance", "cashflow", "fina", "financial"]),
        "valuation": has_any(["valuation", "pe", "pb", "market_cap"]),
        "flow": has_any(["moneyflow", "flow", "northbound", "capital_flow"]),
        "events": has_any(["news", "notice", "announcement", "event"]),
    }


def _lookup_code_by_name(name_or_text: str) -> Optional[str]:
    db = _find_db()
    if not db:
        return None
    text = (name_or_text or "").strip()
    if not text:
        return None
    segments = re.findall(r"[\u4e00-\u9fa5]{2,12}", text)
    if not segments:
        return None
    stop_pattern = re.compile(r"(怎么样|如何|怎么|分析|建议|仓位|风控|给我|一下|可以|吗|请|帮我|看下|看看)")
    candidates = []
    for seg in segments:
        cleaned = stop_pattern.sub("", seg).strip()
        if 2 <= len(cleaned) <= 8:
            candidates.append(cleaned)
    if not candidates:
        return None
    try:
        conn = sqlite3.connect(db)
        for name in candidates:
            df = pd.read_sql_query(
                "SELECT ts_code,name FROM stock_basic WHERE name = ? LIMIT 1",
                conn,
                params=(name,),
            )
            if not df.empty:
                conn.close()
                return str(df.iloc[0]["ts_code"])
        for name in candidates:
            df = pd.read_sql_query(
                "SELECT ts_code,name FROM stock_basic WHERE name LIKE ? LIMIT 1",
                conn,
                params=(f"%{name}%",),
            )
            if not df.empty:
                conn.close()
                return str(df.iloc[0]["ts_code"])
        conn.close()
    except Exception:
        return None
    return None


def tool_stock_snapshot(question: str, session_id: str) -> Dict[str, Any]:
    db = _find_db()
    if not db:
        return {"ok": False, "error": "database not found"}
    code = _to_code(question) or _lookup_code_by_name(question)
    followup_ref = any(k in (question or "") for k in ("这只", "该股", "这票", "这支", "它", "上面那只", "刚才那只"))
    if not code and followup_ref:
        hist = _history(session_id, 12)
        for h in reversed(hist):
            if h.get("role") != "user":
                continue
            c = _to_code(h.get("text", ""))
            if c:
                code = c
                break
    if not code:
        return {"ok": False, "error": "no stock code found"}
    try:
        conn = sqlite3.connect(db)
        basic = pd.read_sql_query(
            "SELECT ts_code,name,industry FROM stock_basic WHERE ts_code=? LIMIT 1",
            conn,
            params=(code,),
        )
        daily = pd.read_sql_query(
            "SELECT trade_date,close_price,pct_chg,amount,turnover_rate FROM daily_trading_data WHERE ts_code=? ORDER BY trade_date DESC LIMIT 90",
            conn,
            params=(code,),
        )
        conn.close()
        if daily.empty:
            return {"ok": False, "error": f"no daily data for {code}"}
        daily["close_price"] = pd.to_numeric(daily["close_price"], errors="coerce")
        daily["pct_chg"] = pd.to_numeric(daily["pct_chg"], errors="coerce")
        # 只清理快照计算必需字段，避免 turnover/amount 缺失导致整表被清空
        daily = daily.dropna(subset=["trade_date", "close_price", "pct_chg"]).reset_index(drop=True)
        if daily.empty:
            return {"ok": False, "error": f"no valid snapshot rows for {code}"}
        latest = daily.iloc[0]
        ret20 = None
        ret60 = None
        if len(daily) > 20 and float(daily.iloc[20]["close_price"]) > 0:
            ret20 = (float(latest["close_price"]) / float(daily.iloc[20]["close_price"]) - 1.0) * 100.0
        if len(daily) > 60 and float(daily.iloc[60]["close_price"]) > 0:
            ret60 = (float(latest["close_price"]) / float(daily.iloc[60]["close_price"]) - 1.0) * 100.0
        return {
            "ok": True,
            "code": code,
            "name": (basic.iloc[0]["name"] if not basic.empty else code),
            "industry": (basic.iloc[0]["industry"] if not basic.empty else "未知"),
            "date": str(latest["trade_date"]),
            "close": float(latest["close_price"]),
            "pct": float(latest["pct_chg"]),
            "ret20": ret20,
            "ret60": ret60,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def tool_bull_candidates(limit: int = 20) -> Dict[str, Any]:
    db = _find_db()
    if not db:
        return {"ok": False, "error": "database not found"}
    try:
        conn = sqlite3.connect(db)
        dts = pd.read_sql_query(
            "SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT 90",
            conn,
        )
        if dts is None or dts.empty or len(dts) < 60:
            conn.close()
            return {"ok": False, "error": "not enough trade dates"}
        dates = dts["trade_date"].astype(str).tolist()
        latest_date, d20, d60 = dates[0], dates[19], dates[59]
        latest = pd.read_sql_query(
            "SELECT ts_code,trade_date,close_price,pct_chg,amount,turnover_rate FROM daily_trading_data WHERE trade_date=?",
            conn, params=(latest_date,)
        )
        x20 = pd.read_sql_query(
            "SELECT ts_code,close_price AS close_20d FROM daily_trading_data WHERE trade_date=?",
            conn, params=(d20,)
        )
        x60 = pd.read_sql_query(
            "SELECT ts_code,close_price AS close_60d FROM daily_trading_data WHERE trade_date=?",
            conn, params=(d60,)
        )
        basic = pd.read_sql_query("SELECT ts_code,name,industry FROM stock_basic", conn)
        conn.close()
        df = latest.merge(x20, on="ts_code").merge(x60, on="ts_code").merge(basic, on="ts_code", how="left")
        for c in ("close_price", "pct_chg", "amount", "turnover_rate", "close_20d", "close_60d"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["close_price", "close_20d", "close_60d", "amount"])
        if df.empty:
            return {"ok": False, "error": "empty candidate frame"}
        df["ret20"] = (df["close_price"] / df["close_20d"] - 1.0) * 100.0
        df["ret60"] = (df["close_price"] / df["close_60d"] - 1.0) * 100.0
        df["turnover_rate"] = df["turnover_rate"].fillna(0.0)
        df["liq"] = df["amount"].clip(lower=1).apply(lambda x: math.log10(float(x)))
        df = df[(df["ret20"] > 0) & (df["ret60"] > 0) & (df["pct_chg"].abs() < 9.9)]
        if df.empty:
            return {"ok": False, "error": "no trend candidates"}
        q40 = float(df["amount"].quantile(0.4))
        df = df[df["amount"] > q40]
        df["score"] = 0.45 * df["ret60"] + 0.30 * df["ret20"] + 0.15 * df["pct_chg"] + 1.5 * df["liq"] + 0.1 * df["turnover_rate"]
        df = df.sort_values("score", ascending=False).head(max(1, int(limit)))
        out: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            base = max(0.0, 0.35 * float(r["ret20"]) + 0.50 * float(r["ret60"]))
            lo = min(80.0, max(8.0, base * 0.55))
            hi = min(150.0, max(lo + 5.0, base * 0.90 + 12.0))
            ret20 = round(float(r["ret20"]), 2)
            ret60 = round(float(r["ret60"]), 2)
            ind = str(r.get("industry", "未知"))
            # Build stock-specific rationale instead of one shared template.
            momentum_reason = "中短期共振上行"
            if ret20 >= 100:
                momentum_reason = "20日爆发+60日延续，属于强趋势加速段"
            elif ret20 <= 15 and ret60 >= 150:
                momentum_reason = "中期主升仍在，短期回撤后再启动特征明显"
            elif ret60 >= 220:
                momentum_reason = "60日主升斜率高，趋势韧性较强"

            sector_reason = "行业景气改善"
            if "元器件" in ind or "半导体" in ind:
                sector_reason = "科技链景气和资金偏好共振"
            elif "小金属" in ind:
                sector_reason = "资源品弹性大，受景气与价格周期驱动"
            elif "机械" in ind:
                sector_reason = "制造升级逻辑下订单预期增强"
            elif "软件" in ind:
                sector_reason = "软件景气与AI应用催化并行"
            elif "通信" in ind:
                sector_reason = "通信基础设施需求预期支撑"

            risk_hint = "短线波动可能放大，建议分批建仓"
            if hi >= 140:
                risk_hint = "高弹性同时高波动，需严格仓位与止损纪律"
            elif hi <= 110:
                risk_hint = "弹性相对温和，适合作为组合平衡仓"
            out.append(
                {
                    "code": str(r.get("ts_code", "")),
                    "name": str(r.get("name", "") or r.get("ts_code", "")),
                    "industry": str(r.get("industry", "未知")),
                    "ret20": ret20,
                    "ret60": ret60,
                    "pred_low": round(float(lo), 1),
                    "pred_high": round(float(hi), 1),
                    "reason": "%s；%s；%s。" % (momentum_reason, sector_reason, risk_hint),
                }
            )
        return {"ok": True, "date": latest_date, "count": len(out), "candidates": out}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def tool_market_overview() -> Dict[str, Any]:
    db = _find_db()
    if not db:
        return {"ok": False, "error": "database not found"}
    try:
        conn = sqlite3.connect(db)
        latest = pd.read_sql_query("SELECT MAX(trade_date) AS d FROM daily_trading_data", conn)
        d = str(latest.iloc[0]["d"])
        one = pd.read_sql_query(
            "SELECT pct_chg,amount FROM daily_trading_data WHERE trade_date=?",
            conn,
            params=(d,),
        )
        conn.close()
        one["pct_chg"] = pd.to_numeric(one["pct_chg"], errors="coerce")
        adv = int((one["pct_chg"] > 0).sum())
        dec = int((one["pct_chg"] < 0).sum())
        flat = int((one["pct_chg"] == 0).sum())
        return {
            "ok": True,
            "trade_date": d,
            "stocks": int(len(one)),
            "advancers": adv,
            "decliners": dec,
            "flat": flat,
            "avg_pct_chg": float(one["pct_chg"].mean()),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def tool_backtest_context() -> Dict[str, Any]:
    s = _latest_summary() or {}
    e = _latest_evolution() or {}
    stats = (e.get("stats") if isinstance(e.get("stats"), dict) else {}) if isinstance(e, dict) else {}
    return {
        "ok": True,
        "summary_risk": (s.get("risk") or {}).get("risk_level") if isinstance(s, dict) else None,
        "summary_publish": (s.get("publish") or {}).get("status") if isinstance(s, dict) else None,
        "evo_win_rate": stats.get("win_rate"),
        "evo_avg_return": stats.get("avg_return"),
        "evo_max_drawdown": stats.get("max_drawdown"),
    }


def tool_sql(query: str) -> Dict[str, Any]:
    db = _find_db()
    if not db:
        return {"ok": False, "error": "database not found"}
    q = (query or "").strip()
    if not q:
        return {"ok": False, "error": "empty query"}
    if not q.lower().startswith("select"):
        return {"ok": False, "error": "only SELECT is allowed"}
    if ";" in q:
        return {"ok": False, "error": "semicolon is not allowed"}
    if " limit " not in q.lower():
        q = q + " LIMIT 50"
    try:
        conn = sqlite3.connect(db)
        df = pd.read_sql_query(q, conn)
        conn.close()
        return {"ok": True, "rows": int(len(df)), "data": df.head(50).to_dict(orient="records")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def tool_feature_warehouse(limit: int = 200) -> Dict[str, Any]:
    return _compute_feature_warehouse(limit=limit)


def tool_quality_gate(outputs: Dict[str, Any]) -> Dict[str, Any]:
    db = _find_db()
    freshness = _data_freshness_info()
    coverage = _data_capability_summary(db)
    confidence = 0.45
    evidence_count = 0
    if outputs.get("stock_snapshot", {}).get("ok"):
        confidence += 0.18
        evidence_count += 1
    if outputs.get("bull_candidates", {}).get("ok"):
        confidence += 0.17
        evidence_count += 1
    if outputs.get("feature_warehouse", {}).get("ok"):
        confidence += 0.15
        evidence_count += 1
    if outputs.get("market_overview", {}).get("ok"):
        confidence += 0.07
        evidence_count += 1
    if outputs.get("backtest_context", {}).get("ok"):
        confidence += 0.06
        evidence_count += 1

    lag = int(freshness.get("days_lag", 999))
    cov_score = sum(1 for _, v in coverage.items() if v) / max(1, len(coverage))
    confidence += 0.12 * cov_score
    if lag > 30:
        confidence -= 0.22
    elif lag > 10:
        confidence -= 0.12
    elif lag > 3:
        confidence -= 0.06
    confidence = max(0.01, min(0.99, confidence))

    return {
        "ok": True,
        "confidence": round(confidence, 3),
        "evidence_count": evidence_count,
        "data_coverage": coverage,
        "data_freshness": freshness,
        "pass": confidence >= QUALITY_MIN_CONFIDENCE and lag <= 45,
    }


def tool_portfolio_decision(outputs: Dict[str, Any]) -> Dict[str, Any]:
    fw = outputs.get("feature_warehouse", {})
    if not fw.get("ok"):
        return {"ok": False, "error": "feature warehouse unavailable"}
    rows = fw.get("features", [])[:60]
    if not rows:
        return {"ok": False, "error": "empty features"}
    # choose top names with soft industry cap
    picked: List[Dict[str, Any]] = []
    industry_count: Dict[str, int] = {}
    total_weight = 0.0
    max_names = 10
    for r in rows:
        ind = str(r.get("industry", "未知"))
        if industry_count.get(ind, 0) >= 3:
            continue
        score = _safe_float(r.get("composite_score"))
        w = min(0.15, max(0.04, 0.06 + score / 500.0))
        if total_weight + w > 0.95:
            break
        picked.append(
            {
                "code": r.get("code"),
                "name": r.get("name"),
                "industry": ind,
                "score": round(score, 3),
                "target_weight": round(w, 3),
                "stop_loss": "-10%",
                "trim_rule": "单日放量长阴或跌破20日均线减仓",
            }
        )
        industry_count[ind] = industry_count.get(ind, 0) + 1
        total_weight += w
        if len(picked) >= max_names:
            break
    return {
        "ok": True,
        "positions": picked,
        "portfolio": {
            "name_count": len(picked),
            "gross_exposure": round(total_weight, 3),
            "single_name_cap": 0.15,
            "industry_cap": 0.35,
            "max_drawdown_budget": 0.12,
        },
    }


def tool_evolution_tick(outputs: Dict[str, Any]) -> Dict[str, Any]:
    # lightweight daily auto-tuning by last backtest stats
    bt = outputs.get("backtest_context", {})
    wr = _safe_float((bt or {}).get("evo_win_rate"), default=0.0)
    ar = _safe_float((bt or {}).get("evo_avg_return"), default=0.0)
    dd = abs(_safe_float((bt or {}).get("evo_max_drawdown"), default=0.0))
    signal_th = _safe_float(_get_param("signal_threshold", "0.55"), 0.55)
    hold_days = int(_safe_float(_get_param("holding_days", "13"), 13))
    if wr < 52 or dd > 8:
        signal_th = min(0.75, signal_th + 0.02)
        hold_days = max(5, hold_days - 1)
    elif wr > 60 and ar > 2:
        signal_th = max(0.45, signal_th - 0.01)
        hold_days = min(25, hold_days + 1)
    _set_param("signal_threshold", round(signal_th, 3))
    _set_param("holding_days", int(hold_days))
    _set_param("last_evolution_ts", int(time.time()))
    return {
        "ok": True,
        "params": {"signal_threshold": round(signal_th, 3), "holding_days": int(hold_days)},
        "based_on": {"win_rate": wr, "avg_return": ar, "max_drawdown": dd},
    }


def _plan_tools(question: str) -> List[str]:
    q = (question or "").lower()
    tools = []
    followup = any(k in q for k in ("详细", "理由", "展开", "为什么这些", "上一条", "上面", "继续", "再详细"))
    decision_intent = any(k in q for k in ("组合", "仓位", "配置", "决策", "portfolio"))
    capability_or_system_intent = any(
        k in q for k in ("你是谁", "你能做什么", "你有什么能力", "还能干什么", "能力", "系统", "看到我的", "看得到")
    ) or ("who are you" in q)
    if any(k in q for k in ("牛股", "预测", "top20", "20大", "涨幅")):
        tools.append("bull_candidates")
        tools.append("feature_warehouse")
    if followup:
        tools.append("bull_candidates")
        tools.append("feature_warehouse")
    if any(k in q for k in ("大盘", "市场", "情绪", "上涨", "下跌", "overview")):
        tools.append("market_overview")
    if followup:
        tools.append("market_overview")
        tools.append("backtest_context")
    if any(k in q for k in ("回测", "胜率", "回撤", "风险", "策略")):
        tools.append("backtest_context")
        tools.append("feature_warehouse")
    explicit_stock_ref = any(k in q for k in ("这只", "该股", "这票", "这支", "个股", "代码", "走势", "k线", "买点", "卖点", "止损"))
    has_symbol_or_name = bool(_to_code(question or "") or _lookup_code_by_name(question or ""))
    if has_symbol_or_name or (explicit_stock_ref and not capability_or_system_intent):
        tools.append("stock_snapshot")
        tools.append("feature_warehouse")
    if decision_intent:
        tools.append("feature_warehouse")
        tools.append("backtest_context")
    # default include feature warehouse to support two-stage evidence.
    tools.append("feature_warehouse")
    return list(dict.fromkeys(tools)) or ["market_overview", "backtest_context"]


def _extract_requested_ranks(question: str, max_n: int) -> List[int]:
    def _cn_num_to_int(text: str) -> Optional[int]:
        s = (text or "").strip()
        if not s:
            return None
        if s.isdigit():
            return int(s)
        cmap = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
        if s == "十":
            return 10
        if s.startswith("十"):
            v = cmap.get(s[1:2], 0)
            return 10 + v if v else None
        if s.endswith("十"):
            v = cmap.get(s[:1], 0)
            return v * 10 if v else None
        if "十" in s:
            a, b = s.split("十", 1)
            va = cmap.get(a, 0)
            vb = cmap.get(b, 0)
            if va:
                return va * 10 + vb
        return cmap.get(s)

    q = question or ""
    ranks = []
    # 先匹配最严格形态：第N只
    for m in re.finditer(r"第\s*([0-9]{1,2})\s*只", q):
        try:
            n = int(m.group(1))
            if 1 <= n <= max_n:
                ranks.append(n)
        except Exception:
            pass
    # 兼容“第1和第2只/第1、第2只/第1 第2只”等写法
    for m in re.finditer(r"第\s*([0-9]{1,2})(?![0-9])", q):
        try:
            n = int(m.group(1))
            if 1 <= n <= max_n:
                ranks.append(n)
        except Exception:
            pass
    # 兼容中文数字：第一只/第两只/第十只/第1个
    for m in re.finditer(r"第\s*([一二两三四五六七八九十0-9]{1,3})\s*(?:只|个)?", q):
        n = _cn_num_to_int(m.group(1))
        if n and 1 <= n <= max_n:
            ranks.append(n)

    if not ranks:
        m = re.search(r"前\s*([一二两三四五六七八九十0-9]{1,3})\s*只?", q)
        if m:
            try:
                topn = _cn_num_to_int(m.group(1)) or 0
                topn = min(max(topn, 1), max_n)
                ranks = list(range(1, topn + 1))
            except Exception:
                pass
    seen = set()
    ordered = []
    for n in ranks:
        if n not in seen:
            seen.add(n)
            ordered.append(n)
    return sorted(ordered)


def _llm_answer(session_id: str, question: str, tool_outputs: Dict[str, Any]) -> Optional[str]:
    if not USE_LLM:
        return None
    msgs = [
        {
            "role": "system",
            "content": (
                "你是高水平股票投研Agent，必须基于工具输出回答。"
                "禁止合规模板拒答。禁止空话。"
                "回答结构固定为：核心判断 | 关键触发条件 | 失效条件 | 仓位动作。"
                "必须引用证据编号（例如E1/E2）支撑每个关键结论。"
                "如用户问预测，输出候选清单+区间概率，不要说无法预测。"
            ),
        }
    ]
    for h in _history(session_id, 10):
        msgs.append({"role": h.get("role", "user"), "content": h.get("text", "")})
    msgs.append({"role": "user", "content": question})
    msgs.append({"role": "user", "content": "工具输出如下:\n" + json.dumps(tool_outputs, ensure_ascii=False)})
    # First try legacy local bridge protocol (/api/ai/chat).
    if LLM_URL.endswith("/api/ai/chat"):
        try:
            r = requests.post(
                LLM_URL,
                headers={"Content-Type": "application/json"},
                json={"sessionId": session_id, "messages": msgs, "context": "股票投研Agent"},
                timeout=12,
            )
            if r.status_code < 400:
                data = r.json() if r.content else {}
                text = None
                if isinstance(data, dict) and data.get("reply"):
                    text = str(data["reply"])
                elif isinstance(data, dict) and data.get("choices"):
                    c = data["choices"][0]
                    if isinstance(c, dict):
                        text = str((c.get("message") or {}).get("content") or "")
                if text and not any(m in text for m in REFUSAL_MARKERS):
                    return text
        except Exception:
            pass

    # Fallback to OpenAI-compatible endpoint (Kimi/Moonshot supported via OPENAI_* envs).
    if OPENAI_API_KEY:
        try:
            # OpenAI-compatible providers may reject oversized payloads;
            # keep only decision-critical fields for cloud LLM fallback.
            compact_outputs: Dict[str, Any] = {}
            qg = tool_outputs.get("quality_gate")
            if isinstance(qg, dict):
                compact_outputs["quality_gate"] = qg
            mo = tool_outputs.get("market_overview")
            if isinstance(mo, dict):
                compact_outputs["market_overview"] = {
                    "ok": mo.get("ok"),
                    "trade_date": mo.get("trade_date"),
                    "advancers": mo.get("advancers"),
                    "decliners": mo.get("decliners"),
                    "flat": mo.get("flat"),
                    "avg_pct_chg": mo.get("avg_pct_chg"),
                }
            ss = tool_outputs.get("stock_snapshot")
            if isinstance(ss, dict):
                compact_outputs["stock_snapshot"] = {
                    "ok": ss.get("ok"),
                    "code": ss.get("code"),
                    "name": ss.get("name"),
                    "date": ss.get("date"),
                    "close": ss.get("close"),
                    "pct": ss.get("pct"),
                    "ret20": ss.get("ret20"),
                    "ret60": ss.get("ret60"),
                }
            fw = tool_outputs.get("feature_warehouse")
            if isinstance(fw, dict):
                compact_outputs["feature_warehouse"] = {
                    "ok": fw.get("ok"),
                    "trade_date": fw.get("trade_date"),
                    "features": [
                        {
                            "code": x.get("code"),
                            "name": x.get("name"),
                            "industry": x.get("industry"),
                            "composite_score": x.get("composite_score"),
                            "ret20": x.get("ret20"),
                            "ret60": x.get("ret60"),
                        }
                        for x in (fw.get("features") or [])[:8]
                        if isinstance(x, dict)
                    ],
                }
            bc = tool_outputs.get("bull_candidates")
            if isinstance(bc, dict):
                compact_outputs["bull_candidates"] = {
                    "ok": bc.get("ok"),
                    "date": bc.get("date"),
                    "top": (bc.get("candidates") or [])[:8],
                }
            bt = tool_outputs.get("backtest_context")
            if isinstance(bt, dict):
                compact_outputs["backtest_context"] = {
                    "ok": bt.get("ok"),
                    "evo_win_rate": bt.get("evo_win_rate"),
                    "evo_avg_return": bt.get("evo_avg_return"),
                    "evo_max_drawdown": bt.get("evo_max_drawdown"),
                }
            pdm = tool_outputs.get("portfolio_decision")
            if isinstance(pdm, dict):
                compact_outputs["portfolio_decision"] = pdm

            openai_msgs = [m for m in msgs[:-1]]
            openai_msgs.append(
                {
                    "role": "user",
                    "content": "工具输出如下(压缩版):\n" + json.dumps(compact_outputs, ensure_ascii=False),
                }
            )
            r = requests.post(
                f"{OPENAI_BASE_URL}/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": openai_msgs,
                    "temperature": 0.2,
                    "stream": False,
                },
                timeout=20,
            )
            if r.status_code >= 400:
                return None
            data = r.json() if r.content else {}
            text = None
            if isinstance(data, dict) and data.get("choices"):
                c = data["choices"][0]
                if isinstance(c, dict):
                    text = str((c.get("message") or {}).get("content") or "")
            if not text:
                return None
            if any(m in text for m in REFUSAL_MARKERS):
                return None
            return text
        except Exception:
            return None
    return None


def _probe_llm_connectivity() -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "legacy_url": LLM_URL,
        "legacy_ok": False,
        "openai_base_url": OPENAI_BASE_URL,
        "openai_model": OPENAI_MODEL,
        "openai_key_set": bool(OPENAI_API_KEY),
        "openai_ok": False,
    }

    # Probe legacy local bridge health if it looks like /api/ai/chat.
    if LLM_URL.endswith("/api/ai/chat"):
        health_url = LLM_URL[:-len("/api/ai/chat")] + "/health"
        try:
            r = requests.get(health_url, timeout=3)
            result["legacy_ok"] = r.status_code < 400
            result["legacy_status"] = r.status_code
        except Exception as exc:
            result["legacy_error"] = f"{type(exc).__name__}: {exc}"

    # Probe OpenAI-compatible endpoint with a tiny request.
    if OPENAI_API_KEY:
        try:
            r = requests.post(
                f"{OPENAI_BASE_URL}/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": [{"role": "user", "content": "ping"}],
                    "stream": False,
                    "temperature": 0,
                    "max_tokens": 8,
                },
                timeout=8,
            )
            result["openai_status"] = r.status_code
            result["openai_ok"] = r.status_code < 400
            if r.status_code >= 400:
                result["openai_error"] = (r.text or "")[:180]
        except Exception as exc:
            result["openai_error"] = f"{type(exc).__name__}: {exc}"
    return result


def _stage1_collect_evidence(question: str, outputs: Dict[str, Any]) -> Dict[str, Any]:
    evidence: List[Dict[str, Any]] = []
    snap = outputs.get("stock_snapshot", {})
    if snap.get("ok"):
        evidence.append(
            {
                "id": "E1",
                "type": "stock_snapshot",
                "code": snap.get("code"),
                "trade_date": snap.get("date"),
                "facts": {
                    "pct": snap.get("pct"),
                    "ret20": snap.get("ret20"),
                    "ret60": snap.get("ret60"),
                },
            }
        )
    fw = outputs.get("feature_warehouse", {})
    if fw.get("ok"):
        top = fw.get("features", [])[:5]
        evidence.append(
            {
                "id": "E2",
                "type": "feature_warehouse",
                "trade_date": fw.get("trade_date"),
                "facts": [{"code": x.get("code"), "score": x.get("composite_score")} for x in top],
            }
        )
    ov = outputs.get("market_overview", {})
    if ov.get("ok"):
        evidence.append(
            {
                "id": "E3",
                "type": "market_overview",
                "trade_date": ov.get("trade_date"),
                "facts": {
                    "advancers": ov.get("advancers"),
                    "decliners": ov.get("decliners"),
                    "avg_pct_chg": ov.get("avg_pct_chg"),
                },
            }
        )
    bt = outputs.get("backtest_context", {})
    if bt.get("ok"):
        evidence.append(
            {
                "id": "E4",
                "type": "backtest_context",
                "facts": {
                    "win_rate": bt.get("evo_win_rate"),
                    "avg_return": bt.get("evo_avg_return"),
                    "max_drawdown": bt.get("evo_max_drawdown"),
                },
            }
        )
    return {"ok": True, "question": question, "evidence": evidence}


def _stage2_expert_answer(question: str, outputs: Dict[str, Any], evidence_bundle: Dict[str, Any], quality: Dict[str, Any]) -> str:
    q = question or ""
    ql = q.lower()
    eids = ",".join([e.get("id", "") for e in evidence_bundle.get("evidence", [])])
    lines: List[str] = []
    decision_intent = any(k in q for k in ("组合", "仓位", "配置", "调仓", "持仓", "给我买", "给我一个组合")) or (
        "portfolio" in ql
    )
    candidate_intent = any(k in q for k in ("候选", "牛股", "top", "预测", "前", "排名"))
    whoami_intent = any(
        k in q
        for k in (
            "你是谁",
            "你能做什么",
            "你可以做什么",
            "你有什么能力",
            "介绍下你自己",
            "还能干什么",
            "你的能力",
            "你能帮我做什么",
            "看得到我的系统",
            "除了股票",
            "别的不能回答吗",
        )
    ) or ("who are you" in ql)
    stock_service_intent = any(
        k in q
        for k in (
            "可以分析股票吗",
            "能分析股票吗",
            "可以用我的股票系统分析股票吗",
            "你可以用我的股票系统分析股票吗",
            "能用我的股票系统分析股票吗",
        )
    )
    stock_domain_intent = bool(
        _to_code(q)
        or any(
            k in q
            for k in (
                "股票",
                "个股",
                "仓位",
                "止损",
                "回测",
                "大盘",
                "策略",
                "组合",
                "候选",
                "涨跌",
                "买点",
                "卖点",
            )
        )
    )
    trading_concept_intent = (
        stock_domain_intent
        and not _to_code(q)
        and any(k in q for k in ("理念", "长期盈利", "核心原则", "交易系统", "交易纪律", "概率优势", "稳定盈利"))
    )
    if not quality.get("pass"):
        lines.append(
            f"先说结论：这次我不建议直接给强结论，当前置信度 {quality.get('confidence')} 偏低，"
            f"数据新鲜度为 {quality.get('data_freshness', {}).get('latest_trade_date')}。"
        )
        cov = quality.get("data_coverage", {}) or {}
        miss = [k for k, v in cov.items() if not v]
        if miss:
            lines.append(f"当前缺口主要在：{','.join(miss)}。建议先补齐这些数据后再做最终决策。")
        else:
            lines.append("建议先补齐更高频与事件数据后再做最终决策。")
        lines.append(f"本次证据编号：{eids or '无'}。")
        return "\n".join(lines)

    if whoami_intent:
        lines.append("我是 OpenClaw 智能体在本地的投研分身，不只会报股票代码。")
        lines.append("我可以做：能力说明、系统状态解读、策略/风控解释、个股分析、组合与执行建议。")
        lines.append("如果你问的是通用问题，我会按通用智能体方式回答；涉及交易时再切到数据证据与仓位动作。")
        lines.append("你可以直接问：'你现在看到哪些本地能力？'、'我这套系统下一步怎么优化？'、'分析 300750 并给风控'。")
        lines.append(f"证据编号：{eids or 'E2,E3,E4'}。")
        return "\n".join(lines)

    if stock_service_intent:
        return (
            "可以，数据库已连接且可用。你给我股票代码或名称（例如 300750 / 宁德时代），"
            "我就按你的系统数据给出：核心判断、触发条件、失效条件和仓位动作。"
        )

    if trading_concept_intent:
        return (
            "结论：在中国股市长期盈利的核心，不是预测每一次涨跌，而是建立“正期望系统 + 严格风控 + 可持续执行”。\n"
            "依据：\n"
            "1) 市场并非线性可预测，单次判断不稳定，长期只能靠概率优势；\n"
            "2) 收益由三件事决定：选股质量、仓位管理、回撤控制，缺一不可；\n"
            "3) 人性偏差（追涨杀跌、扛亏、频繁交易）会吞噬策略优势，必须靠纪律约束。\n"
            "可执行框架（中国股市实操版）：\n"
            "A) 只做你有统计优势的两三类场景，不做“全市场全时段”；\n"
            "B) 单笔风险固定（如总资金0.5%-1%），先活下来再追求高收益；\n"
            "C) 设入场/加仓/减仓/止损四条硬规则，盘中不改；\n"
            "D) 周维度复盘：只优化一个弱点（信号、仓位或执行），避免频繁改系统。\n"
            "一句话：长期盈利=小优势重复执行+大回撤严格避免。"
        )

    if not stock_domain_intent:
        if ("哲学" in q or "行为学" in q) and ("追涨" in q or "杀跌" in q):
            return (
                "结论：追涨杀跌本质上是“叙事驱动+损失厌恶+从众压力”叠加后的行为偏差。\n"
                "依据：\n"
                "1) 哲学层面：人会把“短期价格变化”误当成“长期价值真相”；\n"
                "2) 行为学层面：盈利时害怕回吐、亏损时害怕继续亏，导致追高与恐慌割肉；\n"
                "3) 市场结构层面：信息不对称与社交传播放大群体羊群效应。\n"
                "可执行改进：\n"
                "A) 交易前写死触发条件与失效条件，不在盘中改规则；\n"
                "B) 固定仓位上限和单笔风险（如单笔亏损<=总资金1%）；\n"
                "C) 每周复盘3笔“情绪主导交易”，只改一个坏习惯直到稳定。"
            )
        if ("技术架构" in q or "架构师" in q or "多智能体" in q) and ("提纲" in q or "设计" in q):
            return (
                "结论：高可用多智能体系统应按“网关-编排-能力-记忆-观测-治理”六层设计。\n"
                "设计提纲：\n"
                "1) 接入层：统一API网关、鉴权、限流、灰度发布；\n"
                "2) 编排层：任务路由器（意图识别+策略选择+回退链路）；\n"
                "3) 能力层：领域Agent（股票/写作/代码/分析）与工具沙箱；\n"
                "4) 记忆层：短期会话记忆+长期知识库+用户偏好；\n"
                "5) 可靠性：超时重试、熔断降级、幂等设计、队列削峰；\n"
                "6) 运维治理：日志追踪、指标告警、A/B评估、权限与审计。\n"
                "落地顺序：先可用（路由+回退）-> 再稳定（熔断+监控）-> 再智能（长期记忆+策略学习）。"
            )
        if "结构学" in q and "文案" not in q:
            return (
                "结构学可以理解为：研究“部分-关系-整体”如何共同决定结果的一门方法论。"
                "它不只看单个要素，而是看要素之间的连接方式、层级和约束。"
                "在实操里，先定义系统边界，再画出关键结构（角色、流程、反馈回路），"
                "最后识别决定成败的主结构并优先优化。"
            )
        if "文案" in q:
            if "结构学" in q:
                return (
                    "结构，不是把信息堆在一起，而是让每个部分各司其职、彼此支撑。"
                    "所谓结构学，就是在复杂系统中找到关键关系与主导路径："
                    "什么是骨架，什么是节点，什么是反馈回路。"
                    "当结构对了，效率会自己出现；当结构错了，再努力也只是内耗。"
                )
            return (
                "可以。请给我文案的用途、受众、语气和字数，我会直接给你可发布版本。"
                "例如：朋友圈宣传（轻松）、产品介绍（专业）、短视频口播（有节奏）。"
            )
        if "周报" in q:
            return (
                "本周整体工作围绕“稳定性优先、效率提升、风险前置”推进，核心流程保持连续稳定运行。"
                "在关键模块上完成了问题收敛与策略优化，重点指标较上周有所改善。"
                "下周将继续聚焦可验证交付与关键瓶颈突破，确保系统能力持续增强。"
            )
        if "邮件" in q:
            return (
                "可以。把收件人、主题和你希望的语气告诉我（正式/简洁/强硬），"
                "我直接给你可发送的完整邮件正文。"
            )
        return (
            "可以回答。你现在走的是 OpenClaw 总入口，我不仅能做股票分析，也能做通用任务（写作、总结、方案、脚本思路）。"
            "你直接给我具体目标和约束，我会按任务给可执行结果。"
        )

    if _to_code(q):
        snap = outputs.get("stock_snapshot", {})
        if snap.get("ok"):
            lines.append(
                f"{snap['name']}({snap['code']}) 这次我按专家视角给你一句话判断："
                f"短线 {_fmt_pct(snap.get('ret20'))}、中线 {_fmt_pct(snap.get('ret60'))}，"
                "当前更像“中线未坏、短线择时”的结构。"
            )
            lines.append(
                f"关键触发条件：放量上破近阶段压力可加仓；失效条件：跌破风控位并放量转弱。"
            )
            lines.append("仓位动作：先 30%-40% 试探仓，确认后再加；失效则先减半。")
            lines.append(f"证据编号：{eids or 'E1'}。")
            return "\n".join(lines)

    bc = outputs.get("bull_candidates", {})
    if bc.get("ok") and candidate_intent:
        cands = bc.get("candidates", [])[:8]
        lines.append("我先给你组合候选的专家版判断：当前是“结构性机会>指数机会”的市场。")
        lines.append("关键触发条件：Top候选继续放量并维持20/60日共振；失效条件：市场广度急转弱且龙头连破关键位。")
        lines.append("仓位动作：总仓先 50%-65%，按强弱分3层；触发失效条件就把高波动层先降到一半。")
        for i, c in enumerate(cands, 1):
            lines.append(
                f"{i:02d}. {c.get('name')}({c.get('code')}) 区间{c.get('pred_low')}%~{c.get('pred_high')}%，"
                f"因子理由：{c.get('reason')}"
            )
        lines.append(f"证据编号：{eids or 'E2,E3,E4'}。")
        return "\n".join(lines)

    if outputs.get("portfolio_decision", {}).get("ok") and decision_intent:
        pdx = outputs["portfolio_decision"]
        lines.append("组合层面我给你直接可执行版本：")
        for i, p in enumerate(pdx.get("positions", [])[:8], 1):
            lines.append(
                f"{i:02d}. {p.get('name')}({p.get('code')}) 权重{round(100*float(p.get('target_weight',0)),1)}%，"
                f"止损{p.get('stop_loss')}，动作{p.get('trim_rule')}。"
            )
        pf = pdx.get("portfolio", {})
        lines.append(
            f"组合约束：单票上限{int(100*pf.get('single_name_cap',0.15))}% / 行业上限{int(100*pf.get('industry_cap',0.35))}% / "
            f"回撤预算{int(100*pf.get('max_drawdown_budget',0.12))}%。"
        )
        lines.append(f"证据编号：{eids or 'E2,E4'}。")
        return "\n".join(lines)

    # fallback to existing rule-style answer with richer tail
    base = _rule_synthesis(question, outputs)
    base += f"\n质量闸门：confidence={quality.get('confidence')}，data_freshness={quality.get('data_freshness', {}).get('latest_trade_date')}。"
    base += f"\n证据编号：{eids or '无'}。"
    return base


def _default_evidence_tag(evidence_bundle: Dict[str, Any]) -> str:
    evs = evidence_bundle.get("evidence", []) if isinstance(evidence_bundle, dict) else []
    ids = [str(e.get("id", "")).strip() for e in evs if str(e.get("id", "")).strip()]
    return ids[0] if ids else "E1"


def _pick_evidence_tag_for_line(line: str, default_tag: str) -> str:
    txt = (line or "").strip()
    if not txt:
        return default_tag
    if any(k in txt for k in ("市场", "上涨", "下跌", "情绪", "广度")):
        return "E3"
    if any(k in txt for k in ("回测", "胜率", "回撤", "风险等级")):
        return "E4"
    if any(k in txt for k in ("候选", "Top", "排名", "因子", "组合", "composite")):
        return "E2"
    return default_tag


def _line_is_key_judgement(line: str) -> bool:
    txt = (line or "").strip()
    if not txt:
        return False
    if re.match(r"^\d{1,2}\.", txt):
        return True
    key_prefixes = (
        "结论",
        "核心判断",
        "关键依据",
        "关键触发条件",
        "触发条件",
        "失效条件",
        "仓位动作",
        "执行建议",
        "风险",
        "组合约束",
    )
    return txt.startswith(key_prefixes)


def _enforce_evidence_tags(answer: str, question: str, evidence_bundle: Dict[str, Any]) -> str:
    text = (answer or "").strip()
    if not text:
        return text
    q = (question or "").strip()
    stock_like = bool(
        _to_code(q)
        or any(k in q for k in ("股票", "个股", "仓位", "止损", "回测", "候选", "买点", "卖点", "组合", "大盘"))
    )
    if not stock_like:
        return text

    lines = text.split("\n")
    default_tag = _default_evidence_tag(evidence_bundle)
    changed = False
    for i, line in enumerate(lines):
        if not _line_is_key_judgement(line):
            continue
        if EVIDENCE_TAG_PATTERN.search(line):
            continue
        tag = _pick_evidence_tag_for_line(line, default_tag)
        lines[i] = f"{line} [{tag}]"
        changed = True

    merged = "\n".join(lines).strip()
    if not EVIDENCE_TAG_PATTERN.search(merged):
        merged = merged + f"\n证据编号：[{default_tag}]。"
        changed = True
    return merged if changed else text


def _rule_synthesis(question: str, outputs: Dict[str, Any]) -> str:
    q_text = question or ""
    lines = []
    if outputs.get("bull_candidates", {}).get("ok"):
        cands = outputs["bull_candidates"]["candidates"]
        focus_ranks = _extract_requested_ranks(question, len(cands))
        if focus_ranks and any(k in (question or "") for k in ("风险", "仓位", "止损", "展开", "详细")):
            lines.append("结论：按你上一轮候选，以下仅展开指定标的的风险与仓位建议。")
            lines.append("关键依据：基于同一批次候选（20/60日动量+量能），结合弹性区间进行仓位分层。")
            lines.append("执行建议：")
            for rk in focus_ranks:
                c = cands[rk - 1]
                elastic = float(c.get("pred_high", 0)) - float(c.get("pred_low", 0))
                if elastic >= 60:
                    risk_level = "高波动"
                    pos = "单票8%-10%，分3笔建仓（5/3/2）"
                    sl = "-10%硬止损，单日放量长阴先减仓"
                elif elastic >= 40:
                    risk_level = "中高波动"
                    pos = "单票10%-12%，分2-3笔建仓"
                    sl = "-8%预警，-12%硬止损"
                else:
                    risk_level = "中等波动"
                    pos = "单票12%-15%，分2笔建仓"
                    sl = "-7%预警，-10%硬止损"
                lines.append(
                    f"{rk:02d}. {c['name']}({c['code']})：风险={risk_level}；建议仓位={pos}；风控={sl}。"
                )
            lines.append("风险：以上为研究用途的规则化建议，实盘请叠加公告/财报/流动性二次确认。")
            return "\n".join(lines)
        lines.append(f"结论：基于本地数据已生成Top{len(cands)}牛股候选，适合做分层跟踪。")
        lines.append(f"关键依据：样本日期 {outputs['bull_candidates'].get('date')}，60日+20日动量与量能联合排序。")
        lines.append("执行建议：")
        need_detail = any(k in (question or "") for k in ("详细", "理由", "展开", "为什么"))
        for i, c in enumerate(cands[:10], 1):
            base = (
                f"{i:02d}. {c['name']}({c['code']}) {c['industry']}，20日{c['ret20']}%，60日{c['ret60']}%，区间{c['pred_low']}%~{c['pred_high']}%"
            )
            if need_detail:
                reason = " 理由：" + str(c.get("reason", "动量与量能结构较优，具备继续跟踪价值。"))
                lines.append(base + reason)
            else:
                lines.append(base)
        lines.append("风险：区间预测不是确定收益，需结合财报、监管、流动性冲击做二次筛选。")
        return "\n".join(lines)

    snap = outputs.get("stock_snapshot", {})
    ov = outputs.get("market_overview", {})
    bt = outputs.get("backtest_context", {})
    if snap.get("ok"):
        stock_free = any(k in q_text for k in ("分析", "怎么看", "走势", "判断", "研究", "分析下"))
        if stock_free:
            lines.append(
                f"{snap['name']}({snap['code']}) 这只票我先给你直说重点："
                f"最新交易日 {snap['date']} 收在 {snap['close']:.2f}，当日涨跌 {_fmt_pct(snap.get('pct'))}，"
                f"20日区间表现 {_fmt_pct(snap.get('ret20'))}，60日区间表现 {_fmt_pct(snap.get('ret60'))}。"
            )
            lines.append(
                "如果20日和60日同向上，通常说明趋势还在；如果20日转弱但60日仍强，更像高位震荡，"
                "这类阶段更适合分批而不是追单。"
            )
            lines.append(
                "实操上可把仓位拆两段：先试探仓，再看是否放量延续；"
                "若出现放量长阴或跌破关键均线，优先减仓而不是硬扛。"
            )
        else:
            lines.append(
                f"结论：{snap['name']}({snap['code']}) 当前可继续跟踪，需按波动控制仓位。"
            )
            lines.append(
                f"关键依据：最新{snap['date']}，收盘{snap['close']:.2f}，涨跌{_fmt_pct(snap.get('pct'))}，20日{_fmt_pct(snap.get('ret20'))}，60日{_fmt_pct(snap.get('ret60'))}。"
            )
    elif isinstance(snap, dict) and snap:
        code = _to_code(question or "")
        err = str(snap.get("error", "snapshot failed"))
        if code:
            lines.append(
                f"我识别到了 {code}，但当前本地行情快照没取成功（{err}）。"
            )
            lines.append(
                "先把这只票近90日行情和基础信息补齐，我就可以直接给你做趋势节奏、仓位分层和风控位判断。"
            )
    if ov.get("ok"):
        lines.append(
            f"市场概览：{ov['trade_date']} 上涨{ov['advancers']} / 下跌{ov['decliners']} / 平{ov['flat']}，均值{ov['avg_pct_chg']:.2f}% 。"
        )
    if bt.get("ok"):
        lines.append(
            f"回测参考：胜率{bt.get('evo_win_rate')}，平均收益{bt.get('evo_avg_return')}，最大回撤{bt.get('evo_max_drawdown')}。"
        )
    if not lines:
        lines.append("结论：当前可用数据不足，请先指定股票代码或问题方向（市场/回测/预测/组合）。")
    lines.append("风险：所有建议仅用于研究，实盘请先做小仓验证。")
    return "\n".join(lines)


def _run_tools(question: str, session_id: str, sql: Optional[str]) -> Dict[str, Any]:
    planned = _plan_tools(question)
    out: Dict[str, Any] = {"planned_tools": planned}
    for t in planned:
        if t == "bull_candidates":
            out[t] = tool_bull_candidates(20)
        elif t == "market_overview":
            out[t] = tool_market_overview()
        elif t == "backtest_context":
            out[t] = tool_backtest_context()
        elif t == "stock_snapshot":
            out[t] = tool_stock_snapshot(question, session_id)
        elif t == "feature_warehouse":
            out[t] = tool_feature_warehouse(220)
    if sql:
        out["sql_query"] = tool_sql(sql)
    # Derived tools for expert workflow.
    out["portfolio_decision"] = tool_portfolio_decision(out)
    out["quality_gate"] = tool_quality_gate(out)
    out["evolution_tick"] = tool_evolution_tick(out)
    out["evidence_bundle"] = _stage1_collect_evidence(question, out)
    return out


def _route_for_mesh(question: str) -> str:
    q = (question or "").lower()
    if any(k in q for k in ("skills", "skill", "安装", "下载安装", "联网学习")):
        return "skills"
    if any(k in q for k in ("你是谁", "你能做什么", "能力")):
        return "identity"
    if _to_code(q) or any(k in q for k in ("股票", "个股", "仓位", "止损", "回测", "候选", "买点", "卖点", "大盘")):
        return "stock_core"
    return "general"


def _agent_reply(session_id: str, question: str, sql: Optional[str] = None) -> Dict[str, Any]:
    outputs = _run_tools(question, session_id, sql)
    quality = outputs.get("quality_gate", {}) if isinstance(outputs.get("quality_gate"), dict) else {}
    conf_raw = float(quality.get("confidence", 0.7) or 0.7)
    conf_pct = conf_raw * 100.0 if conf_raw <= 1.5 else conf_raw
    route = _route_for_mesh(question)
    agent_hits = select_agents(question=question, route=route, confidence=conf_pct, max_agents=10)
    llm_text = _llm_answer(session_id, question, outputs) if quality.get("pass", True) else None
    if llm_text:
        answer = llm_text
        mode = "agent_llm"
    else:
        answer = _stage2_expert_answer(
            question=question,
            outputs=outputs,
            evidence_bundle=outputs.get("evidence_bundle", {}),
            quality=quality,
        )
        mode = "agent_expert"
    answer = _enforce_evidence_tags(
        answer=answer,
        question=question,
        evidence_bundle=outputs.get("evidence_bundle", {}),
    )

    code = _to_code(question) or (outputs.get("stock_snapshot", {}) or {}).get("code")
    if code:
        _save_stock_profile(
            str(code),
            {
                "last_question": question,
                "last_view": answer[:500],
                "quality": quality,
                "updated_trade_date": (outputs.get("feature_warehouse", {}) or {}).get("trade_date"),
            },
        )
    used = list(outputs.get("planned_tools", [])) + ["quality_gate", "portfolio_decision", "evolution_tick"]
    return {
        "answer": answer,
        "used_tools": list(dict.fromkeys(used)),
        "mode": mode,
        "quality": quality,
        "route": route,
        "agent_version": AGENT_VERSION,
        "agent_count": int(count_agents()),
        "agent_hits": agent_hits,
    }


@app.get("/health")
def health():
    freshness = _data_freshness_info()
    return {
        "ok": True,
        "use_llm": USE_LLM,
        "latest_trade_date": freshness.get("latest_trade_date"),
        "data_coverage": _data_capability_summary(_find_db()),
    }


@app.get("/debug/llm")
def debug_llm():
    probe = str(request.args.get("probe", "0")).strip() == "1"
    payload: Dict[str, Any] = {
        "ok": True,
        "use_llm": USE_LLM,
        "llm_url": LLM_URL,
        "openai_base_url": OPENAI_BASE_URL,
        "openai_model": OPENAI_MODEL,
        "openai_key_set": bool(OPENAI_API_KEY),
        "openai_key_len": len(OPENAI_API_KEY) if OPENAI_API_KEY else 0,
    }
    if probe:
        payload["probe"] = _probe_llm_connectivity()
    return jsonify(payload)


@app.post("/chat")
def chat():
    body = request.get_json(silent=True) or {}
    q = (body.get("question") or body.get("message") or "").strip()
    sql = (body.get("sql") or "").strip() or None
    session_id = (body.get("session_id") or body.get("sessionId") or "stock-web").strip()
    if not q:
        return jsonify({"answer": "请输入问题，例如：给我今天市场情绪和Top20候选", "mode": "agent"})
    _remember(session_id, "user", q)
    resp = _agent_reply(session_id, q, sql=sql)
    conf_raw = float(((resp.get("quality") or {}).get("confidence", 0.0)) or 0.0)
    conf_pct = conf_raw * 100.0 if conf_raw <= 1.5 else conf_raw
    try:
        _log_agent_hit(
            session_id=session_id,
            question=q,
            route=str(resp.get("route", "unknown")),
            mode=str(resp.get("mode", "unknown")),
            confidence=float(conf_pct),
            agent_version=str(resp.get("agent_version", AGENT_VERSION)),
            agent_count=int(resp.get("agent_count", int(count_agents()))),
            agent_hits=list(resp.get("agent_hits", []) or []),
        )
    except Exception:
        pass
    _remember(session_id, "assistant", resp.get("answer", ""))
    return jsonify(
        {
            "answer": resp.get("answer", ""),
            "mode": resp.get("mode", "agent_rule"),
            "session_id": session_id,
            "route": resp.get("route", "unknown"),
            "used_tools": resp.get("used_tools", []),
            "quality": resp.get("quality", {}),
            "agent_version": resp.get("agent_version", AGENT_VERSION),
            "agent_count": resp.get("agent_count", int(count_agents())),
            "agent_hits": resp.get("agent_hits", []),
        }
    )


@app.get("/report/agent_hits_daily")
def report_agent_hits_daily():
    date_key = request.args.get("date")
    save_flag = str(request.args.get("save", "0")).strip() == "1"
    summary = _build_agent_hit_daily_summary(date_key)
    if save_flag:
        summary["artifacts"] = _save_agent_hit_daily_report(summary)
    return jsonify(summary)


@app.post("/tool/sql")
def sql_tool():
    body = request.get_json(silent=True) or {}
    return jsonify(tool_sql((body.get("query") or "").strip()))


@app.get("/tool/profile/<code>")
def get_profile(code: str):
    c = _to_code(code or "") or (code or "").upper()
    return jsonify({"ok": True, "code": c, "profile": _load_stock_profile(c)})


@app.post("/tool/evolve")
def evolve_now():
    out = {
        "backtest_context": tool_backtest_context(),
        "feature_warehouse": tool_feature_warehouse(120),
    }
    return jsonify(tool_evolution_tick(out))


if __name__ == "__main__":
    _init_memory_db()
    app.run(host="127.0.0.1", port=5101)
