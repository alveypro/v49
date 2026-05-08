from flask import Flask, request, jsonify
import glob
import json
import os
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

app = Flask(__name__)


MEMORY: Dict[str, List[Dict[str, str]]] = defaultdict(list)


def _load_dotenv_if_exists() -> None:
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
        return


_load_dotenv_if_exists()


def _evo_metric(evo: Optional[dict], key: str) -> Any:
    if not isinstance(evo, dict):
        return None
    if evo.get(key) is not None:
        return evo.get(key)
    stats = evo.get("stats") or {}
    if isinstance(stats, dict):
        return stats.get(key)
    return None


def _to_code(q: str) -> Optional[str]:
    # Use plain numeric match so Chinese text like "688608怎么样" is also recognized.
    m = re.search(r"(\d{6}(?:\.(?:SZ|SH))?)", (q or "").upper())
    if not m:
        return None
    c = m.group(1)
    if "." not in c:
        c = c + (".SH" if c.startswith(("5", "6", "9")) else ".SZ")
    return c


def _lookup_code_by_name(name_or_text: str) -> Optional[str]:
    db = _find_db()
    if not db:
        return None
    text = (name_or_text or "").strip()
    if not text:
        return None
    # Extract possible Chinese segments and strip common query suffix/prefix.
    segments = re.findall(r"[\u4e00-\u9fa5]{2,12}", text)
    if not segments:
        return None
    stop_pattern = re.compile(r"(怎么样|如何|怎么|分析|建议|仓位|风控|给我|一下|可以|吗|请|帮我|看下|看看)")
    candidates: List[str] = []
    for seg in segments:
        cleaned = stop_pattern.sub("", seg).strip()
        if 2 <= len(cleaned) <= 8:
            candidates.append(cleaned)
        if 2 <= len(seg) <= 8:
            candidates.append(seg)
    # keep order and uniqueness
    uniq: List[str] = []
    seen = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    if not uniq:
        return None
    try:
        conn = sqlite3.connect(db)
        df = pd.DataFrame()
        # exact match first, then fuzzy match
        for name in uniq:
            df = pd.read_sql_query(
                "SELECT ts_code,name FROM stock_basic WHERE name = ? LIMIT 1",
                conn,
                params=(name,),
            )
            if not df.empty:
                break
        if df.empty:
            for name in uniq:
                df = pd.read_sql_query(
                    "SELECT ts_code,name FROM stock_basic WHERE name LIKE ? LIMIT 1",
                    conn,
                    params=("%" + name + "%",),
                )
                if not df.empty:
                    break
        conn.close()
        if df.empty:
            return None
        return str(df.iloc[0]["ts_code"])
    except Exception:
        return None


def _find_db() -> Optional[str]:
    cands = [
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
    for pat in ("/opt/airivo/app/logs/openclaw/run_summary_*.json", "/opt/openclaw/logs/openclaw/run_summary_*.json"):
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
    for p in (
        "/opt/openclaw/evolution/last_run.json",
        "/opt/openclaw/qa-stock/evolution/last_run.json",
        "/opt/airivo/app/evolution/last_run.json",
        "evolution/last_run.json",
    ):
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    return None


def _stock_snapshot(ts_code: str) -> Optional[dict]:
    db = _find_db()
    if not db:
        return None
    try:
        conn = sqlite3.connect(db)
        # auto-adapt to schema differences
        b_cols = pd.read_sql_query("PRAGMA table_info(stock_basic)", conn)["name"].astype(str).tolist()
        d_cols = pd.read_sql_query("PRAGMA table_info(daily_trading_data)", conn)["name"].astype(str).tolist()

        code_col = "ts_code" if "ts_code" in d_cols else ("symbol" if "symbol" in d_cols else None)
        if not code_col:
            conn.close()
            return None

        date_col = "trade_date" if "trade_date" in d_cols else ("date" if "date" in d_cols else None)
        close_col = "close_price" if "close_price" in d_cols else ("close" if "close" in d_cols else None)
        pct_col = "pct_chg" if "pct_chg" in d_cols else ("pct_change" if "pct_change" in d_cols else None)
        if not date_col or not close_col:
            conn.close()
            return None

        # try multiple code formats
        code_candidates = [ts_code]
        raw = ts_code.replace(".SH", "").replace(".SZ", "")
        code_candidates.extend([raw, f"SH{raw}", f"SZ{raw}", f"{raw}.SH", f"{raw}.SZ"])
        code_candidates = list(dict.fromkeys(code_candidates))

        basic_code_col = "ts_code" if "ts_code" in b_cols else ("symbol" if "symbol" in b_cols else None)
        name_col = "name" if "name" in b_cols else ("stock_name" if "stock_name" in b_cols else None)
        ind_col = "industry" if "industry" in b_cols else ("industry_name" if "industry_name" in b_cols else None)

        b = pd.DataFrame()
        if basic_code_col and name_col:
            industry_expr = (ind_col + " as industry") if ind_col else "'' as industry"
            for c in code_candidates:
                q = (
                    f"SELECT {basic_code_col} as ts_code,{name_col} as name,{industry_expr} "
                    f"FROM stock_basic WHERE {basic_code_col}=? LIMIT 1"
                )
                b = pd.read_sql_query(q, conn, params=(c,))
                if not b.empty:
                    break

        d = pd.DataFrame()
        for c in code_candidates:
            q = (
                f"SELECT {date_col} as trade_date,{close_col} as close_price,"
                f"{(pct_col + ' as pct_chg') if pct_col else 'NULL as pct_chg'} "
                f"FROM daily_trading_data WHERE {code_col}=? ORDER BY {date_col} DESC LIMIT 60"
            )
            d = pd.read_sql_query(q, conn, params=(c,))
            if not d.empty:
                break
        conn.close()
        if d.empty:
            return None
        d["close_price"] = pd.to_numeric(d["close_price"], errors="coerce")
        d["pct_chg"] = pd.to_numeric(d["pct_chg"], errors="coerce")
        d = d.dropna().reset_index(drop=True)
        x = d.iloc[0]
        ret20 = None
        if len(d) > 20 and float(d.iloc[20]["close_price"]) > 0:
            ret20 = (float(x["close_price"]) / float(d.iloc[20]["close_price"]) - 1.0) * 100.0
        vol20 = float(d["pct_chg"].head(20).std()) if len(d) >= 20 else None
        return {
            "name": b.iloc[0]["name"] if not b.empty else ts_code,
            "code": ts_code,
            "industry": b.iloc[0]["industry"] if not b.empty else "未知",
            "date": str(x["trade_date"]),
            "close": float(x["close_price"]),
            "pct": float(x["pct_chg"]),
            "ret20": ret20,
            "vol20": vol20,
        }
    except Exception:
        return None


def _intent(q: str) -> str:
    t = (q or "").lower()
    if any(k in t for k in ("你是谁", "who are you", "介绍你自己")):
        return "whoami"
    if any(k in t for k in ("回测", "胜率", "最大回撤", "风险", "orange", "日报")):
        return "risk_backtest"
    if any(k in t for k in ("优化", "调参", "系统问题", "改进")):
        return "optimize"
    if _to_code(q or ""):
        return "stock"
    return "general"


def _route_skill(q: str) -> str:
    t = (q or "").lower()
    if any(k in t for k in ("回测", "胜率", "最大回撤", "夏普", "日报")):
        return "backtest_diag"
    if any(k in t for k in ("风险", "回撤", "波动", "止损", "orange")):
        return "risk_alert"
    if any(k in t for k in ("优化", "调参", "参数", "系统问题", "改进")):
        return "system_opt"
    return "stock_analysis"


def _llm_answer(session_id: str, question: str, skill: str, stock: Optional[dict], summary: Optional[dict], evo: Optional[dict]) -> Optional[str]:
    # Dedicated stock-agent LLM endpoint (independent from telegram/medical agent memory).
    if os.getenv("STOCK_AGENT_USE_LLM", "1") != "1":
        return None
    # Prefer local bridge API that is already connected to OpenClaw model runtime.
    url = os.getenv("STOCK_LLM_URL", "http://127.0.0.1:3443/api/ai/chat")

    context_bits = []
    if stock:
        context_bits.append(
            "stock=%s(%s), industry=%s, close=%.4f, pct=%.4f, date=%s, ret20=%s, vol20=%s"
            % (
                stock.get("name", ""),
                stock.get("code", ""),
                stock.get("industry", ""),
                float(stock.get("close", 0.0)),
                float(stock.get("pct", 0.0)),
                stock.get("date", ""),
                stock.get("ret20"),
                stock.get("vol20"),
            )
        )
    if summary:
        risk = (summary.get("risk") or {}).get("risk_level")
        publish = (summary.get("publish") or {}).get("status")
        context_bits.append("run_summary: risk=%s publish=%s" % (risk, publish))
    if evo:
        context_bits.append(
            "evolution: win_rate=%s avg_return=%s max_drawdown=%s"
            % (_evo_metric(evo, "win_rate"), _evo_metric(evo, "avg_return"), _evo_metric(evo, "max_drawdown"))
        )
    context_text = "\n".join(context_bits) if context_bits else "no local context"

    history = MEMORY.get(session_id, [])[-8:]
    msgs = [
        {
            "role": "system",
            "content": (
                "你是股票系统专属OpenClaw智能体。你可以使用本地数据库与上下文数据。"
                "只回答股票/回测/风控/系统优化，不要医学内容。"
                "按用户问题灵活回答，不要固定模板。"
                "有数据就引用数据；没数据就明确缺口并给下一步操作。"
                "禁止说“我无法访问数据库”这类无效话术。"
                "当前skill=%s。\n上下文:\n%s" % (skill, context_text)
            ),
        }
    ]
    for h in history:
        msgs.append({"role": h.get("role", "user"), "content": h.get("text", "")})
    msgs.append({"role": "user", "content": question})

    try:
        r = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"sessionId": session_id, "messages": msgs, "context": "股票投研场景，不要医学内容"},
            timeout=30,
        )
        if r.status_code >= 400:
            return None
        data = r.json() if r.content else {}
        if isinstance(data, dict):
            if data.get("reply"):
                text = str(data.get("reply"))
                if stock and ("无法获取" in text or "没有数据" in text or "无数据" in text):
                    return None
                return text
            c = data.get("choices") or []
            if c and isinstance(c[0], dict):
                m = c[0].get("message") or {}
                if m.get("content"):
                    text = str(m.get("content"))
                    if stock and ("无法获取" in text or "没有数据" in text or "无数据" in text):
                        return None
                    return text
        return None
    except Exception:
        return None


def _remember(session_id: str, role: str, text: str) -> None:
    hist = MEMORY[session_id]
    hist.append({"role": role, "text": text})
    if len(hist) > 20:
        del hist[:-20]


def _last_stock_code(session_id: str) -> Optional[str]:
    for x in reversed(MEMORY.get(session_id, [])):
        c = _to_code(x.get("text", ""))
        if c:
            return c
    return None


def _reply(session_id: str, q: str) -> str:
    intent = _intent(q)
    skill = _route_skill(q)
    s = _latest_summary()
    e = _latest_evolution()
    code = _to_code(q) or _lookup_code_by_name(q) or _last_stock_code(session_id)
    st = _stock_snapshot(code) if code else None

    llm = _llm_answer(session_id, q, skill, st, s, e)
    if llm:
        return llm

    if intent == "whoami":
        return (
            "我是股票系统专属 OpenClaw 智能体（与 Telegram/呼吸机系统隔离）。\n"
            "我负责：个股分析、回测诊断、风险解释、参数优化建议。"
        )

    if intent in ("stock", "general", "optimize"):
        lines = []
        pos_m = re.search(r"(\d{1,3})\s*%?\s*仓", q)
        pos_text = None
        if pos_m:
            pos_text = int(pos_m.group(1))
        if st:
            lines.append(f"{st['name']}({st['code']}) 目前价格 {st['close']:.2f}，当日 {st['pct']:.2f}%，行业 {st['industry']}。")
            if st.get("ret20") is not None:
                trend = "偏强" if st["ret20"] > 8 else ("偏弱" if st["ret20"] < -8 else "震荡")
                lines.append(f"近20日收益 {st['ret20']:.2f}%，20日波动 {st.get('vol20') or 0:.2f}%，当前节奏偏{trend}。")
            else:
                lines.append("历史样本不足，先做观察仓。")
            if pos_text is not None:
                if pos_text >= 60:
                    lines.append(f"你当前仓位约{pos_text}%，不建议直接加仓，先等回踩确认或放量突破再评估。")
                elif pos_text >= 30:
                    lines.append(f"你当前仓位约{pos_text}%，如要加仓建议小幅分批，不要一次性打满。")
                else:
                    lines.append(f"你当前仓位约{pos_text}%，可以考虑试探仓，但仍需分批与止损。")
            lines.append("风控上建议：单笔不过重、分2-3笔、若2-3天无延续就降仓，止损止盈纪律先于观点。")
        else:
            lines.append("我暂时没读到这只股票的本地行情明细。先补齐数据后，我可以给你更具体的仓位与风控建议。")
        lines.append("系统优化建议：提高信号阈值、抑制低密度触发、按近期胜率动态调仓与持有期。")
        if s:
            risk = (s.get("risk") or {}).get("risk_level", "unknown")
            pub = (s.get("publish") or {}).get("status", "unknown")
            lines.append(f"当前系统状态：risk={risk}, publish={pub}。")
        elif e:
            wr = _evo_metric(e, "win_rate")
            ar = _evo_metric(e, "avg_return")
            md = _evo_metric(e, "max_drawdown")
            lines.append(f"回测参考：win_rate={wr}, avg_return={ar}, max_drawdown={md}。")
        return "\n".join(lines)

    # risk_backtest
    if s:
        risk = (s.get("risk") or {}).get("risk_level", "unknown")
        rules = (s.get("risk") or {}).get("triggered_rules") or []
        pub = (s.get("publish") or {}).get("status", "unknown")
        return (
            f"当前风险等级 {risk}，发布状态 {pub}。"
            f"{' 触发规则：' + '、'.join(rules) if rules else ''}"
            " 建议在风险升高阶段主动降仓，并提高入场阈值。"
        )
    if e:
        return (
            f"回测侧当前可读到：win_rate={_evo_metric(e, 'win_rate')}，"
            f"avg_return={_evo_metric(e, 'avg_return')}，max_drawdown={_evo_metric(e, 'max_drawdown')}。"
            " 实盘上建议先稳回撤，再谈提收益。"
        )
    return "暂未读取到 run_summary 或 evolution 回测文件，请先跑一次日报任务。"


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/chat")
def chat():
    body = request.get_json(silent=True) or {}
    q = (body.get("question") or body.get("message") or "").strip()
    session_id = (body.get("session_id") or body.get("sessionId") or "stock-web").strip()
    if not q:
        return jsonify({"answer": "请输入问题，例如：688608怎么样？", "mode": "stock_agent"})
    _remember(session_id, "user", q)
    skill = _route_skill(q)
    ans = _reply(session_id, q)
    _remember(session_id, "assistant", ans)
    return jsonify({"answer": ans, "mode": "stock_agent", "session_id": session_id, "skill": skill})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5101)
