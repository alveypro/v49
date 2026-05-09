from flask import Flask, request, jsonify
import os, re, glob, json, sqlite3
import pandas as pd
import requests

app = Flask(__name__)


def to_code(q: str):
    m = re.search(r"\b(\d{6}(?:\.(?:SZ|SH))?)\b", (q or '').upper())
    if not m:
        return None
    c = m.group(1)
    if "." not in c:
        c = c + (".SH" if c.startswith(("5", "6", "9")) else ".SZ")
    return c


def find_db():
    cands = [
        "/opt/airivo/data/permanent_stock_database.db",
        "/opt/airivo/permanent_stock_database.db",
        "/opt/airivo/app/permanent_stock_database.db",
        "/opt/openclaw/permanent_stock_database.db",
    ]
    for p in cands:
        if os.path.exists(p):
            return p
    return None


def latest_summary():
    cands = [
        "/opt/airivo/app/logs/openclaw/run_summary_*.json",
        "/opt/openclaw/logs/openclaw/run_summary_*.json",
    ]
    files = []
    for pat in cands:
        files.extend(glob.glob(pat))
    files = sorted(files, reverse=True)
    if not files:
        return None
    try:
        with open(files[0], "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def stock_snapshot(ts_code: str):
    db = find_db()
    if not db or not ts_code:
        return None
    try:
        conn = sqlite3.connect(db)
        b = pd.read_sql_query("SELECT ts_code,name,industry FROM stock_basic WHERE ts_code=? LIMIT 1", conn, params=(ts_code,))
        d = pd.read_sql_query(
            "SELECT trade_date,close_price,pct_chg,vol FROM daily_trading_data WHERE ts_code=? ORDER BY trade_date DESC LIMIT 60",
            conn,
            params=(ts_code,),
        )
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
            "name": (b.iloc[0]["name"] if not b.empty else ts_code),
            "code": ts_code,
            "industry": (b.iloc[0]["industry"] if not b.empty else "未知"),
            "date": str(x["trade_date"]),
            "close": float(x["close_price"]),
            "pct": float(x["pct_chg"]),
            "ret20": ret20,
            "vol20": vol20,
        }
    except Exception:
        return None


def llm_answer(question: str, context: dict):
    if os.getenv("OPENCLAW_QA_USE_LLM", "0") != "1":
        return None
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        return None
    base = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    try:
        r = requests.post(
            f"{base}/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "temperature": 0.2,
                "messages": [
                    {"role": "system", "content": "你是OpenClaw高级股票专家。输出结论+依据+风险+执行建议，禁止编造。"},
                    {"role": "user", "content": json.dumps({"question": question, "context": context}, ensure_ascii=False)},
                ],
            },
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]
    except Exception:
        return None


@app.get('/health')
def health():
    return {"ok": True}


@app.post('/chat')
def chat():
    q = ((request.get_json(silent=True) or {}).get("question") or "").strip()
    if not q:
        return jsonify({"answer": "请输入问题", "mode": "expert"})

    s = latest_summary()
    c = to_code(q)
    st = stock_snapshot(c)
    context = {"summary": s, "stock": st}

    ai = llm_answer(q, context)
    if ai:
        return jsonify({"answer": ai, "mode": "expert_llm"})

    lines = ["以下为 OpenClaw 高级投研助手结论（仅供参考，不构成投资建议）。"]
    if "你是谁" in q:
        lines.append("1) 我是 OpenClaw 智能体，负责：个股研判、系统体检、优化建议、风险提醒。")
    if st:
        lines.append(f"2) 标的画像：{st['name']}({st['code']})，行业={st['industry']}，最新价={st['close']:.2f}，当日涨跌={st['pct']:.2f}%，日期={st['date']}。")
        if st.get("ret20") is not None:
            lines.append(f"3) 趋势强弱：近20日收益={st['ret20']:.2f}%。")
        if st.get("vol20") is not None:
            lines.append(f"4) 波动评估：20日波动(涨跌标准差)={st['vol20']:.2f}%。")
    if s:
        risk = (s.get("risk") or {}).get("risk_level", "unknown")
        publish = (s.get("publish") or {}).get("status", "unknown")
        lines.append(f"5) 系统体检：risk={risk}，publish={publish}。")
        tr = (s.get("risk") or {}).get("triggered_rules") or []
        if tr:
            lines.append("6) 问题定位：" + "、".join(tr) + "。")
    else:
        lines.append("5) 系统体检：暂未读取到最新 run_summary。")

    lines.append("7) 执行建议：分批建仓、单票仓位控制、止损先行；参数优化优先提升信号质量并抑制低密度触发。")
    return jsonify({"answer": "\n".join(lines), "mode": "expert"})


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
