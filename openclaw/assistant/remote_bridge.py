import os
import re
import time
import logging
import glob
import sqlite3
from urllib.parse import urlparse
from typing import Optional
from pathlib import Path

import requests
import urllib3


_SESSION = requests.Session()
_LOG = logging.getLogger("openclaw.remote_bridge")
_LAST_BRIDGE_ERROR: dict = {}
_LOW_QUALITY_HINTS = (
    "无法回答",
    "无法提供",
    "无法预测",
    "不构成投资建议",
    "仅供参考",
    "建议咨询专业人士",
    "请自行判断",
)
_LOCAL_CONTEXT_MAX_CHARS = max(400, int(os.getenv("OPENCLAW_LOCAL_CONTEXT_MAX_CHARS", "2200")))


def _set_last_bridge_error(**kwargs) -> None:
    global _LAST_BRIDGE_ERROR
    _LAST_BRIDGE_ERROR = dict(kwargs)


def get_last_bridge_error() -> dict:
    return dict(_LAST_BRIDGE_ERROR or {})


def _load_dotenv_if_exists() -> None:
    """Load workspace .env so standalone callers match service runtime behavior."""
    root = Path(os.getenv("OPENCLAW_WORKSPACE_ROOT", "/Users/mac/2026Qlin")).resolve()
    env_path = root / ".env"
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


def _extract_answer(data: dict) -> Optional[str]:
    answer = data.get("answer") or data.get("reply") or data.get("content")
    if not answer:
        choices = data.get("choices") or []
        if choices and isinstance(choices[0], dict):
            answer = ((choices[0].get("message") or {}).get("content"))
    if answer:
        return str(answer)
    return None


def _is_low_quality_answer(question: str, answer: str) -> bool:
    q = (question or "").strip().lower()
    a = (answer or "").strip().lower()
    if not a:
        return True
    if len(a) < max(24, int(os.getenv("OPENCLAW_MIN_ANSWER_CHARS", "48"))):
        return True
    low_quality_hits = sum(1 for x in _LOW_QUALITY_HINTS if x in a)
    if low_quality_hits >= 2:
        return True
    # Stock answers should be structured and actionable.
    if _is_stock_question(q):
        stock_structure_hits = sum(
            1
            for k in ("结论", "触发", "失效", "仓位", "止损", "买点", "卖点")
            if k in a
        )
        if stock_structure_hits < 2:
            return True
    return False


def _quality_upgrade_question(question: str) -> str:
    q = (question or "").strip()
    if not q:
        return q
    if _is_stock_question(q):
        return (
            "请以专业投研方式回答，禁止模板化空话。\n"
            "回答必须包含：核心结论、关键依据、触发条件、失效条件、仓位动作。\n"
            "如果信息不足，给出最小可行下一步，而不是泛泛提示。\n\n"
            f"问题：{q}"
        )
    return (
        "请给出高密度、可执行回答：先结论，再依据，再步骤。"
        "避免通用套话和空洞免责声明。\n\n"
        f"问题：{q}"
    )


def _is_ipv4_host(host: str) -> bool:
    return bool(re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", host or ""))


def _expand_general_urls() -> list[str]:
    env_general = os.getenv("OPENCLAW_GENERAL_URL", "").strip()
    env_general_urls = os.getenv("OPENCLAW_GENERAL_URLS", "").strip()
    host_header = os.getenv("OPENCLAW_GENERAL_HOST_HEADER", "").strip()
    urls: list[str] = []
    if env_general_urls:
        for u in env_general_urls.split(","):
            u = u.strip()
            if u:
                urls.append(u.rstrip("/"))
    if env_general:
        urls.append(env_general.rstrip("/"))
    # Optional: mirror IP URL to domain URL for active/active routing.
    enable_domain_mirror = os.getenv("OPENCLAW_ENABLE_DOMAIN_MIRROR", "0").strip() == "1"
    if enable_domain_mirror:
        for u in list(urls):
            p = urlparse(u)
            if p.scheme == "https" and _is_ipv4_host(p.hostname or "") and host_header:
                port = f":{p.port}" if p.port else ""
                domain_url = f"{p.scheme}://{host_header}{port}{p.path}"
                urls.insert(0, domain_url.rstrip("/"))
                break
    return list(dict.fromkeys(urls))


def _is_cloud_general_api_url(url: str) -> bool:
    p = urlparse(url)
    host = p.hostname or ""
    if not url.endswith("/api/ai/chat"):
        return False
    if host in {"127.0.0.1", "localhost"}:
        return False
    return p.scheme in {"http", "https"}


def _is_ip_cloud_url(url: str) -> bool:
    p = urlparse(url)
    return p.scheme == "https" and _is_ipv4_host(p.hostname or "") and url.endswith("/api/ai/chat")


def _attach_local_context_enabled() -> bool:
    # Keep cloud-only network routing, but allow local system snapshot injection.
    return os.getenv("OPENCLAW_ATTACH_LOCAL_SYSTEM_CONTEXT", "1").strip() == "1"


def _should_attach_local_context(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return False
    hints = (
        "v49",
        "系统",
        "优化",
        "改进",
        "研究",
        "调用",
        "策略",
        "回测",
        "仓位",
        "股票",
        "交易",
        "诊断",
    )
    return any(h in q for h in hints)


def _safe_int_query(conn: sqlite3.Connection, sql: str) -> Optional[int]:
    try:
        row = conn.execute(sql).fetchone()
        if not row:
            return None
        return int(row[0]) if row[0] is not None else None
    except Exception:
        return None


def _safe_text_query(conn: sqlite3.Connection, sql: str) -> Optional[str]:
    try:
        row = conn.execute(sql).fetchone()
        if not row or row[0] is None:
            return None
        return str(row[0])
    except Exception:
        return None


def _local_v49_context_snapshot() -> str:
    root = Path(os.getenv("OPENCLAW_WORKSPACE_ROOT", "/Users/mac/2026Qlin")).resolve()
    v49_dir = root / "v49"
    lines: list[str] = []
    lines.append("【本地系统快照】")
    lines.append(f"- workspace={root}")
    lines.append(f"- v49_exists={v49_dir.exists()}")
    if v49_dir.exists():
        core_files = [
            "trading_assistant.py",
            "comprehensive_stock_evaluator_v8_ultimate.py",
            "comprehensive_stock_evaluator_v7_ultimate.py",
            "v6_data_provider_optimized.py",
            "v49_app.py",
        ]
        present = [f for f in core_files if (v49_dir / f).exists()]
        lines.append(f"- v49_core_files={','.join(present) if present else 'none'}")

    db_candidates = [
        root / "permanent_stock_database.db",
        root / "stock_data.db",
        root / "trading_assistant.db",
    ]
    db_path = next((p for p in db_candidates if p.exists()), None)
    lines.append(f"- db_path={db_path if db_path else 'none'}")
    if db_path:
        try:
            conn = sqlite3.connect(str(db_path))
            stock_cnt = _safe_int_query(conn, "SELECT COUNT(*) FROM stock_basic")
            daily_cnt = _safe_int_query(conn, "SELECT COUNT(*) FROM daily_trading_data")
            latest_td = _safe_text_query(conn, "SELECT MAX(trade_date) FROM daily_trading_data")
            conn.close()
            lines.append(f"- stock_basic_count={stock_cnt if stock_cnt is not None else 'n/a'}")
            lines.append(f"- daily_rows={daily_cnt if daily_cnt is not None else 'n/a'}")
            lines.append(f"- latest_trade_date={latest_td or 'n/a'}")
        except Exception:
            lines.append("- db_probe=failed")

    # Include latest v49 report/log hints so cloud can provide concrete optimization advice.
    report_patterns = [
        str(root / "v49*report*.md"),
        str(root / "v49*summary*.md"),
        str(root / "v49*.log"),
    ]
    reports: list[str] = []
    for pat in report_patterns:
        reports.extend(glob.glob(pat))
    reports = sorted(set(reports), reverse=True)[:5]
    if reports:
        compact = ",".join(Path(p).name for p in reports)
        lines.append(f"- recent_v49_artifacts={compact}")
    else:
        lines.append("- recent_v49_artifacts=none")

    text = "\n".join(lines).strip()
    if len(text) > _LOCAL_CONTEXT_MAX_CHARS:
        return text[:_LOCAL_CONTEXT_MAX_CHARS] + "\n...(local context truncated)"
    return text


def _augment_question_with_local_context(question: str) -> str:
    q = (question or "").strip()
    if not q:
        return q
    if not _attach_local_context_enabled() or not _should_attach_local_context(q):
        return q
    snapshot = _local_v49_context_snapshot()
    return (
        "请基于下面的本地系统快照进行分析与建议，禁止只说“无法访问系统”。\n"
        "如果数据不足，请指出最小缺口并给可执行采集步骤。\n\n"
        f"{snapshot}\n\n"
        f"用户问题：{q}"
    )


def _cloud_brain_only_enabled() -> bool:
    # Default to hybrid mode so local 5101 fallback remains available.
    return os.getenv("OPENCLAW_CLOUD_BRAIN_ONLY", "0").strip() == "1"


def _local_timeout_for_url(url: str, stock_like_question: bool) -> float:
    if not url.startswith("http://127.0.0.1:"):
        return 9999.0
    if ":5101/" in url:
        # Primary local stock expert endpoint can be slower than other local gateways.
        return float(max(4, int(os.getenv("OPENCLAW_LOCAL_5101_TIMEOUT_SEC", "14"))))
    return float(max(2, int(os.getenv("OPENCLAW_LOCAL_TIMEOUT_SEC", "4"))))


def _master_system_prompt() -> str:
    override = os.getenv("OPENCLAW_MASTER_SYSTEM_PROMPT", "").strip()
    if override:
        return override
    return (
        "你是OpenClaw总智能体（Master Agent），以中文回答。"
        "你的角色融合为：股票专家、哲学家、技术架构专家、人类行为学专家、政治经济研究专家、"
        "中国股市从业者、量化交易专家、新闻分析专家、历史研究者、概率与统计专家，以及跨学科通才。"
        "要求："
        "1) 先给结论，再给依据，再给可执行步骤；"
        "2) 避免空话和模板化拒答，优先给用户可直接用的产物（方案/文案/清单/脚本思路）；"
        "3) 如果信息不足，明确缺口并给最小可行下一步，不要只说“无法回答”；"
        "4) 股票与交易问题必须结构化给出：核心判断、触发条件、失效条件、仓位动作；"
        "5) 通用问题按跨学科专家方式回答，允许结合哲学、历史、行为和政治经济视角。"
    )


def _request_headers(url: str) -> dict:
    headers = {}
    host_header = os.getenv("OPENCLAW_GENERAL_HOST_HEADER", "").strip()
    parsed = urlparse(url)
    if host_header and parsed.scheme == "https" and _is_ipv4_host(parsed.hostname or ""):
        headers["Host"] = host_header
    return headers


def _request_verify(url: str) -> bool:
    insecure = os.getenv("OPENCLAW_TLS_INSECURE", "0").strip() == "1"
    if insecure and url.startswith("https://"):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return False
    return True


def _is_stock_question(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return False
    if re.search(r"(\d{6}(?:\.(?:sz|sh))?)", q):
        return True
    stock_hints = (
        "股票",
        "个股",
        "代码",
        "仓位",
        "回测",
        "止损",
        "买点",
        "卖点",
        "大盘",
        "宁德时代",
        "上证",
        "深证",
    )
    return any(k in q for k in stock_hints)


def _has_stock_code(question: str) -> bool:
    q = (question or "").strip().lower()
    return bool(re.search(r"(\d{6}(?:\.(?:sz|sh))?)", q))


def _is_trade_execution_query(question: str) -> bool:
    q = (question or "").strip().lower()
    exec_hints = (
        "怎么看",
        "今日",
        "今天",
        "买点",
        "卖点",
        "仓位",
        "止损",
        "止盈",
        "回测",
        "建仓",
        "加仓",
        "减仓",
    )
    return any(k in q for k in exec_hints)


def _is_stock_concept_query(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return False
    concept_hints = (
        "哲学",
        "理念",
        "原则",
        "交易系统",
        "长期盈利",
        "概率优势",
        "纪律",
        "方法论",
    )
    return any(k in q for k in concept_hints)


def _candidate_urls(question: str, route_mode: str = "auto") -> list[str]:
    env_url = os.getenv("OPENCLAW_QA_REMOTE_URL", "").strip()
    expanded_general_urls = _expand_general_urls()
    urls: list[str] = []
    stock_like = _is_stock_question(question)
    hard_stock = _has_stock_code(question) or _is_trade_execution_query(question)
    if env_url:
        urls.append(env_url.rstrip("/"))
    urls.extend(expanded_general_urls)
    local_general_urls = [
        "http://127.0.0.1:3443/api/ai/chat",   # local general AI runtime
        "http://127.0.0.1:15101/api/ai/chat",  # tunneled AI gateway
    ]
    cloud_general_urls = [u for u in urls if _is_cloud_general_api_url(u)]
    # keep only non-cloud entries from pre-populated list (e.g., env_url might be local)
    urls = [u for u in urls if not _is_cloud_general_api_url(u)]
    general_urls = cloud_general_urls + local_general_urls + urls
    stock_urls = [
        "http://127.0.0.1:5101/chat",   # local stock qa api
        "http://127.0.0.1:15101/chat",  # stock agent gateway
        "http://127.0.0.1:15000/chat",  # ssh tunnel / legacy proxy
        "http://127.0.0.1:5000/chat",   # local flask fallback
    ]
    allow_non_stock_fallback = os.getenv("OPENCLAW_NON_STOCK_ALLOW_STOCK_FALLBACK", "0").strip() == "1"
    emergency_non_stock_fallback = os.getenv("OPENCLAW_NON_STOCK_EMERGENCY_FALLBACK", "1").strip() == "1"
    if route_mode == "general_only":
        # Strict general-only route: never include stock endpoints.
        urls.extend(cloud_general_urls)
        urls.extend([u for u in general_urls if u not in cloud_general_urls])
    elif hard_stock:
        urls.extend(stock_urls + general_urls)
    elif stock_like:
        # Stock conceptual questions should prefer cloud brain first.
        stock_concept_cloud_first = os.getenv("OPENCLAW_STOCK_CONCEPT_CLOUD_FIRST", "1").strip() == "1"
        if stock_concept_cloud_first and cloud_general_urls:
            urls.extend(general_urls + stock_urls)
        else:
            urls.extend(stock_urls + general_urls)
    else:
        # For non-stock questions, prefer only the general OpenClaw brain by default.
        urls.extend(cloud_general_urls)
        if allow_non_stock_fallback or emergency_non_stock_fallback:
            # Emergency local expert fallback should be quick and near-front.
            urls.extend(stock_urls)
        urls.extend([u for u in general_urls if u not in cloud_general_urls])
    # de-duplicate while preserving order
    ordered = list(dict.fromkeys(urls))
    if _cloud_brain_only_enabled():
        # Hard disable local gateways/rescues when cloud-only is required.
        return [u for u in ordered if _is_cloud_general_api_url(u)]
    return ordered


def _build_payload(url: str, question: str, session_id: str) -> dict:
    if url.endswith("/api/ai/chat"):
        return {
            "sessionId": session_id,
            "messages": [
                {"role": "system", "content": _master_system_prompt()},
                {"role": "user", "content": question},
            ],
        }
    return {"question": question, "session_id": session_id}


def _build_lite_payload(url: str, question: str, session_id: str) -> dict:
    if url.endswith("/api/ai/chat"):
        return {
            "sessionId": session_id,
            "messages": [
                {"role": "user", "content": question},
            ],
        }
    return {"question": question, "session_id": session_id}


def _fresh_retry_session_id(base_session_id: str) -> str:
    return f"{base_session_id}-r{int(time.time() * 1000)}"


def _final_local_rescue(question: str, session_id: str) -> Optional[dict]:
    rescue_urls = [
        "http://127.0.0.1:5101/chat",
        "http://127.0.0.1:5000/chat",
    ]
    rescue_timeout = max(3, int(os.getenv("OPENCLAW_FINAL_RESCUE_TIMEOUT_SEC", "12")))
    for url in rescue_urls:
        try:
            r = _SESSION.post(url, json={"question": question, "session_id": session_id}, timeout=rescue_timeout)
            r.raise_for_status()
            data = r.json() if r.content else {}
            if not isinstance(data, dict):
                continue
            answer = _extract_answer(data)
            if answer:
                return {
                    "answer": answer,
                    "mode": data.get("mode", "local_rescue"),
                    "sources": data.get("sources", []),
                }
        except Exception:
            continue
    return None


def _attempt_request(
    url: str,
    question: str,
    effective_question: str,
    sid: str,
    req_timeout: float,
    *,
    use_lite: bool = False,
    mode_label: str = "remote_bridge",
) -> Optional[dict]:
    """Single HTTP POST attempt. Returns result dict or None."""
    payload = (
        _build_lite_payload(url, effective_question, sid)
        if use_lite
        else _build_payload(url, effective_question, sid)
    )
    r = _SESSION.post(
        url,
        json=payload,
        headers=_request_headers(url),
        timeout=req_timeout,
        verify=_request_verify(url),
    )
    r.raise_for_status()
    data = r.json() if r.content else {}
    if not isinstance(data, dict):
        return None
    answer = _extract_answer(data)
    if not answer:
        return None
    return {
        "answer": answer,
        "mode": data.get("mode", mode_label),
        "sources": data.get("sources", []),
    }


def _retry_loop(
    url: str,
    question: str,
    effective_question: str,
    base_sid: str,
    *,
    max_attempts: int,
    cap_sec: float,
    started_at: float,
    total_budget_sec: float,
    use_lite: bool = True,
    mode_label: str = "remote_bridge",
    upgraded_question: Optional[str] = None,
) -> Optional[dict]:
    """Run up to max_attempts retries within remaining time budget."""
    q = upgraded_question or effective_question
    for _ in range(max_attempts):
        remaining = total_budget_sec - (time.monotonic() - started_at)
        if remaining <= 1.2:
            break
        try:
            retry_sid = _fresh_retry_session_id(base_sid)
            retry_timeout = min(float(cap_sec), max(1.0, remaining - 0.8))
            result = _attempt_request(
                url, question, q, retry_sid, retry_timeout,
                use_lite=use_lite, mode_label=mode_label,
            )
            if result:
                return result
        except Exception as exc:
            if os.getenv("OPENCLAW_BRIDGE_DEBUG", "0").strip() == "1":
                _LOG.warning("bridge retry failed: %s (%s)", url, type(exc).__name__)
    return None


def _resolve_request_timeout(
    url: str,
    per_request_cap_sec: float,
    remaining: float,
    stock_like_question: bool,
    stock_concept_question: bool,
) -> float:
    """Compute per-request timeout respecting all caps."""
    req_timeout = min(per_request_cap_sec, max(1.0, remaining))
    if url.startswith("http://127.0.0.1:"):
        req_timeout = min(req_timeout, _local_timeout_for_url(url, stock_like_question))
    if _is_cloud_general_api_url(url):
        cloud_cap_default = "28" if _is_ip_cloud_url(url) else "12"
        cloud_cap_sec = max(3, int(os.getenv("OPENCLAW_CLOUD_TIMEOUT_SEC", cloud_cap_default)))
        req_timeout = min(req_timeout, float(cloud_cap_sec))
    if stock_like_question and _is_cloud_general_api_url(url):
        stock_cloud_default = "32" if stock_concept_question else "5"
        stock_cloud_cap_sec = max(2, int(os.getenv("OPENCLAW_STOCK_CLOUD_TIMEOUT_SEC", stock_cloud_default)))
        req_timeout = min(req_timeout, float(stock_cloud_cap_sec))
    return req_timeout


def query_openclaw_remote(
    question: str,
    timeout: int = 15,
    session_id: Optional[str] = None,
    route_mode: str = "auto",
):
    _set_last_bridge_error()
    sid = (session_id or "").strip() or "stock-site"
    effective_question = _augment_question_with_local_context(question)
    stock_like_question = _is_stock_question(question)
    stock_concept_question = stock_like_question and _is_stock_concept_query(question)
    total_budget_sec = max(8, int(timeout))
    per_request_cap_sec = max(4, int(os.getenv("OPENCLAW_SINGLE_REQUEST_TIMEOUT_SEC", "20")))
    started_at = time.monotonic()
    candidates = _candidate_urls(question, route_mode=route_mode)
    if _cloud_brain_only_enabled() and not candidates:
        _set_last_bridge_error(stage="prepare", reason="no_candidate_urls")
        return None

    for idx, url in enumerate(candidates):
        remaining = total_budget_sec - (time.monotonic() - started_at)
        if remaining <= 0.8:
            break

        req_timeout = _resolve_request_timeout(
            url, per_request_cap_sec, remaining,
            stock_like_question, stock_concept_question,
        )
        is_cloud = _is_cloud_general_api_url(url)
        use_lite_first = (
            is_cloud
            and not stock_like_question
            and os.getenv("OPENCLAW_CLOUD_USE_LITE_FIRST", "1").strip() == "1"
        )

        # --- Primary attempt ---
        try:
            result = _attempt_request(
                url, question, effective_question, sid, req_timeout,
                use_lite=use_lite_first,
            )
            if result:
                if _is_low_quality_answer(question, result["answer"]):
                    # Quality upgrade retry on same endpoint.
                    upgraded = _retry_loop(
                        url, question, effective_question, sid,
                        max_attempts=1,
                        cap_sec=req_timeout,
                        started_at=started_at,
                        total_budget_sec=total_budget_sec,
                        use_lite=is_cloud,
                        mode_label="remote_bridge_quality_retry",
                        upgraded_question=_quality_upgrade_question(effective_question),
                    )
                    if upgraded:
                        return upgraded
                    continue
                return result
        except Exception as exc:
            _err = {"stage": "request", "url": url, "error_type": type(exc).__name__, "error": str(exc)}
            try:
                if isinstance(exc, requests.HTTPError) and getattr(exc, "response", None) is not None:
                    _err["status_code"] = int(exc.response.status_code)
                    _err["response_text"] = (exc.response.text or "")[:200]
            except Exception:
                pass
            _set_last_bridge_error(**_err)
            if os.getenv("OPENCLAW_BRIDGE_DEBUG", "0").strip() == "1":
                _LOG.warning("bridge url failed: %s (%s)", url, type(exc).__name__)

        # --- Retry strategies after primary failure ---
        if is_cloud and stock_concept_question:
            retry = _retry_loop(
                url, question, effective_question, sid,
                max_attempts=max(1, int(os.getenv("OPENCLAW_STOCK_CONCEPT_CLOUD_RETRY_ATTEMPTS", "3"))),
                cap_sec=max(4, int(os.getenv("OPENCLAW_STOCK_CONCEPT_CLOUD_RETRY_TIMEOUT_SEC", "12"))),
                started_at=started_at,
                total_budget_sec=total_budget_sec,
            )
            if retry:
                return retry

        if is_cloud and not stock_like_question:
            retry = _retry_loop(
                url, question, effective_question, sid,
                max_attempts=max(1, int(os.getenv("OPENCLAW_CLOUD_RETRY_ATTEMPTS", "2"))),
                cap_sec=max(2, int(os.getenv("OPENCLAW_CLOUD_RETRY_TIMEOUT_SEC", "6"))),
                started_at=started_at,
                total_budget_sec=total_budget_sec,
            )
            if retry:
                return retry

        if idx == 0 and not stock_like_question:
            retry = _retry_loop(
                url, question, effective_question, sid,
                max_attempts=1,
                cap_sec=max(2, int(os.getenv("OPENCLAW_FIRST_RETRY_TIMEOUT_SEC", "6"))),
                started_at=started_at,
                total_budget_sec=total_budget_sec,
                use_lite=False,
            )
            if retry:
                return retry

    if _cloud_brain_only_enabled():
        _set_last_bridge_error(stage="final", reason="cloud_only_no_success")
        return None
    rescue = _final_local_rescue(question, sid)
    if rescue is None:
        _set_last_bridge_error(stage="final", reason="all_routes_failed")
    return rescue
