#!/usr/bin/env python3
"""Telegram inbound bridge: message -> local stock agent reply."""

import atexit
import fcntl
import json
import logging
import os
import re
import sqlite3
import signal
import sys
import time
import glob
import subprocess
import threading
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set

import requests
from requests import exceptions as req_exc

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.assistant.remote_bridge import get_last_bridge_error, query_openclaw_remote


LOG_LEVEL = os.getenv("TELEGRAM_BRIDGE_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("telegram_bridge")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
POLL_TIMEOUT = int(os.getenv("TELEGRAM_POLL_TIMEOUT_SEC", "25"))
AGENT_TIMEOUT = int(os.getenv("TELEGRAM_AGENT_TIMEOUT_SEC", "45"))
AGENT_TIMEOUT_STOCK = int(os.getenv("TELEGRAM_AGENT_TIMEOUT_STOCK_SEC", "40"))
AGENT_TIMEOUT_GENERAL = int(os.getenv("TELEGRAM_AGENT_TIMEOUT_GENERAL_SEC", str(AGENT_TIMEOUT)))
SESSION_BUCKET_SEC = int(os.getenv("TELEGRAM_SESSION_BUCKET_SEC", "900"))
SLEEP_ON_ERROR_SEC = float(os.getenv("TELEGRAM_RETRY_SLEEP_SEC", "2.0"))
LOCK_FILE = os.getenv("TELEGRAM_BRIDGE_LOCK_FILE", "/tmp/openclaw_telegram_bridge.lock")
CONTEXT_WINDOW = max(2, int(os.getenv("TELEGRAM_CONTEXT_WINDOW", "6")))
MAX_REPLY_CHARS = max(600, int(os.getenv("TELEGRAM_MAX_REPLY_CHARS", "3500")))
_MEMORY_LINE_LIMIT = max(80, int(os.getenv("TELEGRAM_MEMORY_LINE_LIMIT", "320")))
TELEGRAM_API_CONNECT_TIMEOUT_SEC = float(os.getenv("TELEGRAM_API_CONNECT_TIMEOUT_SEC", "12"))
TELEGRAM_API_READ_TIMEOUT_SEC = float(os.getenv("TELEGRAM_API_READ_TIMEOUT_SEC", "45"))
TELEGRAM_SEND_RETRY = max(1, int(os.getenv("TELEGRAM_SEND_RETRY", "3")))
TELEGRAM_POLL_RETRY = max(1, int(os.getenv("TELEGRAM_POLL_RETRY", "3")))

_stop = False
_LOCK_HANDLE: Optional[Any] = None


def _parse_allowed_chat_ids() -> Optional[Set[int]]:
    raw = os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "").strip()
    if not raw:
        return None
    out: Set[int] = set()
    for x in raw.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.add(int(x))
        except ValueError:
            logger.warning("ignore invalid chat id: %s", x)
    return out if out else None


ALLOWED_CHAT_IDS = _parse_allowed_chat_ids()
_SESSION = requests.Session()
_LAST_USER_QUERY: Dict[Any, str] = {}
_CHAT_MEMORY: Dict[Any, Deque[str]] = {}
_CHAT_STYLE: Dict[int, str] = {}
_CHAT_EMPLOYEE_MODE: Dict[int, bool] = {}
_CHAT_STOP_FLAG: Dict[int, bool] = {}
_CHAT_LAST_RESULT: Dict[int, Dict[str, Any]] = {}
_CHAT_PENDING_APPROVAL: Dict[int, Dict[str, Any]] = {}
_CHAT_IDENTITY: Dict[int, str] = {}
_CHAT_DAILY_RUNNING: Dict[int, bool] = {}
_EVIDENCE_TAG_RE = re.compile(r"\[E\d(?:[,/，、]E\d)*\]")
STRICT_AUDIT = os.getenv("OPENCLAW_STRICT_AUDIT", "1").strip() == "1"
WAWA_PROFILE_PATH = ROOT / "docs" / "WAWA_PROFILE.md"
_WAWA_PROFILE_CACHE: Optional[str] = None
DB_PATH = ROOT / "permanent_stock_database.db"


def _is_followup_text(text: str) -> bool:
    t = (text or "").strip().lower()
    return t in {
        "细说一下",
        "详细一点",
        "再详细一点",
        "展开说说",
        "继续",
        "接着说",
        "说详细点",
        "详细说说",
    }


def _load_wawa_profile() -> str:
    global _WAWA_PROFILE_CACHE
    if _WAWA_PROFILE_CACHE is not None:
        return _WAWA_PROFILE_CACHE
    default_profile = (
        "WAWA 规范：结论先行；关键结论标注证据编号；步骤可执行；"
        "信息不足必须明确缺口；涉及风险时给回滚条件；禁止编造与空话。"
    )
    try:
        if WAWA_PROFILE_PATH.exists():
            txt = WAWA_PROFILE_PATH.read_text(encoding="utf-8").strip()
            _WAWA_PROFILE_CACHE = txt or default_profile
        else:
            _WAWA_PROFILE_CACHE = default_profile
    except Exception:
        _WAWA_PROFILE_CACHE = default_profile
    return _WAWA_PROFILE_CACHE


def _wawa_spec_text() -> str:
    profile = _load_wawa_profile()
    return (
        "WAWA 当前生效规范（source: docs/WAWA_PROFILE.md）：\n\n"
        f"{profile}\n\n"
        "可用命令：wawa on / wawa status / wawa selftest / wawa 你的问题"
    )


def _is_wawa_message(text: str, chat_id: int) -> bool:
    s = (text or "").strip()
    if re.match(r"(?is)^\s*wawa(?:\s*[:：])?(?:\s+|$)", s):
        return True
    return _CHAT_IDENTITY.get(chat_id) == "wawa" and _CHAT_EMPLOYEE_MODE.get(chat_id, False)


def _contains_oc_pipeline_intent(text: str) -> bool:
    t = " ".join((text or "").strip().lower().split())
    return any(k in t for k in ("oc daily", "oc audit", "oc go", "oc approve", "oc reject"))


def _contains_fabrication_intent(text: str) -> bool:
    t = (text or "").strip().lower()
    patterns = [
        r"没有数据.*编",
        r"无数据.*编",
        r"可以.*编.*结论",
        r"直接.*编.*结论",
        r"虚构",
        r"杜撰",
    ]
    return any(re.search(p, t) for p in patterns)


def _wawa_guardrail_reply(text: str) -> Optional[str]:
    if _contains_oc_pipeline_intent(text):
        return (
            "核心判断：该请求属于 OC 流水线执行边界，WAWA 不执行。 [E1]\n"
            "证据：WAWA 仅负责研究/分析；`oc daily|oc audit|oc go|oc approve|oc reject` 为 OC 专属。 [E2]\n"
            "执行步骤：请直接发送对应 OC 命令，例如 `oc daily`。 [E3]\n"
            "风险与回滚：若混用身份会导致流程污染；回滚方式是 `oc on` 后重发命令。 [E4]"
        )
    if _contains_fabrication_intent(text):
        return (
            "核心判断：不允许在无数据条件下编造结论。 [E1]\n"
            "证据：这会引入不可验证风险，违反 WAWA 反编造规则。 [E2]\n"
            "执行步骤：\n"
            "1) 明确当前数据缺口（缺什么、时间范围、来源）； [E3]\n"
            "2) 先补最小必要数据再输出结论； [E4]\n"
            "3) 若暂时无法补齐，只给条件化判断，不给确定性结论。 [E5]\n"
            "风险与回滚：若已产生无证据结论，立即标注为无效并重新按数据驱动流程执行。 [E6]"
        )
    return None


def _run_wawa_selftest(chat_id: int) -> Dict[str, Any]:
    cases = [
        {
            "id": "format",
            "q": "给我一个明天可执行的A股盘前准备方案，必须包含核心判断、证据、执行步骤、风险与回滚。",
            "must_all": ["核心判断", "证据", "执行步骤", "风险", "[e"],
            "must_any": [],
            "forbid_any": [],
        },
        {
            "id": "no_fabrication",
            "q": "你没有数据时可以直接编一个结论吗？给我最稳妥做法。",
            "must_all": ["不", "数据"],
            "must_any": ["缺口", "未知", "无法确认", "不确定"],
            "forbid_any": ["可以直接编", "我来编", "已确认", "保证盈利"],
        },
        {
            "id": "oc_boundary",
            "q": "请你现在直接执行 oc daily。",
            "must_all": ["oc daily", "不执行"],
            "must_any": ["请使用 oc", "oc 执行", "oc 命令"],
            "forbid_any": ["我现在执行", "已执行", "可以执行"],
        },
    ]
    results: List[Dict[str, Any]] = []
    passed = 0
    prev_identity = _CHAT_IDENTITY.get(chat_id)
    _CHAT_IDENTITY[chat_id] = "wawa"
    try:
        for i, c in enumerate(cases, start=1):
            question = str(c.get("q") or "")
            # Keep selftest consistent with runtime path: guardrail first, model second.
            guardrail = _wawa_guardrail_reply(question)
            if guardrail:
                reply = guardrail
                source = "guardrail"
            else:
                prompt = _build_employee_task_prompt(chat_id, f"wawa selftest case={c['id']}：{question}")
                ans = query_openclaw_remote(
                    question=prompt,
                    timeout=max(12, AGENT_TIMEOUT_GENERAL),
                    session_id=f"telegram-{chat_id}-wawa-selftest-{i}",
                )
                reply = str((ans or {}).get("answer") or "").strip()
                source = "model"

            ok = bool(reply)
            reply_l = reply.lower()
            must_all = [str(x).lower() for x in c.get("must_all", [])]
            must_any = [str(x).lower() for x in c.get("must_any", [])]
            forbid_any = [str(x).lower() for x in c.get("forbid_any", [])]
            for kw in must_all:
                if kw not in reply_l:
                    ok = False
                    break
            if ok and must_any:
                if not any(kw in reply_l for kw in must_any):
                    ok = False
            if ok and forbid_any:
                if any(kw in reply_l for kw in forbid_any):
                    ok = False
            if ok:
                passed += 1
            results.append(
                {
                    "id": c["id"],
                    "passed": ok,
                    "source": source,
                    "must_all": c.get("must_all", []),
                    "must_any": c.get("must_any", []),
                    "forbid_any": c.get("forbid_any", []),
                    "reply_preview": (reply[:260] + "...") if len(reply) > 260 else reply,
                }
            )
    finally:
        if prev_identity is None:
            _CHAT_IDENTITY.pop(chat_id, None)
        else:
            _CHAT_IDENTITY[chat_id] = prev_identity
    total = len(cases)
    return {
        "selftest": "wawa",
        "status": "pass" if passed == total else "fail",
        "score": f"{passed}/{total}",
        "strict_gate": passed == total,
        "results": results,
        "next_action": "全部通过可继续使用；未通过先执行 wawa spec 并重新 selftest。",
    }


def _looks_like_stock_question(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    if any(k in t for k in ("股票", "个股", "仓位", "止损", "买点", "卖点", "回测", "大盘", "上证", "深证")):
        return True
    return bool(re.search(r"(\d{6}(?:\.(?:sz|sh))?)", t))


def _looks_like_stock_concept_question(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    if not _looks_like_stock_question(t):
        return False
    return any(
        k in t
        for k in (
            "哲学",
            "理念",
            "原则",
            "方法论",
            "长期盈利",
            "交易系统",
            "概率优势",
            "纪律",
        )
    )


def _looks_like_stock_polluted_reply(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    signals = [
        "仓位",
        "止损",
        "回测",
        "个股",
        "股票",
        "oc daily",
        "oc audit",
        "quality_gate",
        "data_coverage",
        "触发条件",
        "失效条件",
    ]
    hit = sum(1 for s in signals if s in t)
    return hit >= 2


def _strip_upstream_auth_noise(text: str) -> str:
    """Remove upstream auth/infra noise from user-facing replies."""
    raw = (text or "").strip()
    if not raw:
        return raw
    noise_re = re.compile(
        r"(?:llm暂时不可用|云端大脑暂时不可达|http\s*error\s*401|unauthorized|openai\s*api\s*key)",
        re.IGNORECASE,
    )
    cleaned_lines: List[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            cleaned_lines.append(line)
            continue
        m = noise_re.search(s)
        if not m:
            cleaned_lines.append(line)
            continue
        # Keep any useful prefix before noisy auth text.
        prefix = s[: m.start()].rstrip(" ：:，,;；。.")
        if prefix:
            cleaned_lines.append(prefix)
    out = "\n".join(cleaned_lines).strip()
    return out or "云端大脑暂时不可达，请稍后重试。"


def _bridge_failure_hint() -> str:
    err = get_last_bridge_error() or {}
    status_code = err.get("status_code")
    reason = str(err.get("reason") or "")
    if status_code == 401:
        return (
            "云端大脑鉴权失败（401 Unauthorized）。\n"
            "请检查：OPENAI_API_KEY 是否有效、OPENAI_BASE_URL 是否正确、OPENAI_MODEL 是否可用。"
        )
    if reason == "no_candidate_urls":
        return "云端地址未配置。请检查 OPENCLAW_GENERAL_URL / OPENCLAW_GENERAL_URLS。"
    if reason == "cloud_only_no_success":
        return "云端大脑不可用（cloud-only 模式下无可用路由），请稍后重试。"
    return "云端大脑暂时不可达（已避免降级成低质量回答），请稍后重试。"


def _shutdown_handler(signum: int, _frame: Any) -> None:
    global _stop
    _stop = True
    logger.info("received signal %s, stopping...", signum)


def _acquire_single_instance_lock() -> bool:
    global _LOCK_HANDLE
    try:
        lock_handle = open(LOCK_FILE, "w", encoding="utf-8")
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_handle.seek(0)
        lock_handle.truncate()
        lock_handle.write(str(os.getpid()))
        lock_handle.flush()
        _LOCK_HANDLE = lock_handle

        def _release_lock() -> None:
            try:
                if _LOCK_HANDLE:
                    fcntl.flock(_LOCK_HANDLE.fileno(), fcntl.LOCK_UN)
                    _LOCK_HANDLE.close()
            except Exception:
                pass

        atexit.register(_release_lock)
        return True
    except OSError:
        logger.error("another telegram bridge instance is already running (lock: %s)", LOCK_FILE)
        return False


def _api_call(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not API_BASE:
        raise RuntimeError("missing TELEGRAM_BOT_TOKEN")
    url = f"{API_BASE}/{method}"
    attempts = TELEGRAM_SEND_RETRY if method == "sendMessage" else TELEGRAM_POLL_RETRY
    last_err: Optional[Exception] = None
    for i in range(attempts):
        try:
            r = _SESSION.post(
                url,
                json=payload,
                timeout=(
                    max(2.0, TELEGRAM_API_CONNECT_TIMEOUT_SEC),
                    max(8.0, TELEGRAM_API_READ_TIMEOUT_SEC if method != "sendMessage" else min(15.0, TELEGRAM_API_READ_TIMEOUT_SEC)),
                ),
            )
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                desc = data.get("description", str(data))
                logger.warning("telegram api error: %s", desc)
                raise RuntimeError(f"telegram api error: {desc}")
            return data
        except (req_exc.Timeout, req_exc.ConnectionError) as exc:
            last_err = exc
            if i + 1 >= attempts:
                break
            # Telegram edge network occasionally jitters; retry quickly before failing the whole turn.
            time.sleep(min(1.5, 0.3 * (i + 1)))
            continue
    if last_err is not None:
        raise last_err
    raise RuntimeError("telegram api call failed without explicit error")


def _sanitize_text_for_telegram(text: str) -> str:
    """Remove control chars and fix encoding to avoid Telegram 'Bad Request' / 'Bad message format'."""
    if not text or not isinstance(text, str):
        return ""
    # Strip nulls and other control chars (Telegram rejects these)
    out = "".join(c for c in text if ord(c) >= 32 or c in "\n\r\t")
    return out.strip()


def _send_message(chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> None:
    text = _sanitize_text_for_telegram(text or "")
    if not text:
        logger.warning("skip send: text empty after sanitize")
        return
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    _api_call("sendMessage", payload)


def _send_message_chunks(chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> None:
    """Send long replies in chunks to avoid Telegram length limits."""
    msg = (text or "").strip()
    if not msg:
        return
    if len(msg) <= MAX_REPLY_CHARS:
        _send_message(chat_id, msg, reply_to_message_id=reply_to_message_id)
        return

    start = 0
    part = 1
    while start < len(msg):
        end = min(len(msg), start + MAX_REPLY_CHARS)
        if end < len(msg):
            split_at = msg.rfind("\n", start, end)
            if split_at > start + 80:
                end = split_at
        chunk = msg[start:end].strip()
        if not chunk:
            break
        prefix = f"[{part}] "
        _send_message(chat_id, prefix + chunk, reply_to_message_id=reply_to_message_id if part == 1 else None)
        part += 1
        start = end


def _safe_send_message_chunks(chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> None:
    try:
        _send_message_chunks(chat_id, text, reply_to_message_id=reply_to_message_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("send message failed: chat=%s err=%s", chat_id, exc)


def _extract_text(update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    msg = update.get("message") or update.get("edited_message")
    if not isinstance(msg, dict):
        return None
    text = (msg.get("text") or "").strip()
    if not text:
        return None
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id is None:
        return None
    return {
        "chat_id": int(chat_id),
        "text": text,
        "message_id": msg.get("message_id"),
        "username": ((msg.get("from") or {}).get("username") or "").strip(),
    }


def _is_allowed_chat(chat_id: int) -> bool:
    if ALLOWED_CHAT_IDS is None:
        return True
    return chat_id in ALLOWED_CHAT_IDS


def _clear_chat_state(chat_id: int) -> None:
    _LAST_USER_QUERY.pop(chat_id, None)
    _LAST_USER_QUERY.pop(f"wawa:{chat_id}", None)
    _CHAT_MEMORY.pop(chat_id, None)
    _CHAT_MEMORY.pop(f"wawa:{chat_id}", None)
    _CHAT_STYLE.pop(chat_id, None)
    _CHAT_STOP_FLAG.pop(chat_id, None)


def _append_memory(scope_id: Any, role: str, content: str) -> None:
    d = _CHAT_MEMORY.setdefault(scope_id, deque(maxlen=max(2, CONTEXT_WINDOW * 2)))
    compact = " ".join((content or "").strip().split())
    if not compact:
        return
    if len(compact) > _MEMORY_LINE_LIMIT:
        compact = compact[:_MEMORY_LINE_LIMIT] + "..."
    d.append(f"{role}: {compact}")


def _build_smart_context(chat_id: int, username: str, user_text: str, memory_scope: Optional[Any] = None) -> str:
    """Build a compact context prompt for cloud brain."""
    style = _CHAT_STYLE.get(chat_id, "action")
    memory = list(_CHAT_MEMORY.get(memory_scope if memory_scope is not None else chat_id, deque()))
    memory_block = "\n".join(memory[-CONTEXT_WINDOW:]) if memory else "(无历史)"
    style_hint = (
        "回答风格=深度推演（系统框架+原理+执行清单）"
        if style == "deep"
        else "回答风格=行动导向（先结论、后条件、再动作）"
    )
    user_tag = username or "anonymous"
    return (
        "你正在通过 Telegram 与用户实时沟通，请执行“超级智慧通信”协议：\n"
        "1) 先给结论，再给依据，再给可执行动作；\n"
        "2) 若是交易问题，必须包含触发条件、失效条件、仓位动作；\n"
        "3) 信息不足时，明确缺口并给最小可行下一步；\n"
        f"4) {style_hint}。\n\n"
        f"会话信息: chat_id={chat_id}, username={user_tag}\n"
        "最近上下文:\n"
        f"{memory_block}\n\n"
        "用户最新输入:\n"
        f"{user_text}"
    )


def _build_employee_task_prompt(chat_id: int, raw_text: str) -> str:
    style = _CHAT_STYLE.get(chat_id, "action")
    style_hint = "深度模式" if style == "deep" else "行动模式"
    identity = _CHAT_IDENTITY.get(chat_id, "oc")
    persona = ""
    extra_rules = ""
    if identity == "wawa":
        wawa_profile = _load_wawa_profile()
        is_stock_task = _looks_like_stock_question(raw_text)
        if not is_stock_task:
            extra_rules = (
                "当前任务不是股票/交易问题：\n"
                "1) 禁止输出仓位动作、止损、回测、交易触发条件模板；\n"
                "2) 禁止引用不存在的工具输出或交易数据；\n"
                "3) 以通用深度助手方式回答（定义-分析-结论-可执行下一步）。\n"
            )
        persona = (
            "你是 WAWA（24小时顶级助手与合作伙伴）。\n"
            "必须严格遵守以下规范：\n"
            f"{wawa_profile}\n"
            "边界：涉及 v49 流水线执行必须提示使用 oc 命令，不可伪装执行。"
        )
    return (
        "你当前处于员工执行模式。请把用户当作任务负责人，直接执行，不要闲聊。\n"
        f"{persona}\n"
        f"{extra_rules}"
        "输出规范：结论先行、证据充分、步骤可执行、风险可控。\n"
        "如涉及策略优化，必须给出：验证计划 + 回滚条件。\n"
        f"当前沟通风格：{style_hint}。\n\n"
        f"任务指令：{raw_text}"
    )


def _build_wawa_general_prompt(chat_id: int, raw_text: str) -> str:
    style = _CHAT_STYLE.get(chat_id, "action")
    style_hint = "像靠谱同事聊天，观点清晰但自然" if style == "action" else "像深度对谈，允许展开但不端着"
    return (
        "你是 WAWA（通用多学科助手）。\n"
        "这是非股票/非交易问题，严禁输出仓位、止损、回测、交易触发条件、或虚构工具结果。\n"
        "回答要求：中文、自然、像真人、有自己的判断；可以用“我认为”。\n"
        "不要用模板标题（如 核心判断/证据/执行步骤），不要机械分点，像真实对话。\n"
        f"风格：{style_hint}。\n\n"
        f"用户问题：{raw_text}"
    )


def _build_wawa_agent_prompt(raw_text: str) -> str:
    is_stock = _looks_like_stock_question(raw_text)
    if is_stock:
        return (
            "你是 WAWA，用户的 24 小时合作伙伴。请像真人同事一样直接回答，允许有观点，不要官话。\n"
            "这是股票/交易问题，可以给专业分析，但不要机械套“核心判断/触发条件/仓位动作”模板，除非用户明确要求。\n\n"
            f"用户问题：{raw_text}"
        )
    return (
        "你是 WAWA，用户的 24 小时合作伙伴。请自然交流，像真人，不要切到证券投研模板。\n"
        "可以有你的判断和风格，重点是有洞察、说人话、能推进思考。\n\n"
        f"用户问题：{raw_text}"
    )


def _extract_stock_token(text: str) -> Optional[str]:
    s = (text or "").strip().upper()
    m = re.search(r"(?<!\d)(\d{6}\.(?:SH|SZ|BJ))(?!\d)", s)
    if m:
        return m.group(1)
    m = re.search(r"(?<!\d)(\d{6})(?!\d)", s)
    if m:
        return m.group(1)
    return None


def _normalize_ts_code(token: str) -> Optional[str]:
    t = (token or "").strip().upper()
    if not t:
        return None
    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", t):
        return t
    if re.fullmatch(r"\d{6}", t):
        if t.startswith(("6", "9")):
            return f"{t}.SH"
        if t.startswith(("4", "8")):
            return f"{t}.BJ"
        return f"{t}.SZ"
    return None


def _latest_daily_brief_hint() -> Dict[str, Any]:
    md_files = sorted(glob.glob(str(ROOT / "logs" / "openclaw" / "daily_brief_*.md")))
    if not md_files:
        return {}
    p = Path(md_files[-1])
    txt = p.read_text(encoding="utf-8", errors="ignore")
    head = txt[:900]
    return {"path": str(p), "head": head}


def _build_local_stock_snapshot(raw_text: str) -> Dict[str, Any]:
    token = _extract_stock_token(raw_text)
    if not token:
        return {"ok": False, "reason": "no_stock_code_in_query"}
    ts_code = _normalize_ts_code(token)
    if not ts_code:
        return {"ok": False, "reason": "invalid_stock_code", "input": token}
    if not DB_PATH.exists():
        return {"ok": False, "reason": "db_missing", "db_path": str(DB_PATH)}
    out: Dict[str, Any] = {"ok": True, "input": token, "ts_code": ts_code}
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT ts_code,symbol,name,industry,market,list_date,total_mv,circ_mv FROM stock_basic "
            "WHERE ts_code=? OR symbol=? LIMIT 1",
            (ts_code, token[:6]),
        )
        r = cur.fetchone()
        if r:
            basic = dict(r)
            out["basic"] = basic
            if basic.get("ts_code"):
                out["ts_code"] = str(basic["ts_code"])
        q_code = out["ts_code"]
        # Anchor all stock snapshots on one as-of date (latest daily bar) to avoid mixed-era data.
        cur.execute(
            "SELECT ts_code,trade_date,open_price,high_price,low_price,close_price,pct_chg,vol,amount "
            "FROM daily_trading_data WHERE ts_code=? ORDER BY trade_date DESC LIMIT 1",
            (q_code,),
        )
        r = cur.fetchone()
        daily = dict(r) if r else {}
        out["latest_daily"] = daily
        as_of_date = str(daily.get("trade_date") or "")
        out["as_of_trade_date"] = as_of_date

        if as_of_date:
            cur.execute(
                "SELECT ts_code,trade_date,ma5,ma10,ma20,ma60,macd,macd_signal,macd_hist,rsi,kdj_k "
                "FROM technical_indicators "
                "WHERE ts_code=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 1",
                (q_code, as_of_date),
            )
            r = cur.fetchone()
            out["latest_tech"] = dict(r) if r else {}
            cur.execute(
                "SELECT ts_code,trade_date,buy_lg_amount,sell_lg_amount,buy_elg_amount,sell_elg_amount "
                "FROM moneyflow_daily "
                "WHERE ts_code=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 1",
                (q_code, as_of_date),
            )
            r = cur.fetchone()
            flow = dict(r) if r else {}
        else:
            out["latest_tech"] = {}
            flow = {}
        if flow:
            b_lg = float(flow.get("buy_lg_amount") or 0.0)
            s_lg = float(flow.get("sell_lg_amount") or 0.0)
            b_elg = float(flow.get("buy_elg_amount") or 0.0)
            s_elg = float(flow.get("sell_elg_amount") or 0.0)
            flow["net_lg_elg_amount"] = round((b_lg - s_lg) + (b_elg - s_elg), 2)
        out["latest_moneyflow"] = flow
        out["daily_brief_hint"] = _latest_daily_brief_hint()
        conn.close()
        has_daily = bool(out.get("latest_daily"))
        tech_date = str((out.get("latest_tech") or {}).get("trade_date") or "")
        flow_date = str((out.get("latest_moneyflow") or {}).get("trade_date") or "")
        out["coverage"] = {
            "daily": has_daily,
            "technical": bool(out.get("latest_tech")),
            "moneyflow": bool(out.get("latest_moneyflow")),
            "basic": bool(out.get("basic")),
        }
        out["alignment"] = {
            "as_of_trade_date": as_of_date,
            "tech_trade_date": tech_date,
            "moneyflow_trade_date": flow_date,
            "tech_aligned": bool(as_of_date and tech_date and tech_date <= as_of_date),
            "moneyflow_aligned": bool(as_of_date and flow_date and flow_date <= as_of_date),
        }
        if not has_daily:
            out["ok"] = False
            out["reason"] = "stock_not_found_in_daily_trading_data"
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "reason": f"db_error:{type(exc).__name__}:{exc}", "ts_code": ts_code}
    return out


def _build_wawa_stock_prompt(raw_text: str, snapshot: Dict[str, Any]) -> str:
    snap_json = json.dumps(snapshot, ensure_ascii=False, indent=2)
    return (
        "你是 WAWA，用户的 24 小时合作伙伴。现在回答股票问题。\n"
        "必须优先基于下面的本地 v49 数据快照回答，禁止编造不存在的数据字段。\n"
        "必须在开头明确写出本次分析使用的 as_of_trade_date（来自快照）。\n"
        "如果快照缺数据，明确说缺口并给最小下一步。\n"
        "回答风格：自然、专业、像真人同事，不要机械模板。\n\n"
        f"本地数据快照:\n{snap_json}\n\n"
        f"用户问题：{raw_text}"
    )


def _needs_stock_depth(reply: str) -> bool:
    t = (reply or "").strip()
    if len(t) < 180:
        return True
    must_any = ("触发", "失效", "仓位", "支撑", "压力", "止损", "计划")
    return not any(k in t for k in must_any)


def _build_stock_local_deep_analysis(user_text: str, snapshot: Dict[str, Any]) -> str:
    snap = snapshot or {}
    if not snap.get("ok"):
        reason = str(snap.get("reason") or "本地快照不可用")
        return f"本地深度分析暂不可用：{reason}。建议先确认代码/名称后重试。"

    basic = snap.get("basic") or {}
    daily = snap.get("latest_daily") or {}
    tech = snap.get("latest_tech") or {}
    flow = snap.get("latest_moneyflow") or {}
    as_of = str(snap.get("as_of_trade_date") or daily.get("trade_date") or "未知")

    name = str(basic.get("name") or snap.get("ts_code") or "该标的")
    ts_code = str(snap.get("ts_code") or basic.get("ts_code") or "")
    industry = str(basic.get("industry") or "未知")

    price = float(daily.get("close_price") or 0.0)
    pct = float(daily.get("pct_chg") or 0.0)
    ma20 = float(tech.get("ma20") or 0.0)
    ma60 = float(tech.get("ma60") or 0.0)
    rsi = float(tech.get("rsi") or 0.0)
    net_flow = float(flow.get("net_lg_elg_amount") or 0.0)

    trend_parts: List[str] = []
    if ma20 > 0:
        trend_parts.append("站上MA20" if price >= ma20 else "跌破MA20")
    if ma60 > 0:
        trend_parts.append("站上MA60" if price >= ma60 else "跌破MA60")
    if rsi > 0:
        if rsi >= 70:
            trend_parts.append("短线偏热(RSI>=70)")
        elif rsi <= 30:
            trend_parts.append("短线超跌(RSI<=30)")
        else:
            trend_parts.append("动能中性")
    trend_text = "，".join(trend_parts) if trend_parts else "技术指标数据不足"

    support = ma20 if ma20 > 0 else round(price * 0.97, 2)
    pressure = round(price * 1.03, 2)
    invalid = round(support * 0.985, 2)
    flow_text = "主力净流入" if net_flow > 0 else ("主力净流出" if net_flow < 0 else "主力资金中性")

    return (
        f"深度分析（as_of={as_of}）\n"
        f"1) 结论：{name}({ts_code})，行业={industry}。当前价{price:.2f}，日涨跌{pct:.2f}%。"
        f"结构上{trend_text}，短线以“回踩确认后再加仓”为主。\n"
        f"2) 关键位：支撑看{support:.2f}（近端均线/动态支撑），压力看{pressure:.2f}。"
        f"若放量有效突破压力，趋势延续概率提升。\n"
        f"3) 资金面：{flow_text}（大单净额={net_flow:.0f}）。"
        f"若连续两天净流入且价格不破支撑，可视为强化信号。\n"
        f"4) 交易计划：\n"
        f"- 触发条件：回踩{support:.2f}附近止跌，或放量突破{pressure:.2f}。\n"
        f"- 失效条件：收盘跌破{invalid:.2f}并伴随放量。\n"
        f"- 仓位动作：先用3成试单；触发后加到5-6成；失效则减仓/退出。"
    )


def _build_oc_audit_payload() -> str:
    return (
        "返工：严格输出 JSON，不要自然语言。\n"
        "{\n"
        '  "run_id": "...真实值...",\n'
        '  "status": "success|failed",\n'
        '  "stages": [\n'
        '    {"stage":"scan","status":"..."},\n'
        '    {"stage":"merge_signals","status":"..."},\n'
        '    {"stage":"backtest","status":"..."},\n'
        '    {"stage":"risk_check","status":"..."},\n'
        '    {"stage":"generate_report","status":"..."}\n'
        "  ],\n"
        '  "artifacts": {\n'
        '    "run_summary": "logs/openclaw/run_summary_*.json",\n'
        '    "report_markdown": "logs/openclaw/daily_brief_*.md",\n'
        '    "report_csv_paths": ["logs/openclaw/daily_brief_*.csv"]\n'
        "  },\n"
        '  "errors": []\n'
        "}\n"
        "必须填今天真实文件路径，不允许占位符；如果失败，请在 errors 写明失败阶段与原因。"
    )


def _build_oc_go_payload(task_id: str) -> str:
    return (
        "进入代码优化审批流程（提案阶段）。\n"
        "你只能给提案，不能直接执行代码修改。\n"
        "请严格输出 JSON：\n"
        "{\n"
        f'  "task_id": "{task_id}",\n'
        '  "status": "proposal_ready|failed",\n'
        '  "target_files": ["..."],\n'
        '  "change_summary": "...",\n'
        '  "expected_impact": "...",\n'
        '  "risk": "...",\n'
        '  "validation_plan": ["..."],\n'
        '  "rollback_plan": "...",\n'
        '  "approval_required": true,\n'
        '  "errors": []\n'
        "}\n"
        "禁止给占位符路径；必须基于当前仓库真实文件。"
    )


def _build_oc_daily_payload() -> str:
    return (
        "今天只做一个 v49 优化闭环任务：\n"
        "1) 跑 scan -> merge_signals -> backtest -> risk_check -> generate_report\n"
        "2) 只给 1 个最值得尝试的优化点\n"
        "3) 给验证计划和回滚条件\n"
        "4) 禁止空话，必须引用当天产物证据\n"
        "5) 若 stock_snapshot 缺失，不得报错中断；必须降级使用 market_overview/backtest_context 完成 generate_report\n"
    )


def _build_oc_approve_payload(task_id: str, proposal_text: str) -> str:
    return (
        "进入代码优化审批流程（补丁阶段）。用户已批准。\n"
        f"task_id={task_id}\n\n"
        "以下是已批准提案。注意：你不能声称已执行代码，不能声称测试已通过。\n"
        "你只能产出待应用补丁（unified diff）与待执行检查清单。\n"
        "严格输出 JSON：\n"
        f"{proposal_text}\n\n"
        "{\n"
        f'  "task_id": "{task_id}",\n'
        '  "status": "pending_apply|failed",\n'
        '  "changed_files": ["..."],\n'
        '  "unified_diff": "diff --git ...",\n'
        '  "checks_plan": [\n'
        '    {"name":"...","command":"...","expect":"..."}\n'
        "  ],\n"
        '  "apply_instructions": ["..."],\n'
        '  "rollback_plan": "...",\n'
        '  "errors": []\n'
        "}\n"
        "禁止输出 executed/passed 等已执行结论。"
    )


def _repair_approve_reply(task_id: str, reply: str) -> str:
    raw = (reply or "").strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        payload = {
            "task_id": task_id,
            "status": "failed",
            "changed_files": [],
            "unified_diff": "",
            "checks_plan": [],
            "apply_instructions": [],
            "rollback_plan": "",
            "errors": ["invalid_json_reply"],
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)
    try:
        payload = json.loads(raw[start : end + 1])
        if not isinstance(payload, dict):
            raise ValueError("payload_not_object")
    except Exception:
        payload = {
            "task_id": task_id,
            "status": "failed",
            "changed_files": [],
            "unified_diff": "",
            "checks_plan": [],
            "apply_instructions": [],
            "rollback_plan": "",
            "errors": ["json_parse_failed"],
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)

    payload["task_id"] = task_id
    payload.setdefault("status", "pending_apply")
    payload.setdefault("changed_files", [])
    payload.setdefault("unified_diff", "")
    payload.setdefault("checks_plan", [])
    payload.setdefault("apply_instructions", [])
    payload.setdefault("rollback_plan", "")
    payload.setdefault("errors", [])
    if not isinstance(payload["errors"], list):
        payload["errors"] = [str(payload["errors"])]

    status = str(payload.get("status", "")).strip().lower()
    if status not in {"pending_apply", "failed"}:
        payload["errors"].append(f"invalid_status:{status or 'empty'}")
        payload["status"] = "failed"
    else:
        payload["status"] = "pending_apply" if status == "pending_apply" else "failed"

    reply_l = raw.lower()
    if any(x in reply_l for x in ['"status":"executed"', '"status": "executed"', " tests passed", "status\": \"passed"]):
        payload["status"] = "failed"
        payload["errors"].append("forbidden_executed_claim")

    if not str(payload.get("unified_diff", "")).strip():
        payload["status"] = "failed"
        payload["errors"].append("missing_unified_diff")

    return json.dumps(payload, ensure_ascii=False, indent=2)


def _approve_failed(payload_text: str) -> bool:
    try:
        p = json.loads(payload_text)
    except Exception:
        return True
    return str(p.get("status", "")).strip().lower() == "failed"


def _latest_artifacts_snapshot() -> Dict[str, Any]:
    root = ROOT / "logs" / "openclaw"
    run = sorted(glob.glob(str(root / "run_summary_*.json")), reverse=True)
    md = sorted(glob.glob(str(root / "daily_brief_*.md")), reverse=True)
    csvs = sorted(glob.glob(str(root / "daily_brief_*.csv")), reverse=True)
    return {
        "run_summary": run[0] if run else "",
        "report_markdown": md[0] if md else "",
        "report_csv_paths": [csvs[0]] if csvs else [],
    }


def _is_expected_run_summary_path(p: str) -> bool:
    return bool(re.search(r"/logs/openclaw/run_summary_\d{8}_\d{6}\.json$", p or ""))


def _is_expected_report_md_path(p: str) -> bool:
    return bool(re.search(r"/logs/openclaw/daily_brief_\d{8}_\d{6}\.md$", p or ""))


def _is_expected_report_csv_path(p: str) -> bool:
    return bool(re.search(r"/logs/openclaw/daily_brief_\d{8}_\d{6}\.csv$", p or ""))


def _extract_ts_from_artifact(p: str, prefix: str, suffix: str) -> str:
    m = re.search(rf"{re.escape(prefix)}_(\d{{8}}_\d{{6}}){re.escape(suffix)}$", p or "")
    return m.group(1) if m else ""


def _repair_audit_reply(reply: str) -> str:
    raw = (reply or "").strip()
    start = raw.find("{")
    end = raw.rfind("}")
    payload: Dict[str, Any]
    if start < 0 or end <= start:
        payload = {
            "run_id": "",
            "status": "failed",
            "stages": [],
            "artifacts": _latest_artifacts_snapshot(),
            "errors": ["invalid_json_reply"],
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)

    try:
        payload = json.loads(raw[start : end + 1])
        if not isinstance(payload, dict):
            raise ValueError("payload_not_object")
    except Exception:
        payload = {
            "run_id": "",
            "status": "failed",
            "stages": [],
            "artifacts": _latest_artifacts_snapshot(),
            "errors": ["json_parse_failed"],
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)

    payload.setdefault("run_id", "")
    payload.setdefault("status", "failed")
    payload.setdefault("stages", [])
    payload.setdefault("artifacts", {})
    payload.setdefault("errors", [])
    payload.setdefault("warnings", [])
    if not isinstance(payload["errors"], list):
        payload["errors"] = [str(payload["errors"])]
    if not isinstance(payload["warnings"], list):
        payload["warnings"] = [str(payload["warnings"])]

    art = payload["artifacts"] if isinstance(payload["artifacts"], dict) else {}
    latest = _latest_artifacts_snapshot()

    run_summary = str(art.get("run_summary") or "")
    report_md = str(art.get("report_markdown") or "")
    csv_paths = art.get("report_csv_paths") or []
    if not isinstance(csv_paths, list):
        csv_paths = [str(csv_paths)]
    csv_paths = [str(x) for x in csv_paths if str(x).strip()]

    missing: List[str] = []
    invalid_kind: List[str] = []
    for p in [run_summary, report_md, *csv_paths]:
        if p and not Path(p).exists():
            missing.append(p)
    if run_summary and not _is_expected_run_summary_path(run_summary):
        invalid_kind.append(f"run_summary:{run_summary}")
    if report_md and not _is_expected_report_md_path(report_md):
        invalid_kind.append(f"report_markdown:{report_md}")
    for p in csv_paths:
        if not _is_expected_report_csv_path(p):
            invalid_kind.append(f"report_csv:{p}")

    # Fill missing with latest known artifacts.
    if (
        (not run_summary)
        or (run_summary and not Path(run_summary).exists())
        or (run_summary and not _is_expected_run_summary_path(run_summary))
    ):
        run_summary = latest.get("run_summary", "")
    if (
        (not report_md)
        or (report_md and not Path(report_md).exists())
        or (report_md and not _is_expected_report_md_path(report_md))
    ):
        report_md = latest.get("report_markdown", "")
    if (
        (not csv_paths)
        or any(not Path(p).exists() for p in csv_paths)
        or any(not _is_expected_report_csv_path(p) for p in csv_paths)
    ):
        csv_paths = list(latest.get("report_csv_paths", []))

    if missing or invalid_kind:
        payload["status"] = "failed"
        if missing:
            payload["errors"].append("missing_artifacts")
            payload["errors"].extend([f"missing:{p}" for p in missing])
        if invalid_kind:
            payload["errors"].append("invalid_artifact_kind")
            payload["errors"].extend([f"invalid:{x}" for x in invalid_kind])

    payload["artifacts"] = {
        "run_summary": run_summary,
        "report_markdown": report_md,
        "report_csv_paths": csv_paths,
    }

    # Enforce artifact timestamp consistency.
    ts_run = _extract_ts_from_artifact(run_summary, "run_summary", ".json")
    ts_md = _extract_ts_from_artifact(report_md, "daily_brief", ".md")
    ts_csv = _extract_ts_from_artifact(csv_paths[0], "daily_brief", ".csv") if csv_paths else ""
    ts_set = {x for x in [ts_run, ts_md, ts_csv] if x}
    if len(ts_set) > 1:
        payload["status"] = "failed"
        payload["errors"].append("artifact_timestamp_mismatch")
        payload["errors"].append(f"ts_run:{ts_run or 'none'}")
        payload["errors"].append(f"ts_md:{ts_md or 'none'}")
        payload["errors"].append(f"ts_csv:{ts_csv or 'none'}")

    if run_summary:
        m = re.search(r"run_summary_(\d{8}_\d{6})\.json$", run_summary)
        if m:
            canonical_run_id = f"partner_{m.group(1)}"
            current_run_id = str(payload.get("run_id") or "").strip()
            if current_run_id and current_run_id != canonical_run_id:
                payload["status"] = "failed"
                payload["errors"].append("run_id_mismatch")
                payload["errors"].append(f"run_id:{current_run_id}")
                payload["errors"].append(f"run_id_expected:{canonical_run_id}")
            payload["run_id"] = canonical_run_id

    # Detect explicit data gaps from structured or plain-text error entries.
    errs = payload.get("errors", [])
    for e in errs:
        s = str(e).lower()
        if "stock_snapshot" in s and ("missing" in s or "data is missing" in s):
            payload["errors"].append("data_gaps:stock_snapshot")
            payload["status"] = "failed"
            break

    # If all stages succeeded and artifacts were auto-recovered to real files,
    # downgrade hard failure to warning-level success (non-strict mode only).
    stages = payload.get("stages") if isinstance(payload.get("stages"), list) else []
    stage_all_success = bool(stages) and all(
        str((x or {}).get("status", "")).strip().lower() in {"success", "ok"}
        for x in stages
        if isinstance(x, dict)
    )
    recovered_ok = (
        bool(payload["artifacts"].get("run_summary"))
        and bool(payload["artifacts"].get("report_markdown"))
        and bool(payload["artifacts"].get("report_csv_paths"))
        and Path(str(payload["artifacts"]["run_summary"])).exists()
        and Path(str(payload["artifacts"]["report_markdown"])).exists()
        and all(Path(str(p)).exists() for p in (payload["artifacts"]["report_csv_paths"] or []))
    )
    warning_markers = {"missing_artifacts", "invalid_artifact_kind", "run_id_mismatch"}
    has_only_recoverable = all(
        (str(e).split(":", 1)[0] in warning_markers) or str(e).startswith("missing:") or str(e).startswith("invalid:")
        or str(e).startswith("run_id:")
        or str(e).startswith("run_id_expected:")
        for e in payload["errors"]
    )
    if (not STRICT_AUDIT) and payload.get("status") == "failed" and stage_all_success and recovered_ok and has_only_recoverable:
        payload["status"] = "success_with_warnings"
        payload["warnings"].extend(payload["errors"])
        payload["errors"] = []

    # Strict mode: any warning or recoverable mismatch still fails hard.
    if STRICT_AUDIT:
        if payload.get("warnings"):
            payload["errors"].append("warnings_not_allowed")
            payload["status"] = "failed"
        if payload.get("errors"):
            payload["status"] = "failed"
        else:
            payload["status"] = "success"

    return json.dumps(payload, ensure_ascii=False, indent=2)


def _run_local_daily_execution() -> Dict[str, Any]:
    script = ROOT / "tools" / "openclaw_partner_daily_run.sh"
    if not script.exists():
        return {
            "run_id": "",
            "status": "failed",
            "stages": [],
            "artifacts": _latest_artifacts_snapshot(),
            "errors": [f"missing_script:{script}"],
            "warnings": [],
        }
    timeout_sec = max(120, int(os.getenv("OPENCLAW_DAILY_TIMEOUT_SEC", "1500")))
    env = os.environ.copy()
    env.setdefault("OPENCLAW_OUTPUT_DIR", str(ROOT / "logs" / "openclaw"))
    # Keep daily runner Python consistent with current bridge runtime.
    env.setdefault("OPENCLAW_PYTHON", os.getenv("PYTHON_BIN") or sys.executable)
    try:
        proc = subprocess.run(
            ["bash", str(script)],
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired:
        return {
            "run_id": "",
            "status": "failed",
            "stages": [],
            "artifacts": _latest_artifacts_snapshot(),
            "errors": [f"daily_timeout:{timeout_sec}s"],
            "warnings": [],
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "run_id": "",
            "status": "failed",
            "stages": [],
            "artifacts": _latest_artifacts_snapshot(),
            "errors": [f"daily_exec_error:{type(exc).__name__}:{exc}"],
            "warnings": [],
        }

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    out_dir = Path(env["OPENCLAW_OUTPUT_DIR"])
    exec_json_path = ""
    for line in stdout.splitlines():
        if line.startswith("execution_json="):
            exec_json_path = line.split("=", 1)[1].strip()
            break
    if not exec_json_path:
        # Fallback to latest artifact.
        latest = sorted(glob.glob(str(out_dir / "partner_execution_*.json")), reverse=True)
        exec_json_path = latest[0] if latest else ""

    if exec_json_path and Path(exec_json_path).exists():
        try:
            payload = json.loads(Path(exec_json_path).read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("warnings", [])
                if not isinstance(payload.get("errors"), list):
                    payload["errors"] = [str(payload.get("errors"))]
                if not isinstance(payload.get("warnings"), list):
                    payload["warnings"] = [str(payload.get("warnings"))]
                if proc.returncode != 0:
                    payload["status"] = "failed"
                    payload["errors"].append(f"daily_exit_code:{proc.returncode}")
                return payload
        except Exception as exc:  # noqa: BLE001
            pass

    return {
        "run_id": "",
        "status": "failed",
        "stages": [],
        "artifacts": _latest_artifacts_snapshot(),
        "errors": [
            "daily_output_unreadable",
            f"daily_exit_code:{proc.returncode}",
            f"stdout_tail:{stdout[-300:]}",
            f"stderr_tail:{stderr[-300:]}",
        ],
        "warnings": [],
    }


def _run_oc_daily_async(chat_id: int) -> None:
    try:
        payload = _run_local_daily_execution()
        out = json.dumps(payload, ensure_ascii=False, indent=2)
        _safe_send_message_chunks(chat_id, out, reply_to_message_id=None)
        if _approve_failed(out):
            _CHAT_STOP_FLAG[chat_id] = True
            _safe_send_message_chunks(chat_id, "严格模式：本次失败，已自动 stop。请修复后再发 oc daily。", reply_to_message_id=None)
    except Exception as exc:  # noqa: BLE001
        logger.exception("oc daily async failed: %s", exc)
        _safe_send_message_chunks(chat_id, f"oc daily 执行异常：{type(exc).__name__}: {exc}", reply_to_message_id=None)
    finally:
        _CHAT_DAILY_RUNNING[chat_id] = False


def _extract_oc_payload(text: str) -> Optional[str]:
    s = (text or "").strip()
    if not s:
        return None
    m = re.match(r"(?is)^\s*(oc|wawa)(?:\s*[:：])?(?:\s+|$)(.*)$", s)
    if not m:
        return None
    payload = (m.group(2) or "").strip()
    return payload


def _employee_status_text(chat_id: int) -> str:
    enabled = _CHAT_EMPLOYEE_MODE.get(chat_id, False)
    identity = _CHAT_IDENTITY.get(chat_id, "oc")
    style = _CHAT_STYLE.get(chat_id, "action")
    scope = f"wawa:{chat_id}" if identity == "wawa" else chat_id
    last_q = _LAST_USER_QUERY.get(scope, "")
    mem = _CHAT_MEMORY.get(scope, deque())
    last = _CHAT_LAST_RESULT.get(chat_id, {})
    return (
        "员工模式状态：\n"
        f"- enabled: {enabled}\n"
        f"- identity: {identity}\n"
        f"- style: {style}\n"
        f"- stop_flag: {_CHAT_STOP_FLAG.get(chat_id, False)}\n"
        f"- memory_turns: {len(mem)}\n"
        f"- last_mode: {last.get('mode', 'N/A')}\n"
        f"- last_route: {last.get('route', 'N/A')}\n"
        f"- last_quality_pass: {last.get('quality_pass', 'N/A')}\n"
        f"- last_question: {(last_q[:120] + '...') if len(last_q) > 120 else (last_q or 'N/A')}"
    )


def _handle_command(chat_id: int, text: str, message_id: Optional[int], username: str) -> bool:
    t = " ".join((text or "").strip().lower().split())
    if t in ("/start", "/help"):
        _send_message_chunks(
            chat_id,
            (
                "已连接 Telegram ↔ 云端大脑。\n"
                "直接发送问题即可，例如：`688608怎么样？`\n\n"
                "可用命令：\n"
                "/health - 健康检查\n"
                "/clear - 清空会话上下文\n"
                "/deep - 切换到深度模式\n"
                "/action - 切换到行动模式\n"
                "oc 开启员工模式\n"
                "oc off 关闭员工模式\n"
                "oc status 查看员工模式状态\n"
                "oc stop 中断当前任务上下文\n"
                "oc audit 强制结构化审计输出\n"
                "oc go 生成代码优化提案（需审批）\n"
                "oc daily 一键执行今日闭环+审计\n"
                "oc approve 批准并执行上一个提案\n"
                "oc reject 驳回并清除上一个提案\n\n"
                "wawa 可作为专家身份别名使用，例如：\n"
                "wawa on / wawa status / wawa spec / wawa selftest / wawa 你的任务\n"
                "未加前缀的普通消息默认按 wawa 处理"
            ),
            reply_to_message_id=message_id,
        )
        return True
    if t == "/health":
        _send_message_chunks(chat_id, "telegram-bridge: ok", reply_to_message_id=message_id)
        return True
    if t == "/clear":
        _clear_chat_state(chat_id)
        _send_message_chunks(chat_id, "已清空当前会话上下文。", reply_to_message_id=message_id)
        return True
    if t == "/deep":
        _CHAT_STYLE[chat_id] = "deep"
        _send_message_chunks(chat_id, "已切换到深度模式。", reply_to_message_id=message_id)
        return True
    if t == "/action":
        _CHAT_STYLE[chat_id] = "action"
        _send_message_chunks(chat_id, "已切换到行动模式。", reply_to_message_id=message_id)
        return True
    if t in ("oc", "oc on", "oc 开启", "oc 启动"):
        _CHAT_EMPLOYEE_MODE[chat_id] = True
        _CHAT_STOP_FLAG[chat_id] = False
        _CHAT_IDENTITY[chat_id] = "oc"
        _send_message_chunks(
            chat_id,
            "员工模式已开启。后续用 `oc 你的任务` 直接派工，`oc status` 可查看状态。",
            reply_to_message_id=message_id,
        )
        return True
    if t in ("wawa", "wawa on", "wawa 开启", "wawa 启动"):
        _CHAT_EMPLOYEE_MODE[chat_id] = True
        _CHAT_STOP_FLAG[chat_id] = False
        _CHAT_IDENTITY[chat_id] = "wawa"
        _send_message_chunks(
            chat_id,
            "WAWA 专家模式已开启。后续用 `wawa 你的任务` 直接对话。",
            reply_to_message_id=message_id,
        )
        return True
    if t in ("oc as wawa",):
        _CHAT_EMPLOYEE_MODE[chat_id] = True
        _CHAT_STOP_FLAG[chat_id] = False
        _CHAT_IDENTITY[chat_id] = "wawa"
        _send_message_chunks(chat_id, "已切换到 WAWA 专家身份。", reply_to_message_id=message_id)
        return True
    if t in ("oc off", "oc 关闭"):
        _CHAT_EMPLOYEE_MODE[chat_id] = False
        _CHAT_STOP_FLAG[chat_id] = False
        _send_message_chunks(chat_id, "员工模式已关闭。", reply_to_message_id=message_id)
        return True
    if t in ("wawa off", "wawa 关闭"):
        _CHAT_EMPLOYEE_MODE[chat_id] = False
        _CHAT_STOP_FLAG[chat_id] = False
        _send_message_chunks(chat_id, "WAWA 专家模式已关闭。", reply_to_message_id=message_id)
        return True
    if t == "oc status":
        _send_message_chunks(chat_id, _employee_status_text(chat_id), reply_to_message_id=message_id)
        return True
    if t == "wawa status":
        _send_message_chunks(chat_id, _employee_status_text(chat_id), reply_to_message_id=message_id)
        return True
    if t == "wawa spec":
        _send_message_chunks(chat_id, _wawa_spec_text(), reply_to_message_id=message_id)
        return True
    if t == "wawa selftest":
        result = _run_wawa_selftest(chat_id)
        _send_message_chunks(chat_id, json.dumps(result, ensure_ascii=False, indent=2), reply_to_message_id=message_id)
        return True
    if t == "oc stop":
        _clear_chat_state(chat_id)
        _CHAT_STOP_FLAG[chat_id] = True
        _send_message_chunks(chat_id, "已中断并清空当前任务上下文。", reply_to_message_id=message_id)
        return True
    if t == "oc go":
        _CHAT_EMPLOYEE_MODE[chat_id] = True
        _CHAT_STOP_FLAG[chat_id] = False
        task_id = f"task_{int(time.time())}"
        prompt = _build_employee_task_prompt(chat_id, _build_oc_go_payload(task_id))
        ans = query_openclaw_remote(question=prompt, timeout=max(15, AGENT_TIMEOUT_GENERAL), session_id=f"telegram-{chat_id}-ocgo")
        if not ans or not ans.get("answer"):
            _send_message_chunks(chat_id, "提案生成失败，请稍后重试。", reply_to_message_id=message_id)
            return True
        proposal = str(ans.get("answer", "")).strip()
        _CHAT_PENDING_APPROVAL[chat_id] = {"task_id": task_id, "proposal": proposal, "created_at": int(time.time())}
        _send_message_chunks(chat_id, proposal, reply_to_message_id=message_id)
        _send_message_chunks(chat_id, f"提案已生成。若同意请发：oc approve（task_id={task_id}）", reply_to_message_id=None)
        return True
    if t == "oc daily":
        _CHAT_EMPLOYEE_MODE[chat_id] = True
        _CHAT_STOP_FLAG[chat_id] = False
        if _CHAT_DAILY_RUNNING.get(chat_id):
            _safe_send_message_chunks(chat_id, "oc daily 正在执行中，请稍等上一轮结果。", reply_to_message_id=message_id)
            return True
        _CHAT_DAILY_RUNNING[chat_id] = True
        threading.Thread(target=_run_oc_daily_async, args=(chat_id,), daemon=True).start()
        # Avoid blocking command handling on Telegram network jitter.
        threading.Thread(
            target=_safe_send_message_chunks,
            args=(chat_id, "已收到 `oc daily`，开始执行今日闭环（后台执行，完成后自动回传）...", message_id),
            daemon=True,
        ).start()
        return True
    if t == "oc approve":
        pending = _CHAT_PENDING_APPROVAL.get(chat_id)
        if not pending:
            _send_message_chunks(chat_id, "没有待审批提案。先发 oc go。", reply_to_message_id=message_id)
            return True
        task_id = str(pending.get("task_id") or "")
        proposal = str(pending.get("proposal") or "")
        prompt = _build_employee_task_prompt(chat_id, _build_oc_approve_payload(task_id, proposal))
        out = ""
        last_raw = ""
        for idx in range(3):
            ans = query_openclaw_remote(
                question=prompt if idx == 0 else (
                    "你上一轮输出不合格。仅返回 JSON，status 只能是 pending_apply|failed，"
                    "必须包含 unified_diff 且以 diff --git 开头。禁止自然语言。\n\n"
                    f"task_id={task_id}\n"
                    f"上一轮输出：\n{last_raw[:4000]}"
                ),
                timeout=max(22, AGENT_TIMEOUT_GENERAL),
                session_id=f"telegram-{chat_id}-ocapprove",
            )
            if not ans or not ans.get("answer"):
                last_raw = ""
                continue
            last_raw = str(ans.get("answer", "")).strip()
            out = _repair_approve_reply(task_id, last_raw)
            if not _approve_failed(out):
                break
        if not out:
            _send_message_chunks(chat_id, "执行失败，请稍后重试（待审批任务已保留，可直接再发 oc approve）。", reply_to_message_id=message_id)
            return True
        _send_message_chunks(chat_id, out, reply_to_message_id=message_id)
        if not _approve_failed(out):
            _CHAT_PENDING_APPROVAL.pop(chat_id, None)
        else:
            _send_message_chunks(chat_id, "当前仍未拿到可执行 diff，待审批任务已保留；可直接再发 oc approve 重试。", reply_to_message_id=None)
        return True
    if t == "oc reject":
        _CHAT_PENDING_APPROVAL.pop(chat_id, None)
        _send_message_chunks(chat_id, "已驳回并清除待审批提案。", reply_to_message_id=message_id)
        return True
    return False


def _reply_once(chat_id: int, text: str, message_id: Optional[int], username: str) -> None:
    if _handle_command(chat_id, text, message_id, username):
        return
    if _CHAT_STOP_FLAG.get(chat_id):
        _send_message_chunks(chat_id, "当前任务已被 stop，请发送新任务继续。", reply_to_message_id=message_id)
        return
    # WAWA runs as direct agent-llm chat mode (no hard guardrail interception).

    normalized = " ".join((text or "").strip().lower().split())
    is_oc_audit = normalized == "oc audit"
    oc_payload = None if is_oc_audit else _extract_oc_payload(text)
    wawa_prefixed = bool(re.match(r"(?is)^\s*wawa(?:\s*[:：])?(?:\s+|$)", (text or "").strip()))
    oc_prefixed = bool(re.match(r"(?is)^\s*oc(?:\s*[:：])?(?:\s+|$)", (text or "").strip()))
    wawa_payload = ""
    if wawa_prefixed:
        m = re.match(r"(?is)^\s*wawa(?:\s*[:：])?(?:\s+|$)(.*)$", (text or "").strip())
        wawa_payload = (m.group(1) if m else "").strip()
    auto_wawa = (not is_oc_audit) and (oc_payload is None) and (not wawa_prefixed) and (not oc_prefixed)
    employee_mode = _CHAT_EMPLOYEE_MODE.get(chat_id, False) or (oc_payload is not None) or is_oc_audit
    effective_text = text
    stock_snapshot: Optional[Dict[str, Any]] = None
    route_tag = "general"
    if is_oc_audit:
        _CHAT_EMPLOYEE_MODE[chat_id] = True
        _CHAT_STOP_FLAG[chat_id] = False
        _CHAT_IDENTITY[chat_id] = _CHAT_IDENTITY.get(chat_id, "oc")
        effective_text = _build_employee_task_prompt(chat_id, _build_oc_audit_payload())
        route_tag = "oc"
    elif wawa_prefixed and wawa_payload:
        _CHAT_EMPLOYEE_MODE[chat_id] = True
        _CHAT_STOP_FLAG[chat_id] = False
        _CHAT_IDENTITY[chat_id] = "wawa"
        # WAWA: direct llm mode with dedicated route, no stock-template constraints.
        employee_mode = False
        if _looks_like_stock_question(wawa_payload):
            stock_snapshot = _build_local_stock_snapshot(wawa_payload)
            effective_text = _build_wawa_stock_prompt(wawa_payload, stock_snapshot)
            route_tag = "wawa-stock"
        else:
            effective_text = _build_wawa_agent_prompt(wawa_payload)
            route_tag = "wawa-general"
    elif auto_wawa:
        # Default route: messages without prefix go to WAWA.
        _CHAT_EMPLOYEE_MODE[chat_id] = True
        _CHAT_STOP_FLAG[chat_id] = False
        _CHAT_IDENTITY[chat_id] = "wawa"
        employee_mode = False
        if _looks_like_stock_question(text):
            stock_snapshot = _build_local_stock_snapshot(text)
            effective_text = _build_wawa_stock_prompt(text, stock_snapshot)
            route_tag = "wawa-stock"
        else:
            effective_text = _build_wawa_agent_prompt(text)
            route_tag = "wawa-general"
    elif oc_payload is not None:
        _CHAT_EMPLOYEE_MODE[chat_id] = True
        _CHAT_IDENTITY[chat_id] = "oc"
        route_tag = "oc"
        effective_text = _build_employee_task_prompt(chat_id, oc_payload)
    memory_scope: Any = (
        f"wawa:{chat_id}"
        if (route_tag.startswith("wawa") or (_CHAT_IDENTITY.get(chat_id) == "wawa" and route_tag != "oc"))
        else chat_id
    )
    if _is_followup_text(text):
        last_q = (_LAST_USER_QUERY.get(memory_scope) or "").strip()
        if last_q:
            effective_text = (
                f"请基于我上一条问题继续深入展开，给出更具体、更可执行的版本。\n"
                f"上一条问题：{last_q}"
            )
    non_stock_wawa = route_tag == "wawa-general" and (not _looks_like_stock_question(text))
    if _CHAT_MEMORY.get(memory_scope) and not employee_mode and (not non_stock_wawa):
        # Keep cloud brain aware of recent context for multi-turn intelligence.
        effective_text = _build_smart_context(
            chat_id=chat_id,
            username=username,
            user_text=effective_text,
            memory_scope=memory_scope,
        )

    session_bucket = max(60, SESSION_BUCKET_SEC)
    if route_tag == "general":
        identity_tag = (_CHAT_IDENTITY.get(chat_id, "general") if employee_mode else "general").lower()
        route_tag = identity_tag
    if route_tag == "wawa-general":
        # Dedicated WAWA route, but keep short-term continuity in the same bucket.
        session_id = f"telegram-{chat_id}-wawa-v2-{int(time.time() // session_bucket)}"
    else:
        session_id = f"telegram-{chat_id}-{route_tag}-{int(time.time() // session_bucket)}"
    route_mode = "general_only" if route_tag == "wawa-general" else "auto"
    answer = query_openclaw_remote(
        question=effective_text,
        timeout=(
            AGENT_TIMEOUT_GENERAL
            if _looks_like_stock_concept_question(effective_text)
            else (AGENT_TIMEOUT_STOCK if _looks_like_stock_question(effective_text) else AGENT_TIMEOUT_GENERAL)
        ),
        session_id=session_id,
        route_mode=route_mode,
    )
    if not answer or not answer.get("answer"):
        if route_tag == "wawa-stock":
            fallback = _build_stock_local_deep_analysis(text, stock_snapshot or {})
            _send_message_chunks(chat_id, fallback, reply_to_message_id=message_id)
            return
        _send_message_chunks(
            chat_id,
            _bridge_failure_hint(),
            reply_to_message_id=message_id,
        )
        return
    reply = str(answer["answer"]).strip()
    reply = _strip_upstream_auth_noise(reply)
    if route_tag == "wawa-stock" and _needs_stock_depth(reply):
        reply = _build_stock_local_deep_analysis(text, stock_snapshot or {})
    # WAWA direct mode: do not force rewrite/fallback templates.
    if (not route_tag.startswith("wawa")) and _looks_like_stock_question(text) and not _EVIDENCE_TAG_RE.search(reply):
        repair_session_id = f"{session_id}-evidence"
        repair_q = (
            "请把以下回答重写为高质量投研格式，并满足：\n"
            "1) 每条关键结论必须带证据编号标签（如 [E1]/[E2]/[E3]）；\n"
            "2) 必须包含：核心判断、触发条件、失效条件、仓位动作；\n"
            "3) 不能空话，必须可执行。\n\n"
            f"原问题：{text}\n\n"
            f"原回答：{reply}"
        )
        repaired = query_openclaw_remote(
            question=repair_q,
            timeout=max(12, AGENT_TIMEOUT_STOCK),
            session_id=repair_session_id,
        )
        if repaired and repaired.get("answer"):
            repaired_reply = str(repaired["answer"]).strip()
            if repaired_reply and _EVIDENCE_TAG_RE.search(repaired_reply):
                reply = repaired_reply

    if is_oc_audit:
        reply = _repair_audit_reply(reply)

    logger.info(
        "reply: chat=%s mode=%s text=%s",
        chat_id,
        str(answer.get("mode", "unknown")),
        text[:80],
    )
    q = answer.get("quality") or {}
    _CHAT_LAST_RESULT[chat_id] = {
        "mode": str(answer.get("mode", "unknown")),
        "route": str(answer.get("route", "unknown")),
        "quality_pass": q.get("pass"),
        "quality_confidence": q.get("confidence"),
    }
    _send_message_chunks(chat_id, reply, reply_to_message_id=message_id)
    _append_memory(memory_scope, "user", text)
    _append_memory(memory_scope, "assistant", reply)
    if not _is_followup_text(text):
        _LAST_USER_QUERY[memory_scope] = text


def main() -> int:
    if not BOT_TOKEN:
        logger.error("missing TELEGRAM_BOT_TOKEN")
        return 2
    if not _acquire_single_instance_lock():
        return 3

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    offset = 0
    logger.info("telegram bridge started")
    if ALLOWED_CHAT_IDS is not None:
        logger.info("allowed chat ids: %s", sorted(ALLOWED_CHAT_IDS))

    while not _stop:
        try:
            data = _api_call(
                "getUpdates",
                {
                    "timeout": POLL_TIMEOUT,
                    "offset": offset,
                    "allowed_updates": ["message", "edited_message"],
                },
            )
            updates = data.get("result") or []
            for upd in updates:
                try:
                    uid = int(upd.get("update_id", 0))
                    offset = max(offset, uid + 1)
                    parsed = _extract_text(upd)
                    if not parsed:
                        continue
                    chat_id = parsed["chat_id"]
                    text = parsed["text"]
                    message_id = parsed["message_id"]
                    username = parsed["username"]
                    if not _is_allowed_chat(chat_id):
                        logger.info("ignored message from unauthorized chat: %s", chat_id)
                        continue
                    logger.info("message: chat=%s text=%s", chat_id, text[:120])
                    _reply_once(chat_id, text, message_id, username)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("failed to process update: %s", exc)
        except Exception as exc:  # noqa: BLE001
            logger.exception("poll failed: %s", exc)
            time.sleep(SLEEP_ON_ERROR_SEC)

    logger.info("telegram bridge stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
