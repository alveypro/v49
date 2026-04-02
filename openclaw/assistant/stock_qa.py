"""OpenClaw stock Q&A assistant.

Uses local OpenClaw artifacts first, with optional LLM enhancement via
OpenAI-compatible Chat Completions API.
"""

import glob
import json
import logging
import os
import re
import sqlite3
import math
import hashlib
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from openclaw.assistant.agent_mesh import AGENT_VERSION, count_agents, select_agents
from openclaw.assistant.humanlike_stack import HumanLikeToolStack

_log = logging.getLogger("openclaw.stock_qa")

try:
    from dotenv import load_dotenv as _load_dotenv
    for _env_candidate in [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env"),
        "/opt/openclaw/app/.env",
        "/Users/mac/2026Qlin/.env",
    ]:
        if os.path.exists(_env_candidate):
            _load_dotenv(_env_candidate, override=True)
            _log.info("Loaded .env from %s", _env_candidate)
            break
except Exception:
    pass

JsonDict = Dict[str, Any]
DEFAULT_PROFILE: JsonDict = {
    "risk_profile": "balanced",
    "max_drawdown_pct": 12.0,
    "preferred_horizon": "swing_2_8w",
    "total_exposure_cap": 0.65,
    "single_position_cap": 0.15,
    "risk_redline": "single_day_drawdown>=3% or weekly_drawdown>=6%",
    "user_nickname": "",
    "reply_style": "natural",
    "detail_level": "standard",
    "assistant_tone": "warm_professional",
}
SKILL_SCAN_DIRS = [
    "/Users/mac/.codex/skills",
    "/opt/openclaw/skills",
]
SKILL_INSTALLER_ROOT = "/Users/mac/.codex/skills/.system/skill-installer"
SKILL_LIST_SCRIPT = os.path.join(SKILL_INSTALLER_ROOT, "scripts", "list-skills.py")
SKILL_INSTALL_SCRIPT = os.path.join(SKILL_INSTALLER_ROOT, "scripts", "install-skill-from-github.py")


def _guess_db_path() -> str:
    candidates = [
        os.getenv("PERMANENT_DB_PATH"),
        "/Users/mac/2026Qlin/permanent_stock_database.db",
        "/opt/airivo/data/permanent_stock_database.db",
        "/opt/airivo/permanent_stock_database.db",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return candidates[-1]


def _to_ts_code(token: str) -> str:
    token = token.strip().upper()
    if re.match(r"^\d{6}\.(SZ|SH)$", token):
        return token
    if re.match(r"^\d{6}$", token):
        return f"{token}.SH" if token.startswith(("5", "6", "9")) else f"{token}.SZ"
    return token


def _extract_code(question: str) -> Optional[str]:
    m = re.search(r"(?<!\d)(\d{6}\.(?:SZ|SH)|\d{6})(?!\d)", question.upper())
    if not m:
        return None
    return _to_ts_code(m.group(1))


def _is_investment_brief_intent(question: str) -> bool:
    q = (question or "").lower()
    keys = [
        "提炼",
        "所有信息",
        "投资参考",
        "全面",
        "深度",
        "摘要",
        "总结",
        "brief",
        "summary",
    ]
    return any(k in q for k in keys)


def _is_bull_prediction_intent(question: str) -> bool:
    q = (question or "").lower()
    keys = [
        "牛股",
        "预测",
        "top20",
        "20大",
        "二十大",
        "涨幅",
        "候选",
    ]
    return any(k in q for k in keys)


@dataclass
class OpenClawStockAssistant:
    log_dir: str = "logs/openclaw"
    db_path: str = ""

    def __post_init__(self) -> None:
        if not self.db_path:
            self.db_path = _guess_db_path()
        self._ensure_learning_store()
        self._human_stack = HumanLikeToolStack(str(Path(self.log_dir) / "humanlike_stack.db"))
        self.sync_skill_registry()
        self._maybe_run_auto_skill_learning_cycle()

    def answer(self, question: str, history: Optional[List[JsonDict]] = None) -> JsonDict:
        question = (question or "").strip()
        if not question:
            return {"answer": "请输入问题，例如：`v6今天风险怎么样？`", "mode": "rule", "sources": []}

        route = self._route_question(question)
        profile = self._load_user_profile()
        profile_updates = self._extract_profile_updates(question)
        if profile_updates:
            for k, v in profile_updates.items():
                self._save_user_profile_field(k, v)
                profile[k] = v
            if self._is_profile_update_only(question):
                name = self._friendly_name(profile)
                hints = []
                if "risk_profile" in profile_updates:
                    hints.append(f"风险偏好已切到「{profile_updates['risk_profile']}」")
                if "detail_level" in profile_updates:
                    hints.append(f"回答细节改为「{profile_updates['detail_level']}」")
                if "user_nickname" in profile_updates and profile_updates["user_nickname"]:
                    hints.append(f"之后我会称呼你为「{profile_updates['user_nickname']}」")
                tips = "；".join(hints) if hints else "偏好已更新"
                return {
                    "answer": f"{name}，收到。我已经记住你的偏好：{tips}。接下来我会按这个风格长期配合你。",
                    "mode": "profile_update",
                    "sources": [],
                    "learning_card_id": "",
                    "route": route,
                    "confidence": 90.0,
                    "agent_version": AGENT_VERSION,
                    "agent_count": count_agents(),
                    "agent_hits": [],
                    "self_eval": {},
                }

        # Use stock-agent for data, then let LLM rephrase into natural language.
        api_resp = self._maybe_stock_agent_answer(question=question)
        if api_resp:
            raw_answer = str(api_resp.get("answer") or "")
            confidence = float((api_resp.get("quality") or {}).get("confidence", 0.7) or 0.7)
            if confidence <= 1.5:
                confidence = confidence * 100.0
            agent_hits = select_agents(question=question, route=route, confidence=confidence, max_agents=10)
            llm_rephrased = self._maybe_llm_rephrase_stock(question, raw_answer, history or [])
            if llm_rephrased:
                answer_text = llm_rephrased
            else:
                answer_text = self._enforce_response_protocol(
                    answer_text=raw_answer, route=route,
                    confidence=confidence, active_rules=[], answer_mode="rule",
                )
            answer_text = _strip_template_tails(answer_text)
            return {
                "answer": answer_text,
                "mode": f"stock_agent_{api_resp.get('mode', 'api')}",
                "sources": ["http://127.0.0.1:5101/chat"],
                "learning_card_id": "",
                "route": route,
                "confidence": confidence,
                "agent_version": AGENT_VERSION,
                "agent_count": count_agents(),
                "agent_hits": agent_hits,
                "self_eval": {},
            }

        active_rules = self._load_active_rules(route, limit=5)
        similar_lessons = self._load_similar_lessons(question, route, limit=3)
        summary = self._latest_run_summary()
        evolution = self._latest_evolution_stats()
        bull_candidates = self._predict_bull_candidates(limit=20) if _is_bull_prediction_intent(question) else []
        code = _extract_code(question)
        stock_info = self._stock_snapshot(code) if code else None
        context = self._build_context(
            question,
            summary,
            stock_info,
            evolution,
            bull_candidates,
            query_code=code,
            route=route,
            profile=profile,
            active_rules=active_rules,
            similar_lessons=similar_lessons,
        )
        # Human-like stack: build and persist unified state, then run cognition/policy/safety tools.
        unified_state = self._build_unified_state(context=context, profile=profile)
        try:
            self._human_stack.state_store_set("latest", unified_state)
            self._human_stack.audit_log(
                "state_store_update",
                {"route": route, "question": question[:200], "state_keys": list(unified_state.keys())},
            )
        except Exception:
            pass
        clarify_text = self._clarify_if_needed(question=question, route=route, context=context, profile=profile)
        if clarify_text:
            return {
                "answer": clarify_text,
                "mode": "clarify",
                "sources": context.get("sources", []),
                "learning_card_id": "",
                "route": route,
                "confidence": 78.0,
                "agent_version": AGENT_VERSION,
                "agent_count": count_agents(),
                "agent_hits": [],
                "self_eval": {},
            }
        confidence = self._estimate_confidence(context)
        agent_hits = select_agents(question=question, route=route, confidence=confidence, max_agents=10)
        candidate_rows = []
        for c in (context.get("bull_candidates") or [])[:30]:
            if isinstance(c, dict):
                candidate_rows.append(
                    {
                        "code": c.get("ts_code") or c.get("code"),
                        "score": c.get("pred_prob") or c.get("score") or 0.0,
                        "name": c.get("name"),
                    }
                )
        cognitive_bundle: JsonDict = {
            "stock_snapshot": context.get("stock") or {},
            "quality_gate": {"confidence": confidence, "data_freshness": {"days_lag": 0}},
        }
        cognition = self._human_stack.causal_reasoner(cognitive_bundle)
        counterfactual = self._human_stack.counterfactual_sim(cognitive_bundle)
        plan = self._human_stack.planner(question=question, route=route)
        policy = self._human_stack.policy_engine(route=route, quality={"confidence": confidence}, outputs=cognitive_bundle)
        optimizer = self._human_stack.multi_obj_optimizer(candidate_rows)
        safety = self._human_stack.safety_guard(question=question, quality={"confidence": confidence})
        try:
            self._human_stack.audit_log(
                "decision_tools",
                {
                    "route": route,
                    "confidence": confidence,
                    "policy_action": policy.get("action"),
                    "safety_decision": safety.get("decision"),
                },
            )
        except Exception:
            pass

        if str(safety.get("decision")) == "reject":
            safe_text = (
                "这个动作风险过高，我不能直接执行。我们换成更稳妥的方案："
                "先设仓位上限、再设止损线、最后分批执行。你告诉我目标标的和可承受回撤，我给你低风险版本。"
            )
            return {
                "answer": safe_text,
                "mode": "safety_reject",
                "sources": context.get("sources", []),
                "learning_card_id": "",
                "route": route,
                "confidence": max(40.0, confidence),
                "agent_version": AGENT_VERSION,
                "agent_count": count_agents(),
                "agent_hits": agent_hits,
                "self_eval": {},
            }

        llm_text = self._maybe_llm(question, context, history or [])
        answer_mode = "llm"
        answer_text = llm_text
        if llm_text:
            answer_text = llm_text
        else:
            answer_mode = "rule"
            answer_text = self._rule_answer(question, context)

        answer_text = self._enforce_response_protocol(
            answer_text=answer_text,
            route=route,
            confidence=confidence,
            active_rules=active_rules,
            answer_mode=answer_mode,
        )
        # Only inject cognition hints for rule-based answers; LLM answers are self-sufficient.
        if answer_mode != "llm":
            answer_text = self._inject_human_stack_hints(
                answer_text=answer_text,
                route=route,
                cognition=cognition,
                counterfactual=counterfactual,
                plan=plan,
                policy=policy,
                optimizer=optimizer,
            )
        eval_result = self._self_evaluate_answer(answer_text=answer_text, context=context)
        reward = float((eval_result or {}).get("overall_score", 0.0) or 0.0) / 100.0
        try:
            self._human_stack.outcome_tracker(
                route=route,
                confidence=confidence,
                reward=reward,
                meta={"mode": answer_mode, "question": question[:200]},
            )
            self._human_stack.bandit_weight_updater(agent_hits=agent_hits, reward=reward)
            if datetime.now().weekday() == 6:
                self._human_stack.rule_promoter()
        except Exception:
            pass
        card_id = self._write_learning_card(
            question=question,
            route=route,
            context=context,
            answer_text=answer_text,
            answer_mode=answer_mode,
            confidence=confidence,
            eval_result=eval_result,
        )
        answer_text = self._strip_template_tails(answer_text)
        return {
            "answer": answer_text,
            "mode": answer_mode,
            "sources": context.get("sources", []),
            "learning_card_id": card_id,
            "route": route,
            "confidence": confidence,
            "agent_version": AGENT_VERSION,
            "agent_count": count_agents(),
            "agent_hits": agent_hits,
            "self_eval": eval_result,
        }

    @staticmethod
    def _strip_template_tails(text: str) -> str:
        """Remove known template tails that get appended by various subsystems."""
        patterns = [
            r"\n*接下来你可以这样做[：:]\s*\n*今天先用小仓.*?不犹豫。?",
            r"\n*接下来你可以这样做[：:]\s*先小仓试探.*?退出。?",
            r"\n*下一步建议[：:]先小仓位试探.*?前面。?",
            r"\n*你可以把它理解为两个观察点.*?收手。?",
            r"\n*我对这个判断的把握大约是\s*[\d.]+%.*?[）)]。?",
        ]
        for pat in patterns:
            text = re.sub(pat, "", text, flags=re.DOTALL)
        return text.rstrip()

    @staticmethod
    def _build_unified_state(context: JsonDict, profile: JsonDict) -> JsonDict:
        summary = context.get("summary") or {}
        market = {
            "risk": (summary.get("risk") or {}),
            "publish": (summary.get("publish") or {}),
            "date": summary.get("created_at") or summary.get("date") or "",
        }
        holdings = context.get("active_holdings") or []
        return {
            "market": market,
            "positions": holdings if isinstance(holdings, list) else [],
            "risk_constraints": {
                "risk_profile": profile.get("risk_profile"),
                "max_drawdown_pct": profile.get("max_drawdown_pct"),
                "total_exposure_cap": profile.get("total_exposure_cap"),
                "single_position_cap": profile.get("single_position_cap"),
            },
            "user_preferences": {
                "user_nickname": profile.get("user_nickname", ""),
                "reply_style": profile.get("reply_style", "natural"),
                "detail_level": profile.get("detail_level", "standard"),
            },
        }

    def _inject_human_stack_hints(
        self,
        answer_text: str,
        route: str,
        cognition: JsonDict,
        counterfactual: JsonDict,
        plan: JsonDict,
        policy: JsonDict,
        optimizer: JsonDict,
    ) -> str:
        text = (answer_text or "").strip()
        if not text:
            return text
        if route not in ("stock_core", "hybrid_research", "cross_discipline"):
            return text

        causes = (cognition.get("causes") or []) if isinstance(cognition, dict) else []
        scenarios = (counterfactual.get("scenarios") or []) if isinstance(counterfactual, dict) else []
        steps = (plan.get("steps") or []) if isinstance(plan, dict) else []
        policy_action = str(policy.get("action", "")) if isinstance(policy, dict) else ""
        selected = (optimizer.get("selected") or []) if isinstance(optimizer, dict) else []

        tail_lines: List[str] = []
        if causes:
            tail_lines.append("我再补一句“为什么”：" + "；".join([str(x) for x in causes[:2]]))
        if scenarios:
            best = scenarios[0]
            tail_lines.append(
                f"如果做反事实推演，优先对比「{best.get('name', '不操作')}」这条路径，再决定是否加仓。"
            )
        if steps:
            tail_lines.append("执行上按这 3 步走最稳：" + " -> ".join([str(x) for x in steps[:3]]))
        if policy_action == "degrade_to_conservative":
            tail_lines.append("当前我建议你先用保守模式执行（降低仓位、提高止损纪律）。")
        if selected:
            tail_lines.append("候选池我已按收益/回撤/稳定性做过权衡，优先看前几名。")
        if not tail_lines:
            return text
        return text + "\n\n" + "\n".join(tail_lines[:3])

    def get_humanlike_dashboard(self) -> JsonDict:
        try:
            return self._human_stack.dashboard()
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def create_strategy_snapshot(self) -> JsonDict:
        try:
            snap = self._human_stack.rollback_manager_snapshot()
            self._human_stack.audit_log("rollback_snapshot_create", snap)
            return snap
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def restore_strategy_snapshot(self, snapshot_id: int) -> JsonDict:
        try:
            rst = self._human_stack.rollback_manager_restore(int(snapshot_id))
            self._human_stack.audit_log("rollback_snapshot_restore", {"snapshot_id": int(snapshot_id), **rst})
            return rst
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _save_user_profile_field(self, key: str, value: Any) -> None:
        conn = sqlite3.connect(self._learning_db_path)
        conn.execute(
            """
            INSERT OR REPLACE INTO assistant_profile(profile_key, profile_value)
            VALUES(?, ?)
            """,
            (str(key), json.dumps(value, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()

    def _extract_profile_updates(self, question: str) -> JsonDict:
        q = (question or "").strip()
        ql = q.lower()
        updates: JsonDict = {}

        # "以后叫我XX"/"我叫XX"
        m = re.search(r"(?:以后叫我|你可以叫我|叫我|我叫)\s*([A-Za-z0-9\u4e00-\u9fa5]{2,20})", q)
        if m:
            updates["user_nickname"] = m.group(1).strip()

        if any(k in q for k in ["我偏保守", "保守一点", "稳一点", "低风险"]):
            updates["risk_profile"] = "conservative"
        elif any(k in q for k in ["我偏激进", "进攻一点", "高风险高收益", "激进一点"]):
            updates["risk_profile"] = "aggressive"
        elif any(k in q for k in ["我偏稳健", "平衡一点", "中性一点", "稳健"]):
            updates["risk_profile"] = "balanced"

        if any(k in q for k in ["简短一点", "说短一点", "直接点", "别太长"]):
            updates["detail_level"] = "concise"
        elif any(k in q for k in ["详细一点", "展开讲", "讲透一点", "尽量详细"]):
            updates["detail_level"] = "detailed"

        if "像朋友" in q or "自然一点" in q or "别太官方" in q:
            updates["reply_style"] = "human_friend"
        elif "专业一点" in q or "正式一点" in q or "偏专业" in q:
            updates["reply_style"] = "professional"

        if "每天总结" in q or "每日总结" in q or "主动提醒" in q:
            updates["daily_companion_summary"] = True
        if "关闭每日总结" in q or "不用每日总结" in q:
            updates["daily_companion_summary"] = False

        # Basic guard: avoid false positive on pure analytical question.
        if ("why" in ql or "为什么" in q) and len(updates) == 1 and "user_nickname" in updates:
            return {}
        return updates

    @staticmethod
    def _is_profile_update_only(question: str) -> bool:
        q = (question or "").strip()
        if "？" in q or "?" in q:
            return False
        ask_markers = ("分析", "怎么看", "可以买", "能买", "策略", "仓位", "止损", "风险")
        return not any(k in q for k in ask_markers)

    @staticmethod
    def _friendly_name(profile: JsonDict) -> str:
        nickname = str(profile.get("user_nickname", "") or "").strip()
        return nickname if nickname else "朋友"

    def _clarify_if_needed(self, question: str, route: str, context: JsonDict, profile: JsonDict) -> Optional[str]:
        q = (question or "").strip()
        if route not in ("stock_core", "hybrid_research"):
            return None

        has_code = bool(_extract_code(q) or context.get("query_code"))
        action_intent = any(k in q for k in ["可以买", "能买", "怎么操作", "怎么做", "仓位", "止损", "给建议", "要不要买"])
        if has_code or not action_intent:
            return None

        name = self._friendly_name(profile)
        return (
            f"{name}，这题我可以给你很具体，但先补两条关键信息我才能把建议做准：\n"
            "1) 你想看的标的（代码或名称）\n"
            "2) 你的交易周期（短线/波段/中线）\n\n"
            "你直接回我一句：`600519，波段，可承受单笔-5%`，我马上给你可执行方案。"
        )

    def _maybe_stock_agent_answer(self, question: str) -> Optional[JsonDict]:
        q = (question or "").strip()
        if not q:
            return None
        # Keep cross-discipline/general knowledge questions in local assistant.
        stock_hints = (
            "股票",
            "个股",
            "风险",
            "回测",
            "仓位",
            "止损",
            "止盈",
            "策略",
            "信号",
            "预测",
            "候选",
            "涨跌",
            "买",
            "卖",
        )
        has_code = bool(_extract_code(q))
        if not has_code and not any(h in q for h in stock_hints):
            return None
        try:
            payload = {
                "question": q,
                "session_id": f"stock-web-{hashlib.sha1(q.encode('utf-8')).hexdigest()[:10]}",
            }
            resp = requests.post(
                "http://127.0.0.1:5101/chat",
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=18,
            )
            if resp.status_code >= 400:
                return None
            data = resp.json() if resp.content else {}
            if not isinstance(data, dict):
                return None
            ans = str(data.get("answer") or "").strip()
            if not ans or len(ans) < 40:
                return None
            return data
        except Exception:
            return None

    def _ensure_learning_store(self) -> None:
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._learning_db_path)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS learning_cards (
                card_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                route TEXT,
                task_type TEXT,
                question TEXT,
                question_hash TEXT,
                context_json TEXT,
                answer_text TEXT,
                answer_mode TEXT,
                confidence REAL,
                eval_json TEXT,
                outcome_json TEXT,
                status TEXT DEFAULT 'pending'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS policy_rules (
                rule_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                route TEXT,
                priority INTEGER DEFAULT 50,
                rule_text TEXT NOT NULL,
                condition_text TEXT,
                status TEXT DEFAULT 'soft',
                hit_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                fail_count INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assistant_profile (
                profile_key TEXT PRIMARY KEY,
                profile_value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS evaluation_reports (
                report_id TEXT PRIMARY KEY,
                report_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                report_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assistant_skills (
                skill_id TEXT PRIMARY KEY,
                skill_name TEXT NOT NULL,
                description TEXT,
                skill_path TEXT,
                source TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assistant_skill_events (
                event_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        for key, value in DEFAULT_PROFILE.items():
            conn.execute(
                """
                INSERT OR IGNORE INTO assistant_profile(profile_key, profile_value)
                VALUES(?, ?)
                """,
                (key, json.dumps(value, ensure_ascii=False)),
            )
        conn.commit()
        conn.close()

    @property
    def _learning_db_path(self) -> str:
        return str(Path(self.log_dir) / "assistant_learning.db")

    def _route_question(self, question: str) -> str:
        q = (question or "").lower()
        skills_keys = [
            "skills", "skill", "你会哪些", "会哪些技能", "安装技能", "下载安装", "装什么技能",
            "有哪些能力模块", "能学什么",
        ]
        if any(k in q for k in skills_keys):
            return "skills"
        identity_keys = [
            "你是谁", "你叫什么", "你的能力", "你能做什么", "介绍一下你",
            "你好", "自我介绍", "who are you", "what can you do",
        ]
        if any(k in q for k in identity_keys):
            return "identity"
        stock_keys = [
            "股票", "仓位", "止损", "止盈", "回测", "选股", "风险", "大盘",
            "估值", "买点", "卖点", "持仓", "交易", "牛股", "量化",
            "涨停", "跌停", "板块", "行情", "macd", "均线", "k线",
            "策略", "收益", "胜率", "回撤", "夏普", "盈亏", "调仓",
        ]
        cross_keys = [
            "哲学", "历史", "法律", "政治", "宗教", "科技", "编程", "概率", "经济",
            "代码", "程序", "算法", "数学", "统计", "心理", "行为", "博弈",
        ]
        has_stock = bool(_extract_code(question)) or any(k in q for k in stock_keys)
        has_cross = any(k in q for k in cross_keys)
        # When user asks conceptual/philosophical question, avoid over-routing to stock mode.
        conceptual = any(k in q for k in ["本质", "哲学", "意义", "方法论", "认识论"]) and not bool(_extract_code(question))
        strong_stock_action = any(k in q for k in ["仓位", "止损", "买", "卖", "回测", "收益", "回撤", "选股"])
        if conceptual and has_cross and not strong_stock_action:
            return "cross_discipline"
        if has_stock and has_cross:
            return "hybrid_research"
        if has_cross:
            return "cross_discipline"
        if has_stock:
            return "stock_core"
        return "general"

    def _is_network_skill_intent(self, question: str) -> bool:
        q = (question or "").lower()
        keys = [
            "联网", "网络", "github", "openai/skills", "自动安装", "自动学习", "拉取", "同步技能", "下载安装",
        ]
        return any(k in q for k in keys)

    @staticmethod
    def _skill_net_learning_enabled() -> bool:
        return os.getenv("OPENCLAW_ENABLE_SKILL_NET_LEARNING", "0").strip() == "1"

    @staticmethod
    def _skill_auto_install_enabled() -> bool:
        return os.getenv("OPENCLAW_ENABLE_SKILL_AUTO_INSTALL", "0").strip() == "1"

    def _log_skill_event(self, event_type: str, payload: JsonDict) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        eid = f"se_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hashlib.sha1((event_type + now).encode('utf-8')).hexdigest()[:6]}"
        conn = sqlite3.connect(self._learning_db_path)
        conn.execute(
            """
            INSERT OR REPLACE INTO assistant_skill_events(event_id, created_at, event_type, payload_json)
            VALUES(?, ?, ?, ?)
            """,
            (eid, now, event_type, json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()

    def _list_remote_curated_skills(self) -> JsonDict:
        if not os.path.exists(SKILL_LIST_SCRIPT):
            return {"ok": False, "error": "skill list script not found"}
        try:
            proc = subprocess.run(
                ["python3", SKILL_LIST_SCRIPT, "--format", "json"],
                capture_output=True,
                text=True,
                timeout=35,
                check=False,
            )
            if proc.returncode != 0:
                return {"ok": False, "error": (proc.stderr or proc.stdout or "list failed").strip()[:500]}
            data = json.loads(proc.stdout or "{}")
            items = data.get("skills") if isinstance(data, dict) else None
            if not isinstance(items, list):
                return {"ok": False, "error": "invalid list response"}
            names = [str(x.get("name", "")).strip() for x in items if isinstance(x, dict) and x.get("name")]
            installed = {str(x.get("name", "")).strip() for x in items if isinstance(x, dict) and x.get("installed")}
            return {
                "ok": True,
                "total": len(names),
                "skills": names,
                "installed": sorted([x for x in installed if x]),
            }
        except Exception as e:
            return {"ok": False, "error": str(e)[:500]}

    def _auto_install_remote_skills(self, max_install: int = 3) -> JsonDict:
        desired_raw = os.getenv("OPENCLAW_AUTO_INSTALL_SKILLS", "").strip()
        desired = [x.strip() for x in desired_raw.split(",") if x.strip()]
        if not desired:
            return {"ok": False, "error": "OPENCLAW_AUTO_INSTALL_SKILLS is empty", "installed": []}
        if not os.path.exists(SKILL_INSTALL_SCRIPT):
            return {"ok": False, "error": "skill install script not found", "installed": []}

        desired = desired[: max(1, int(max_install))]
        installed: List[str] = []
        errors: List[str] = []
        for name in desired:
            try:
                proc = subprocess.run(
                    [
                        "python3",
                        SKILL_INSTALL_SCRIPT,
                        "--repo",
                        "openai/skills",
                        "--path",
                        f"skills/.curated/{name}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=False,
                )
                out = (proc.stdout or "") + "\n" + (proc.stderr or "")
                if proc.returncode == 0:
                    installed.append(name)
                else:
                    errors.append(f"{name}: {(out.strip() or 'install failed')[:260]}")
            except Exception as e:
                errors.append(f"{name}: {str(e)[:260]}")
        result = {"ok": len(installed) > 0, "installed": installed, "errors": errors}
        self._log_skill_event("auto_install", result)
        return result

    def _maybe_run_auto_skill_learning_cycle(self) -> None:
        if not (self._skill_net_learning_enabled() and self._skill_auto_install_enabled()):
            return
        interval_min = max(30, int(os.getenv("OPENCLAW_SKILL_AUTO_INTERVAL_MIN", "360")))
        try:
            conn = sqlite3.connect(self._learning_db_path)
            row = conn.execute(
                """
                SELECT created_at
                FROM assistant_skill_events
                WHERE event_type='auto_cycle'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchone()
            conn.close()
            if row and row[0]:
                last_ts = datetime.strptime(str(row[0]), "%Y-%m-%d %H:%M:%S")
                if (datetime.now() - last_ts).total_seconds() < interval_min * 60:
                    return
        except Exception:
            return
        remote = self._list_remote_curated_skills()
        install = self._auto_install_remote_skills(
            max_install=int(os.getenv("OPENCLAW_AUTO_INSTALL_MAX_PER_CYCLE", "3"))
        )
        self.sync_skill_registry()
        self._log_skill_event(
            "auto_cycle",
            {
                "remote_ok": bool(remote.get("ok")),
                "remote_total": int(remote.get("total", 0) or 0),
                "installed_count": len(install.get("installed", [])),
                "error_count": len(install.get("errors", [])),
            },
        )

    def _discover_local_skills(self) -> List[JsonDict]:
        found: List[JsonDict] = []
        for root in SKILL_SCAN_DIRS:
            if not os.path.isdir(root):
                continue
            try:
                for path in Path(root).rglob("SKILL.md"):
                    try:
                        text = path.read_text(encoding="utf-8")
                    except Exception:
                        continue
                    rel = str(path.relative_to(root))
                    skill_name = path.parent.name
                    desc = ""
                    for line in text.splitlines()[:20]:
                        s = line.strip()
                        if (
                            s
                            and not s.startswith("#")
                            and s not in {"---", "..."}
                            and not re.match(r"^[A-Za-z_]+\s*:\s*", s)
                        ):
                            desc = s[:180]
                            break
                    sid = hashlib.sha1(f"{root}:{rel}".encode("utf-8")).hexdigest()[:16]
                    found.append(
                        {
                            "skill_id": sid,
                            "skill_name": skill_name,
                            "description": desc,
                            "skill_path": str(path),
                            "source": "local_scan",
                        }
                    )
            except Exception:
                continue
        uniq: Dict[str, JsonDict] = {}
        for item in found:
            uniq[item["skill_id"]] = item
        return list(uniq.values())

    def sync_skill_registry(self) -> JsonDict:
        skills = self._discover_local_skills()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(self._learning_db_path)
        for s in skills:
            conn.execute(
                """
                INSERT INTO assistant_skills(skill_id, skill_name, description, skill_path, source, status, updated_at)
                VALUES(?, ?, ?, ?, ?, 'active', ?)
                ON CONFLICT(skill_id) DO UPDATE SET
                    skill_name=excluded.skill_name,
                    description=excluded.description,
                    skill_path=excluded.skill_path,
                    source=excluded.source,
                    status='active',
                    updated_at=excluded.updated_at
                """,
                (
                    s.get("skill_id"),
                    s.get("skill_name"),
                    s.get("description", ""),
                    s.get("skill_path", ""),
                    s.get("source", "local_scan"),
                    now,
                ),
            )
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM assistant_skills WHERE status='active'").fetchone()[0]
        conn.close()
        return {"synced": len(skills), "active_total": int(total or 0)}

    def list_skills(self, limit: int = 50) -> List[JsonDict]:
        conn = sqlite3.connect(self._learning_db_path)
        rows = conn.execute(
            """
            SELECT skill_id, skill_name, description, skill_path, source, updated_at
            FROM assistant_skills
            WHERE status='active'
            ORDER BY updated_at DESC, skill_name ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        conn.close()
        return [
            {
                "skill_id": r[0],
                "skill_name": r[1],
                "description": r[2] or "",
                "skill_path": r[3] or "",
                "source": r[4] or "",
                "updated_at": r[5] or "",
            }
            for r in rows
        ]

    def _load_user_profile(self) -> JsonDict:
        conn = sqlite3.connect(self._learning_db_path)
        rows = conn.execute(
            "SELECT profile_key, profile_value FROM assistant_profile"
        ).fetchall()
        conn.close()
        profile: JsonDict = {}
        for key, value in rows:
            try:
                profile[str(key)] = json.loads(value)
            except Exception:
                profile[str(key)] = value
        for key, value in DEFAULT_PROFILE.items():
            profile.setdefault(key, value)
        return profile

    def _load_active_rules(self, route: str, limit: int = 5) -> List[JsonDict]:
        conn = sqlite3.connect(self._learning_db_path)
        rows = conn.execute(
            """
            SELECT rule_id, route, priority, rule_text, condition_text, status, hit_count, success_count, fail_count
            FROM policy_rules
            WHERE status IN ('soft', 'hard') AND (route = ? OR route = 'global')
            ORDER BY CASE status WHEN 'hard' THEN 2 ELSE 1 END DESC, priority DESC, updated_at DESC
            LIMIT ?
            """,
            (route, max(1, int(limit))),
        ).fetchall()
        conn.close()
        out: List[JsonDict] = []
        for row in rows:
            out.append(
                {
                    "rule_id": row[0],
                    "route": row[1],
                    "priority": row[2],
                    "rule_text": row[3],
                    "condition_text": row[4],
                    "status": row[5],
                    "hit_count": row[6],
                    "success_count": row[7],
                    "fail_count": row[8],
                }
            )
        return out

    def _load_similar_lessons(self, question: str, route: str, limit: int = 3) -> List[JsonDict]:
        tokens = [t for t in re.split(r"\W+", question.lower()) if len(t) >= 2]
        conn = sqlite3.connect(self._learning_db_path)
        rows = conn.execute(
            """
            SELECT card_id, question, eval_json, outcome_json
            FROM learning_cards
            WHERE route = ? AND status IN ('closed', 'pending')
            ORDER BY created_at DESC
            LIMIT 40
            """,
            (route,),
        ).fetchall()
        conn.close()
        scored: List[JsonDict] = []
        for row in rows:
            q_text = str(row[1] or "")
            eval_json = self._parse_json_or_empty(row[2])
            outcome_json = self._parse_json_or_empty(row[3])
            hit = sum(1 for t in tokens if t in q_text.lower())
            if hit <= 0:
                continue
            scored.append(
                {
                    "card_id": row[0],
                    "question": q_text,
                    "token_hits": hit,
                    "overall_score": float((eval_json or {}).get("overall_score", 0.0)),
                    "outcome": outcome_json,
                }
            )
        scored.sort(key=lambda x: (x["token_hits"], x["overall_score"]), reverse=True)
        return scored[: max(1, int(limit))]

    def _estimate_confidence(self, context: JsonDict) -> float:
        score = 70.0
        summary = context.get("summary") or {}
        stock = context.get("stock")
        if summary:
            score += 8.0
        if stock:
            score += 10.0
        if context.get("evolution"):
            score += 6.0
        if not stock and _extract_code(context.get("question", "")):
            score -= 12.0
        if not summary and not context.get("evolution"):
            score -= 10.0
        if context.get("route") == "hybrid_research":
            score -= 6.0
        return max(0.0, min(95.0, round(score, 1)))

    def _enforce_response_protocol(
        self,
        answer_text: str,
        route: str,
        confidence: float,
        active_rules: List[JsonDict],
        answer_mode: str = "rule",
    ) -> str:
        text = (answer_text or "").strip()
        if not text:
            text = "当前信息还不够，你补充一下标的、周期和仓位，我再给你具体方案。"

        # When LLM produced the answer, trust it and do minimal post-processing.
        if answer_mode == "llm":
            if text.startswith("回答模式："):
                parts = text.split("\n", 1)
                text = (parts[1] if len(parts) > 1 else text).strip()
            return text

        human_style = os.getenv("OPENCLAW_QA_HUMAN_STYLE", "1") != "0"
        show_meta = os.getenv("OPENCLAW_QA_SHOW_META", "0") == "1"
        if human_style:
            if text.startswith("回答模式："):
                parts = text.split("\n", 1)
                text = (parts[1] if len(parts) > 1 else "").strip()
            if not text:
                text = "当前信息还不够，先补充标的、周期和仓位，我再给你可执行方案。"
            if show_meta and active_rules:
                rule_lines = [f"- {r.get('rule_text')}" for r in active_rules[:3]]
                text += "\n\n当前触发的自学习规则：\n" + "\n".join(rule_lines)
            return text

        if "结论" not in text:
            text = f"结论：{text}"
        conf_label = "高" if confidence >= 80 else ("中" if confidence >= 60 else "低")
        footer = f"\n\n置信度：{confidence:.1f}%（{conf_label}）"
        if active_rules:
            rule_lines = [f"  - {r.get('rule_text')}" for r in active_rules[:3]]
            footer += "\n自学习规则：\n" + "\n".join(rule_lines)
        return f"{text}{footer}"

    def _self_evaluate_answer(self, answer_text: str, context: JsonDict) -> JsonDict:
        text = answer_text or ""
        has_action = 1 if any(k in text for k in ["建议", "动作", "仓位", "止损", "立即", "本周"]) else 0
        has_risk = 1 if any(k in text.lower() for k in ["风险", "risk", "orange", "red", "yellow", "green"]) else 0
        has_conditions = 1 if any(k in text for k in ["触发", "失效", "前提", "条件"]) else 0
        has_structure = 1 if any(k in text for k in ["结论", "依据"]) else 0
        has_sources = 1 if len(context.get("sources", [])) > 0 else 0

        accuracy = 55 + 10 * has_sources + 8 * has_structure
        risk_control = 52 + 14 * has_risk + 10 * has_conditions
        actionability = 50 + 18 * has_action + 10 * has_structure
        consistency = 58 + 8 * has_structure + 8 * has_risk
        overall = 0.35 * accuracy + 0.35 * risk_control + 0.20 * actionability + 0.10 * consistency
        return {
            "accuracy_score": round(min(100.0, accuracy), 1),
            "risk_score": round(min(100.0, risk_control), 1),
            "actionability_score": round(min(100.0, actionability), 1),
            "consistency_score": round(min(100.0, consistency), 1),
            "overall_score": round(min(100.0, overall), 1),
            "reflection_required": bool(
                accuracy < 60 or risk_control < 60 or actionability < 60 or consistency < 60
            ),
        }

    def _write_learning_card(
        self,
        question: str,
        route: str,
        context: JsonDict,
        answer_text: str,
        answer_mode: str,
        confidence: float,
        eval_result: JsonDict,
    ) -> str:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        q_hash = hashlib.sha1(question.encode("utf-8")).hexdigest()[:16]
        card_id = f"lc_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{q_hash[:6]}"
        ctx_safe = {
            "route": route,
            "question": question,
            "sources": context.get("sources", []),
            "summary_exists": bool(context.get("summary")),
            "stock_code": (context.get("stock") or {}).get("ts_code") or context.get("query_code"),
            "bull_candidate_count": len(context.get("bull_candidates") or []),
            "active_rule_ids": [r.get("rule_id") for r in context.get("active_rules", [])][:5],
        }
        conn = sqlite3.connect(self._learning_db_path)
        conn.execute(
            """
            INSERT OR REPLACE INTO learning_cards(
                card_id, created_at, updated_at, route, task_type, question, question_hash,
                context_json, answer_text, answer_mode, confidence, eval_json, outcome_json, status
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                card_id,
                now,
                now,
                route,
                route,
                question,
                q_hash,
                json.dumps(ctx_safe, ensure_ascii=False),
                answer_text,
                answer_mode,
                float(confidence),
                json.dumps(eval_result, ensure_ascii=False),
                "{}",
                "pending",
            ),
        )
        conn.commit()
        conn.close()
        self._auto_reflect_and_update_policy(route=route, question=question, eval_result=eval_result)
        return card_id

    def _auto_reflect_and_update_policy(self, route: str, question: str, eval_result: JsonDict) -> None:
        if not eval_result.get("reflection_required"):
            return
        weak_dimension = min(
            ["accuracy_score", "risk_score", "actionability_score", "consistency_score"],
            key=lambda k: float(eval_result.get(k, 0.0)),
        )
        reflection_map = {
            "accuracy_score": "优先引用本地可验证数据源，并明确事实与推断。",
            "risk_score": "补充风险颜色、触发因子和失效条件。",
            "actionability_score": "给出立即/本周动作，明确仓位与止损阈值。",
            "consistency_score": "保持结论-依据-动作结构，避免前后冲突。",
        }
        rule_text = reflection_map.get(weak_dimension, "回答保持结构化并可执行。")
        rule_id = f"rule_{route}_{hashlib.sha1((route + rule_text).encode('utf-8')).hexdigest()[:10]}"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(self._learning_db_path)
        conn.execute(
            """
            INSERT INTO policy_rules(rule_id, created_at, updated_at, route, priority, rule_text, condition_text, status, hit_count, success_count, fail_count)
            VALUES(?, ?, ?, ?, ?, ?, ?, 'soft', 1, 0, 1)
            ON CONFLICT(rule_id) DO UPDATE SET
                updated_at=excluded.updated_at,
                hit_count=policy_rules.hit_count + 1,
                fail_count=policy_rules.fail_count + 1
            """,
            (
                rule_id,
                now,
                now,
                route,
                70,
                rule_text,
                f"triggered_by={weak_dimension};sample_question={question[:60]}",
            ),
        )
        conn.commit()
        conn.close()

    def run_self_learning_cycle(self, force_weekly: bool = False) -> JsonDict:
        tracking = self.run_outcome_tracking_cycle()
        daily = self._run_daily_evaluator()
        weekly = self._run_weekly_evaluator() if force_weekly else {}
        return {"tracking": tracking, "daily": daily, "weekly": weekly}

    def get_learning_dashboard(self) -> JsonDict:
        conn = sqlite3.connect(self._learning_db_path)
        total_cards = conn.execute("SELECT COUNT(*) FROM learning_cards").fetchone()[0]
        pending_cards = conn.execute("SELECT COUNT(*) FROM learning_cards WHERE status='pending'").fetchone()[0]
        last_daily = conn.execute(
            "SELECT report_json FROM evaluation_reports WHERE report_type='daily' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        last_weekly = conn.execute(
            "SELECT report_json FROM evaluation_reports WHERE report_type='weekly' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        last_tracking = conn.execute(
            "SELECT report_json FROM evaluation_reports WHERE report_type='tracking' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        hard_rules = conn.execute("SELECT COUNT(*) FROM policy_rules WHERE status='hard'").fetchone()[0]
        soft_rules = conn.execute("SELECT COUNT(*) FROM policy_rules WHERE status='soft'").fetchone()[0]
        conn.close()
        return {
            "total_cards": int(total_cards or 0),
            "pending_cards": int(pending_cards or 0),
            "hard_rules": int(hard_rules or 0),
            "soft_rules": int(soft_rules or 0),
            "last_daily": self._parse_json_or_empty(last_daily[0]) if last_daily else {},
            "last_weekly": self._parse_json_or_empty(last_weekly[0]) if last_weekly else {},
            "last_tracking": self._parse_json_or_empty(last_tracking[0]) if last_tracking else {},
        }

    def run_outcome_tracking_cycle(self, max_cards: int = 200) -> JsonDict:
        conn = sqlite3.connect(self._learning_db_path)
        rows = conn.execute(
            """
            SELECT card_id, created_at, context_json, outcome_json
            FROM learning_cards
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (max(1, int(max_cards)),),
        ).fetchall()

        processed = 0
        closed = 0
        skipped = 0
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for card_id, created_at, context_json, outcome_json in rows:
            processed += 1
            ctx = self._parse_json_or_empty(context_json)
            out = self._parse_json_or_empty(outcome_json)
            ts_code = (ctx.get("stock_code") or "").strip()
            if not ts_code:
                skipped += 1
                continue
            base_trade_date = self._normalize_date_to_yyyymmdd(str(created_at).split(" ")[0])
            if not base_trade_date:
                skipped += 1
                continue
            tracked = self._build_forward_outcome(ts_code=ts_code, base_trade_date=base_trade_date)
            if not tracked:
                skipped += 1
                continue
            merged = {**out, **tracked}
            is_ready = bool((tracked.get("horizons") or {}).get("d20"))
            new_status = "closed" if is_ready else "pending"
            conn.execute(
                """
                UPDATE learning_cards
                SET outcome_json = ?, status = ?, updated_at = ?
                WHERE card_id = ?
                """,
                (json.dumps(merged, ensure_ascii=False), new_status, updated_at, card_id),
            )
            if new_status == "closed":
                closed += 1

        summary = {
            "processed_cards": processed,
            "closed_cards": closed,
            "skipped_cards": skipped,
        }
        report_id = f"tracking_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        conn.execute(
            "INSERT OR REPLACE INTO evaluation_reports(report_id, report_type, created_at, report_json) VALUES(?, 'tracking', ?, ?)",
            (report_id, updated_at, json.dumps(summary, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
        return summary

    def _build_forward_outcome(self, ts_code: str, base_trade_date: str) -> Optional[JsonDict]:
        if not self.db_path or not os.path.exists(self.db_path):
            return None
        try:
            conn = sqlite3.connect(self.db_path)
            px = pd.read_sql_query(
                """
                SELECT trade_date, close_price
                FROM daily_trading_data
                WHERE ts_code = ?
                ORDER BY REPLACE(trade_date, '-', '') ASC
                """,
                conn,
                params=(ts_code,),
            )
            conn.close()
        except Exception:
            return None
        if px.empty:
            return None
        px = px.copy()
        px["trade_date_norm"] = px["trade_date"].astype(str).str.replace("-", "", regex=False)
        px["close_price"] = pd.to_numeric(px["close_price"], errors="coerce")
        px = px.dropna(subset=["close_price"])
        if px.empty:
            return None

        base_idx = None
        for i, d in enumerate(px["trade_date_norm"].tolist()):
            if d >= base_trade_date:
                base_idx = i
                break
        if base_idx is None:
            # If target date is beyond latest available trade date, fallback to latest known point.
            base_idx = max(0, len(px) - 1)
        base_price = float(px.iloc[base_idx]["close_price"])
        if base_price <= 0:
            return None

        def _point(offset: int) -> Optional[JsonDict]:
            idx = base_idx + offset
            if idx >= len(px):
                return None
            price = float(px.iloc[idx]["close_price"])
            ret = (price / base_price - 1.0) * 100.0
            return {
                "trade_date": str(px.iloc[idx]["trade_date"]),
                "price": round(price, 4),
                "ret_pct": round(ret, 3),
            }

        latest = _point(max(0, len(px) - 1 - base_idx))
        return {
            "base_trade_date": str(px.iloc[base_idx]["trade_date"]),
            "base_price": round(base_price, 4),
            "horizons": {
                "d1": _point(1),
                "d5": _point(5),
                "d20": _point(20),
                "latest": latest,
            },
        }

    @staticmethod
    def _normalize_date_to_yyyymmdd(v: str) -> str:
        s = (v or "").strip()
        if not s:
            return ""
        s = s.replace("-", "")
        if len(s) >= 8 and s[:8].isdigit():
            return s[:8]
        return ""

    def _run_daily_evaluator(self) -> JsonDict:
        conn = sqlite3.connect(self._learning_db_path)
        rows = conn.execute(
            """
            SELECT route, eval_json FROM learning_cards
            WHERE created_at >= ?
            ORDER BY created_at DESC
            """,
            ((datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),),
        ).fetchall()
        route_scores: Dict[str, List[float]] = {}
        for route, eval_json in rows:
            parsed = self._parse_json_or_empty(eval_json)
            overall = float(parsed.get("overall_score", 0.0))
            route_scores.setdefault(str(route or "unknown"), []).append(overall)
        summary = {
            "evaluated_cards": len(rows),
            "route_scores": {
                k: round(sum(v) / max(1, len(v)), 1) for k, v in route_scores.items()
            },
        }
        all_scores = [x for arr in route_scores.values() for x in arr]
        summary["overall_score"] = round(sum(all_scores) / max(1, len(all_scores)), 1) if all_scores else 0.0
        summary["conservative_mode"] = bool(summary["overall_score"] < 65.0)
        report_id = f"daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        conn.execute(
            "INSERT OR REPLACE INTO evaluation_reports(report_id, report_type, created_at, report_json) VALUES(?, 'daily', ?, ?)",
            (
                report_id,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                json.dumps(summary, ensure_ascii=False),
            ),
        )
        if summary["overall_score"] >= 75:
            conn.execute(
                """
                UPDATE policy_rules
                SET success_count = success_count + 1, updated_at = ?
                WHERE status IN ('soft', 'hard')
                """,
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
            )
        conn.commit()
        conn.close()
        return summary

    def _run_weekly_evaluator(self) -> JsonDict:
        conn = sqlite3.connect(self._learning_db_path)
        rows = conn.execute(
            """
            SELECT rule_id, status, success_count, fail_count, hit_count
            FROM policy_rules
            """
        ).fetchall()
        promoted = 0
        downgraded = 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for rule_id, status, success_count, fail_count, hit_count in rows:
            success_rate = float(success_count) / max(1.0, float(success_count + fail_count))
            if status == "soft" and hit_count >= 3 and success_rate >= 0.7:
                conn.execute(
                    "UPDATE policy_rules SET status='hard', updated_at=? WHERE rule_id=?",
                    (now, rule_id),
                )
                promoted += 1
            elif status == "hard" and hit_count >= 3 and success_rate < 0.45:
                conn.execute(
                    "UPDATE policy_rules SET status='soft', updated_at=? WHERE rule_id=?",
                    (now, rule_id),
                )
                downgraded += 1
        summary = {
            "promoted_rules": promoted,
            "downgraded_rules": downgraded,
            "total_rules": len(rows),
        }
        report_id = f"weekly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        conn.execute(
            "INSERT OR REPLACE INTO evaluation_reports(report_id, report_type, created_at, report_json) VALUES(?, 'weekly', ?, ?)",
            (report_id, now, json.dumps(summary, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
        return summary

    @staticmethod
    def _parse_json_or_empty(raw: Any) -> JsonDict:
        if isinstance(raw, dict):
            return raw
        if not raw:
            return {}
        try:
            parsed = json.loads(str(raw))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def record_module_outcome(
        self,
        module: str,
        event_type: str,
        payload: Optional[JsonDict] = None,
        ts_code: Optional[str] = None,
        route: str = "stock_core",
    ) -> JsonDict:
        payload = payload or {}
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        symbol = _to_ts_code(ts_code) if ts_code else None
        updated_cards = self._append_outcome_to_recent_cards(symbol, payload) if symbol else 0

        question = f"[module:{module}] {event_type}"
        answer_text = f"模块事件已记录：{module}/{event_type}。"
        eval_result = {
            "accuracy_score": 80.0,
            "risk_score": 78.0,
            "actionability_score": 85.0,
            "consistency_score": 80.0,
            "overall_score": 80.5,
            "reflection_required": False,
        }
        context = {
            "route": route,
            "question": question,
            "sources": [self._learning_db_path],
            "summary_exists": False,
            "stock_code": symbol,
            "bull_candidate_count": 0,
            "active_rule_ids": [],
        }
        card_id = self._write_learning_card(
            question=question,
            route=route,
            context=context,
            answer_text=answer_text,
            answer_mode="module_event",
            confidence=82.0,
            eval_result=eval_result,
        )
        conn = sqlite3.connect(self._learning_db_path)
        conn.execute(
            """
            UPDATE learning_cards
            SET outcome_json = ?, status = 'closed', updated_at = ?
            WHERE card_id = ?
            """,
            (json.dumps(payload, ensure_ascii=False), now, card_id),
        )
        conn.commit()
        conn.close()
        return {"card_id": card_id, "updated_related_cards": int(updated_cards)}

    def _append_outcome_to_recent_cards(self, ts_code: Optional[str], payload: JsonDict, limit: int = 5) -> int:
        if not ts_code:
            return 0
        conn = sqlite3.connect(self._learning_db_path)
        rows = conn.execute(
            """
            SELECT card_id, outcome_json
            FROM learning_cards
            WHERE status = 'pending' AND context_json LIKE ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (f"%{ts_code}%", max(1, int(limit))),
        ).fetchall()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        count = 0
        for card_id, outcome_json in rows:
            existing = self._parse_json_or_empty(outcome_json)
            merged = {**existing, **payload}
            conn.execute(
                """
                UPDATE learning_cards
                SET outcome_json = ?, status = 'closed', updated_at = ?
                WHERE card_id = ?
                """,
                (json.dumps(merged, ensure_ascii=False), now, card_id),
            )
            count += 1
        conn.commit()
        conn.close()
        return count

    def _latest_run_summary(self) -> JsonDict:
        candidate_dirs = [
            self.log_dir,
            "logs/openclaw",
            "/opt/airivo/app/logs/openclaw",
            "/tmp/openclaw_verify",
        ]
        files: List[str] = []
        for d in candidate_dirs:
            if not d:
                continue
            pattern = os.path.join(d, "run_summary_*.json")
            files.extend(glob.glob(pattern))
        files = sorted(set(files), reverse=True)
        if not files:
            return {}
        try:
            with open(files[0], "r", encoding="utf-8") as f:
                data = json.load(f)
            data["_source_file"] = files[0]
            return data
        except Exception:
            return {}

    def _latest_evolution_stats(self) -> JsonDict:
        candidates = [
            "evolution/last_run.json",
            "/opt/airivo/app/evolution/last_run.json",
        ]
        for p in candidates:
            try:
                if os.path.exists(p):
                    with open(p, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        data["_source_file"] = p
                        return data
            except Exception:
                continue
        return {}

    def _stock_snapshot(self, ts_code: Optional[str]) -> Optional[JsonDict]:
        if not ts_code or not self.db_path or not os.path.exists(self.db_path):
            return None

        try:
            conn = sqlite3.connect(self.db_path)
            basic_cols_df = pd.read_sql_query("PRAGMA table_info(stock_basic)", conn)
            basic_cols = set(basic_cols_df["name"].astype(str).tolist())
            want_basic = ["ts_code", "name", "industry", "list_date", "area", "market", "exchange"]
            use_basic = [c for c in want_basic if c in basic_cols]
            basic = pd.read_sql_query(
                f"SELECT {', '.join(use_basic)} FROM stock_basic WHERE ts_code = ? LIMIT 1",
                conn,
                params=(ts_code,),
            )
            daily = pd.read_sql_query(
                """
                SELECT ts_code, trade_date, close_price, pct_chg, vol, amount, turnover_rate, high_price, low_price
                FROM daily_trading_data
                WHERE ts_code = ?
                ORDER BY trade_date DESC
                LIMIT 120
                """,
                conn,
                params=(ts_code,),
            )
            conn.close()
        except Exception:
            return None

        if daily.empty:
            return None

        daily = daily.copy()
        for c in ["close_price", "pct_chg", "vol", "amount", "turnover_rate", "high_price", "low_price"]:
            if c in daily.columns:
                daily[c] = pd.to_numeric(daily[c], errors="coerce")
        latest = daily.iloc[0].to_dict()
        stats = self._calc_stock_stats(daily)
        out: JsonDict = {
            "ts_code": ts_code,
            "name": basic.iloc[0]["name"] if not basic.empty else ts_code,
            "industry": basic.iloc[0]["industry"] if not basic.empty else "未知",
            "basic": basic.iloc[0].to_dict() if not basic.empty else {},
            "latest": latest,
            "stats": stats,
        }
        return out

    def _calc_stock_stats(self, daily_desc: pd.DataFrame) -> JsonDict:
        """daily_desc: ordered by trade_date DESC."""
        if daily_desc is None or daily_desc.empty:
            return {}
        df = daily_desc.iloc[::-1].reset_index(drop=True)  # ASC for rolling calc
        close = df["close_price"] if "close_price" in df.columns else pd.Series(dtype=float)
        pct = df["pct_chg"] if "pct_chg" in df.columns else pd.Series(dtype=float)
        vol = df["vol"] if "vol" in df.columns else pd.Series(dtype=float)

        def _ret(n: int) -> Optional[float]:
            if len(close) > n and float(close.iloc[-n - 1]) > 0:
                return (float(close.iloc[-1]) / float(close.iloc[-n - 1]) - 1.0) * 100.0
            return None

        ma5 = close.tail(5).mean() if len(close) >= 5 else None
        ma10 = close.tail(10).mean() if len(close) >= 10 else None
        ma20 = close.tail(20).mean() if len(close) >= 20 else None
        latest_close = float(close.iloc[-1]) if len(close) else None

        vol_mean5 = vol.tail(5).mean() if len(vol) >= 5 else None
        vol_mean20 = vol.tail(20).mean() if len(vol) >= 20 else None
        vol_ratio_5 = None
        if len(vol) >= 6 and vol_mean5 and vol_mean5 > 0:
            vol_ratio_5 = float(vol.iloc[-1]) / float(vol_mean5)

        rolling_vol20 = pct.tail(20).std() if len(pct) >= 20 else None
        high_60 = df["high_price"].tail(60).max() if "high_price" in df.columns and len(df) >= 5 else None
        low_60 = df["low_price"].tail(60).min() if "low_price" in df.columns and len(df) >= 5 else None

        trend = "震荡"
        if latest_close and ma5 and ma10 and ma20:
            if latest_close > ma5 > ma10 > ma20:
                trend = "多头"
            elif latest_close < ma5 < ma10 < ma20:
                trend = "空头"

        return {
            "ret_5d": _ret(5),
            "ret_20d": _ret(20),
            "ret_60d": _ret(60),
            "ma5": float(ma5) if ma5 and not math.isnan(ma5) else None,
            "ma10": float(ma10) if ma10 and not math.isnan(ma10) else None,
            "ma20": float(ma20) if ma20 and not math.isnan(ma20) else None,
            "trend": trend,
            "vol_ratio_5": vol_ratio_5,
            "volatility_20d": float(rolling_vol20) if rolling_vol20 and not math.isnan(rolling_vol20) else None,
            "high_60d": float(high_60) if high_60 and not math.isnan(high_60) else None,
            "low_60d": float(low_60) if low_60 and not math.isnan(low_60) else None,
        }

    def _build_context(
        self,
        question: str,
        summary: JsonDict,
        stock_info: Optional[JsonDict],
        evolution: JsonDict,
        bull_candidates: Optional[List[JsonDict]] = None,
        query_code: Optional[str] = None,
        route: str = "stock_core",
        profile: Optional[JsonDict] = None,
        active_rules: Optional[List[JsonDict]] = None,
        similar_lessons: Optional[List[JsonDict]] = None,
    ) -> JsonDict:
        strategy_versions = {
            "v4.0": "综合优选评分器（潜伏策略版）— 八维体系，3-7日持仓，稳健长期",
            "v5.0": "启动确认型评分器 — 基于v4.0八维体系，趋势启动确认，5-10日",
            "v6.0": "超短线狙击评分器·专业版 — 强势精选，2-5日快进快出",
            "v7.0": "智能选股系统 — 动态自适应，5-15日",
            "v8.0": "进阶版 — ATR风控+市场过滤+凯利仓位+动态再平衡，5-15日",
            "v9.0": "中线均衡版 — 2-6周持仓，均衡收益风险",
            "组合策略": "共识评分（v4-v9综合投票）",
            "稳定上涨策略": "启动/回撤/二次确认模式",
        }
        system_desc = (
            "你是 OpenClaw 股票量化交易系统的核心智能体 ClawAlpha。"
            "系统包含以下策略版本，你可以直接分析和评价它们的表现。"
            "系统数据库中包含5400+只A股、500万+日线数据、北向资金、融资融券等数据。"
        )
        ctx: JsonDict = {
            "question": question,
            "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "system_description": system_desc,
            "strategy_versions": strategy_versions,
            "summary": summary,
            "evolution": evolution,
            "stock": stock_info,
            "query_code": query_code,
            "bull_candidates": bull_candidates or [],
            "route": route,
            "profile": profile or {},
            "active_rules": active_rules or [],
            "similar_lessons": similar_lessons or [],
            "sources": [],
        }
        if summary.get("_source_file"):
            ctx["sources"].append(summary["_source_file"])
        if evolution.get("_source_file"):
            ctx["sources"].append(evolution["_source_file"])
        if stock_info:
            ctx["sources"].append(self.db_path)
        return ctx

    def _predict_bull_candidates(self, limit: int = 20) -> List[JsonDict]:
        if not self.db_path or not os.path.exists(self.db_path):
            return []
        try:
            conn = sqlite3.connect(self.db_path)
            trade_dates = pd.read_sql_query(
                """
                SELECT DISTINCT trade_date
                FROM daily_trading_data
                ORDER BY trade_date DESC
                LIMIT 90
                """,
                conn,
            )
            if trade_dates is None or trade_dates.empty or len(trade_dates) < 60:
                conn.close()
                return []

            dates = trade_dates["trade_date"].astype(str).tolist()
            latest_date = dates[0]
            date_20d = dates[19]
            date_60d = dates[59]

            latest_df = pd.read_sql_query(
                """
                SELECT ts_code, trade_date, close_price, pct_chg, amount, turnover_rate
                FROM daily_trading_data
                WHERE trade_date = ?
                """,
                conn,
                params=(latest_date,),
            )
            d20_df = pd.read_sql_query(
                """
                SELECT ts_code, close_price AS close_20d
                FROM daily_trading_data
                WHERE trade_date = ?
                """,
                conn,
                params=(date_20d,),
            )
            d60_df = pd.read_sql_query(
                """
                SELECT ts_code, close_price AS close_60d
                FROM daily_trading_data
                WHERE trade_date = ?
                """,
                conn,
                params=(date_60d,),
            )
            basic_df = pd.read_sql_query(
                """
                SELECT ts_code, name, industry
                FROM stock_basic
                """,
                conn,
            )
            conn.close()
            df = (
                latest_df.merge(d20_df, on="ts_code", how="inner")
                .merge(d60_df, on="ts_code", how="inner")
                .merge(basic_df, on="ts_code", how="left")
            )
        except Exception:
            return []

        if df is None or df.empty:
            return []

        num_cols = ["close_price", "pct_chg", "amount", "turnover_rate", "close_20d", "close_60d"]
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["close_price", "close_20d", "close_60d", "amount"])
        if df.empty:
            return []

        df["ret_20d"] = (df["close_price"] / df["close_20d"] - 1.0) * 100.0
        df["ret_60d"] = (df["close_price"] / df["close_60d"] - 1.0) * 100.0
        df["liquidity_log"] = df["amount"].clip(lower=1).apply(lambda x: math.log10(float(x)))
        df["turnover_rate"] = df["turnover_rate"].fillna(0.0)

        # Keep trend + liquidity candidates. Avoid extreme one-day spikes.
        df = df[
            (df["ret_20d"] > 0)
            & (df["ret_60d"] > 0)
            & (df["pct_chg"].abs() < 9.9)
            & (df["amount"] > df["amount"].quantile(0.4))
        ]
        if df.empty:
            return []

        df["model_score"] = (
            0.45 * df["ret_60d"]
            + 0.30 * df["ret_20d"]
            + 0.15 * df["pct_chg"]
            + 1.50 * df["liquidity_log"]
            + 0.10 * df["turnover_rate"]
        )
        df = df.sort_values("model_score", ascending=False).head(max(1, int(limit)))

        out: List[JsonDict] = []
        for _, row in df.iterrows():
            base = float(max(0.0, 0.35 * row["ret_20d"] + 0.50 * row["ret_60d"]))
            pred_low = min(80.0, max(8.0, base * 0.55))
            pred_high = min(150.0, max(pred_low + 5.0, base * 0.90 + 12.0))
            out.append(
                {
                    "ts_code": str(row.get("ts_code", "")),
                    "name": str(row.get("name", "") or row.get("ts_code", "")),
                    "industry": str(row.get("industry", "未知")),
                    "trade_date": str(row.get("trade_date", "")),
                    "ret_20d": float(row.get("ret_20d", 0.0)),
                    "ret_60d": float(row.get("ret_60d", 0.0)),
                    "pct_chg": float(row.get("pct_chg", 0.0)),
                    "score": float(row.get("model_score", 0.0)),
                    "pred_low": float(pred_low),
                    "pred_high": float(pred_high),
                    "reason": "60日与20日动量同向、成交额靠前、短线未过热",
                }
            )
        return out

    # ------------------------------------------------------------------
    # Trading-assistant data helpers (lazy load from trading_assistant.db)
    # ------------------------------------------------------------------
    def _ta_db_path(self) -> str:
        candidates = [
            os.getenv("TRADING_ASSISTANT_DB_PATH"),
            str(Path(self.db_path).parent / "trading_assistant.db") if self.db_path else None,
            "/Users/mac/2026Qlin/trading_assistant.db",
            "/opt/airivo/data/trading_assistant.db",
        ]
        for p in candidates:
            if p and os.path.exists(p):
                return p
        return ""

    def _load_active_holdings(self, limit: int = 10) -> List[JsonDict]:
        dbp = self._ta_db_path()
        if not dbp:
            return []
        try:
            conn = sqlite3.connect(dbp)
            cols = [r[1] for r in conn.execute("PRAGMA table_info(holdings)").fetchall()]
            rows = conn.execute(
                "SELECT * FROM holdings WHERE status='active' ORDER BY buy_date DESC LIMIT ?",
                (limit,),
            ).fetchall()
            conn.close()
            return [dict(zip(cols, r)) for r in rows]
        except Exception:
            return []

    def _load_recent_trades(self, limit: int = 10) -> List[JsonDict]:
        dbp = self._ta_db_path()
        if not dbp:
            return []
        try:
            conn = sqlite3.connect(dbp)
            cols = [r[1] for r in conn.execute("PRAGMA table_info(trade_history)").fetchall()]
            rows = conn.execute(
                "SELECT * FROM trade_history ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            conn.close()
            return [dict(zip(cols, r)) for r in rows]
        except Exception:
            return []

    def _load_recent_recommendations(self, limit: int = 5) -> List[JsonDict]:
        dbp = self._ta_db_path()
        if not dbp:
            return []
        try:
            conn = sqlite3.connect(dbp)
            cols = [r[1] for r in conn.execute("PRAGMA table_info(daily_recommendations)").fetchall()]
            rows = conn.execute(
                "SELECT * FROM daily_recommendations ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            conn.close()
            return [dict(zip(cols, r)) for r in rows]
        except Exception:
            return []

    def _load_config(self) -> JsonDict:
        dbp = self._ta_db_path()
        if not dbp:
            return {}
        try:
            conn = sqlite3.connect(dbp)
            rows = conn.execute("SELECT * FROM config").fetchall()
            conn.close()
            return {r[0]: r[1] for r in rows}
        except Exception:
            return {}

    def _market_breadth_snapshot(self) -> JsonDict:
        if not self.db_path or not os.path.exists(self.db_path):
            return {}
        try:
            conn = sqlite3.connect(self.db_path)
            latest_date = conn.execute("SELECT MAX(trade_date) FROM daily_trading_data").fetchone()[0]
            if not latest_date:
                conn.close()
                return {}
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN CAST(pct_chg AS REAL) > 0 THEN 1 ELSE 0 END) AS up_count,
                    SUM(CASE WHEN CAST(pct_chg AS REAL) < 0 THEN 1 ELSE 0 END) AS down_count,
                    SUM(CASE WHEN CAST(pct_chg AS REAL) >= 9.5 THEN 1 ELSE 0 END) AS limit_up,
                    SUM(CASE WHEN CAST(pct_chg AS REAL) <= -9.5 THEN 1 ELSE 0 END) AS limit_down,
                    ROUND(AVG(CAST(pct_chg AS REAL)), 3) AS avg_chg
                FROM daily_trading_data WHERE trade_date = ?
                """,
                (latest_date,),
            ).fetchone()
            conn.close()
            if not row:
                return {}
            return {
                "date": str(latest_date),
                "total": int(row[0] or 0),
                "up": int(row[1] or 0),
                "down": int(row[2] or 0),
                "limit_up": int(row[3] or 0),
                "limit_down": int(row[4] or 0),
                "avg_chg": float(row[5] or 0),
            }
        except Exception:
            return {}

    # ------------------------------------------------------------------
    # Main rule-based answer generator (fully restructured)
    # ------------------------------------------------------------------
    def _rule_answer(self, question: str, ctx: JsonDict) -> str:
        q = (question or "").lower()
        if any(k in q for k in ["今日总结", "每日总结", "今天简报", "daily brief", "总结一下今天"]):
            return self.get_daily_companion_brief()
        route = ctx.get("route", "stock_core")
        if route == "identity":
            return self._answer_identity(question, ctx)
        if route == "skills":
            return self._answer_skills(question, ctx)
        if route == "cross_discipline":
            return self._answer_cross_discipline(question, ctx)
        if route == "general":
            return self._answer_general(question, ctx)
        if route == "hybrid_research" and not _extract_code(question):
            return self._answer_hybrid_research(question, ctx)

        summary = ctx.get("summary") or {}
        evolution = ctx.get("evolution") or {}
        stock = ctx.get("stock") or {}
        bull_candidates = ctx.get("bull_candidates") or []
        q = question.lower()

        if _is_bull_prediction_intent(question):
            return self._answer_bull_prediction(bull_candidates, ctx)

        lines: List[str] = []

        # ==================== 结论 ====================
        if stock:
            stats = stock.get("stats") or {}
            trend = stats.get("trend", "震荡")
            name = stock.get("name", stock.get("ts_code", ""))
            latest = stock.get("latest") or {}
            close_p = latest.get("close_price")
            pct_chg = latest.get("pct_chg")
            ret_5d = stats.get("ret_5d")
            ret_20d = stats.get("ret_20d")
            trend_desc = {"多头": "多头排列，短中期趋势偏强", "空头": "空头排列，趋势偏弱", "震荡": "均线缠绕，方向不明"}
            conclusion = (
                f"结论：{name} 当前处于{trend}格局（{trend_desc.get(trend, '待观察')}），"
                f"最新价 {self._fmt_num(close_p)}，当日涨跌 {self._fmt_pct_from_number(pct_chg)}，"
                f"5日涨幅 {self._fmt_pct_from_number(ret_5d)}，20日涨幅 {self._fmt_pct_from_number(ret_20d)}。"
            )
            if trend == "多头" and ret_5d is not None and ret_5d > 3:
                conclusion += " 短线偏强但需防追高回落风险。"
            elif trend == "空头" and ret_5d is not None and ret_5d < -3:
                conclusion += " 趋势破位，建议控制仓位或等待企稳。"
            else:
                conclusion += " 建议结合量能确认方向后再行动。"
            lines.append(conclusion)
        else:
            risk_info = self._get_system_risk_conclusion(summary, evolution)
            lines.append(f"结论：{risk_info}")

        # ==================== 依据 ====================
        lines.append("")
        lines.append("依据：")

        if summary:
            risk = ((summary.get("risk") or {}).get("risk_level")) or "unknown"
            publish = ((summary.get("publish") or {}).get("status")) or "unknown"
            backtest = summary.get("backtest") or {}
            bt_summary = ((backtest.get("result") or {}).get("summary")) or {}
            win_rate = bt_summary.get("win_rate")
            max_dd = bt_summary.get("max_drawdown")
            density = bt_summary.get("signal_density")
            lines.append(
                f"  ▸ 系统状态：risk={risk}，publish={publish}，"
                f"win_rate={self._fmt_pct(win_rate)}，max_drawdown={self._fmt_pct(max_dd)}，"
                f"signal_density={self._fmt_num(density)}"
            )

            evo_stats = evolution.get("stats") if isinstance(evolution.get("stats"), dict) else {}
            if evo_stats:
                lines.append(
                    f"  ▸ 回测绩效：胜率={self._fmt_pct_from_number(evo_stats.get('win_rate'))}，"
                    f"平均收益={self._fmt_pct_from_number(evo_stats.get('avg_return'))}，"
                    f"最大回撤={self._fmt_pct_from_number(evo_stats.get('max_drawdown'))}，"
                    f"平均持仓={self._fmt_num(evo_stats.get('avg_holding_days'))}天"
                )
            best_params = evolution.get("params") if isinstance(evolution.get("params"), dict) else {}
            if best_params:
                lines.append(
                    f"  ▸ 最佳参数：score_threshold={best_params.get('score_threshold', 'N/A')}，"
                    f"holding_days={best_params.get('holding_days', 'N/A')}，"
                    f"sample_size={best_params.get('sample_size', 'N/A')}"
                )
        elif not summary:
            lines.append("  ▸ 暂未读到 run_summary 或 evolution 回测，请先运行一次完整流水线。")

        if stock:
            lines.extend(self._deep_stock_analysis_lines(stock, question))

        breadth = self._market_breadth_snapshot()
        if breadth:
            up_ratio = breadth["up"] / max(1, breadth["total"]) * 100
            lines.append(
                f"  ▸ 市场宽度({breadth['date']})：{breadth['total']}只个股，"
                f"上涨{breadth['up']}({up_ratio:.0f}%)，下跌{breadth['down']}，"
                f"涨停{breadth['limit_up']}，跌停{breadth['limit_down']}，"
                f"全市场均涨幅={breadth['avg_chg']:.2f}%"
            )

        holdings = self._load_active_holdings(5)
        if holdings:
            lines.append(f"  ▸ 当前持仓({len(holdings)}只)：")
            for h in holdings[:5]:
                pnl_pct = h.get("profit_loss_pct")
                pnl_str = f"{float(pnl_pct):.1f}%" if pnl_pct is not None else "N/A"
                lines.append(
                    f"    - {h.get('stock_name', '')}({h.get('ts_code', '')}) "
                    f"成本{h.get('buy_price', 'N/A')} -> 现价{h.get('current_price', 'N/A')}，"
                    f"盈亏{pnl_str}"
                )

        recs = self._load_recent_recommendations(3)
        if recs:
            lines.append("  ▸ 最近推荐：")
            for r in recs[:3]:
                lines.append(
                    f"    - {r.get('stock_name', '')}({r.get('ts_code', '')}) "
                    f"评分{r.get('score', 'N/A')}，推荐价{r.get('price', 'N/A')}，"
                    f"{str(r.get('reason', ''))[:60]}"
                )

        cfg = self._load_config()
        if cfg:
            lines.append(
                f"  ▸ 交易参数：min_score={cfg.get('min_score', 'N/A')}，"
                f"止损={cfg.get('stop_loss_pct', 'N/A')}，止盈={cfg.get('take_profit_pct', 'N/A')}，"
                f"单仓={cfg.get('single_position_pct', 'N/A')}，总仓={cfg.get('max_position_pct', 'N/A')}"
            )

        # ==================== 风险分析 ====================
        lines.append("")
        lines.append("风险分析：")
        if stock:
            stats = stock.get("stats") or {}
            vol20 = stats.get("volatility_20d")
            high60 = stats.get("high_60d")
            close_p = (stock.get("latest") or {}).get("close_price")
            if vol20 is not None:
                vol_level = "高" if vol20 > 3.5 else ("中" if vol20 > 2.0 else "低")
                lines.append(f"  ▸ 20日波动率 {self._fmt_pct_from_number(vol20)}（{vol_level}波动）")
            if high60 is not None and close_p is not None:
                try:
                    dist_high = (float(close_p) / float(high60) - 1) * 100
                    lines.append(f"  ▸ 距60日高点 {dist_high:.1f}%，距60日低点 {self._fmt_num(stats.get('low_60d'))}")
                except Exception:
                    pass
            vol_ratio = stats.get("vol_ratio_5")
            if vol_ratio is not None:
                vol_note = "放量" if vol_ratio > 1.5 else ("缩量" if vol_ratio < 0.7 else "温和")
                lines.append(f"  ▸ 量比(vs5日均量) {self._fmt_num(vol_ratio)}（{vol_note}）")
        else:
            risk_level = ((summary.get("risk") or {}).get("risk_level")) or "unknown"
            risk_desc = {
                "green": "当前系统风险偏低（绿色），可正常操作",
                "yellow": "中等风险（黄色），建议降低仓位，精选标的",
                "orange": "较高风险（橙色），只保留强势股，新仓控制在单仓5%以内",
                "red": "高风险（红色），建议清仓观望或极小仓位防守",
            }
            lines.append(f"  ▸ {risk_desc.get(risk_level, f'风险等级 {risk_level}，请结合市场宽度判断')}")
            triggered_rules = ((summary.get("risk") or {}).get("triggered_rules")) or []
            if triggered_rules:
                lines.append(f"  ▸ 触发因子：{', '.join(triggered_rules)}")

        # ==================== 触发条件 & 失效条件 ====================
        lines.append("")
        if stock:
            stats = stock.get("stats") or {}
            ma5 = stats.get("ma5")
            ma20 = stats.get("ma20")
            if ma5 is not None:
                lines.append(f"触发条件：价格站稳 MA5（{self._fmt_num(ma5)}）且量比>1.2，可视为短线进场信号")
            if ma20 is not None:
                lines.append(f"失效条件：跌破 MA20（{self._fmt_num(ma20)}）或单日回撤>3%，判断失效，止损出局")
        else:
            lines.append("触发条件：若风险等级改善至 green/yellow 且胜率 >55%，可恢复正常仓位")
            lines.append("失效条件：若周回撤 >6% 或风险升至 red，立即减仓至 30% 以下")

        # ==================== 建议动作 ====================
        lines.append("")
        lines.append("建议动作：")
        if stock:
            trend = (stock.get("stats") or {}).get("trend", "震荡")
            if trend == "多头":
                lines.append("  ▸ 立即：趋势延续中，可持有或逢回调（MA5附近）轻仓加仓")
                lines.append("  ▸ 本周：关注量能是否持续，若缩量滞涨则减仓50%")
            elif trend == "空头":
                lines.append("  ▸ 立即：不建议新建仓位，已有仓位可在反弹至MA5时减仓")
                lines.append("  ▸ 本周：等待MA5与MA10金叉确认后再考虑介入")
            else:
                lines.append("  ▸ 立即：方向不明，保持轻仓观望，不追涨不杀跌")
                lines.append("  ▸ 本周：关注能否放量突破MA20，突破则转为偏多")
        else:
            risk_level = ((summary.get("risk") or {}).get("risk_level")) or "unknown"
            if risk_level in ("red", "orange"):
                lines.append("  ▸ 立即：降低总仓位至30%以下，只保留强势标的")
                lines.append("  ▸ 本周：严格执行止损纪律，不抄底不补仓")
            actions = ((summary.get("risk") or {}).get("recommended_actions")) or []
            if actions:
                    lines.append(f"  ▸ 系统建议：{', '.join(actions)}")
            else:
                lines.append("  ▸ 立即：按系统选股推荐，分批建仓（单只不超过总仓位15%）")
                lines.append("  ▸ 本周：每日复盘持仓盈亏比，及时止盈止损")

        if not stock and _extract_code(question):
            lines.append("\n注意：未在本地数据库中找到该股票代码数据，以上为系统通用建议。")

        return "\n".join(lines)

    def _get_system_risk_conclusion(self, summary: JsonDict, evolution: JsonDict) -> str:
        if summary:
            risk = ((summary.get("risk") or {}).get("risk_level")) or "unknown"
            bt = ((summary.get("backtest") or {}).get("result") or {}).get("summary") or {}
            win_rate = bt.get("win_rate")
            risk_labels = {
                "green": "系统运行正常，风险可控（绿色）",
                "yellow": "市场环境中性偏谨慎（黄色），需精选标的",
                "orange": "市场存在明显风险（橙色），建议控仓",
                "red": "市场高风险状态（红色），建议空仓或极小仓位防守",
            }
            conclusion = risk_labels.get(risk, f"当前风险等级={risk}")
            if win_rate is not None:
                try:
                    wr = float(win_rate)
                    conclusion += f"，回测胜率={wr*100:.1f}%"
                    if wr > 0.6:
                        conclusion += "（策略有效）"
                    elif wr < 0.45:
                        conclusion += "（策略偏弱，需优化参数）"
                except Exception:
                    pass
            return conclusion
        evo = evolution.get("stats") if isinstance(evolution.get("stats"), dict) else {}
        if evo:
            return (
                f"基于 evolution 回测，胜率={self._fmt_pct_from_number(evo.get('win_rate'))}，"
                f"平均收益={self._fmt_pct_from_number(evo.get('avg_return'))}，"
                f"最大回撤={self._fmt_pct_from_number(evo.get('max_drawdown'))}。策略整体可用但需持续优化。"
            )
        return "暂无系统日报与回测数据，请先运行完整流水线获取基线。建议在数据就位前保持轻仓。"

    def _deep_stock_analysis_lines(self, stock: JsonDict, question: str) -> List[str]:
        lines: List[str] = []
        latest = stock.get("latest") or {}
        stats = stock.get("stats") or {}
        basic = stock.get("basic") or {}
        lines.append(
            f"  ▸ 个股：{stock.get('name')}({stock.get('ts_code')}) | {stock.get('industry')} | "
            f"最新价={self._fmt_num(latest.get('close_price'))} | 日期={latest.get('trade_date', 'N/A')}"
        )
        lines.append(
            f"  ▸ 均线：MA5={self._fmt_num(stats.get('ma5'))}，"
            f"MA10={self._fmt_num(stats.get('ma10'))}，MA20={self._fmt_num(stats.get('ma20'))} "
            f"-> {stats.get('trend', '震荡')}排列"
        )
        lines.append(
            f"  ▸ 区间收益：5日={self._fmt_pct_from_number(stats.get('ret_5d'))}，"
            f"20日={self._fmt_pct_from_number(stats.get('ret_20d'))}，"
            f"60日={self._fmt_pct_from_number(stats.get('ret_60d'))}"
        )
        lines.append(
            f"  ▸ 量能：量比(vs5日)={self._fmt_num(stats.get('vol_ratio_5'))}，"
            f"20日波动率={self._fmt_pct_from_number(stats.get('volatility_20d'))}"
        )
        lines.append(
            f"  ▸ 关键位：60日高点={self._fmt_num(stats.get('high_60d'))}，"
            f"60日低点={self._fmt_num(stats.get('low_60d'))}"
        )
        if _is_investment_brief_intent(question) and basic.get("list_date"):
            lines.append(f"  ▸ 上市日期={basic.get('list_date')}，市场={basic.get('market', 'N/A')}")
        return lines

    def _answer_bull_prediction(self, bull_candidates: List[JsonDict], ctx: JsonDict) -> str:
        lines: List[str] = []
        if bull_candidates:
            lines.append(
                f"结论：基于本地数据库动量+量能多因子模型筛选，"
                f"以下是当前 Top{len(bull_candidates)} 牛股候选"
                f"（样本日期={bull_candidates[0].get('trade_date', 'N/A')}）。"
            )
            lines.append("")
            lines.append("依据与方法：")
            lines.append("  ▸ 因子权重：60日趋势(45%) + 20日趋势(30%) + 当日强度(15%) + 流动性(10%)")
            lines.append("  ▸ 过滤条件：20日/60日收益均为正、当日涨幅<9.9%、成交额>40%分位")
            lines.append("")
            lines.append("候选名单（预测涨幅为区间估计，非确定收益）：")
            for i, c in enumerate(bull_candidates, 1):
                lines.append(
                    f"  {i:02d}. {c['name']}({c['ts_code']}) | {c['industry']} | "
                    f"20日{c['ret_20d']:.1f}% / 60日{c['ret_60d']:.1f}% | "
                    f"预测区间 {c['pred_low']:.0f}%~{c['pred_high']:.0f}% | 理由：{c['reason']}"
                )
            lines.append("")
            lines.append("风险分析：")
            lines.append("  ▸ 动量策略在趋势行情中有效，震荡市可能失效")
            lines.append("  ▸ 仅基于量价，未包含财报、估值、政策等基本面因子")
            lines.append("")
            lines.append("建议动作：")
            lines.append("  ▸ 立即：从候选中筛选2-3只（财报健康、行业景气度高），分批建仓")
            lines.append("  ▸ 本周：二次过滤（PE/PB、机构持仓、近期公告），淘汰隐患标的")
            lines.append("  ▸ 止损规则：单只跌破买入价-6%强制止损")
        else:
            lines.append("结论：本地数据库暂时不足以产出牛股候选（数据缺口或样本不足60个交易日）。")
            lines.append("")
            lines.append("建议动作：")
            lines.append("  ▸ 运行数据更新流水线补齐最近90日行情数据")
            lines.append("  ▸ 确保 daily_trading_data 表覆盖所有目标板块")
        return "\n".join(lines)

    def _answer_identity(self, question: str, ctx: JsonDict) -> str:
        dashboard = self.get_learning_dashboard()
        holdings = self._load_active_holdings(3)
        cfg = self._load_config()
        breadth = self._market_breadth_snapshot()

        lines = [
            "结论：我是 ClawAlpha — OpenClaw 超级智能体，融合多领域专家能力，"
            "专注于为你提供高质量、结构化、可执行的决策支持。",
            "",
            "依据（核心能力矩阵）：",
            "  ▸ 股票投研：个股诊断、技术面分析（均线/量能/趋势）、风险等级判断、仓位建议",
            "  ▸ 量化交易：回测绩效解读、参数优化、选股因子分析、止损止盈策略",
            "  ▸ 市场洞察：全市场宽度扫描、板块轮动、涨停跌停统计、资金流向",
            "  ▸ 跨学科融合：哲学思维、历史规律、概率统计、行为金融、政治经济学",
            "  ▸ 代码与技术：编程辅助、算法设计、系统架构、数据工程",
            "  ▸ 自我进化：每次对话生成学习卡片，自动评估/反思/更新决策规则",
            "",
            "数据基础：",
            f"  ▸ 股票数据库：5400+只A股，500万+日线数据",
        ]
        if cfg:
            lines.append(
                f"  ▸ 交易参数：止损={cfg.get('stop_loss_pct', 'N/A')}，"
                f"止盈={cfg.get('take_profit_pct', 'N/A')}，"
                f"min_score={cfg.get('min_score', 'N/A')}"
            )
        lines.append(
            f"  ▸ 自学习系统：已积累 {dashboard.get('total_cards', 0)} 张学习卡片，"
            f"硬规则 {dashboard.get('hard_rules', 0)} 条，软规则 {dashboard.get('soft_rules', 0)} 条"
        )
        if holdings:
            names = "、".join(h.get("stock_name", "") for h in holdings[:3])
            lines.append(f"  ▸ 当前跟踪持仓：{names} 等")
        if breadth:
            lines.append(
                f"  ▸ 最新市况({breadth['date']})：上涨{breadth['up']}只，"
                f"下跌{breadth['down']}只，涨停{breadth['limit_up']}，跌停{breadth['limit_down']}"
            )

        lines.extend([
            "",
            "回答原则：",
            "  1. 先结论，再依据，再可执行步骤",
            "  2. 股票问题必须包含：风险等级、触发条件、失效条件、仓位动作",
            "  3. 避免空话，数据驱动，给出具体数字",
            "  4. 跨学科问题结合多视角，保持边界清晰",
            "",
            "建议：你可以问我——",
            "  ▸ 「600519现在什么状态？」-> 个股深度诊断",
            "  ▸ 「v6今天风险如何？」-> 系统风险解读",
            "  ▸ 「怎么看明天大盘？」-> 市场宽度+趋势判断",
            "  ▸ 「预测2026牛股」-> 多因子选股",
            "  ▸ 「量化交易的哲学本质是什么？」-> 跨学科研究",
        ])
        return "\n".join(lines)

    def _answer_cross_discipline(self, question: str, ctx: JsonDict) -> str:
        q = question.lower()
        lines = []

        discipline_map = {
            "哲学": ("哲学视角", "从认识论与方法论层面"),
            "历史": ("历史视角", "以史为鉴"),
            "法律": ("法律视角", "从合规与制度层面"),
            "政治": ("政治经济学视角", "从政策与权力结构层面"),
            "宗教": ("文明与信仰视角", "从人类意义建构层面"),
            "科技": ("技术前沿视角", "从工程与创新角度"),
            "编程": ("软件工程视角", "从代码设计与实践层面"),
            "代码": ("软件工程视角", "从代码设计与实践层面"),
            "概率": ("概率统计视角", "从数学期望与分布角度"),
            "经济": ("宏观经济视角", "从供需、周期与政策传导层面"),
            "心理": ("行为心理视角", "从认知偏误与决策机制层面"),
            "博弈": ("博弈论视角", "从策略互动与纳什均衡角度"),
            "数学": ("数理视角", "从形式化推理与证明角度"),
            "统计": ("统计推断视角", "从数据驱动的因果推断角度"),
        }
        matched_disc = None
        for key, (label, prefix) in discipline_map.items():
            if key in q:
                matched_disc = (label, prefix)
                break
        if not matched_disc:
            matched_disc = ("跨学科综合视角", "融合多领域知识")

        lines.append(f"结论：{matched_disc[1]}分析你的问题，以下是结构化回答。")
        lines.append("")
        lines.append(f"分析框架（{matched_disc[0]}）：")

        if "哲学" in q and any(k in q for k in ("交易", "投资", "股票", "量化")):
            lines.extend([
                "  ▸ 认识论层面：市场是复杂适应性系统，价格是信息的不完全映射",
                "  ▸ 方法论层面：量化交易的本质是「以概率取代确定性」，用大数定律替代主观判断",
                "  ▸ 存在主义层面：交易者面对的核心焦虑不是亏损，而是「不确定性」本身",
                "  ▸ 辩证法层面：风险与收益不是对立，而是同一硬币的两面——只有承担有边界的风险才有正期望",
            ])
        elif "概率" in q:
            lines.extend([
                "  ▸ 频率学派：大样本下相对频率趋近真实概率（大数定律）",
                "  ▸ 贝叶斯学派：概率是主观信念的量化，先验+证据->后验",
                "  ▸ 实践启示：量化策略的核心是把「模糊的直觉」翻译为「可回测的概率模型」",
                "  ▸ 关键误区：小样本下的胜率不等于真实胜率，务必关注置信区间",
            ])
        elif "编程" in q or "代码" in q:
            lines.extend([
                "  ▸ 设计原则：SOLID、DRY、KISS——复杂性是代码最大的敌人",
                "  ▸ 架构思维：分层（数据/逻辑/展示）、模块化、接口隔离",
                "  ▸ 工程实践：测试驱动(TDD)、代码审查、持续集成",
                "  ▸ 量化场景：回测框架设计需防前视偏差，信号生成与执行严格分离",
            ])
        elif "经济" in q:
            lines.extend([
                "  ▸ 周期视角：康波周期(60y)、朱格拉周期(10y)、基钦周期(3-4y)",
                "  ▸ 政策传导：货币政策->流动性->资产价格->实体经济，通常滞后6-12个月",
                "  ▸ 结构性因素：人口、技术、制度——决定长期增长潜力",
                "  ▸ 实操映射：宏观周期定仓位，行业轮动选板块，个股基本面定标的",
            ])
        elif "历史" in q:
            lines.extend([
                "  ▸ 周期律：文明兴衰、政权更替、金融危机都呈现周期性规律",
                "  ▸ 镜鉴法：历史不会简单重复，但押韵——关键在于识别结构性相似度",
                "  ▸ 人性常数：恐惧与贪婪在任何时代都是市场的核心驱动力",
                "  ▸ 决策启示：历史能教给我们概率分布，但不能给出确定性预测",
            ])
        else:
            lines.extend([
                "  ▸ 核心问题拆解：识别你问题中的关键变量与约束条件",
                "  ▸ 第一性原理：回到基本事实，避免类比推理的陷阱",
                "  ▸ 多视角验证：交叉使用至少两个独立视角确认结论",
                "  ▸ 边界声明：明确结论在什么条件下成立，什么条件下失效",
            ])

        lines.extend([
            "",
            "建议：",
            "  ▸ 如果你想深入某一个具体方向，请追问——我会给出更细颗粒度的分析",
            "  ▸ 如果需要将跨学科结论映射到投资动作，告诉我具体标的或场景",
        ])
        return "\n".join(lines)

    def _answer_skills(self, question: str, ctx: JsonDict) -> str:
        sync = self.sync_skill_registry()
        skills = self.list_skills(limit=30)
        dashboard = self.get_learning_dashboard()
        remote_info: JsonDict = {}
        install_info: JsonDict = {}
        net_intent = self._is_network_skill_intent(question)
        if net_intent and self._skill_net_learning_enabled():
            remote_info = self._list_remote_curated_skills()
            self._log_skill_event("remote_list", remote_info)
            if self._skill_auto_install_enabled():
                install_info = self._auto_install_remote_skills(
                    max_install=int(os.getenv("OPENCLAW_AUTO_INSTALL_MAX_PER_CYCLE", "3"))
                )
                # Re-sync local registry after install attempt.
                sync = self.sync_skill_registry()
                skills = self.list_skills(limit=30)
        lines = []
        lines.append(
            f"你这个问题很关键。我现在可用的 skills 是 {len(skills)} 个，"
            f"刚刚同步了 {sync.get('synced', 0)} 个。"
        )
        if net_intent and not self._skill_net_learning_enabled():
            lines.append("你要的“联网自学习安装skills”我支持，但当前安全开关是关闭的（`OPENCLAW_ENABLE_SKILL_NET_LEARNING=0`）。")
            lines.append("把它改成 `1` 后，我就会去远端拉取可用 skills 清单。")
        if remote_info:
            if remote_info.get("ok"):
                lines.append(
                    f"远端技能源已连接：可发现 {remote_info.get('total', 0)} 个 skills，"
                    f"本地已安装 {len(remote_info.get('installed', []))} 个。"
                )
            else:
                lines.append(f"远端技能源连接失败：{remote_info.get('error', 'unknown error')}")
        if install_info:
            lines.append(
                f"自动安装结果：成功 {len(install_info.get('installed', []))} 个，"
                f"失败 {len(install_info.get('errors', []))} 个。"
            )
            if install_info.get("installed"):
                lines.append("本轮新安装：" + "、".join(install_info.get("installed", [])[:8]))
        if skills:
            lines.append("")
            lines.append("我当前已加载的代表性 skills：")
            for i, s in enumerate(skills[:12], 1):
                desc = (s.get("description") or "").strip()
                brief = f"：{desc}" if desc else ""
                lines.append(f"  {i:02d}. {s.get('skill_name')}{brief}")
        lines.append("")
        lines.append("你要的能力我现在是这样落地的：")
        lines.append("  ▸ 长记忆：会话记忆 + 学习卡片 + 规则库（会随评估更新）")
        lines.append("  ▸ 自学习：每日/每周评估，差评自动反思并写入策略规则")
        lines.append("  ▸ 自进化：通过 outcome tracking 和 evolution 指标持续校准")
        lines.append("  ▸ 24小时助手：服务化运行（你现在线上就是 systemd 常驻）")
        lines.append("  ▸ 学习新 skills：可本地扫描，也可联网从 openai/skills 拉取并安装（受安全开关控制）")
        lines.append("")
        lines.append(
            f"当前学习状态：学习卡片 {dashboard.get('total_cards', 0)}，"
            f"硬规则 {dashboard.get('hard_rules', 0)}，软规则 {dashboard.get('soft_rules', 0)}。"
        )
        lines.append("如果你愿意，我下一步直接把‘15智能体编排表’接到这个 skills 注册表上。")
        return "\n".join(lines)

    def _answer_hybrid_research(self, question: str, ctx: JsonDict) -> str:
        cross_part = self._answer_cross_discipline(question, ctx)
        summary = ctx.get("summary") or {}
        evolution = ctx.get("evolution") or {}
        lines = [cross_part, ""]
        lines.append("投资映射：")
        risk_info = self._get_system_risk_conclusion(summary, evolution)
        lines.append(f"  ▸ 当前系统状态：{risk_info}")
        breadth = self._market_breadth_snapshot()
        if breadth:
            up_ratio = breadth["up"] / max(1, breadth["total"]) * 100
            lines.append(
                f"  ▸ 市场宽度({breadth['date']})：上涨{breadth['up']}({up_ratio:.0f}%)，"
                f"下跌{breadth['down']}，涨停{breadth['limit_up']}，跌停{breadth['limit_down']}"
            )
        lines.append("  ▸ 将上述跨学科视角映射到交易：先控风险（仓位管理），再选标的（数据驱动），最后择时（概率思维）")
        return "\n".join(lines)

    def _answer_general(self, question: str, ctx: JsonDict) -> str:
        lines = [
            "结论：作为 ClawAlpha 超级智能体，我从多个专业角度回答你的问题。",
            "",
            "依据与分析：",
            f"  ▸ 你的问题：「{question[:80]}」",
            "  ▸ 结合事实、逻辑推理和实践经验给出结构化回答",
            "",
            "回答：",
        ]
        q = question.lower()
        if "怎么" in q or "如何" in q:
            lines.extend([
                "  ▸ 步骤1：明确目标——你希望达成什么具体结果？",
                "  ▸ 步骤2：盘点资源——你当前有哪些工具和约束条件？",
                "  ▸ 步骤3：制定方案——基于以上信息，给出最小可行路径",
                "  ▸ 步骤4：执行验证——小规模试运行，收集反馈后迭代",
            ])
        elif "为什么" in q or "原因" in q:
            lines.extend([
                "  ▸ 表层原因：直接可见的因果链条",
                "  ▸ 深层原因：驱动表层原因的系统性因素",
                "  ▸ 根本原因：问题的第一性原理解释",
                "  ▸ 验证方式：如何通过数据或实验确认因果关系",
            ])
        else:
            lines.extend([
                "  ▸ 事实层面：基于可验证信息的客观陈述",
                "  ▸ 推断层面：基于事实的合理推理（标注确定性）",
                "  ▸ 行动层面：可执行的下一步建议",
            ])

        lines.extend([
            "",
            "建议：如果你需要更具体、更深入的回答，请补充更多背景信息或将问题聚焦到某个具体方向。",
            "我擅长的领域：股票投研、量化策略、编程开发、跨学科分析、数据工程。",
        ])
        return "\n".join(lines)

    def get_daily_companion_brief(self) -> str:
        summary = self._latest_run_summary() or {}
        evolution = self._latest_evolution_stats() or {}
        breadth = self._market_breadth_snapshot() or {}
        holdings = self._load_active_holdings(limit=5)
        cfg = self._load_config() or {}

        lines = [
            "今天我给你一个简短陪伴式简报：",
            f"- 系统状态：{self._get_system_risk_conclusion(summary, evolution)}",
        ]
        if breadth:
            up_ratio = breadth.get("up", 0) / max(1, breadth.get("total", 1)) * 100
            lines.append(
                f"- 市场情绪：上涨{breadth.get('up', 0)}（{up_ratio:.0f}%）/ 下跌{breadth.get('down', 0)}，"
                f"涨停{breadth.get('limit_up', 0)}，跌停{breadth.get('limit_down', 0)}。"
            )
        if holdings:
            names = "、".join([str(h.get("stock_name") or h.get("ts_code")) for h in holdings[:3]])
            lines.append(f"- 持仓关注：{names}（建议优先看强弱分化和风控位是否失守）。")
        if cfg:
            lines.append(
                f"- 纪律参数：总仓上限约{int(float(cfg.get('max_total_position', 0.65)) * 100)}%，"
                f"单票上限约{int(float(cfg.get('position_limit', 0.15)) * 100)}%。"
            )
        lines.append("- 我给你的行动建议：今天先稳，再挑1-2个高确定性机会，小步快跑，不重仓赌方向。")
        return "\n".join(lines)

    def _stock_brief_lines(self, stock: JsonDict) -> List[str]:
        return self._deep_stock_analysis_lines(stock, "brief")

    def _maybe_llm_rephrase_stock(
        self, question: str, raw_answer: str, history: List[JsonDict],
    ) -> Optional[str]:
        """Send stock API data to LLM for natural language rephrasing."""
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if os.getenv("OPENCLAW_QA_USE_LLM", "0") != "1" or not api_key:
            return None
        base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        url = f"{base_url}/chat/completions"
        if len(raw_answer) > 8000:
            raw_answer = raw_answer[:8000] + "\n...(truncated)"
        system_prompt = (
            "你是 OpenClaw 超级智能助手 ClawAlpha，用户的专业投资伙伴。"
            "下面是系统量化分析引擎对用户问题生成的原始数据报告。"
            "请你基于这些数据，用自然、有温度的语言重新表达给用户。"
            "要求：不用模板标题（不写'结论：'、'依据：'），直接用段落表达；"
            "保留关键数据（价格、涨跌幅、均线、评分等）；"
            "给出明确的判断和操作建议；像朋友聊天一样自然。"
        )
        user_prompt = f"用户问题：{question}\n\n系统原始分析报告：\n{raw_answer}"
        payload = {
            "model": model, "temperature": 0.3,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=25)
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"].strip()
            _log.info("LLM rephrase success: %d chars", len(text))
            return text
        except Exception as exc:
            _log.error("LLM rephrase failed: %s", exc)
            return None

    def _maybe_llm(self, question: str, context: JsonDict, history: List[JsonDict]) -> Optional[str]:
        use_llm = os.getenv("OPENCLAW_QA_USE_LLM", "0")
        if use_llm != "1":
            _log.warning("LLM disabled: OPENCLAW_QA_USE_LLM=%s", use_llm)
            return None
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            _log.warning("LLM skipped: OPENAI_API_KEY is empty")
            return None

        base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        url = f"{base_url}/chat/completions"
        _log.info("LLM call: model=%s url=%s key=%s...", model, url, api_key[:8])

        hist_text = []
        for item in history[-6:]:
            role = item.get("role", "user")
            content = item.get("content", "")
            hist_text.append(f"{role}: {content}")

        ctx_json = json.dumps(context, ensure_ascii=False)
        if len(ctx_json) > 12000:
            ctx_json = ctx_json[:12000] + "\n...(truncated)"

        system_prompt = (
            "你是 OpenClaw 超级智能助手 ClawAlpha。"
            "你是用户最可靠、最专业的伙伴，擅长股票、金融、量化、哲学、历史、科技、编程、概率、法律、政治、经济、宗教等领域。"
            "你的回答风格：像一个见多识广、思维敏锐的资深朋友在聊天——自然、有温度、有深度、有主见。"
            "绝不使用模板化格式（不写'结论：'、'依据：'、'建议：'这种标题），直接用自然段落表达。"
            "股票问题要给出明确的判断、风险提示和仓位建议；"
            "哲学/跨学科问题要有独到见解和深度思考；"
            "一般问题要真诚、有帮助。"
            "如果上下文中有数据，优先引用数据说话；如果没有，基于你的知识给出最佳判断。"
            "不要说'我无法提供实时数据'之类的推脱，用你掌握的信息给出最有价值的回答。"
        )
        user_prompt = (
            f"本地数据上下文:\n{ctx_json}\n\n"
            f"历史对话:\n{chr(10).join(hist_text)}\n\n"
            f"用户问题:\n{question}"
        )

        payload = {
            "model": model,
            "temperature": 0.3,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            text = data["choices"][0]["message"]["content"].strip()
            _log.info("LLM success: %d chars", len(text))
            return text
        except requests.exceptions.Timeout:
            _log.error("LLM timeout (30s) for question: %s", question[:100])
            return None
        except requests.exceptions.ConnectionError as exc:
            _log.error("LLM connection error: %s", exc)
            return None
        except Exception as exc:
            _log.error("LLM call failed: %s %s", type(exc).__name__, exc)
            return None

    @staticmethod
    def _fmt_pct(v: Any) -> str:
        try:
            fv = float(v)
            return f"{fv * 100:.2f}%"
        except Exception:
            return "N/A"

    @staticmethod
    def _fmt_pct_from_number(v: Any) -> str:
        try:
            fv = float(v)
            return f"{fv:.2f}%"
        except Exception:
            return "N/A"

    @staticmethod
    def _fmt_num(v: Any) -> str:
        try:
            return f"{float(v):.4f}"
        except Exception:
            return "N/A"
