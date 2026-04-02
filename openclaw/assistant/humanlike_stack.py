import json
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional


class HumanLikeToolStack(object):
    """Human-like decision tool stack for OpenClaw.

    This is a pragmatic v1 scaffold that provides:
    - state_store
    - causal_reasoner
    - counterfactual_sim
    - planner
    - policy_engine
    - multi_obj_optimizer
    - outcome_tracker
    - bandit_weight_updater
    - rule_promoter
    - safety_guard
    - audit_log
    - rollback_manager
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.enabled = True
        self._init_tables()

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def _init_tables(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS human_state_store (
                    state_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS human_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS human_outcome_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER NOT NULL,
                    route TEXT,
                    confidence REAL,
                    reward REAL,
                    meta_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS human_agent_weights (
                    agent_id TEXT PRIMARY KEY,
                    weight REAL NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS human_state_snapshot (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER NOT NULL,
                    snapshot_json TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def state_store_set(self, key: str, payload: Dict[str, Any]) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO human_state_store(state_key, state_json, updated_at)
                VALUES (?, ?, ?)
                """,
                (str(key), json.dumps(payload, ensure_ascii=False), int(time.time())),
            )
            conn.commit()

    def state_store_get(self, key: str) -> Dict[str, Any]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT state_json FROM human_state_store WHERE state_key=? LIMIT 1",
                (str(key),),
            ).fetchone()
        if not row:
            return {}
        try:
            data = json.loads(row[0] or "{}")
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def causal_reasoner(self, outputs: Dict[str, Any]) -> Dict[str, Any]:
        reasons: List[str] = []
        ss = outputs.get("stock_snapshot", {}) if isinstance(outputs, dict) else {}
        if isinstance(ss, dict) and ss.get("ok"):
            ret20 = ss.get("ret20")
            ret60 = ss.get("ret60")
            if isinstance(ret20, (int, float)) and ret20 > 0:
                reasons.append("短期动量为正，说明近期买盘更强。")
            if isinstance(ret60, (int, float)) and ret60 > 0:
                reasons.append("中期趋势维持正向，说明结构未破坏。")
        qg = outputs.get("quality_gate", {}) if isinstance(outputs, dict) else {}
        if isinstance(qg, dict):
            fresh = (qg.get("data_freshness") or {}).get("days_lag")
            if isinstance(fresh, int) and fresh >= 3:
                reasons.append("数据有滞后，结论应降低执行强度。")
        return {"ok": True, "causes": reasons[:6]}

    def counterfactual_sim(self, outputs: Dict[str, Any]) -> Dict[str, Any]:
        ss = outputs.get("stock_snapshot", {}) if isinstance(outputs, dict) else {}
        if not isinstance(ss, dict) or not ss.get("ok"):
            return {"ok": True, "scenarios": []}
        ret20 = ss.get("ret20") if isinstance(ss.get("ret20"), (int, float)) else 0.0
        base = float(ret20)
        scenarios = [
            {"name": "不操作", "expected_ret_delta": round(base * 0.2, 2), "risk_delta": 0.0},
            {"name": "晚一日执行", "expected_ret_delta": round(base * 0.1, 2), "risk_delta": 0.2},
            {"name": "减半仓位", "expected_ret_delta": round(base * 0.6, 2), "risk_delta": -0.5},
        ]
        return {"ok": True, "scenarios": scenarios}

    def planner(self, question: str, route: str) -> Dict[str, Any]:
        steps = [
            "识别问题意图与约束",
            "读取本地证据并做质量检查",
            "生成结论+触发+失效+动作",
            "输出可执行建议并记录审计",
        ]
        if route == "skills":
            steps = [
                "扫描本地 skills 注册表",
                "按安全开关决定是否联网拉取",
                "生成可安装清单与操作建议",
                "记录技能学习审计日志",
            ]
        return {"ok": True, "route": route, "steps": steps}

    def policy_engine(self, route: str, quality: Dict[str, Any], outputs: Dict[str, Any]) -> Dict[str, Any]:
        conf = float((quality or {}).get("confidence", 0.0) or 0.0)
        if conf <= 1.5:
            conf = conf * 100.0
        action = "normal_execute"
        if conf < 55:
            action = "degrade_to_conservative"
        if route == "skills":
            action = "skills_flow"
        return {"ok": True, "action": action, "confidence": conf}

    def multi_obj_optimizer(self, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(candidates, list):
            return {"ok": True, "selected": []}
        ranked = sorted(candidates, key=lambda x: float(x.get("score", 0.0)), reverse=True)
        return {"ok": True, "selected": ranked[:10]}

    def outcome_tracker(self, route: str, confidence: float, reward: float, meta: Optional[Dict[str, Any]] = None) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO human_outcome_log(created_at, route, confidence, reward, meta_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (int(time.time()), str(route), float(confidence), float(reward), json.dumps(meta or {}, ensure_ascii=False)),
            )
            conn.commit()

    def bandit_weight_updater(self, agent_hits: List[str], reward: float) -> Dict[str, Any]:
        with self._conn() as conn:
            updated = 0
            for aid in (agent_hits or []):
                row = conn.execute("SELECT weight FROM human_agent_weights WHERE agent_id=? LIMIT 1", (str(aid),)).fetchone()
                cur = float(row[0]) if row else 1.0
                nxt = max(0.1, min(5.0, cur + (0.05 * float(reward))))
                conn.execute(
                    "INSERT OR REPLACE INTO human_agent_weights(agent_id, weight, updated_at) VALUES (?, ?, ?)",
                    (str(aid), float(nxt), int(time.time())),
                )
                updated += 1
            conn.commit()
            top = conn.execute(
                "SELECT agent_id, weight FROM human_agent_weights ORDER BY weight DESC LIMIT 20"
            ).fetchall()
        return {"ok": True, "updated": updated, "top_weights": [{"agent_id": r[0], "weight": round(float(r[1]), 4)} for r in top]}

    def rule_promoter(self) -> Dict[str, Any]:
        return {"ok": True, "promoted": 0, "downgraded": 0, "note": "rule promotion hook active"}

    def safety_guard(self, question: str, quality: Dict[str, Any]) -> Dict[str, Any]:
        q = (question or "").lower()
        conf = float((quality or {}).get("confidence", 0.0) or 0.0)
        if conf <= 1.5:
            conf = conf * 100.0
        if any(k in q for k in ("满仓", "梭哈", "all in", "加杠杆", "借钱炒股")):
            return {"ok": True, "decision": "reject", "reason": "high_risk_instruction"}
        if conf and conf < 45:
            return {"ok": True, "decision": "degrade", "reason": "low_confidence"}
        return {"ok": True, "decision": "allow", "reason": "normal"}

    def audit_log(self, event_type: str, payload: Dict[str, Any]) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO human_audit_log(created_at, event_type, payload_json) VALUES (?, ?, ?)",
                (int(time.time()), str(event_type), json.dumps(payload or {}, ensure_ascii=False)),
            )
            conn.commit()

    def rollback_manager_snapshot(self) -> Dict[str, Any]:
        with self._conn() as conn:
            rows = conn.execute("SELECT state_key, state_json FROM human_state_store").fetchall()
            snapshot = {"created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "state": {r[0]: json.loads(r[1]) for r in rows}}
            conn.execute(
                "INSERT INTO human_state_snapshot(created_at, snapshot_json) VALUES (?, ?)",
                (int(time.time()), json.dumps(snapshot, ensure_ascii=False)),
            )
            conn.commit()
            snap_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        return {"ok": True, "snapshot_id": int(snap_id)}

    def rollback_manager_restore(self, snapshot_id: int) -> Dict[str, Any]:
        with self._conn() as conn:
            row = conn.execute("SELECT snapshot_json FROM human_state_snapshot WHERE id=? LIMIT 1", (int(snapshot_id),)).fetchone()
            if not row:
                return {"ok": False, "error": "snapshot_not_found"}
            snap = json.loads(row[0] or "{}")
            state = snap.get("state", {}) if isinstance(snap, dict) else {}
            for k, v in state.items():
                conn.execute(
                    "INSERT OR REPLACE INTO human_state_store(state_key, state_json, updated_at) VALUES (?, ?, ?)",
                    (str(k), json.dumps(v, ensure_ascii=False), int(time.time())),
                )
            conn.commit()
        return {"ok": True, "restored_keys": len(state)}

    def dashboard(self) -> Dict[str, Any]:
        with self._conn() as conn:
            state_cnt = conn.execute("SELECT COUNT(*) FROM human_state_store").fetchone()[0]
            audit_cnt = conn.execute("SELECT COUNT(*) FROM human_audit_log").fetchone()[0]
            out_cnt = conn.execute("SELECT COUNT(*) FROM human_outcome_log").fetchone()[0]
            top = conn.execute("SELECT agent_id, weight FROM human_agent_weights ORDER BY weight DESC LIMIT 10").fetchall()
        return {
            "ok": True,
            "state_count": int(state_cnt or 0),
            "audit_count": int(audit_cnt or 0),
            "outcome_count": int(out_cnt or 0),
            "top_weights": [{"agent_id": r[0], "weight": round(float(r[1]), 4)} for r in top],
        }
