from __future__ import annotations

from typing import Any, Dict, List, Optional

from openclaw.assistant.stock_qa import OpenClawStockAssistant


class AssistantService:
    def __init__(self, log_dir: str = "logs/openclaw", db_path: str = ""):
        self.impl = OpenClawStockAssistant(log_dir=log_dir, db_path=db_path)

    def answer(self, question: str, history: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        return self.impl.answer(question=question, history=history or [])
