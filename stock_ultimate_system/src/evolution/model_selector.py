from __future__ import annotations

import logging
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


class ModelSelector:
    """Select top-performing models based on validation metrics."""

    def __init__(self, top_k: int = 3, metric: str = 'trade_objective') -> None:
        self.top_k = top_k
        self.metric = metric
        self._history: list[dict] = []

    def select_top_models(self, eval_results: dict[str, dict]) -> list[str]:
        scored = []
        for name, result in eval_results.items():
            score = result.get(self.metric, result.get('trade_objective', result.get('accuracy', 0)))
            if isinstance(score, (int, float)) and not np.isnan(score):
                scored.append((name, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        selected = [name for name, _ in scored[:self.top_k]]
        logger.info('Selected models: %s', selected)
        return selected

    def compute_model_weights(self, eval_results: dict[str, dict], selected: list[str]) -> dict[str, float]:
        scores = {}
        for name in selected:
            r = eval_results.get(name, {})
            scores[name] = r.get(self.metric, r.get('trade_objective', r.get('accuracy', 0.5)))

        total = sum(scores.values())
        if total <= 0:
            return {n: 1.0 / len(selected) for n in selected}
        return {n: s / total for n, s in scores.items()}

    def record_selection(self, generation: int, selected: list[str], eval_results: dict) -> None:
        self._history.append({
            'generation': generation,
            'selected': selected,
            'scores': {n: eval_results.get(n, {}) for n in selected},
        })

    def get_selection_history(self) -> list[dict]:
        return self._history
