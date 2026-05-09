from __future__ import annotations

from openclaw.runtime.ui_dependency_loader import load_ui_dependencies
from strategies.evaluators.comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4
from strategies.evaluators.comprehensive_stock_evaluator_v5 import ComprehensiveStockEvaluatorV5


class _Logger:
    def info(self, *_args, **_kwargs):
        pass

    def warning(self, *_args, **_kwargs):
        pass


def test_ui_dependency_loader_uses_real_v5_evaluator_not_v4_fallback():
    deps = load_ui_dependencies(_Logger())

    assert deps["V5_EVALUATOR_AVAILABLE"] is True
    assert deps["ComprehensiveStockEvaluatorV5"] is ComprehensiveStockEvaluatorV5
    assert deps["ComprehensiveStockEvaluatorV5"] is not ComprehensiveStockEvaluatorV4
