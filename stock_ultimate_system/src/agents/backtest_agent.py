from __future__ import annotations

import logging
import os
from typing import Any

from src.backtest_engine.event_engine import EventDrivenBacktester
from src.evaluation.metrics import PerformanceMetrics
from src.evaluation.stability_analyzer import StabilityAnalyzer
from src.evaluation.report_generator import ReportGenerator
from src.visualization.chart_builder import ChartBuilder

logger = logging.getLogger(__name__)


class BacktestAgent:
    """Run backtests and generate reports with visualizations."""

    def __init__(self, config: dict) -> None:
        self.config = config
        self.engine = EventDrivenBacktester(config)
        self.metrics = PerformanceMetrics()
        self.stability = StabilityAnalyzer()
        self.reporter = ReportGenerator()
        self.chart = ChartBuilder()

    def _should_generate_charts(self) -> bool:
        backtest_cfg = (self.config.get('settings', {}) or {}).get('backtest', {}) or {}
        if 'generate_charts' in backtest_cfg:
            return bool(backtest_cfg.get('generate_charts'))
        if os.getenv('STOCK_SYSTEM_SKIP_CHARTS', '').strip().lower() in {'1', 'true', 'yes'}:
            return False
        return os.getenv('PYTEST_CURRENT_TEST') is None

    def run_backtest(
        self,
        stock_pool: list[str],
        start_date: str,
        end_date: str,
        data_dict: dict | None = None,
        signal_func=None,
        *,
        generate_artifacts: bool = True,
    ) -> dict[str, Any]:
        result = self.engine.run(stock_pool, start_date, end_date, data_dict, signal_func)

        if result.get('status') != 'ok':
            return result

        equity_curve = result.get('equity_curve')
        trades = result.get('trades')

        if equity_curve is not None and not equity_curve.empty:
            detailed_metrics = self.metrics.full_report(equity_curve['equity'], trades)
            stability_report = self.stability.full_stability_report(equity_curve)
            detailed_metrics.update(stability_report)
            result['detailed_metrics'] = detailed_metrics
            result['stability'] = stability_report

            if generate_artifacts:
                try:
                    report_path = self.reporter.generate_backtest_report(result)
                    result['report_path'] = report_path
                    logger.info('Report saved to %s', report_path)
                except Exception as e:
                    logger.warning('Report generation failed: %s', e)

                if self._should_generate_charts():
                    try:
                        result['charts'] = {
                            'equity_curve': self.chart.plot_equity_curve(equity_curve),
                            'monthly_returns': self.chart.plot_monthly_returns(equity_curve),
                            'rolling_metrics': self.chart.plot_rolling_metrics(equity_curve),
                        }
                    except Exception as e:
                        logger.warning('Chart generation failed: %s', e)

        return result
