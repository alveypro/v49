from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


class ReportGenerator:
    """Generate markdown and HTML reports from backtest results."""

    def save_markdown_report(self, content: str, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)

    def generate_backtest_report(self, result: dict[str, Any], output_dir: str = 'data/reports') -> str:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        path = f'{output_dir}/backtest_report_{ts}.md'

        metrics = result.get('detailed_metrics', result.get('metrics', {}))
        lines = [
            '# Backtest Report',
            f'\nGenerated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            f'\nStock Pool: {result.get("stock_pool", [])}',
            f'\nPeriod: {result.get("start_date", "")} ~ {result.get("end_date", "")}',
            '\n## Performance Metrics\n',
            '| Metric | Value |',
            '|--------|-------|',
        ]
        for k, v in metrics.items():
            if isinstance(v, float):
                lines.append(f'| {k} | {v:.4f} |')
            else:
                lines.append(f'| {k} | {v} |')

        equity_curve = result.get('equity_curve')
        if isinstance(equity_curve, pd.DataFrame) and not equity_curve.empty:
            lines.append('\n## Equity Curve Summary\n')
            lines.append(f'- Start equity: {equity_curve["equity"].iloc[0]:,.2f}')
            lines.append(f'- End equity: {equity_curve["equity"].iloc[-1]:,.2f}')
            lines.append(f'- Peak equity: {equity_curve["equity"].max():,.2f}')
            lines.append(f'- Trading days: {len(equity_curve)}')

        trades = result.get('trades')
        if isinstance(trades, pd.DataFrame) and not trades.empty:
            lines.append(f'\n## Trade Summary\n')
            lines.append(f'- Total trades: {len(trades)}')
            buys = trades[trades['side'] == 'buy'] if 'side' in trades.columns else pd.DataFrame()
            sells = trades[trades['side'] == 'sell'] if 'side' in trades.columns else pd.DataFrame()
            lines.append(f'- Buy orders: {len(buys)}')
            lines.append(f'- Sell orders: {len(sells)}')
            if 'commission' in trades.columns:
                lines.append(f'- Total commission: {trades["commission"].sum():,.2f}')

        signal_stats = result.get('signal_stats', {})
        if signal_stats:
            lines.append('\n## Signal Flow Summary\n')
            lines.append(f'- Total signals: {signal_stats.get("total", 0)}')
            lines.append(f'- Buy signals: {signal_stats.get("buy", 0)}')
            lines.append(f'- Sell signals: {signal_stats.get("sell", 0)}')

        rule_block_stats = result.get('rule_block_stats', {})
        if rule_block_stats:
            lines.append('\n## Rule Block Summary\n')
            lines.append(f'- Total blocked events: {result.get("rule_block_total", 0)}')
            lines.append('\n| Rule Reason | Count |')
            lines.append('|-------------|-------|')
            for reason, count in sorted(rule_block_stats.items(), key=lambda x: x[1], reverse=True):
                lines.append(f'| {reason} | {count} |')

        signal_logs = result.get('signal_logs')
        if isinstance(signal_logs, pd.DataFrame) and not signal_logs.empty and 'status' in signal_logs.columns:
            lines.append('\n## Signal Execution Breakdown\n')
            status_counts = signal_logs['status'].value_counts()
            lines.append('| Signal Status | Count |')
            lines.append('|---------------|-------|')
            for status, count in status_counts.items():
                lines.append(f'| {status} | {count} |')

        cost_sensitivity = result.get('cost_sensitivity', [])
        if cost_sensitivity:
            lines.append('\n## Cost Sensitivity Analysis\n')
            lines.append('| Cost Multiplier | Adjusted Equity | Adjusted Total Return |')
            lines.append('|-----------------|-----------------|-----------------------|')
            for row in cost_sensitivity:
                lines.append(
                    f'| {float(row.get("cost_multiplier", 1.0)):.2f} | '
                    f'{float(row.get("adjusted_equity", 0.0)):,.2f} | '
                    f'{float(row.get("adjusted_total_return", 0.0)):.4f} |'
                )

        content = '\n'.join(lines)
        self.save_markdown_report(content, path)
        return path

    def generate_signal_report(self, signal_results: list[dict], output_dir: str = 'data/reports') -> str:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        path = f'{output_dir}/signal_report_{ts}.md'

        lines = [
            '# Signal Report',
            f'\nGenerated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            '\n| Stock | Signal | Score | Direction Prob | Confidence |',
            '|-------|--------|-------|---------------|------------|',
        ]
        for r in signal_results:
            code = r.get('ts_code', '')
            sig = r.get('signal_result', {})
            fc = r.get('forecast_result', {})
            lines.append(
                f'| {code} | {sig.get("signal", "")} | {sig.get("score", 0):.1f} '
                f'| {fc.get("direction_prob", 0):.2%} | {fc.get("confidence", 0):.2%} |'
            )

        content = '\n'.join(lines)
        self.save_markdown_report(content, path)
        return path
