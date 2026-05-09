#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务调度服务
自动化执行每日选股、持仓监控等任务
"""

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
import logging
import json
import os
from pathlib import Path
from trading_assistant import TradingAssistant
from notification_service import NotificationService
from openclaw.assistant import OpenClawStockAssistant

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SchedulerService:
    """定时任务调度服务"""
    
    def __init__(self):
        """初始化调度器"""
        self.scheduler = BlockingScheduler(timezone='Asia/Shanghai')
        self.assistant = TradingAssistant()
        self.notifier = NotificationService()
        self.learning_assistant = OpenClawStockAssistant(log_dir="logs/openclaw", db_path=self.assistant.db_path)
        
        logger.info("🚀 定时任务调度器初始化完成")
    
    def daily_stock_scan_job(self):
        """每日选股任务"""
        logger.info("="*80)
        logger.info("📊 执行每日选股任务")
        logger.info("="*80)
        
        try:
            # 执行选股
            recommendations = self.assistant.daily_stock_scan(top_n=5)
            
            if recommendations:
                logger.info(f"✅ 选股完成，找到{len(recommendations)}只推荐")
                
                # 生成通知内容
                message = f"""
📊 每日选股完成 ({datetime.now().strftime('%Y-%m-%d')})

找到{len(recommendations)}只推荐股票：

"""
                for i, rec in enumerate(recommendations, 1):
                    message += f"{i}. {rec['stock_name']}({rec['ts_code']})\n"
                    message += f"   评分: {rec['score']:.1f}分\n"
                    message += f"   价格: ¥{rec['price']:.2f}\n"
                    message += f"   行业: {rec['industry']}\n\n"
                
                # 发送通知
                self.notifier.send_notification(
                    title="📊 每日选股完成",
                    content=message
                )
            else:
                logger.warning("⚠️ 未找到推荐股票")
        
        except Exception as e:
            logger.error(f"❌ 选股任务失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def holdings_monitor_job(self):
        """持仓监控任务"""
        logger.info("="*80)
        logger.info("📈 执行持仓监控任务")
        logger.info("="*80)
        
        try:
            # 更新持仓
            self.assistant.update_holdings()
            logger.info("✅ 持仓更新完成")
            
            # 检查止盈止损
            alerts = self.assistant.check_stop_conditions()
            
            if alerts:
                logger.info(f"⚠️ 发现{len(alerts)}个提醒")
                
                # 生成通知内容
                message = f"""
🔔 持仓监控提醒 ({datetime.now().strftime('%Y-%m-%d %H:%M')})

"""
                for alert in alerts:
                    message += alert['message'] + "\n\n"
                
                # 发送通知
                self.notifier.send_notification(
                    title="🔔 持仓监控提醒",
                    content=message,
                    urgent=True
                )
            else:
                logger.info("📊 持仓正常，无提醒")
        
        except Exception as e:
            logger.error(f"❌ 监控任务失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def daily_report_job(self):
        """每日报告任务"""
        logger.info("="*80)
        logger.info("📝 生成每日报告")
        logger.info("="*80)
        
        try:
            # 生成报告
            report = self.assistant.generate_daily_report()
            logger.info("✅ 报告生成完成")
            
            # 发送通知
            self.notifier.send_notification(
                title="📝 每日交易报告",
                content=report
            )
        
        except Exception as e:
            logger.error(f"❌ 报告生成失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def daily_backtest_job(self):
        """每日自动回测快照任务（收盘后）"""
        logger.info("=" * 80)
        logger.info("🔬 执行每日自动回测快照")
        logger.info("=" * 80)

        # A股周末无交易，跳过可减少无效计算
        if datetime.now().weekday() >= 5:
            logger.info("📅 周末跳过自动回测")
            return

        try:
            from backtest.engine import BacktestEngine
            from openclaw.adapters import V49Adapter
            from openclaw.runtime.v49_handlers import HandlerFactory

            module_path = Path(
                os.getenv(
                    "V49_MODULE_PATH",
                    str(Path(__file__).resolve().parent / "v49_app.py"),
                )
            ).resolve()
            factory = HandlerFactory(module_path=module_path)
            adapter = V49Adapter(module_path=module_path)
            for strategy in ("v6", "v7", "v8", "v9"):
                adapter.register_backtest_handler(strategy, factory.create_backtest_handler(strategy))
            engine = BacktestEngine(adapter)

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            sample_size = int(os.getenv("DAILY_BACKTEST_SAMPLE_SIZE", "300"))

            strategy_params = {
                "v6": {"mode": "single", "sample_size": sample_size, "holding_days": 5, "score_threshold": 75},
                "v7": {"mode": "single", "sample_size": sample_size, "holding_days": 8, "score_threshold": 60},
                "v8": {"mode": "single", "sample_size": sample_size, "holding_days": 8, "score_threshold": 50},
                "v9": {"mode": "single", "sample_size": sample_size, "holding_days": 15, "score_threshold": 60},
            }

            snapshot = {
                "generated_at": datetime.now().isoformat(),
                "date_from": start_date,
                "date_to": end_date,
                "sample_size": sample_size,
                "results": {},
                "auto_tuning": {},
            }
            summary_lines = [f"🔬 每日自动回测快照（{datetime.now().strftime('%Y-%m-%d')}）"]

            for strategy, params in strategy_params.items():
                out = engine.run(strategy=strategy, date_from=start_date, date_to=end_date, params=params)
                if out.get("status") == "success":
                    sm = (((out.get("result") or {}).get("summary")) or {})
                    snapshot["results"][strategy] = {"ok": True, "summary": sm}
                    summary_lines.append(
                        f"- {strategy.upper()}: win_rate={float(sm.get('win_rate', 0))*100:.1f}% "
                        f"max_dd={float(sm.get('max_drawdown', 0))*100:.2f}% "
                        f"density={float(sm.get('signal_density', 0)):.3f}"
                    )
                else:
                    err = out.get("error", "unknown")
                    snapshot["results"][strategy] = {"ok": False, "error": err}
                    summary_lines.append(f"- {strategy.upper()}: FAILED ({err})")

            # 自动优化：回测完成后执行一次自动调参
            tune_result = {"ok": False, "applied": False, "reason": "not_supported"}
            try:
                if hasattr(self.assistant, "get_auto_tuning_recommendation") and hasattr(self.assistant, "apply_auto_tuning"):
                    tuning_rec = self.assistant.get_auto_tuning_recommendation(lookback_days=30, min_samples=8)
                    tune_result = self.assistant.apply_auto_tuning(
                        tuning_rec if isinstance(tuning_rec, dict) else None
                    )
                elif hasattr(self.assistant, "apply_auto_tuning"):
                    tune_result = self.assistant.apply_auto_tuning()
            except Exception as tune_exc:
                tune_result = {"ok": False, "applied": False, "reason": f"tuning_exception:{tune_exc}"}

            snapshot["auto_tuning"] = tune_result
            if tune_result.get("ok") and tune_result.get("applied"):
                summary_lines.append("- AutoTuning: APPLIED")
            elif tune_result.get("ok"):
                summary_lines.append("- AutoTuning: NO_CHANGE")
            else:
                summary_lines.append(f"- AutoTuning: FAILED ({tune_result.get('reason', 'unknown')})")

            out_dir = Path("logs/openclaw")
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"daily_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            out_file.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("✅ 自动回测完成，输出: %s", out_file)

            self.notifier.send_notification(
                title="🔬 每日自动回测快照",
                content="\n".join(summary_lines) + f"\n\nartifact: {out_file}",
            )
        except Exception as e:
            logger.error(f"❌ 自动回测失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def self_learning_job(self):
        """自学习评估任务（日评估+每周一次周评估）"""
        logger.info("="*80)
        logger.info("🧠 执行自学习评估任务")
        logger.info("="*80)
        try:
            force_weekly = datetime.now().weekday() == 6
            result = self.learning_assistant.run_self_learning_cycle(force_weekly=force_weekly)
            daily = result.get("daily") or {}
            weekly = result.get("weekly") or {}
            tuning = self.assistant.apply_auto_tuning()
            logger.info(
                "✅ 自学习评估完成: overall=%s promoted=%s downgraded=%s tuning_applied=%s",
                daily.get("overall_score", "N/A"),
                weekly.get("promoted_rules", 0),
                weekly.get("downgraded_rules", 0),
                tuning.get("applied", False),
            )
        except Exception as e:
            logger.error(f"❌ 自学习评估失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def setup_jobs(self):
        """配置定时任务"""
        logger.info("⏰ 配置定时任务...")
        
        # 1. 每日选股 - 每天9:15
        self.scheduler.add_job(
            self.daily_stock_scan_job,
            CronTrigger(hour=9, minute=15),
            id='daily_stock_scan',
            name='每日选股',
            replace_existing=True
        )
        logger.info("✅ 已添加: 每日选股（9:15）")
        
        # 2. 持仓监控 - 每天10:30, 14:00, 15:00
        self.scheduler.add_job(
            self.holdings_monitor_job,
            CronTrigger(hour='10,14,15', minute='0,30'),
            id='holdings_monitor',
            name='持仓监控',
            replace_existing=True
        )
        logger.info("✅ 已添加: 持仓监控（10:30, 14:00, 15:00）")
        
        # 3. 每日报告 - 每天18:00
        self.scheduler.add_job(
            self.daily_report_job,
            CronTrigger(hour=18, minute=0),
            id='daily_report',
            name='每日报告',
            replace_existing=True
        )
        logger.info("✅ 已添加: 每日报告（18:00）")

        # 4. 每日自动回测 - 默认每个交易日01:20（可通过环境变量覆盖）
        # 说明：回测计算开销较高，默认安排到凌晨以避免影响白天交互性能。
        bt_hour = int(os.getenv("DAILY_BACKTEST_CRON_HOUR", "1"))
        bt_minute = int(os.getenv("DAILY_BACKTEST_CRON_MINUTE", "20"))
        self.scheduler.add_job(
            self.daily_backtest_job,
            CronTrigger(hour=bt_hour, minute=bt_minute),
            id='daily_backtest',
            name='每日自动回测',
            replace_existing=True
        )
        logger.info(f"✅ 已添加: 每日自动回测（{bt_hour:02d}:{bt_minute:02d}）")

        # 5. 自学习评估 - 每天18:20（周日附加周评估）
        self.scheduler.add_job(
            self.self_learning_job,
            CronTrigger(hour=18, minute=20),
            id='self_learning_eval',
            name='自学习评估',
            replace_existing=True
        )
        logger.info("✅ 已添加: 自学习评估（18:20）")
        
        # 列出所有任务
        logger.info("\n📋 已配置的定时任务:")
        for job in self.scheduler.get_jobs():
            next_run = getattr(job, "next_run_time", None)
            logger.info(f"  - {job.name}: {next_run}")
    
    def start(self):
        """启动调度器"""
        logger.info("="*80)
        logger.info("🚀 启动定时任务调度服务")
        logger.info("="*80)
        
        self.setup_jobs()
        
        logger.info("\n✅ 调度器已启动，等待执行任务...")
        logger.info("按 Ctrl+C 停止服务\n")
        
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("\n⏹ 收到停止信号，正在关闭...")
            self.scheduler.shutdown()
            logger.info("✅ 调度器已停止")


def main():
    """主函数"""
    service = SchedulerService()
    service.start()


if __name__ == "__main__":
    main()
