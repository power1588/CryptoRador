#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
基于WebSocket订阅的现货-期货价差监控脚本
实时监控交易所中现货和期货交易对的价格差异，当价差超过阈值时发送警报
"""

import os
import sys
import signal
import asyncio
import logging
import argparse
from datetime import datetime
import time

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.config import settings
from src.fetcher.async_subscription_fetcher import SubscriptionDataFetcher
from src.analyzer.spot_futures_monitor import SpotFuturesMonitor
from src.notifier.lark_notifier import LarkNotifier

# 配置日志
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(settings.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class SubscriptionSpotFuturesMonitor:
    """基于订阅的现货-期货价差监控系统"""
    
    def __init__(self, check_interval: int = 5):
        """初始化监控系统
        
        Args:
            check_interval: 价差检查间隔(秒)
        """
        self.data_fetcher = SubscriptionDataFetcher()
        self.spot_futures_monitor = SpotFuturesMonitor(
            threshold=settings.SPOT_FUTURES_DIFF_THRESHOLD,
            basis_direction=settings.SPOT_FUTURES_BASIS_DIRECTION
        )
        
        # 使用专用的现货-期货价差通知通道
        if settings.SPOT_FUTURES_LARK_WEBHOOK_URL:
            self.notifier = LarkNotifier(
                webhook_url=settings.SPOT_FUTURES_LARK_WEBHOOK_URL,
                secret=settings.SPOT_FUTURES_LARK_SECRET
            )
            logger.info("使用专用的现货-期货价差通知通道")
        else:
            self.notifier = LarkNotifier()
            logger.warning("未配置专用的现货-期货价差通知通道，使用默认通知通道")
        
        self.check_interval = check_interval
        self.running = False
        self.subscription_tasks = []
        self.last_alert_time = {}  # 记录上次报警时间，避免频繁报警
        self.alert_cooldown = 300  # 同一交易对5分钟内不重复报警
    
    async def start(self):
        """启动监控系统"""
        logger.info("正在启动基于订阅的现货-期货价差监控系统...")
        
        # 启动数据订阅服务
        self.subscription_tasks = await self.data_fetcher.start()
        if not self.subscription_tasks:
            logger.error("启动数据订阅服务失败")
            return False
        
        logger.info(f"启动了 {len(self.subscription_tasks)} 个订阅任务")
        
        # 启动价差监控循环
        self.running = True
        asyncio.create_task(self.monitor_price_differences())
        
        return True
    
    async def monitor_price_differences(self):
        """持续监控价差的主循环"""
        logger.info(f"开始监控价差，检查间隔: {self.check_interval}秒，阈值: {settings.SPOT_FUTURES_DIFF_THRESHOLD}%")
        
        while self.running:
            try:
                start_time = time.time()
                
                # 获取当前市场数据
                market_data = self.data_fetcher.get_market_data()
                
                # 检测异常价差
                abnormal_basis = self.spot_futures_monitor.detect_abnormal_basis(market_data)
                
                # 处理报警
                if abnormal_basis:
                    # 过滤掉短时间内重复的报警
                    filtered_alerts = self._filter_cooldown_alerts(abnormal_basis)
                    
                    if filtered_alerts:
                        alert_count = len(filtered_alerts)
                        logger.info(f"检测到 {alert_count} 个现货-期货价差异常，发送通知")
                        
                        for alert in filtered_alerts:
                            exchange = alert.get('exchange', 'Unknown')
                            spot_symbol = alert.get('spot_symbol', 'Unknown')
                            future_symbol = alert.get('future_symbol', 'Unknown')
                            price_diff = alert.get('price_difference_percent', 0.0)
                            
                            logger.info(f"异常基差: {exchange} | 现货: {spot_symbol} | 期货: {future_symbol} | 价差: {price_diff:.4f}%")
                            
                            # 更新上次报警时间
                            alert_key = f"{exchange}:{spot_symbol}:{future_symbol}"
                            self.last_alert_time[alert_key] = time.time()
                        
                        # 发送通知
                        notification_success = self.notifier.send_notification(filtered_alerts)
                        if notification_success:
                            logger.info("价差异常通知发送成功")
                        else:
                            logger.error("价差异常通知发送失败")
                
                # 计算处理时间并等待到下一个检查间隔
                process_time = time.time() - start_time
                wait_time = max(0.1, self.check_interval - process_time)
                
                if process_time > self.check_interval:
                    logger.warning(f"处理时间 ({process_time:.2f}秒) 超过了检查间隔 ({self.check_interval}秒)")
                
                await asyncio.sleep(wait_time)
                
            except asyncio.CancelledError:
                logger.info("价差监控任务被取消")
                break
            except Exception as e:
                logger.error(f"价差监控过程中出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                await asyncio.sleep(5)  # 发生错误后稍等片刻再重试
    
    def _filter_cooldown_alerts(self, alerts):
        """过滤掉冷却期内的报警，避免短时间内重复报警
        
        Args:
            alerts: 原始报警列表
            
        Returns:
            过滤后的报警列表
        """
        current_time = time.time()
        filtered = []
        
        for alert in alerts:
            exchange = alert.get('exchange', 'Unknown')
            spot_symbol = alert.get('spot_symbol', 'Unknown')
            future_symbol = alert.get('future_symbol', 'Unknown')
            
            alert_key = f"{exchange}:{spot_symbol}:{future_symbol}"
            last_time = self.last_alert_time.get(alert_key, 0)
            
            # 如果该交易对的上次报警时间超过冷却期，则允许报警
            if current_time - last_time > self.alert_cooldown:
                filtered.append(alert)
        
        return filtered
    
    async def stop(self):
        """停止监控系统"""
        logger.info("正在停止监控系统...")
        self.running = False
        
        # 取消所有订阅任务
        for task in self.subscription_tasks:
            if not task.done():
                task.cancel()
        
        # 等待任务取消完成
        if self.subscription_tasks:
            await asyncio.gather(*self.subscription_tasks, return_exceptions=True)
        
        # 停止数据获取服务
        await self.data_fetcher.stop()
        
        logger.info("监控系统已停止")
    
    def handle_signal(self, sig, frame):
        """处理信号(如CTRL+C)"""
        logger.info(f"收到信号 {sig}，开始关闭...")
        if self.running:
            asyncio.create_task(self.stop())

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='基于WebSocket订阅的现货-期货价差监控脚本')
    
    parser.add_argument('-e', '--exchanges', type=str,
                        help=f'要监控的交易所，逗号分隔，默认: {",".join(settings.EXCHANGES)}')
    parser.add_argument('-t', '--threshold', type=float,
                        help=f'价差阈值(百分比)，默认: {settings.SPOT_FUTURES_DIFF_THRESHOLD}')
    parser.add_argument('-d', '--direction', type=str, choices=['both', 'premium', 'discount'],
                        help=f'基差方向监控 (both=双向, premium=升水, discount=贴水)，默认: {settings.SPOT_FUTURES_BASIS_DIRECTION}')
    parser.add_argument('-i', '--interval', type=int, default=5,
                        help='检查价差的间隔(秒)，默认: 5')
    parser.add_argument('-c', '--cooldown', type=int, default=300,
                        help='报警冷却时间(秒)，默认: 300')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help=f'日志级别，默认: {settings.LOG_LEVEL}')
    
    return parser.parse_args()

def update_settings_from_args(args):
    """根据命令行参数更新设置"""
    if args.exchanges:
        settings.EXCHANGES = args.exchanges.split(',')
    
    if args.threshold:
        settings.SPOT_FUTURES_DIFF_THRESHOLD = args.threshold
        
    if args.direction:
        settings.SPOT_FUTURES_BASIS_DIRECTION = args.direction
    
    if args.log_level:
        settings.LOG_LEVEL = args.log_level
        # 重新配置日志
        logging.getLogger().setLevel(getattr(logging, settings.LOG_LEVEL))

async def main_async():
    """异步主函数"""
    args = parse_arguments()
    update_settings_from_args(args)
    
    monitor = SubscriptionSpotFuturesMonitor(check_interval=args.interval)
    monitor.alert_cooldown = args.cooldown
    
    # 设置信号处理
    signal.signal(signal.SIGINT, monitor.handle_signal)
    signal.signal(signal.SIGTERM, monitor.handle_signal)
    
    try:
        success = await monitor.start()
        if not success:
            logger.error("启动监控系统失败")
            return
        
        # 保持程序运行直到收到停止信号
        while monitor.running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("收到键盘中断，正在关闭...")
    except Exception as e:
        logger.error(f"运行过程中发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        await monitor.stop()

def main():
    """主入口函数"""
    try:
        # 在Windows上需要使用不同的事件循环策略
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        # 运行异步主函数
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序异常: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main() 