#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
跨所USDT永续合约价差监控工具 (WebSocket版)

此脚本使用WebSocket订阅方式监控Binance、Gate等不同交易所之间的USDT永续合约价格差异，
当价格差异超过设定阈值时，通过Lark(飞书)发送报警通知。
比起REST API轮询方式，WebSocket订阅更高效，可以实时获取价格变化。
"""

import os
import sys
import time
import logging
import asyncio
import pandas as pd
import signal
from datetime import datetime
from typing import Dict, List, Any

# 确保可以导入src模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.fetcher.perp_ws_subscriber import PerpWebSocketSubscriber
from src.analyzer.perp_exchange_monitor import PerpExchangeMonitor
from src.notifier.lark_notifier import LarkNotifier
from src.config import settings

# 配置日志
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(settings.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """异步主函数"""
    
    # 记录启动时间
    start_time = time.time()
    logger.info(f"🚀 启动WebSocket跨所永续合约价差监控器 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"监控交易所: {settings.PERP_EXCHANGES}")
    logger.info(f"价差阈值: {settings.PERP_DIFF_THRESHOLD}%")
    
    # 显示黑名单信息
    if settings.PERP_BLACKLIST and any(settings.PERP_BLACKLIST):
        logger.info(f"已设置币种黑名单: {', '.join(settings.PERP_BLACKLIST)}")
    
    # 初始化WebSocket数据订阅器
    market_subscriber = None
    
    try:
        # 初始化WebSocket数据订阅器
        market_subscriber = PerpWebSocketSubscriber()
        
        # 初始化价差监控器
        perp_monitor = PerpExchangeMonitor(
            exchanges=settings.PERP_EXCHANGES,
            threshold=settings.PERP_DIFF_THRESHOLD
        )
        
        # 初始化Lark通知器 (使用专用的跨所永续合约通知webhook)
        notifier = LarkNotifier(
            webhook_url=settings.PERP_EXCHANGE_LARK_WEBHOOK_URL,
            secret=settings.PERP_EXCHANGE_LARK_SECRET
        )
        
        # 启动WebSocket订阅
        success = await market_subscriber.start()
        if not success:
            logger.error("启动WebSocket订阅失败，退出程序")
            return
            
        logger.info("WebSocket数据订阅启动成功，开始监控价差...")
        
        # 持续监控循环
        scan_count = 0
        while True:
            try:
                # 记录扫描开始时间
                scan_start_time = time.time()
                scan_count += 1
                
                # 获取市场数据 (从WebSocket订阅缓存中获取)
                market_data = market_subscriber.get_market_data()
                
                # 输出订阅统计信息
                active_subscriptions = 0
                subscription_details = []
                for exchange_id, symbols in market_subscriber.active_subscriptions.items():
                    exchange_subscription_count = len(symbols)
                    active_subscriptions += exchange_subscription_count
                    subscription_details.append(f"{exchange_id}: {exchange_subscription_count}个")
                    
                logger.info(f"第 {scan_count} 次扫描: 活跃订阅 {active_subscriptions} 个交易对 ({', '.join(subscription_details)})")
                
                # 检查是否有有效数据
                has_valid_data = False
                for exchange_id, exchange_data in market_data.items():
                    if 'future' in exchange_data and exchange_data['future']:
                        has_valid_data = True
                        break
                
                if not has_valid_data:
                    logger.warning("没有收到任何有效的市场数据，跳过此次分析")
                    await asyncio.sleep(5)
                    continue
                
                # 分析价格差异
                price_diff_alerts = perp_monitor.calculate_price_differences(market_data)
                
                if price_diff_alerts:
                    logger.info(f"发现 {len(price_diff_alerts)} 个超过阈值的跨所永续合约价差")
                    
                    # 发送通知
                    notifier.send_notification(price_diff_alerts)
                
                # 等待下一次扫描
                scan_duration = time.time() - scan_start_time
                sleep_time = max(1, settings.SCAN_INTERVAL_SECONDS - scan_duration)
                logger.debug(f"扫描完成，耗时 {scan_duration:.2f} 秒，休眠 {sleep_time:.2f} 秒")
                
                try:
                    await asyncio.sleep(sleep_time)
                except asyncio.CancelledError:
                    logger.info("任务被取消，正在清理资源...")
                    break
                
            except asyncio.CancelledError:
                logger.info("任务被取消，正在清理资源...")
                break
            except Exception as e:
                logger.error(f"扫描过程中发生错误: {str(e)}", exc_info=True)
                # 发生错误时等待短暂时间再继续
                try:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    logger.info("任务被取消，正在清理资源...")
                    break
    
    except asyncio.CancelledError:
        logger.info("任务被取消，正在清理资源...")
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在关闭...")
    except Exception as e:
        logger.error(f"程序运行过程中发生错误: {str(e)}", exc_info=True)
    finally:
        # 关闭WebSocket订阅
        if market_subscriber:
            try:
                await market_subscriber.stop()
                logger.info("已关闭所有WebSocket连接")
            except Exception as e:
                logger.error(f"关闭WebSocket连接时出错: {str(e)}")
        
        # 记录运行时间
        total_runtime = time.time() - start_time
        logger.info(f"程序运行时间: {total_runtime:.2f} 秒")

def handle_signals():
    """处理系统信号"""
    loop = asyncio.get_event_loop()
    
    # 添加信号处理
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(loop, sig))
        )
    
async def shutdown(loop, sig=None):
    """优雅关闭"""
    if sig:
        logger.info(f"收到信号 {sig.name}，取消所有任务...")
    else:
        logger.info("收到关闭信号，取消所有任务...")
    
    # 获取所有任务
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    if not tasks:
        return
        
    # 取消所有任务
    for task in tasks:
        task.cancel()
    
    logger.info(f"取消了 {len(tasks)} 个任务")
    
    # 等待所有任务完成取消
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    
    # 停止事件循环
    loop.stop()

if __name__ == '__main__':
    # 在 Windows 上需要使用 asyncio 的特定策略
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        try:
            # 运行异步主函数
            asyncio.run(main())
        except KeyboardInterrupt:
            # 如果asyncio.run内部的KeyboardInterrupt未被捕获，这里会捕获
            logger.info("程序已退出")
        except Exception as e:
            logger.error(f"程序运行失败: {str(e)}", exc_info=True)
    else:
        # 在Unix系统上设置信号处理
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        handle_signals()
        
        try:
            # 添加主任务
            main_task = loop.create_task(main())
            # 运行直到收到关闭信号
            loop.run_forever()
        except Exception as e:
            logger.error(f"程序运行过程中发生错误: {str(e)}", exc_info=True)
        finally:
            # 确保事件循环关闭
            if not loop.is_closed():
                loop.close()
            logger.info("事件循环已关闭，程序退出") 