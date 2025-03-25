#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
跨所USDT永续合约价差监控工具

此脚本用于监控Binance、Gate等不同交易所之间的USDT永续合约价格差异，
当价格差异超过设定阈值时，通过Lark(飞书)发送报警通知。
"""

import os
import sys
import time
import logging
import asyncio
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any

# 确保可以导入src模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.fetcher.async_data_fetcher import AsyncMarketDataFetcher
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
    logger.info(f"🚀 启动跨所永续合约价差监控器 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"监控交易所: {settings.PERP_EXCHANGES}")
    logger.info(f"价差阈值: {settings.PERP_DIFF_THRESHOLD}%")
    
    try:
        # 初始化数据获取器（仅初始化需要的交易所）
        market_fetcher = AsyncMarketDataFetcher()
        await market_fetcher.initialize_specific_exchanges(settings.PERP_EXCHANGES)
        
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
        
        # 持续监控循环
        scan_count = 0
        while True:
            # 记录扫描开始时间
            scan_start_time = time.time()
            scan_count += 1
            logger.info(f"开始第 {scan_count} 次扫描...")
            
            try:
                # 获取市场数据（仅获取永续合约数据）
                market_data = await market_fetcher.fetch_perp_contract_data(
                    lookback_minutes=settings.LOOKBACK_MINUTES
                )
                logger.info(f"获取了 {sum(len(exchange_data.get('future', {})) for exchange_data in market_data.values())} 个永续合约的市场数据")
                
                # 添加详细的交易所数据统计
                for exchange_id, exchange_data in market_data.items():
                    future_count = len(exchange_data.get('future', {}))
                    logger.info(f"交易所 {exchange_id} 获取了 {future_count} 个永续合约数据")
                
                # 分析价格差异
                price_diff_alerts = perp_monitor.calculate_price_differences(market_data)
                logger.info(f"发现 {len(price_diff_alerts)} 个超过阈值的跨所永续合约价差")
                
                # 如果有警报，发送通知
                if price_diff_alerts:
                    logger.info(f"发送 {len(price_diff_alerts)} 个价差警报...")
                    notifier.send_notification(price_diff_alerts)
                
                # 等待下一次扫描
                scan_duration = time.time() - scan_start_time
                sleep_time = max(1, settings.SCAN_INTERVAL_SECONDS - scan_duration)
                logger.info(f"扫描完成，耗时 {scan_duration:.2f} 秒，休眠 {sleep_time:.2f} 秒")
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"扫描过程中发生错误: {str(e)}", exc_info=True)
                # 发生错误时等待短暂时间再继续
                await asyncio.sleep(5)
    
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在关闭...")
    except Exception as e:
        logger.error(f"程序运行过程中发生错误: {str(e)}", exc_info=True)
    finally:
        # 关闭所有连接
        if 'market_fetcher' in locals():
            await market_fetcher.close_all()
        
        # 记录运行时间
        total_runtime = time.time() - start_time
        logger.info(f"程序运行时间: {total_runtime:.2f} 秒")

if __name__ == '__main__':
    # 在 Windows 上需要使用 asyncio 的特定策略
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 运行异步主函数
    asyncio.run(main()) 