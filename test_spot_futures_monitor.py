#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试脚本 - 现货与期货价差监控
用于测试和验证现货与期货之间的价差监控功能
"""

import os
import sys
import logging
import asyncio
from datetime import datetime

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.config import settings
from src.fetcher.async_data_fetcher import AsyncMarketDataFetcher
from src.analyzer.spot_futures_monitor import SpotFuturesMonitor
from src.notifier.lark_notifier import LarkNotifier

# 配置日志
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

async def test_spot_futures_monitoring():
    """测试现货与期货价差监控功能"""
    try:
        # 创建必要的组件
        data_fetcher = AsyncMarketDataFetcher()
        spot_futures_monitor = SpotFuturesMonitor(threshold=settings.SPOT_FUTURES_DIFF_THRESHOLD)
        
        # 使用专用的现货-期货价差通知器
        if settings.SPOT_FUTURES_LARK_WEBHOOK_URL:
            notifier = LarkNotifier(
                webhook_url=settings.SPOT_FUTURES_LARK_WEBHOOK_URL,
                secret=settings.SPOT_FUTURES_LARK_SECRET
            )
            logger.info("使用专用的现货-期货价差通知通道")
        else:
            notifier = LarkNotifier(
                webhook_url=settings.LARK_WEBHOOK_URL,
                secret=settings.LARK_SECRET
            )
            logger.warning("未配置专用的现货-期货价差通知通道，使用默认通知通道")
        
        # 初始化交易所连接
        logger.info("正在初始化交易所连接...")
        await data_fetcher.initialize_exchanges(max_concurrent_requests=settings.MAX_CONCURRENT_REQUESTS)
        
        # 获取市场数据
        logger.info(f"正在获取市场数据，回溯 {settings.LOOKBACK_MINUTES} 分钟...")
        market_data = await data_fetcher.fetch_recent_data(
            lookback_minutes=settings.LOOKBACK_MINUTES
        )
        
        # 检查是否获取到了数据
        total_exchanges = len(market_data)
        if total_exchanges == 0:
            logger.error("未能获取到任何交易所数据，请检查网络连接或API设置")
            return
            
        logger.info(f"成功从 {total_exchanges} 个交易所获取数据")
        
        # 检查每个交易所是否同时有现货和期货数据
        for exchange, market_types in market_data.items():
            has_spot = 'spot' in market_types and market_types['spot']
            has_future = 'future' in market_types and market_types['future']
            
            if has_spot and has_future:
                spot_count = len(market_types['spot'])
                future_count = len(market_types['future'])
                logger.info(f"交易所 {exchange}: 现货 {spot_count} 个, 期货 {future_count} 个")
            else:
                missing = []
                if not has_spot:
                    missing.append("现货")
                if not has_future:
                    missing.append("期货")
                logger.warning(f"交易所 {exchange} 缺少 {', '.join(missing)} 数据")
        
        # 分析现货和期货之间的价差
        logger.info(f"正在分析现货和期货之间的价差，阈值: {settings.SPOT_FUTURES_DIFF_THRESHOLD}%")
        spot_futures_alerts = spot_futures_monitor.detect_abnormal_basis(market_data)
        
        # 处理结果
        if spot_futures_alerts:
            logger.info(f"检测到 {len(spot_futures_alerts)} 个现货-期货价差异常")
            
            # 打印详细信息
            for i, alert in enumerate(spot_futures_alerts, 1):
                exchange = alert.get('exchange', 'Unknown')
                spot_symbol = alert.get('spot_symbol', 'Unknown')
                future_symbol = alert.get('future_symbol', 'Unknown')
                spot_price = alert.get('spot_price', 0.0)
                future_price = alert.get('future_price', 0.0)
                price_diff = alert.get('price_difference_percent', 0.0)
                
                logger.info(f"异常 #{i}:")
                logger.info(f"  交易所: {exchange}")
                logger.info(f"  现货: {spot_symbol} @ {spot_price}")
                logger.info(f"  期货: {future_symbol} @ {future_price}")
                logger.info(f"  价差: {price_diff:.4f}%")
                logger.info("------------------")
            
            # 发送通知
            logger.info("正在发送飞书通知...")
            notification_success = notifier.send_notification(spot_futures_alerts)
            
            if notification_success:
                logger.info("飞书通知发送成功")
            else:
                logger.error("飞书通知发送失败")
        else:
            logger.info("未检测到现货-期货价差异常")
        
    except Exception as e:
        logger.error(f"测试过程中出现错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # 关闭所有连接
        logger.info("正在关闭交易所连接...")
        if 'data_fetcher' in locals():
            await data_fetcher.close_all()
        logger.info("测试完成")

if __name__ == "__main__":
    # 在Windows上需要使用不同的事件循环策略
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    # 运行测试函数
    asyncio.run(test_spot_futures_monitoring()) 