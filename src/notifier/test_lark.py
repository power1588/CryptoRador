#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Lark通知测试脚本
用于测试Lark机器人是否能正常发送通知
"""

import os
import sys
import logging
from datetime import datetime

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.notifier.lark_notifier import LarkNotifier
from src.config import settings

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_lark_notification():
    """测试Lark通知功能"""
    
    # 检查是否配置了Lark webhook URL
    if not settings.LARK_WEBHOOK_URL:
        logger.error("未配置Lark Webhook URL，请在.env文件中设置LARK_WEBHOOK_URL")
        return False
    
    # 创建一个测试异常行情数据
    test_movements = [
        {
            'exchange': 'binance',
            'symbol': 'BTC/USDT',
            'price_change_percent': 2.5,
            'volume_ratio': 7.8,
            'current_price': 70000.0,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'is_future': False,
        },
        {
            'exchange': 'okx',
            'symbol': 'ETH/USDT',
            'price_change_percent': 3.2,
            'volume_ratio': 6.3,
            'current_price': 3500.0,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'is_future': False,
        }
    ]
    
    # 创建并初始化通知器
    notifier = LarkNotifier()
    
    # 发送测试通知
    logger.info("正在发送测试通知到Lark...")
    result = notifier.send_notification(test_movements)
    
    if result:
        logger.info("测试通知发送成功!")
    else:
        logger.error("测试通知发送失败，请检查Lark配置")
    
    return result

if __name__ == "__main__":
    test_lark_notification() 