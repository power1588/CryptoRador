#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试Lark (飞书) 通知功能
This script tests the Lark (Feishu) notification functionality.
"""

import os
import sys
import logging
import argparse
import json
import time
from datetime import datetime

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.notifier.lark_notifier import LarkNotifier
from src.config import settings

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Test Lark Notification')
    parser.add_argument('--webhook', type=str, help='Lark webhook URL (overrides settings)')
    parser.add_argument('--secret', type=str, help='Lark webhook secret (overrides settings)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    return parser.parse_args()

def main():
    """主函数"""
    args = parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
    
    logger.info("Testing Lark notification...")
    
    # 获取webhook_url和secret
    webhook_url = args.webhook or settings.LARK_WEBHOOK_URL
    secret = args.secret or settings.LARK_SECRET
    
    if not webhook_url:
        logger.error("Error: Lark webhook URL not provided")
        sys.exit(1)
    
    logger.info(f"Using webhook URL: {webhook_url}")
    logger.info(f"Using secret: {'*' * (len(secret) if secret else 0)}")
    
    # 创建Lark notifier实例
    notifier = LarkNotifier(webhook_url=webhook_url, secret=secret)
    
    # 使用新的测试方法
    result = notifier.test_notification()
    
    if result:
        logger.info("✅ Test passed! Lark notification sent successfully.")
    else:
        logger.error("❌ Test failed! Failed to send Lark notification.")
        sys.exit(1)

if __name__ == "__main__":
    main() 