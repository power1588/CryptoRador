#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CryptoRador异步版本 - 加密货币异常行情监测工具
入口脚本，用于从项目根目录直接启动异步版本的程序
"""

import os
import sys
import argparse
import logging

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# 导入异步主程序
from src.async_main import main
from src.config import settings

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='CryptoRador异步版本 - 加密货币异常行情监测工具')
    
    # 添加基本配置参数
    parser.add_argument('-i', '--interval', type=int, 
                        help=f'扫描间隔(秒)，默认: {settings.SCAN_INTERVAL_SECONDS}')
    parser.add_argument('-e', '--exchanges', type=str,
                        help=f'要扫描的交易所，逗号分隔，默认: {",".join(settings.EXCHANGES)}')
    parser.add_argument('-m', '--markets', type=str,
                        help=f'要扫描的市场类型，逗号分隔(spot,future)，默认: {",".join(settings.MARKET_TYPES)}')
    parser.add_argument('-l', '--lookback', type=int,
                        help=f'历史回溯时间(分钟)，默认: {settings.LOOKBACK_MINUTES}')
    
    # 添加性能相关参数
    parser.add_argument('-c', '--concurrent', type=int,
                        help=f'最大并发请求数，默认: {settings.MAX_CONCURRENT_REQUESTS}')
    parser.add_argument('-t', '--timeout', type=int,
                        help=f'请求超时时间(秒)，默认: {settings.REQUEST_TIMEOUT_SECONDS}')
    parser.add_argument('-r', '--retries', type=int,
                        help=f'最大重试次数，默认: {settings.MAX_RETRIES}')
    
    # 添加日志级别
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help=f'日志级别，默认: {settings.LOG_LEVEL}')
    
    return parser.parse_args()

def update_settings_from_args(args):
    """根据命令行参数更新设置"""
    # 注意：这里我们只在运行时修改设置，不修改.env文件
    if args.interval:
        settings.SCAN_INTERVAL_SECONDS = args.interval
    
    if args.exchanges:
        settings.EXCHANGES = args.exchanges.split(',')
    
    if args.markets:
        settings.MARKET_TYPES = args.markets.split(',')
    
    if args.lookback:
        settings.LOOKBACK_MINUTES = args.lookback
    
    if args.concurrent:
        settings.MAX_CONCURRENT_REQUESTS = args.concurrent
    
    if args.timeout:
        settings.REQUEST_TIMEOUT_SECONDS = args.timeout
    
    if args.retries:
        settings.MAX_RETRIES = args.retries
    
    if args.log_level:
        settings.LOG_LEVEL = args.log_level
        # 重新配置日志
        logging.getLogger().setLevel(getattr(logging, settings.LOG_LEVEL))

if __name__ == "__main__":
    # 解析命令行参数
    args = parse_arguments()
    
    # 更新设置
    update_settings_from_args(args)
    
    # 启动异步版本的程序
    main() 