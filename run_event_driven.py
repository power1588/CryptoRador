#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CryptoRador事件驱动版本 - 基于WebSocket的加密货币异常行情监测工具
入口脚本，用于从项目根目录直接启动事件驱动版本的程序
"""

import os
import sys
import argparse
import logging

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# 导入事件驱动主程序
from src.event_driven_main import main
from src.config import settings

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='CryptoRador事件驱动版本 - 基于WebSocket的加密货币异常行情监测工具')
    
    # 添加基本配置参数
    parser.add_argument('-e', '--exchanges', type=str,
                        help=f'要监控的交易所，逗号分隔，默认: {",".join(settings.EXCHANGES)}')
    parser.add_argument('-m', '--markets', type=str,
                        help=f'要监控的市场类型，逗号分隔(spot,future)，默认: {",".join(settings.MARKET_TYPES)}')
    parser.add_argument('-l', '--lookback', type=int,
                        help=f'历史K线数量，默认: {settings.LOOKBACK_MINUTES}')
    
    # 添加阈值参数
    parser.add_argument('-p', '--price-threshold', type=float,
                        help=f'价格上涨阈值(百分比)，默认: {settings.MIN_PRICE_INCREASE_PERCENT}%')
    parser.add_argument('-v', '--volume-threshold', type=float,
                        help=f'成交量放大阈值(倍数)，默认: {settings.VOLUME_SPIKE_THRESHOLD}x')
    
    # 添加日志级别
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help=f'日志级别，默认: {settings.LOG_LEVEL}')
    
    return parser.parse_args()

def update_settings_from_args(args):
    """根据命令行参数更新设置"""
    # 注意：这里我们只在运行时修改设置，不修改.env文件
    if args.exchanges:
        settings.EXCHANGES = args.exchanges.split(',')
    
    if args.markets:
        settings.MARKET_TYPES = args.markets.split(',')
    
    if args.lookback:
        settings.LOOKBACK_MINUTES = args.lookback
    
    if args.price_threshold:
        settings.MIN_PRICE_INCREASE_PERCENT = args.price_threshold
    
    if args.volume_threshold:
        settings.VOLUME_SPIKE_THRESHOLD = args.volume_threshold
    
    if args.log_level:
        settings.LOG_LEVEL = args.log_level
        # 重新配置日志
        logging.getLogger().setLevel(getattr(logging, settings.LOG_LEVEL))

if __name__ == "__main__":
    # 解析命令行参数
    args = parse_arguments()
    
    # 更新设置
    update_settings_from_args(args)
    
    # 启动事件驱动版本的程序
    main() 