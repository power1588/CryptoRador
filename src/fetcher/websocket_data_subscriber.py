import os
import sys
import time
import logging
import asyncio
from typing import Dict, List, Set, Optional, Callable, Any
import ccxt.pro as ccxtpro
import ccxt
import pandas as pd
from datetime import datetime, timedelta
import json

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class WebSocketDataSubscriber:
    """使用ccxt.pro的WebSocket API订阅市场数据"""
    
    def __init__(self):
        """初始化订阅器"""
        self.exchanges = {}
        self.active_subscriptions = {}  # 存储活跃的订阅
        self.subscription_callbacks = {}  # 存储订阅回调
        self.data_buffers = {}  # 存储每个交易对的最近数据
        self.invalid_symbols = {}  # 记录每个交易所的无效交易对
        self.is_running = False
        self.symbol_metadata = {}  # 存储交易对的元数据信息
        
    async def initialize_exchanges(self):
        """初始化交易所连接"""
        logger.info("初始化交易所WebSocket连接...")
        
        # 初始化数据结构
        for exchange_id in settings.EXCHANGES:
            self.active_subscriptions[exchange_id] = set()
            self.subscription_callbacks[exchange_id] = {}
            self.data_buffers[exchange_id] = {}
            self.invalid_symbols[exchange_id] = set()
            self.symbol_metadata[exchange_id] = {}
        
        # 初始化交易所连接
        for exchange_id in settings.EXCHANGES:
            try:
                # 获取交易所类
                exchange_class = getattr(ccxtpro, exchange_id)
                
                # 配置交易所
                config = {}
                if not settings.USE_PUBLIC_DATA_ONLY and exchange_id in settings.API_KEYS:
                    api_config = settings.API_KEYS[exchange_id]
                    # 只有当凭证不为空时才添加
                    if api_config.get('api_key') and api_config.get('secret'):
                        config = api_config
                        logger.info(f"Using API credentials for {exchange_id}")
                    else:
                        logger.info(f"API credentials for {exchange_id} not provided, using public API")
                else:
                    logger.info(f"Using public API for {exchange_id} (public-only mode: {settings.USE_PUBLIC_DATA_ONLY})")
                
                # 添加通用配置
                config.update({
                    'enableRateLimit': True,
                    'timeout': settings.REQUEST_TIMEOUT_SECONDS * 1000,  # 毫秒
                })
                
                # 创建交易所实例
                self.exchanges[exchange_id] = exchange_class(config)
                logger.info(f"Initialized WebSocket connection for {exchange_id}")
                
                # 预加载市场以验证连接是否工作
                await self.exchanges[exchange_id].load_markets()
                logger.info(f"Successfully loaded markets for {exchange_id}")
                
            except Exception as e:
                logger.error(f"Failed to initialize exchange {exchange_id} WebSocket: {str(e)}")
                if exchange_id in self.exchanges:
                    del self.exchanges[exchange_id]
    
    async def get_active_markets(self, exchange_id: str, market_types: List[str]) -> List[Dict]:
        """获取活跃的市场列表
        
        Args:
            exchange_id: 交易所ID
            market_types: 市场类型列表 ['spot', 'future']
            
        Returns:
            活跃市场列表
        """
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            logger.error(f"Exchange {exchange_id} not initialized")
            return []
        
        try:
            # 重新加载市场数据
            await exchange.load_markets(reload=True)
            
            active_markets = []
            
            for symbol, market in exchange.markets.items():
                # 跳过非活跃市场
                if 'active' in market and not market['active']:
                    continue
                
                # 跳过已知的无效交易对
                if symbol in self.invalid_symbols[exchange_id]:
                    continue
                
                # 检查市场类型
                is_future = (
                    market.get('future', False) or 
                    market.get('swap', False) or
                    ('PERP' in symbol) or 
                    (':USDT' in symbol) or
                    ('_PERP' in symbol) or
                    ('/USDT-PERP' in symbol) or
                    ('-SWAP' in symbol) or
                    ('-FUTURES' in symbol) or
                    ('_usd_' in symbol.lower()) or
                    ('-usd-' in symbol.lower()) or
                    ('/USD:' in symbol) or
                    ('/USDT:' in symbol)
                )
                
                current_type = 'future' if is_future else 'spot'
                
                # 如果市场类型在请求的类型列表中，添加到结果
                if current_type in market_types:
                    # 保存一些元数据，便于后续使用
                    self.symbol_metadata[exchange_id][symbol] = {
                        'type': current_type,
                        'base': market.get('base', ''),
                        'quote': market.get('quote', ''),
                        'precision': market.get('precision', {}),
                        'limits': market.get('limits', {})
                    }
                    active_markets.append(market)
            
            logger.info(f"Found {len(active_markets)} active markets on {exchange_id} for types: {market_types}")
            return active_markets
        
        except Exception as e:
            logger.error(f"Error fetching active markets from {exchange_id}: {str(e)}")
            return []
    
    async def subscribe_to_ohlcv(self, exchange_id: str, symbol: str, callback: Callable, 
                            timeframe: str = '1m', buffer_size: int = 10):
        """订阅交易对的OHLCV数据
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            callback: 当收到新数据时调用的回调函数
            timeframe: 时间周期
            buffer_size: 缓存的K线数量
        """
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            logger.error(f"Exchange {exchange_id} not initialized")
            return False
        
        # 检查交易所是否支持OHLCV WebSocket
        if not exchange.has['watchOHLCV']:
            logger.warning(f"Exchange {exchange_id} does not support OHLCV WebSocket subscriptions")
            return False
        
        # 检查是否已经订阅
        subscription_key = f"{symbol}:{timeframe}"
        if subscription_key in self.active_subscriptions[exchange_id]:
            logger.info(f"Already subscribed to {symbol} OHLCV on {exchange_id}")
            # 更新回调
            self.subscription_callbacks[exchange_id][subscription_key] = callback
            return True
        
        try:
            # 初始化数据缓冲区
            if subscription_key not in self.data_buffers[exchange_id]:
                self.data_buffers[exchange_id][subscription_key] = []
            
            # 添加到活跃订阅列表
            self.active_subscriptions[exchange_id].add(subscription_key)
            
            # 保存回调
            self.subscription_callbacks[exchange_id][subscription_key] = callback
            
            # 启动订阅处理任务
            asyncio.create_task(self._handle_ohlcv_subscription(exchange_id, symbol, timeframe, buffer_size))
            
            logger.info(f"Successfully subscribed to {symbol} OHLCV on {exchange_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to {symbol} OHLCV on {exchange_id}: {str(e)}")
            # 从活跃订阅中移除
            subscription_key = f"{symbol}:{timeframe}"
            if subscription_key in self.active_subscriptions[exchange_id]:
                self.active_subscriptions[exchange_id].remove(subscription_key)
            return False
    
    async def _handle_ohlcv_subscription(self, exchange_id: str, symbol: str, timeframe: str, buffer_size: int):
        """处理OHLCV订阅并调用回调
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            timeframe: 时间周期
            buffer_size: 缓存的K线数量
        """
        exchange = self.exchanges.get(exchange_id)
        subscription_key = f"{symbol}:{timeframe}"
        
        # 初始化重试计数
        retries = 0
        max_retries = settings.MAX_RETRIES
        
        while (subscription_key in self.active_subscriptions[exchange_id] and 
               exchange_id in self.exchanges and self.is_running):
            try:
                # 订阅OHLCV数据
                ohlcv = await exchange.watchOHLCV(symbol, timeframe)
                
                # 重置重试计数
                retries = 0
                
                # 确保是有效数据
                if ohlcv and len(ohlcv) > 0:
                    # 转换为DataFrame
                    current_candle = {
                        'timestamp': ohlcv[-1][0],
                        'open': ohlcv[-1][1], 
                        'high': ohlcv[-1][2],
                        'low': ohlcv[-1][3],
                        'close': ohlcv[-1][4],
                        'volume': ohlcv[-1][5]
                    }
                    
                    # 将数据添加到缓冲区
                    buffer = self.data_buffers[exchange_id][subscription_key]
                    
                    # 检查是否是新的K线或更新现有K线
                    is_new_candle = True
                    if buffer and buffer[-1]['timestamp'] == current_candle['timestamp']:
                        # 更新现有K线
                        buffer[-1] = current_candle
                        is_new_candle = False
                    else:
                        # 添加新K线
                        buffer.append(current_candle)
                        # 保持缓冲区大小限制
                        if len(buffer) > buffer_size:
                            buffer = buffer[-buffer_size:]
                            self.data_buffers[exchange_id][subscription_key] = buffer
                    
                    # 调用回调函数
                    callback = self.subscription_callbacks[exchange_id].get(subscription_key)
                    if callback:
                        df_buffer = pd.DataFrame(buffer)
                        if not df_buffer.empty:
                            df_buffer['timestamp'] = pd.to_datetime(df_buffer['timestamp'], unit='ms')
                            await callback(exchange_id, symbol, df_buffer, is_new_candle)
                
            except ccxt.NetworkError as e:
                retries += 1
                retry_time = settings.RETRY_DELAY_SECONDS * (2 ** (retries - 1))  # 指数退避
                logger.warning(f"Network error in {symbol} OHLCV subscription on {exchange_id}: {str(e)}. "
                              f"Retrying in {retry_time}s ({retries}/{max_retries})")
                
                if retries > max_retries:
                    logger.error(f"Max retries reached for {symbol} OHLCV subscription on {exchange_id}")
                    # 标记为无效交易对
                    self.invalid_symbols[exchange_id].add(symbol)
                    # 从活跃订阅中移除
                    if subscription_key in self.active_subscriptions[exchange_id]:
                        self.active_subscriptions[exchange_id].remove(subscription_key)
                    break
                
                # 等待后重试
                await asyncio.sleep(retry_time)
                
            except ccxt.BaseError as e:
                error_message = str(e)
                
                # 检查是否是交易对相关错误
                if any(err in error_message.lower() for err in 
                      ['invalid symbol', 'symbol not found', 'does not exist', 'invalid symbol status']):
                    logger.warning(f"Invalid symbol {symbol} on {exchange_id}, removing subscription: {error_message}")
                    self.invalid_symbols[exchange_id].add(symbol)
                    # 从活跃订阅中移除
                    if subscription_key in self.active_subscriptions[exchange_id]:
                        self.active_subscriptions[exchange_id].remove(subscription_key)
                    break
                
                # 其他可恢复错误
                retries += 1
                retry_time = settings.RETRY_DELAY_SECONDS * (2 ** (retries - 1))
                logger.warning(f"Error in {symbol} OHLCV subscription on {exchange_id}: {error_message}. "
                              f"Retrying in {retry_time}s ({retries}/{max_retries})")
                
                if retries > max_retries:
                    logger.error(f"Max retries reached for {symbol} OHLCV subscription on {exchange_id}")
                    # 从活跃订阅中移除
                    if subscription_key in self.active_subscriptions[exchange_id]:
                        self.active_subscriptions[exchange_id].remove(subscription_key)
                    break
                
                # 等待后重试
                await asyncio.sleep(retry_time)
                
            except Exception as e:
                logger.error(f"Unexpected error in {symbol} OHLCV subscription on {exchange_id}: {str(e)}")
                # 从活跃订阅中移除
                if subscription_key in self.active_subscriptions[exchange_id]:
                    self.active_subscriptions[exchange_id].remove(subscription_key)
                break
    
    async def unsubscribe_from_ohlcv(self, exchange_id: str, symbol: str, timeframe: str = '1m'):
        """取消订阅交易对的OHLCV数据
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            timeframe: 时间周期
        """
        subscription_key = f"{symbol}:{timeframe}"
        
        if (exchange_id in self.active_subscriptions and 
            subscription_key in self.active_subscriptions[exchange_id]):
            
            # 从活跃订阅中移除
            self.active_subscriptions[exchange_id].remove(subscription_key)
            
            # 从回调中移除
            if subscription_key in self.subscription_callbacks[exchange_id]:
                del self.subscription_callbacks[exchange_id][subscription_key]
            
            logger.info(f"Unsubscribed from {symbol} OHLCV on {exchange_id}")
            return True
        
        return False
    
    async def subscribe_all_markets(self, market_types: List[str], callback: Callable):
        """订阅所有市场的OHLCV数据
        
        Args:
            market_types: 市场类型列表 ['spot', 'future']
            callback: 回调函数
        """
        subscription_tasks = []
        
        for exchange_id in self.exchanges:
            # 获取活跃市场
            markets = await self.get_active_markets(exchange_id, market_types)
            
            logger.info(f"Subscribing to {len(markets)} markets on {exchange_id}")
            
            # 限制处理的市场数量
            if len(markets) > 500:
                logger.info(f"Too many markets ({len(markets)}) on {exchange_id}, sampling a subset for efficiency")
                markets = markets[:500]
            
            # 为每个市场创建订阅任务
            for market in markets:
                symbol = market['symbol']
                subscription_tasks.append(
                    self.subscribe_to_ohlcv(exchange_id, symbol, callback)
                )
        
        # 并行执行所有订阅任务
        if subscription_tasks:
            results = await asyncio.gather(*subscription_tasks, return_exceptions=True)
            success_count = sum(1 for r in results if r is True)
            logger.info(f"Successfully subscribed to {success_count}/{len(subscription_tasks)} markets")
    
    async def start(self):
        """启动订阅器"""
        if self.is_running:
            logger.warning("WebSocket subscriber is already running")
            return
        
        self.is_running = True
        logger.info("Starting WebSocket data subscriber")
        
        # 初始化交易所连接
        await self.initialize_exchanges()
    
    async def stop(self):
        """停止订阅器并清理资源"""
        if not self.is_running:
            return
        
        logger.info("Stopping WebSocket data subscriber")
        self.is_running = False
        
        # 清理所有活跃订阅
        for exchange_id in self.active_subscriptions:
            self.active_subscriptions[exchange_id].clear()
        
        # 关闭所有交易所连接
        close_tasks = []
        for exchange_id, exchange in self.exchanges.items():
            if exchange:
                try:
                    task = asyncio.create_task(exchange.close())
                    close_tasks.append((exchange_id, task))
                except Exception as e:
                    logger.error(f"Error creating close task for {exchange_id}: {str(e)}")
        
        # 等待所有关闭任务完成
        for exchange_id, task in close_tasks:
            try:
                await asyncio.wait_for(task, timeout=5.0)
                logger.info(f"Closed WebSocket connection to {exchange_id}")
            except asyncio.TimeoutError:
                logger.warning(f"Timeout closing WebSocket connection to {exchange_id}")
            except Exception as e:
                logger.error(f"Error closing WebSocket connection to {exchange_id}: {str(e)}")
        
        logger.info("WebSocket data subscriber stopped") 