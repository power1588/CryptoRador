import os
import sys
import time
import logging
import asyncio
from typing import Dict, List, Optional, Tuple, Any
import ccxt.pro as ccxtpro
import ccxt
import pandas as pd
from datetime import datetime, timedelta

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class AsyncMarketDataFetcher:
    """使用ccxt.pro异步获取市场数据"""
    
    def __init__(self):
        self.exchanges = {}
        self.semaphore = None  # 将在initialize_exchanges中初始化
        self.invalid_symbols = {}  # 记录每个交易所的无效交易对
        
    async def initialize_exchanges(self, max_concurrent_requests=None):
        """异步初始化交易所连接"""
        # 如果未指定，则使用配置中的值
        if max_concurrent_requests is None:
            max_concurrent_requests = settings.MAX_CONCURRENT_REQUESTS
            
        # 创建并发控制信号量
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        logger.info(f"Initialized semaphore with {max_concurrent_requests} max concurrent requests")
        
        # 初始化无效交易对字典
        for exchange_id in settings.EXCHANGES:
            self.invalid_symbols[exchange_id] = set()
        
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
                    'rateLimit': int(1000 / settings.RATE_LIMIT_FACTOR),  # 调整速率限制
                })
                
                # 创建交易所实例
                self.exchanges[exchange_id] = exchange_class(config)
                logger.info(f"Initialized exchange: {exchange_id} with timeout={config['timeout']}ms")
                
                # 预加载市场以验证连接是否工作
                await asyncio.wait_for(
                    self.exchanges[exchange_id].load_markets(),
                    timeout=settings.REQUEST_TIMEOUT_SECONDS
                )
                logger.info(f"Successfully loaded markets for {exchange_id}")
                
            except asyncio.TimeoutError:
                logger.error(f"Timeout initializing exchange {exchange_id}")
                if exchange_id in self.exchanges:
                    del self.exchanges[exchange_id]
            except Exception as e:
                logger.error(f"Failed to initialize exchange {exchange_id}: {str(e)}")
                if exchange_id in self.exchanges:
                    del self.exchanges[exchange_id]
    
    async def get_all_markets(self, exchange_id: str, market_type: str = 'spot') -> List[Dict]:
        """异步获取交易所的所有交易对
        
        Args:
            exchange_id: 交易所ID
            market_type: 'spot'或'future'
            
        Returns:
            市场符号列表
        """
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            logger.error(f"Exchange {exchange_id} not initialized")
            return []
        
        try:
            await exchange.load_markets(reload=True)  # 强制重新加载市场数据
            
            # 按类型过滤市场
            markets = []
            for symbol, market in exchange.markets.items():
                # 检查交易对状态 - 过滤掉非活跃状态的交易对
                if 'active' in market and not market['active']:
                    continue
                    
                # 检查是否在已知的无效交易对列表中
                if symbol in self.invalid_symbols[exchange_id]:
                    continue
                
                # 处理不同交易所的期货命名约定
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
                
                if market_type == 'spot' and not is_future:
                    markets.append(market)
                elif market_type == 'future' and is_future:
                    markets.append(market)
            
            logger.info(f"Found {len(markets)} active {market_type} markets on {exchange_id}")
            return markets
        except Exception as e:
            logger.error(f"Error fetching markets from {exchange_id}: {str(e)}")
            return []
    
    async def fetch_ohlcv_with_semaphore(self, exchange_id: str, symbol: str, timeframe: str = '1m', 
                   limit: int = 5, since: Optional[int] = None) -> pd.DataFrame:
        """使用信号量控制并发的OHLCV数据获取
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            timeframe: 时间周期 ('1m', '5m'等)
            limit: 获取的K线数量
            since: 起始时间戳(毫秒)
            
        Returns:
            包含OHLCV数据的DataFrame
        """
        async with self.semaphore:
            return await self.fetch_ohlcv(exchange_id, symbol, timeframe, limit, since)
    
    async def fetch_ohlcv(self, exchange_id: str, symbol: str, timeframe: str = '1m', 
                   limit: int = 5, since: Optional[int] = None) -> pd.DataFrame:
        """异步获取OHLCV(K线)数据，包含重试逻辑
        
        Args:
            exchange_id: 交易所ID
            symbol: A Trading pair symbol
            timeframe: Timeframe ('1m', '5m' etc)
            limit: Number of candles to get
            since: Start timestamp (milliseconds)
            
        Returns:
            DataFrame containing OHLCV data
        """
        # 检查是否已知的无效交易对
        if symbol in self.invalid_symbols[exchange_id]:
            return pd.DataFrame()
            
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            logger.error(f"Exchange {exchange_id} not initialized")
            return pd.DataFrame()
        
        retries = 0
        max_retries = settings.MAX_RETRIES
        retry_delay = settings.RETRY_DELAY_SECONDS
        
        while retries <= max_retries:
            try:
                # 使用超时控制
                ohlcv = await asyncio.wait_for(
                    exchange.fetch_ohlcv(symbol, timeframe, since, limit),
                    timeout=settings.REQUEST_TIMEOUT_SECONDS
                )
                
                # 如果数据为空，返回空DataFrame
                if not ohlcv or len(ohlcv) == 0:
                    return pd.DataFrame()
                    
                # 创建DataFrame并格式化
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                return df
                
            except asyncio.TimeoutError:
                retries += 1
                if retries <= max_retries:
                    logger.warning(f"Timeout fetching OHLCV for {symbol} on {exchange_id}, retry {retries}/{max_retries}")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Max retries reached for {symbol} on {exchange_id}")
                    return pd.DataFrame()
                    
            except ccxt.BaseError as e:
                # 处理CCXT特定错误
                error_message = str(e)
                # 检查是否无效交易对错误
                if any(err in error_message.lower() for err in ['invalid symbol', 'symbol not found', 'does not exist', 'invalid symbol status']):
                    logger.warning(f"Invalid symbol {symbol} on {exchange_id}, excluding from future scans")
                    self.invalid_symbols[exchange_id].add(symbol)
                    return pd.DataFrame()
                elif 'has no symbol' in error_message.lower():
                    logger.warning(f"Symbol {symbol} not available on {exchange_id}, excluding from future scans")
                    self.invalid_symbols[exchange_id].add(symbol)
                    return pd.DataFrame()
                elif 'unknown symbol' in error_message.lower():
                    logger.warning(f"Unknown symbol {symbol} on {exchange_id}, excluding from future scans")
                    self.invalid_symbols[exchange_id].add(symbol)
                    return pd.DataFrame()
                # 检查是否是可重试的错误
                elif any(err in error_message.lower() for err in ['rate limit', 'too many requests', 'ddos', 'nonce', 'busy', 'maintenance', 'temporary']):
                    retries += 1
                    if retries <= max_retries:
                        retry_time = retry_delay * (2 ** (retries - 1))  # 指数退避
                        logger.warning(f"Rate limit or temporary error for {symbol} on {exchange_id}, waiting {retry_time}s before retry {retries}/{max_retries}")
                        await asyncio.sleep(retry_time)
                    else:
                        logger.error(f"Max retries reached for {symbol} on {exchange_id}: {error_message}")
                        return pd.DataFrame()
                else:
                    logger.error(f"Error fetching OHLCV for {symbol} on {exchange_id}: {error_message}")
                    return pd.DataFrame()
                    
            except Exception as e:
                logger.error(f"Unexpected error fetching OHLCV for {symbol} on {exchange_id}: {str(e)}")
                return pd.DataFrame()
    
    async def fetch_recent_data(self, lookback_minutes: int = 5) -> Dict[str, Dict[str, pd.DataFrame]]:
        """异步获取所有配置的交易所和市场类型的近期市场数据
        
        Args:
            lookback_minutes: 获取历史数据的分钟数
            
        Returns:
            交易所 -> 符号 -> DataFrame的字典
        """
        result = {}
        since = int((datetime.now() - timedelta(minutes=lookback_minutes)).timestamp() * 1000)
        
        # 为每个交易所创建结果字典
        for exchange_id in self.exchanges:
            result[exchange_id] = {}
            
        # 创建所有获取任务
        all_tasks = []
        for exchange_id in self.exchanges:
            for market_type in settings.MARKET_TYPES:
                # 获取市场列表
                markets = await self.get_all_markets(exchange_id, market_type)
                
                # 限制处理的市场数量
                if len(markets) > 500:
                    logger.info(f"Too many markets ({len(markets)}) on {exchange_id}, sampling a subset for efficiency")
                    # 根据交易量或其他指标排序可能更有价值，这里简单取前500个
                    markets = markets[:500]
                
                for market in markets:
                    symbol = market['symbol']
                    # 跳过已知的无效交易对
                    if symbol in self.invalid_symbols[exchange_id]:
                        continue
                        
                    # 创建任务信息元组
                    task_info = (exchange_id, symbol)
                    all_tasks.append(task_info)
        
        logger.info(f"Prepared {len(all_tasks)} fetch tasks across all exchanges")
        
        # 使用批处理方式执行任务
        batch_size = min(100, len(all_tasks))
        success_count = 0
        processed = 0
        start_time = time.time()
        
        # 分批处理所有任务
        for i in range(0, len(all_tasks), batch_size):
            batch = all_tasks[i:i+batch_size]
            batch_tasks = []
            
            # 为这一批创建实际的协程任务
            for exchange_id, symbol in batch:
                # 对每个任务创建新的协程，而不是复用
                task = asyncio.create_task(self.fetch_ohlcv_with_semaphore(
                    exchange_id=exchange_id,
                    symbol=symbol,
                    timeframe='1m',
                    limit=lookback_minutes,
                    since=since
                ))
                batch_tasks.append((exchange_id, symbol, task))
            
            # 等待当前批次的所有任务完成
            for exchange_id, symbol, task in batch_tasks:
                try:
                    df = await task
                    if not df.empty:
                        result[exchange_id][symbol] = df
                        success_count += 1
                except Exception as e:
                    logger.warning(f"Failed to fetch data for {symbol} on {exchange_id}: {str(e)}")
                
                processed += 1
                if processed % 100 == 0:
                    logger.info(f"Processed {processed}/{len(all_tasks)} tasks ({success_count} successful)")
            
            # 每批处理完后，让系统有短暂休息
            if i + batch_size < len(all_tasks):
                await asyncio.sleep(0.1)  # 100ms休息，防止系统过载
        
        elapsed = time.time() - start_time
        logger.info(f"Successfully fetched data for {success_count}/{len(all_tasks)} symbols in {elapsed:.2f} seconds")
        
        # 记录每个交易所获取的数据量
        for exchange_id in result:
            logger.info(f"Fetched data for {len(result[exchange_id])} symbols from {exchange_id}")
            
        return result
    
    async def close_all(self):
        """关闭所有交易所连接"""
        close_tasks = []
        
        for exchange_id, exchange in self.exchanges.items():
            if exchange:
                try:
                    # 创建关闭任务
                    task = asyncio.create_task(exchange.close())
                    close_tasks.append((exchange_id, task))
                except Exception as e:
                    logger.error(f"Error creating close task for {exchange_id}: {str(e)}")
        
        # 等待所有关闭任务完成
        for exchange_id, task in close_tasks:
            try:
                await asyncio.wait_for(task, timeout=5.0)  # 设置超时以避免无限等待
                logger.info(f"Closed connection to {exchange_id}")
            except asyncio.TimeoutError:
                logger.warning(f"Timeout closing connection to {exchange_id}")
            except Exception as e:
                logger.error(f"Error closing {exchange_id} connection: {str(e)}")
        
        # 记录收集到的无效交易对信息
        for exchange_id, symbols in self.invalid_symbols.items():
            if symbols:
                logger.info(f"Collected {len(symbols)} invalid symbols for {exchange_id}") 