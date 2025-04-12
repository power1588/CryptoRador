import os
import sys
import logging
import asyncio
from typing import Dict, List, Set, Optional, Callable, Any, Tuple
import ccxt.pro as ccxtpro
import pandas as pd
from datetime import datetime, timedelta
import re
import time

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class PerpWebSocketSubscriber:
    """跨所永续合约WebSocket数据订阅器"""
    
    def __init__(self):
        """初始化订阅器"""
        self.exchanges = {}
        self.active_subscriptions = {}  # 存储活跃的订阅 {exchange_id: set(symbol)}
        self.ohlcv_data = {}  # 存储订阅获取的数据 {exchange_id: {symbol: DataFrame}}
        self.normalized_symbols = {}  # 标准化符号映射 {base: {exchange_id: symbol}}
        self.initialized = False
        self.running = False
        self.stop_event = asyncio.Event()
        self.blacklist = set(base.strip().upper() for base in settings.PERP_BLACKLIST)  # 黑名单集合
        self.data_lock = asyncio.Lock()  # 添加数据锁
        self.market_data = {}  # 添加市场数据存储 {exchange_id: {'future': {symbol: DataFrame}}}
        
        # 记录黑名单
        if self.blacklist:
            logger.info(f"永续合约黑名单: {', '.join(self.blacklist)}")
    
    async def initialize_exchanges(self, exchange_ids: List[str] = None):
        """初始化指定的交易所连接
        
        Args:
            exchange_ids: 要初始化的交易所ID列表，如果为None则使用settings.PERP_EXCHANGES
        """
        target_exchanges = exchange_ids or settings.PERP_EXCHANGES
        logger.info(f"正在初始化交易所连接: {target_exchanges}")
        
        for exchange_id in target_exchanges:
            try:
                # 检查交易所是否存在
                if not hasattr(ccxtpro, exchange_id):
                    logger.error(f"交易所{exchange_id}在ccxt.pro中不存在")
                    continue
                
                # 获取交易所类
                exchange_class = getattr(ccxtpro, exchange_id)
                
                # 配置交易所
                config = {
                    'enableRateLimit': True,
                    'timeout': settings.REQUEST_TIMEOUT_SECONDS * 1000,  # 毫秒
                }
                
                # 如果配置了API密钥，添加到配置中
                if not settings.USE_PUBLIC_DATA_ONLY and exchange_id in settings.API_KEYS:
                    api_config = settings.API_KEYS[exchange_id]
                    if api_config.get('api_key') and api_config.get('secret'):
                        config.update(api_config)
                        logger.info(f"使用API密钥连接 {exchange_id}")
                    else:
                        logger.info(f"未提供{exchange_id}的API密钥，使用公共API")
                else:
                    logger.info(f"使用公共API连接 {exchange_id}")
                
                # 创建交易所实例
                self.exchanges[exchange_id] = exchange_class(config)
                
                # 加载市场数据
                try:
                    await asyncio.wait_for(
                        self.exchanges[exchange_id].load_markets(),
                        timeout=settings.REQUEST_TIMEOUT_SECONDS
                    )
                    logger.info(f"成功加载{exchange_id}的市场数据")
                    
                    # 验证市场数据
                    if not self.exchanges[exchange_id].markets:
                        logger.error(f"{exchange_id} 市场数据为空")
                        continue
                        
                    # 输出市场统计信息
                    market_count = len(self.exchanges[exchange_id].markets)
                    logger.info(f"{exchange_id} 共有 {market_count} 个市场")
                    
                    # 检查是否有永续合约
                    perp_count = 0
                    for symbol, market in self.exchanges[exchange_id].markets.items():
                        if 'USDT' in symbol and (
                            market.get('future', False) or 
                            market.get('swap', False) or
                            'PERP' in symbol or 
                            ':USDT' in symbol
                        ):
                            perp_count += 1
                    
                    logger.info(f"{exchange_id} 共有 {perp_count} 个USDT永续合约")
                    
                except asyncio.TimeoutError:
                    logger.error(f"加载{exchange_id}市场数据超时")
                    if exchange_id in self.exchanges:
                        del self.exchanges[exchange_id]
                    continue
                except Exception as e:
                    logger.error(f"加载{exchange_id}市场数据时出错: {str(e)}")
                    if exchange_id in self.exchanges:
                        del self.exchanges[exchange_id]
                    continue
                
                # 初始化该交易所的数据存储
                self.ohlcv_data[exchange_id] = {}
                self.active_subscriptions[exchange_id] = set()
                
            except Exception as e:
                logger.error(f"初始化交易所{exchange_id}时出错: {str(e)}")
                if exchange_id in self.exchanges:
                    del self.exchanges[exchange_id]
        
        if not self.exchanges:
            logger.error("没有成功初始化任何交易所连接")
            return False
        
        self.initialized = True
        logger.info(f"成功初始化 {len(self.exchanges)} 个交易所连接")
        return True
    
    def _normalize_symbol(self, exchange_id: str, symbol: str) -> str:
        """标准化永续合约符号，便于跨交易所比较
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            
        Returns:
            标准化后的基础符号（如BTC）
        """
        # 移除常见的永续合约标记
        normalized = symbol.replace('PERP', '').replace('-SWAP', '')
        normalized = normalized.replace('_PERP', '').replace('-FUTURES', '')
        normalized = normalized.replace('/USDT-PERP', '').replace(':USDT', '')
        
        # 提取基础交易对
        if '/' in normalized:
            parts = normalized.split('/')
            if len(parts) > 1 and 'USDT' in parts[1]:
                return parts[0].strip()
        
        # 移除USDT后缀
        for marker in ['USDT', '_USDT', '-USDT']:
            if normalized.endswith(marker):
                return normalized[:-len(marker)].strip('_-:/')
        
        return normalized
    
    def _is_blacklisted(self, base_symbol: str) -> bool:
        """检查基础币种是否在黑名单中
        
        Args:
            base_symbol: 基础币种符号（如BTC、ETH等）
            
        Returns:
            是否在黑名单中
        """
        return base_symbol.upper() in self.blacklist
    
    async def get_perp_contracts(self, exchange_id: str) -> List[Dict]:
        """获取指定交易所的USDT永续合约列表
        
        Args:
            exchange_id: 交易所ID
            
        Returns:
            永续合约信息列表
        """
        if exchange_id not in self.exchanges:
            logger.error(f"交易所 {exchange_id} 未初始化")
            return []
            
        try:
            exchange = self.exchanges[exchange_id]
            
            # 重新加载市场数据以确保最新
            await exchange.load_markets(reload=True)
            
            # 筛选USDT永续合约
            perp_contracts = []
            for symbol, market in exchange.markets.items():
                # 检查是否为活跃交易对
                if 'active' in market and not market['active']:
                    continue
                    
                # 检查是否包含USDT
                if 'USDT' not in symbol:
                    continue
                
                # 跳过Gate交易所的期权合约 (含有USDT-日期-价格-P/C格式的合约)
                if exchange_id == 'gate' and (':USDT-' in symbol and (symbol.endswith('-P') or symbol.endswith('-C'))):
                    continue
                
                # 跳过交割合约（一般包含日期格式，如YYYYMMDD或YY-MM-DD）
                # 正则模式：日期格式通常为如下几种：
                # 1. 230628 (YYMMDD)
                # 2. 20230628 (YYYYMMDD)
                # 3. 0628 (MMDD)
                # 4. 23-06 或 23-06-28 或 2023-06-28
                date_patterns = [
                    r'\d{6}',     # YYMMDD
                    r'\d{8}',     # YYYYMMDD
                    r'\d{4}',     # MMDD
                    r'\d{2}-\d{2}', # YY-MM
                    r'\d{2}-\d{2}-\d{2}', # YY-MM-DD
                    r'\d{4}-\d{2}-\d{2}', # YYYY-MM-DD
                    r'\d{4}-\d{2}' # YYYY-MM
                ]
                
                # 检查是否为交割合约
                is_delivery = any(
                    re.search(pattern, symbol) is not None 
                    for pattern in date_patterns
                )
                
                if is_delivery:
                    logger.debug(f"跳过交割合约: {exchange_id}:{symbol}")
                    continue
                
                # 检查是否为永续合约
                is_perp = (
                    market.get('future', False) or 
                    market.get('swap', False) or
                    ('PERP' in symbol) or 
                    (':USDT' in symbol) or
                    ('_PERP' in symbol) or
                    ('/USDT-PERP' in symbol) or
                    ('-SWAP' in symbol) or
                    ('-FUTURES' in symbol)
                )
                
                if is_perp:
                    # 检查是否在黑名单中
                    base_symbol = self._normalize_symbol(exchange_id, symbol)
                    if self._is_blacklisted(base_symbol):
                        logger.debug(f"跳过黑名单币种 {base_symbol} ({exchange_id}:{symbol})")
                        continue
                        
                    perp_contracts.append(market)
            
            logger.info(f"在 {exchange_id} 上找到 {len(perp_contracts)} 个USDT永续合约")
            return perp_contracts
        except Exception as e:
            logger.error(f"获取 {exchange_id} 永续合约列表出错: {str(e)}")
            return []
    
    async def find_common_contracts(self) -> Dict[str, Dict[str, str]]:
        """找到多个交易所共有的永续合约
        
        Returns:
            基础符号到各交易所实际符号的映射: {base_symbol: {exchange_id: actual_symbol}}
        """
        # 确保已经初始化
        if not self.initialized or not self.exchanges:
            logger.error("交易所未初始化，无法查找共同合约")
            return {}
            
        # 按交易所获取永续合约
        exchange_contracts = {}
        for exchange_id in self.exchanges:
            contracts = await self.get_perp_contracts(exchange_id)
            # 将合约按标准化符号分组
            normalized_map = {}
            for contract in contracts:
                symbol = contract['symbol']
                
                # 跳过Gate交易所的期权合约
                if exchange_id == 'gate' and (':USDT-' in symbol and (symbol.endswith('-P') or symbol.endswith('-C'))):
                    continue
                
                # 跳过交割合约（再次检查）
                date_patterns = [
                    r'\d{6}',     # YYMMDD
                    r'\d{8}',     # YYYYMMDD
                    r'\d{4}',     # MMDD
                    r'\d{2}-\d{2}', # YY-MM
                    r'\d{2}-\d{2}-\d{2}', # YY-MM-DD
                    r'\d{4}-\d{2}-\d{2}', # YYYY-MM-DD
                    r'\d{4}-\d{2}' # YYYY-MM
                ]
                is_delivery = any(
                    re.search(pattern, symbol) is not None 
                    for pattern in date_patterns
                )
                if is_delivery:
                    logger.debug(f"在标准化过程中跳过交割合约: {exchange_id}:{symbol}")
                    continue
                    
                base = self._normalize_symbol(exchange_id, symbol)
                if base:
                    # 检查是否在黑名单中
                    if self._is_blacklisted(base):
                        logger.debug(f"跳过黑名单币种 {base} ({exchange_id}:{symbol})")
                        continue
                        
                    normalized_map[base] = symbol
            
            exchange_contracts[exchange_id] = normalized_map
            logger.debug(f"交易所 {exchange_id} 有 {len(normalized_map)} 个标准化永续合约")
        
        # 找到所有交易所共有的基础符号
        if len(exchange_contracts) < 2:
            logger.warning("至少需要两个交易所才能查找共同合约")
            return {}
            
        common_bases = None
        for exchange_id, symbols in exchange_contracts.items():
            if common_bases is None:
                common_bases = set(symbols.keys())
            else:
                common_bases = common_bases.intersection(set(symbols.keys()))
        
        if not common_bases:
            logger.warning("没有找到交易所之间共同的永续合约")
            return {}
            
        # 构建映射关系
        result = {}
        for base in common_bases:
            result[base] = {
                exchange_id: symbols[base] 
                for exchange_id, symbols in exchange_contracts.items() 
                if base in symbols
            }
        
        self.normalized_symbols = result
        logger.info(f"找到 {len(result)} 个在所有交易所中共有的永续合约")
        return result
    
    async def subscribe_to_ohlcv(self, exchange_id: str, symbol: str, timeframe: str = '1m'):
        """订阅指定交易对的OHLCV数据
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            timeframe: 时间周期
        """
        if exchange_id not in self.exchanges:
            logger.error(f"交易所 {exchange_id} 未初始化")
            return False
            
        exchange = self.exchanges[exchange_id]
        
        # 检查是否支持OHLCV WebSocket API
        if not exchange.has['watchOHLCV']:
            logger.warning(f"交易所 {exchange_id} 不支持OHLCV WebSocket订阅")
            return False
            
        # 检查是否已订阅
        if symbol in self.active_subscriptions[exchange_id]:
            logger.debug(f"已订阅 {exchange_id}:{symbol}")
            return True
            
        try:
            # 初始化空DataFrame (不包含任何行，但有正确的列结构)
            if exchange_id not in self.ohlcv_data:
                self.ohlcv_data[exchange_id] = {}
                
            if symbol not in self.ohlcv_data[exchange_id] or not isinstance(self.ohlcv_data[exchange_id][symbol], pd.DataFrame):
                # 使用空列表初始化DataFrame而不是只定义列名
                self.ohlcv_data[exchange_id][symbol] = pd.DataFrame(
                    [], 
                    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                )
            
            # 添加到活跃订阅
            self.active_subscriptions[exchange_id].add(symbol)
            
            # 创建监听任务
            asyncio.create_task(self._watch_ohlcv(exchange_id, symbol, timeframe))
            
            logger.info(f"成功订阅 {exchange_id}:{symbol} 的OHLCV数据")
            return True
        except Exception as e:
            logger.error(f"订阅 {exchange_id}:{symbol} 时出错: {str(e)}")
            return False
    
    async def _watch_ohlcv(self, exchange_id: str, symbol: str, timeframe: str = '1m') -> None:
        """监控特定交易所和交易对的OHLCV数据
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            timeframe: 时间周期，默认为1分钟
        """
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            logger.error(f"交易所 {exchange_id} 未初始化")
            return
            
        retry_count = 0
        max_retries = 3
        
        while not self.stop_event.is_set() and retry_count < max_retries:
            try:
                # 获取OHLCV数据
                ohlcv = await exchange.watchOHLCV(symbol, timeframe)
                if not ohlcv or len(ohlcv) == 0:
                    logger.warning(f"未收到 {exchange_id} {symbol} 的OHLCV数据")
                    await asyncio.sleep(1)
                    continue
                    
                # 获取ticker数据
                ticker = await exchange.watchTicker(symbol)
                if not ticker:
                    logger.warning(f"未收到 {exchange_id} {symbol} 的ticker数据")
                    await asyncio.sleep(1)
                    continue
                    
                # 安全地获取ticker数据中的值
                def safe_get_float(data, key, default=0.0):
                    try:
                        value = data.get(key)
                        if value is None:
                            return default
                        return float(value)
                    except (ValueError, TypeError):
                        return default
                
                # 创建新的数据行
                new_row = pd.DataFrame({
                    'timestamp': [pd.to_datetime(ohlcv[-1][0], unit='ms')],
                    'open': [float(ohlcv[-1][1])],
                    'high': [float(ohlcv[-1][2])],
                    'low': [float(ohlcv[-1][3])],
                    'close': [float(ohlcv[-1][4])],
                    'volume': [float(ohlcv[-1][5])],
                    'quote_volume': [safe_get_float(ticker, 'quoteVolume')],
                    'base_volume': [safe_get_float(ticker, 'baseVolume')],
                    'last_price': [safe_get_float(ticker, 'last', float(ohlcv[-1][4]))],
                    'bid': [safe_get_float(ticker, 'bid')],
                    'ask': [safe_get_float(ticker, 'ask')],
                    'vwap': [safe_get_float(ticker, 'vwap')],
                    'percentage': [safe_get_float(ticker, 'percentage')],
                    'average': [safe_get_float(ticker, 'average')]
                })
                
                # 更新数据存储
                async with self.data_lock:
                    if exchange_id not in self.market_data:
                        self.market_data[exchange_id] = {'future': {}}
                    if symbol not in self.market_data[exchange_id]['future']:
                        self.market_data[exchange_id]['future'][symbol] = pd.DataFrame()
                        
                    df = self.market_data[exchange_id]['future'][symbol]
                    
                    # 检查是否有重复的时间戳
                    if not df.empty and df['timestamp'].iloc[-1] == new_row['timestamp'].iloc[0]:
                        df.iloc[-1] = new_row.iloc[0]
                    else:
                        df = pd.concat([df, new_row], ignore_index=True)
                        
                    # 保持最新的1000条记录
                    if len(df) > 1000:
                        df = df.tail(1000)
                        
                    self.market_data[exchange_id]['future'][symbol] = df
                    
                # 重置重试计数
                retry_count = 0
                
            except Exception as e:
                retry_count += 1
                logger.error(f"监控 {exchange_id} {symbol} 时出错: {str(e)}")
                if retry_count < max_retries:
                    await asyncio.sleep(2 ** retry_count)  # 指数退避
                else:
                    logger.error(f"监控 {exchange_id} {symbol} 失败，已达到最大重试次数")
                    break
    
    async def subscribe_common_contracts(self):
        """订阅所有交易所共有的永续合约"""
        common_contracts = await self.find_common_contracts()
        
        if not common_contracts:
            logger.warning("没有找到可以订阅的共同永续合约")
            return 0
            
        subscription_count = 0
        failed_subscriptions = []
        
        for base, exchange_symbols in common_contracts.items():
            for exchange_id, symbol in exchange_symbols.items():
                try:
                    success = await self.subscribe_to_ohlcv(exchange_id, symbol)
                    if success:
                        subscription_count += 1
                        logger.debug(f"成功订阅 {exchange_id}:{symbol}")
                    else:
                        failed_subscriptions.append(f"{exchange_id}:{symbol}")
                        logger.warning(f"订阅失败 {exchange_id}:{symbol}")
                except Exception as e:
                    failed_subscriptions.append(f"{exchange_id}:{symbol}")
                    logger.error(f"订阅 {exchange_id}:{symbol} 时出错: {str(e)}")
        
        if failed_subscriptions:
            logger.warning(f"以下 {len(failed_subscriptions)} 个订阅失败:")
            for failed in failed_subscriptions:
                logger.warning(f"  - {failed}")
        
        logger.info(f"成功订阅了 {subscription_count} 个永续合约数据流")
        return subscription_count
    
    async def get_market_data(self) -> Dict[str, Dict[str, Dict[str, pd.DataFrame]]]:
        """获取当前市场数据
        
        Returns:
            交易所 -> 市场类型 -> 符号 -> DataFrame的嵌套字典
        """
        result = {}
        
        # 使用 data_lock 保护数据访问
        async with self.data_lock:
            for exchange_id, market_types in self.market_data.items():
                # 确保初始化交易所数据结构
                if exchange_id not in result:
                    result[exchange_id] = {}
                
                # 复制每种市场类型的数据
                for market_type, symbol_data in market_types.items():
                    # 确保市场类型键存在
                    if market_type not in result[exchange_id]:
                        result[exchange_id][market_type] = {}
                    
                    # 复制有效的DataFrame数据
                    valid_symbols = 0
                    for symbol, df in symbol_data.items():
                        try:
                            # 检查DataFrame是否存在且不为空
                            if df is None:
                                logger.warning(f"交易所 {exchange_id} 的 {symbol} DataFrame为None")
                                continue
                                
                            if not isinstance(df, pd.DataFrame):
                                logger.warning(f"交易所 {exchange_id} 的 {symbol} 数据类型错误: {type(df)}")
                                continue
                                
                            if df.empty:
                                logger.warning(f"交易所 {exchange_id} 的 {symbol} DataFrame为空")
                                continue
                            
                            # 验证数据完整性
                            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                            missing_columns = [col for col in required_columns if col not in df.columns]
                            if missing_columns:
                                logger.warning(f"交易所 {exchange_id} 的 {symbol} 缺少必要列: {missing_columns}")
                                continue
                            
                            # 确保数据类型正确
                            df = df.copy()
                            try:
                                df['timestamp'] = pd.to_datetime(df['timestamp'])
                                for col in ['open', 'high', 'low', 'close', 'volume']:
                                    df[col] = pd.to_numeric(df[col], errors='coerce')
                            except Exception as e:
                                logger.error(f"转换 {exchange_id}:{symbol} 数据类型时出错: {str(e)}")
                                continue
                            
                            # 删除包含无效值的行
                            df = df.dropna()
                            
                            # 检查是否有有效数据
                            if df.empty:
                                logger.warning(f"交易所 {exchange_id} 的 {symbol} 数据无效（包含空值）")
                                continue
                            
                            # 检查数据是否过期（超过5分钟）
                            # 注意：交易所数据通常是UTC时间，而本地时间可能是UTC+8
                            # 因此需要调整时间比较逻辑
                            latest_timestamp = df['timestamp'].max()
                            time_diff = (datetime.now() - latest_timestamp).total_seconds()
                            
                            # 如果时间差超过5分钟，记录警告
                            # 但考虑到时区差异，我们允许更大的时间差
                            if time_diff > 300:  # 5分钟
                                # 检查是否是时区问题（时间差接近8小时）
                                if abs(time_diff - 8 * 3600) < 300:  # 如果时间差接近8小时（±5分钟）
                                    logger.debug(f"交易所 {exchange_id} 的 {symbol} 数据时间差 {time_diff/3600:.2f} 小时，可能是时区差异")
                                else:
                                    logger.warning(f"交易所 {exchange_id} 的 {symbol} 数据可能已过期，最新时间: {latest_timestamp}，时间差: {time_diff/60:.2f} 分钟")
                            
                            # 检查价格是否合理
                            if (df['close'] <= 0).any():
                                logger.warning(f"交易所 {exchange_id} 的 {symbol} 包含非正价格")
                                # 过滤掉非正价格
                                df = df[df['close'] > 0]
                                if df.empty:
                                    logger.warning(f"交易所 {exchange_id} 的 {symbol} 过滤后没有有效数据")
                                    continue
                            
                            # 检查价格是否异常
                            if (df['close'] > 1000000).any() or (df['close'] < 0.000001).any():
                                logger.warning(f"交易所 {exchange_id} 的 {symbol} 包含异常价格")
                                # 不跳过异常价格，但仍然记录警告
                            
                            result[exchange_id][market_type][symbol] = df
                            valid_symbols += 1
                            logger.debug(f"交易所 {exchange_id} 的 {symbol} 有 {len(df)} 条有效数据")
                            
                        except Exception as e:
                            logger.error(f"处理 {exchange_id}:{symbol} 数据时出错: {str(e)}")
                    
                    # 添加调试日志
                    if valid_symbols > 0:
                        logger.info(f"交易所 {exchange_id} 的 {market_type} 市场有 {valid_symbols} 个有效的价格数据")
                    else:
                        logger.warning(f"交易所 {exchange_id} 的 {market_type} 市场没有任何有效的价格数据")
        
        return result
    
    async def start(self):
        """启动数据订阅"""
        if not self.initialized:
            success = await self.initialize_exchanges()
            if not success:
                return False
                
        self.stop_event.clear()
        self.running = True
        
        # 订阅共同的永续合约
        subscription_count = await self.subscribe_common_contracts()
        
        if subscription_count == 0:
            logger.warning("没有成功订阅任何永续合约，检查交易所设置和网络连接")
            self.running = False
            return False
            
        logger.info(f"永续合约WebSocket订阅启动成功，订阅了 {subscription_count} 个数据流")
        return True
    
    async def stop(self):
        """停止数据订阅"""
        if not self.running:
            return
            
        logger.info("正在停止永续合约数据订阅...")
        self.stop_event.set()
        self.running = False
        
        # 关闭所有交易所连接
        close_tasks = []
        for exchange_id, exchange in list(self.exchanges.items()):
            try:
                logger.info(f"关闭 {exchange_id} 连接...")
                task = asyncio.create_task(exchange.close())
                close_tasks.append((exchange_id, task))
            except Exception as e:
                logger.error(f"关闭 {exchange_id} 连接时出错: {str(e)}")
        
        # 等待所有关闭任务完成
        for exchange_id, task in close_tasks:
            try:
                await asyncio.wait_for(task, timeout=10.0)
                logger.info(f"成功关闭 {exchange_id} 连接")
            except asyncio.TimeoutError:
                logger.warning(f"关闭 {exchange_id} 连接超时")
            except Exception as e:
                logger.error(f"等待 {exchange_id} 关闭时出错: {str(e)}")
        
        # 清理数据
        self.exchanges = {}
        self.active_subscriptions = {}
        
        logger.info("永续合约数据订阅已停止") 