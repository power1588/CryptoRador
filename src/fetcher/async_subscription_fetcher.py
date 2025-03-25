import os
import sys
import logging
import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
import ccxt.pro as ccxtpro
import pandas as pd
from datetime import datetime, timedelta

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class SubscriptionDataFetcher:
    """使用WebSocket订阅模式获取市场数据"""
    
    def __init__(self):
        """初始化订阅数据获取器"""
        self.exchanges = {}
        self.subscriptions = {}  # 跟踪活跃的订阅
        self.ohlcv_data = {}  # 存储订阅获取的数据
        self.initialized = False
        self.running = False
    
    async def initialize_exchanges(self):
        """初始化交易所连接"""
        logger.info("正在初始化交易所连接...")
        
        for exchange_id in settings.EXCHANGES:
            try:
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
                await self.exchanges[exchange_id].load_markets()
                logger.info(f"成功加载{exchange_id}的市场数据")
                
                # 初始化该交易所的数据存储
                self.ohlcv_data[exchange_id] = {}
                self.subscriptions[exchange_id] = set()
                
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
    
    async def find_spot_futures_pairs(self) -> Dict[str, List[Tuple[str, str]]]:
        """查找每个交易所中可用的现货-期货交易对
        
        Returns:
            Dict[exchange_id, List[(spot_symbol, future_symbol)]]
        """
        result = {}
        
        for exchange_id, exchange in self.exchanges.items():
            # 初始化结果
            result[exchange_id] = []
            
            try:
                markets = exchange.markets
                
                # 分类现货和期货市场
                spot_markets = {}
                future_markets = {}
                
                for symbol, market in markets.items():
                    # 跳过非活跃交易对
                    if 'active' in market and not market['active']:
                        continue
                    
                    # 只处理USDT计价的交易对
                    if 'USDT' not in symbol:
                        continue
                    
                    # 判断是否为期货
                    is_future = (
                        market.get('future', False) or 
                        market.get('swap', False) or
                        ('PERP' in symbol) or 
                        (':USDT' in symbol) or
                        ('_PERP' in symbol) or
                        ('/USDT-PERP' in symbol) or
                        ('-SWAP' in symbol) or
                        ('-FUTURES' in symbol)
                    )
                    
                    if is_future:
                        future_markets[symbol] = market
                    else:
                        spot_markets[symbol] = market
                
                # 匹配现货和期货交易对
                matched_pairs = self._find_matching_pairs(spot_markets, future_markets)
                result[exchange_id] = matched_pairs
                
                logger.info(f"在{exchange_id}上找到 {len(matched_pairs)} 对匹配的现货-期货交易对")
                
            except Exception as e:
                logger.error(f"查找{exchange_id}的现货-期货对时出错: {str(e)}")
        
        return result
    
    def _extract_base_symbol(self, symbol: str) -> str:
        """从交易对符号中提取基础符号"""
        # 处理常见的期货命名约定
        symbol = symbol.replace('PERP', '').replace('-SWAP', '')
        symbol = symbol.replace('_PERP', '').replace('-FUTURES', '')
        
        # 处理币对格式
        if '/' in symbol:
            parts = symbol.split('/')
            if len(parts) > 1:
                return parts[0].strip()
        
        # 处理其他格式
        for stablecoin in ['USDT', 'BUSD', 'USDC', 'USD']:
            if symbol.endswith(stablecoin):
                return symbol[:-len(stablecoin)].strip('_-:/')
        
        return symbol
    
    def _find_matching_pairs(self, spot_markets, future_markets) -> List[Tuple[str, str]]:
        """找到匹配的现货和期货交易对"""
        matched_pairs = []
        
        # 创建现货交易对映射
        spot_symbols_map = {}
        for spot_symbol in spot_markets:
            base = self._extract_base_symbol(spot_symbol)
            if base:
                spot_symbols_map[base] = spot_symbol
        
        # 从期货开始，查找对应的现货
        for future_symbol in future_markets:
            base = self._extract_base_symbol(future_symbol)
            if base in spot_symbols_map:
                matched_pairs.append((spot_symbols_map[base], future_symbol))
                logger.debug(f"匹配到交易对: 现货 {spot_symbols_map[base]} - 期货 {future_symbol}")
        
        return matched_pairs
    
    async def subscribe_to_ohlcv(self, exchange_id: str, symbol: str, timeframe: str = '1m'):
        """订阅指定交易对的OHLCV数据
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            timeframe: 时间周期
        """
        if exchange_id not in self.exchanges:
            logger.error(f"交易所{exchange_id}未初始化")
            return
        
        exchange = self.exchanges[exchange_id]
        
        # 检查是否已订阅
        sub_key = f"{exchange_id}:{symbol}:{timeframe}"
        if sub_key in self.subscriptions.get(exchange_id, set()):
            logger.debug(f"已订阅 {sub_key}")
            return
        
        try:
            # 初始化数据存储
            if exchange_id not in self.ohlcv_data:
                self.ohlcv_data[exchange_id] = {}
            
            if symbol not in self.ohlcv_data[exchange_id]:
                # 创建具有明确数据类型的空DataFrame
                self.ohlcv_data[exchange_id][symbol] = pd.DataFrame({
                    'timestamp': pd.Series(dtype='datetime64[ns]'),
                    'open': pd.Series(dtype='float64'),
                    'high': pd.Series(dtype='float64'),
                    'low': pd.Series(dtype='float64'),
                    'close': pd.Series(dtype='float64'),
                    'volume': pd.Series(dtype='float64')
                })
            
            # 添加到活跃订阅
            self.subscriptions[exchange_id].add(sub_key)
            
            logger.info(f"开始订阅 {exchange_id} 的 {symbol} {timeframe} 数据")
            
            # 这里不立即订阅，而是在watch_ohlcv_for_all中处理
            
        except Exception as e:
            logger.error(f"订阅{exchange_id}的{symbol} OHLCV数据时出错: {str(e)}")
    
    async def watch_ohlcv_handler(self, exchange_id: str, symbol: str, timeframe: str = '1m'):
        """处理单个交易对的OHLCV数据订阅和更新
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            timeframe: 时间周期
        """
        if exchange_id not in self.exchanges:
            return
        
        exchange = self.exchanges[exchange_id]
        exchange_name = exchange_id.lower()
        retry_count = 0
        max_retries = 3
        
        # 记录一些统计信息
        successful_updates = 0
        failed_updates = 0
        
        try:
            while self.running:
                try:
                    # 获取最新的K线数据
                    ohlcv = await exchange.watch_ohlcv(symbol, timeframe)
                    
                    # 重置重试计数
                    retry_count = 0
                    
                    # 验证数据有效性
                    if not ohlcv or len(ohlcv) == 0:
                        logger.warning(f"从 {exchange_id} 收到的 {symbol} 数据为空，跳过")
                        await asyncio.sleep(1)
                        continue
                    
                    # 确保数据格式正确
                    if len(ohlcv[-1]) != 6:
                        logger.warning(f"从 {exchange_id} 收到的 {symbol} 数据格式不是OHLCV标准格式，列数={len(ohlcv[-1])}，跳过")
                        await asyncio.sleep(1)
                        continue
                    
                    # 额外验证交易所特定的问题
                    if exchange_name == 'binance':
                        # 币安特别处理：确保数据格式完全符合标准
                        try:
                            # 确保每个字段都可以转换为正确的类型
                            timestamp, open_price, high, low, close, volume = ohlcv[-1]
                            
                            # 验证类型
                            if not (isinstance(timestamp, (int, float)) and 
                                   isinstance(open_price, (int, float)) and
                                   isinstance(high, (int, float)) and
                                   isinstance(low, (int, float)) and
                                   isinstance(close, (int, float)) and
                                   isinstance(volume, (int, float))):
                                logger.warning(f"从 {exchange_id} 收到的 {symbol} 数据类型不正确，跳过")
                                await asyncio.sleep(1)
                                continue
                        except Exception as e:
                            logger.warning(f"验证 {exchange_id} 的 {symbol} 数据时出错: {str(e)}，跳过")
                            await asyncio.sleep(1)
                            continue
                    
                    # 转换为DataFrame
                    last_candle = pd.DataFrame(
                        [ohlcv[-1]], 
                        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    )
                    
                    # 确保数据类型正确
                    try:
                        # 转换时间戳
                        last_candle['timestamp'] = pd.to_datetime(last_candle['timestamp'], unit='ms')
                        
                        # 确保价格和交易量是数值类型
                        for col in ['open', 'high', 'low', 'close', 'volume']:
                            last_candle[col] = pd.to_numeric(last_candle[col], errors='coerce')
                        
                        # 检查是否有无效值
                        if last_candle.isnull().any().any():
                            logger.warning(f"从 {exchange_id} 收到的 {symbol} 数据包含无效值，跳过")
                            await asyncio.sleep(1)
                            continue
                    except Exception as e:
                        logger.warning(f"处理 {exchange_id} 的 {symbol} 数据时出错: {str(e)}，跳过此更新")
                        await asyncio.sleep(1)
                        continue
                    
                    # 确保已初始化了DataFrame（可能在错误处理后重置）
                    if symbol not in self.ohlcv_data.get(exchange_id, {}):
                        # 创建具有明确数据类型的空DataFrame
                        self.ohlcv_data[exchange_id][symbol] = pd.DataFrame({
                            'timestamp': pd.Series(dtype='datetime64[ns]'),
                            'open': pd.Series(dtype='float64'),
                            'high': pd.Series(dtype='float64'),
                            'low': pd.Series(dtype='float64'),
                            'close': pd.Series(dtype='float64'),
                            'volume': pd.Series(dtype='float64')
                        })

                    # 更新数据存储
                    try:
                        # 如果已存在相同时间戳的数据，则更新它
                        existing_idx = self.ohlcv_data[exchange_id][symbol]['timestamp'] == last_candle['timestamp'].iloc[0]
                        
                        if existing_idx.any():
                            # 逐列更新，避免长度不匹配错误
                            for col in self.ohlcv_data[exchange_id][symbol].columns:
                                if col in last_candle.columns:
                                    self.ohlcv_data[exchange_id][symbol].loc[existing_idx, col] = last_candle[col].iloc[0]
                        else:
                            # 修复: 检查是否为首次添加数据
                            if len(self.ohlcv_data[exchange_id][symbol]) == 0:
                                # 如果是空的DataFrame，直接赋值而不是连接
                                self.ohlcv_data[exchange_id][symbol] = last_candle
                            else:
                                # 否则添加新行，使用ignore_index=True和经过验证的数据类型
                                # 确保列类型匹配
                                for col in self.ohlcv_data[exchange_id][symbol].columns:
                                    if col != 'timestamp' and col in last_candle.columns:
                                        last_candle[col] = last_candle[col].astype(self.ohlcv_data[exchange_id][symbol][col].dtype)
                                
                                # 检查并确保列完全匹配
                                missing_columns = set(self.ohlcv_data[exchange_id][symbol].columns) - set(last_candle.columns)
                                if missing_columns:
                                    # 添加缺失的列，填充适当的默认值
                                    for col in missing_columns:
                                        if col == 'timestamp':
                                            last_candle[col] = pd.to_datetime(last_candle['timestamp'].iloc[0])
                                        else:
                                            # 数值列使用0作为默认值
                                            last_candle[col] = 0.0
                                
                                # 安全连接
                                try:
                                    # 确保列顺序一致
                                    last_candle = last_candle[self.ohlcv_data[exchange_id][symbol].columns]
                                    self.ohlcv_data[exchange_id][symbol] = pd.concat(
                                        [self.ohlcv_data[exchange_id][symbol], last_candle], 
                                        ignore_index=True,
                                        sort=False  # 不对列进行排序
                                    )
                                except Exception as e:
                                    logger.error(f"连接DataFrame时出错: {str(e)}")
                                    logger.debug(f"现有数据列: {list(self.ohlcv_data[exchange_id][symbol].columns)}")
                                    logger.debug(f"新数据列: {list(last_candle.columns)}")
                                    logger.debug(f"现有数据类型: {self.ohlcv_data[exchange_id][symbol].dtypes}")
                                    logger.debug(f"新数据类型: {last_candle.dtypes}")
                                    
                                    # 使用安全的替代方法
                                    try:
                                        # 创建一行新数据，使用现有DataFrame的结构
                                        new_row = pd.Series(index=self.ohlcv_data[exchange_id][symbol].columns)
                                        
                                        # 填充有效数据
                                        for col in self.ohlcv_data[exchange_id][symbol].columns:
                                            if col in last_candle.columns:
                                                new_row[col] = last_candle[col].iloc[0]
                                            elif col == 'timestamp':
                                                new_row[col] = last_candle['timestamp'].iloc[0] if 'timestamp' in last_candle else pd.Timestamp.now()
                                            else:
                                                # 使用0作为默认值
                                                new_row[col] = 0.0
                                        
                                        # 添加行
                                        self.ohlcv_data[exchange_id][symbol].loc[len(self.ohlcv_data[exchange_id][symbol])] = new_row
                                    except Exception as e2:
                                        logger.error(f"备用添加方法也失败: {str(e2)}")
                                        # 保留现有数据，忽略这个更新
                                        logger.warning(f"忽略 {exchange_id} 的 {symbol} 数据更新")
                            
                            # 保持最新的N条记录
                            max_rows = settings.LOOKBACK_MINUTES
                            if len(self.ohlcv_data[exchange_id][symbol]) > max_rows:
                                self.ohlcv_data[exchange_id][symbol] = self.ohlcv_data[exchange_id][symbol].iloc[-max_rows:]
                        
                        successful_updates += 1
                        if successful_updates % 10 == 0:  # 每10次成功更新记录一次
                            logger.debug(f"{exchange_id} 的 {symbol} 成功更新了 {successful_updates} 次数据")
                    
                    except Exception as e:
                        failed_updates += 1
                        logger.error(f"更新 {exchange_id} 的 {symbol} 数据时出错: {str(e)}")
                        # 如果有太多失败，重新初始化数据结构
                        if failed_updates > 5:
                            logger.warning(f"对于 {exchange_id} 的 {symbol}，失败次数过多，重置数据结构")
                            self.ohlcv_data[exchange_id][symbol] = pd.DataFrame({
                                'timestamp': pd.Series(dtype='datetime64[ns]'),
                                'open': pd.Series(dtype='float64'),
                                'high': pd.Series(dtype='float64'),
                                'low': pd.Series(dtype='float64'),
                                'close': pd.Series(dtype='float64'),
                                'volume': pd.Series(dtype='float64')
                            })
                            failed_updates = 0
                    
                except asyncio.CancelledError:
                    logger.info(f"取消了 {exchange_id} 的 {symbol} 订阅任务")
                    break
                except Exception as e:
                    retry_count += 1
                    logger.error(f"处理 {exchange_id} 的 {symbol} 数据时出错: {str(e)}")
                    
                    # 如果连续失败超过最大重试次数，暂停更长时间
                    if retry_count >= max_retries:
                        logger.warning(f"{exchange_id} 的 {symbol} 连续失败 {retry_count} 次，暂停较长时间")
                        await asyncio.sleep(30)  # 暂停30秒
                        retry_count = 0
                    else:
                        # 短暂等待后重试
                        await asyncio.sleep(5)
        
        except Exception as e:
            logger.error(f"监控 {exchange_id} 的 {symbol} 时发生异常: {str(e)}")
        finally:
            # 从活跃订阅中移除
            sub_key = f"{exchange_id}:{symbol}:{timeframe}"
            if exchange_id in self.subscriptions and sub_key in self.subscriptions[exchange_id]:
                self.subscriptions[exchange_id].remove(sub_key)
            logger.info(f"停止监控 {exchange_id} 的 {symbol} 数据，成功更新: {successful_updates}，失败: {failed_updates}")
    
    async def watch_ohlcv_for_all_pairs(self, spot_futures_pairs: Dict[str, List[Tuple[str, str]]]):
        """为所有匹配的现货-期货对启动OHLCV数据监控
        
        Args:
            spot_futures_pairs: 每个交易所的现货-期货对列表
        """
        tasks = []
        
        for exchange_id, pairs in spot_futures_pairs.items():
            if exchange_id not in self.exchanges:
                continue
                
            for spot_symbol, future_symbol in pairs:
                # 订阅现货数据
                await self.subscribe_to_ohlcv(exchange_id, spot_symbol)
                
                # 订阅期货数据
                await self.subscribe_to_ohlcv(exchange_id, future_symbol)
                
                # 创建监控任务
                spot_task = asyncio.create_task(
                    self.watch_ohlcv_handler(exchange_id, spot_symbol)
                )
                future_task = asyncio.create_task(
                    self.watch_ohlcv_handler(exchange_id, future_symbol)
                )
                
                tasks.extend([spot_task, future_task])
        
        return tasks
    
    def get_market_data(self) -> Dict[str, Dict[str, Dict[str, pd.DataFrame]]]:
        """获取当前市场数据，格式为交易所->市场类型->交易对->数据
        
        Returns:
            市场数据字典
        """
        result = {}
        
        for exchange_id, symbols_data in self.ohlcv_data.items():
            result[exchange_id] = {
                'spot': {},
                'future': {}
            }
            
            for symbol, df in symbols_data.items():
                # 检查DataFrame是否为空
                if df.empty:
                    continue
                
                try:    
                    # 安全地复制数据，保留数据类型
                    df_copy = df.copy(deep=True)
                    
                    # 验证DataFrame格式
                    required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    missing_columns = set(required_columns) - set(df_copy.columns)
                    
                    # 如果缺少必要列，跳过此交易对
                    if missing_columns:
                        logger.warning(f"{exchange_id} 的 {symbol} 数据缺少必需列: {missing_columns}，跳过")
                        continue
                    
                    # 确保timestamp列是日期时间类型
                    if not pd.api.types.is_datetime64_any_dtype(df_copy['timestamp']):
                        try:
                            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
                        except Exception as e:
                            logger.warning(f"无法转换 {exchange_id} 的 {symbol} 的时间戳列: {str(e)}，跳过")
                            continue
                    
                    # 确保价格和成交量列是数值类型
                    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
                    for col in numeric_columns:
                        if not pd.api.types.is_numeric_dtype(df_copy[col]):
                            try:
                                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                            except Exception as e:
                                logger.warning(f"无法转换 {exchange_id} 的 {symbol} 的 {col} 列: {str(e)}，跳过")
                                continue
                    
                    # 检查是否有NaN值，如果有则填充
                    if df_copy.isnull().any().any():
                        logger.debug(f"{exchange_id} 的 {symbol} 数据包含NaN值，尝试填充")
                        # 填充缺失值
                        df_copy = df_copy.fillna(method='ffill').fillna(method='bfill')
                        # 仍然有NaN的情况下，对于价格列使用0填充
                        df_copy = df_copy.fillna(0)
                    
                    # 根据符号特征判断市场类型
                    is_future = (
                        ('PERP' in symbol) or 
                        (':USDT' in symbol) or
                        ('_PERP' in symbol) or
                        ('/USDT-PERP' in symbol) or
                        ('-SWAP' in symbol) or
                        ('-FUTURES' in symbol)
                    )
                    
                    if is_future:
                        result[exchange_id]['future'][symbol] = df_copy
                    else:
                        result[exchange_id]['spot'][symbol] = df_copy
                
                except Exception as e:
                    logger.error(f"处理 {exchange_id} 的 {symbol} 数据时出错: {str(e)}")
                    # 继续处理下一个交易对
                    continue
        
        return result
    
    async def start(self):
        """启动数据获取服务"""
        if not self.initialized:
            success = await self.initialize_exchanges()
            if not success:
                logger.error("无法启动数据获取服务: 初始化交易所失败")
                return False
        
        self.running = True
        
        # 查找现货-期货交易对
        spot_futures_pairs = await self.find_spot_futures_pairs()
        
        # 启动所有订阅
        tasks = await self.watch_ohlcv_for_all_pairs(spot_futures_pairs)
        
        return tasks
    
    async def stop(self):
        """停止数据获取服务"""
        self.running = False
        
        # 关闭所有交易所连接
        close_tasks = []
        for exchange_id, exchange in self.exchanges.items():
            if hasattr(exchange, 'close') and callable(exchange.close):
                close_tasks.append(exchange.close())
        
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        self.exchanges = {}
        logger.info("已停止所有数据订阅") 