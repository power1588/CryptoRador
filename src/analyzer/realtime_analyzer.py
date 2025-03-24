import os
import sys
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Callable, Tuple
from datetime import datetime, timedelta
import asyncio
import ccxt.pro as ccxtpro

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class RealtimeMarketAnalyzer:
    """实时市场分析器，用于分析WebSocket推送的K线数据"""
    
    def __init__(self, price_increase_threshold: float = None, 
                 volume_spike_threshold: float = None, 
                 lookback_periods: int = None):
        """初始化分析器
        
        Args:
            price_increase_threshold: 价格上涨百分比阈值
            volume_spike_threshold: 成交量放大倍数阈值
            lookback_periods: 回溯K线数量
        """
        # 使用配置或默认值
        self.price_increase_threshold = price_increase_threshold or settings.MIN_PRICE_INCREASE_PERCENT
        self.volume_spike_threshold = volume_spike_threshold or settings.VOLUME_SPIKE_THRESHOLD
        self.lookback_periods = lookback_periods or settings.LOOKBACK_MINUTES
        
        # 存储最近检测到的异常
        self.recent_anomalies = {}
        # 防止重复报警
        self.alert_cooldowns = {}
        # 默认冷却时间(秒)
        self.cooldown_seconds = 3600  # 1小时
        
        # 缓存获取的30天历史数据，减少API调用
        self.historical_data_cache = {}
        self.cache_expiry = {}  # 缓存过期时间 
        
        logger.info(f"Initialized RealtimeMarketAnalyzer with: price_threshold={self.price_increase_threshold}%, "
                   f"volume_threshold={self.volume_spike_threshold}x, lookback={self.lookback_periods} periods")
    
    async def get_historical_daily_data(self, exchange_id: str, symbol: str, days: int = 30) -> pd.DataFrame:
        """获取历史日K线数据
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            days: 获取的天数
            
        Returns:
            DataFrame包含历史日K线数据
        """
        cache_key = f"{exchange_id}:{symbol}:daily:{days}"
        
        # 检查缓存
        now = datetime.now()
        if (cache_key in self.historical_data_cache and 
            cache_key in self.cache_expiry and 
            now < self.cache_expiry[cache_key]):
            logger.debug(f"Using cached historical data for {symbol} on {exchange_id}")
            return self.historical_data_cache[cache_key]
        
        logger.info(f"Fetching {days} days historical daily data for {symbol} on {exchange_id}")
        
        try:
            # 创建交易所实例
            exchange_class = getattr(ccxtpro, exchange_id)
            exchange = exchange_class({
                'enableRateLimit': True,
                'timeout': settings.REQUEST_TIMEOUT_SECONDS * 1000,
            })
            
            # 计算开始时间 (当前时间 - days天)
            since = int((now - timedelta(days=days)).timestamp() * 1000)
            
            # 获取日K线数据
            ohlcv = await exchange.fetch_ohlcv(symbol, '1d', since, days + 5)  # 多获取几天以确保足够数据
            
            # 关闭交易所连接
            await exchange.close()
            
            # 如果没有数据，返回空DataFrame
            if not ohlcv or len(ohlcv) == 0:
                logger.warning(f"No historical daily data available for {symbol} on {exchange_id}")
                return pd.DataFrame()
                
            # 创建DataFrame
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # 只保留最近days天的数据
            df = df.sort_values('timestamp', ascending=False).head(days)
            
            # 更新缓存
            self.historical_data_cache[cache_key] = df
            # 设置缓存过期时间 (6小时)
            self.cache_expiry[cache_key] = now + timedelta(hours=6)
            
            logger.info(f"Successfully fetched {len(df)} days of historical data for {symbol} on {exchange_id}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol} on {exchange_id}: {str(e)}")
            return pd.DataFrame()
    
    async def calculate_price_percentile(self, exchange_id: str, symbol: str, current_price: float) -> Tuple[float, pd.DataFrame]:
        """计算当前价格在30天历史价格中的百分位数
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            current_price: 当前价格
            
        Returns:
            (百分位数, 历史数据DataFrame)
        """
        # 获取30天历史数据
        df = await self.get_historical_daily_data(exchange_id, symbol)
        
        if df.empty:
            logger.warning(f"No historical data available to calculate percentile for {symbol} on {exchange_id}")
            return 0.0, df
        
        try:
            # 计算百分位数
            prices = df['close'].values
            percentile = np.sum(prices < current_price) / len(prices) * 100
            
            logger.info(f"Current price {current_price} for {symbol} on {exchange_id} is at {percentile:.2f}% percentile of 30-day range")
            return percentile, df
            
        except Exception as e:
            logger.error(f"Error calculating percentile for {symbol} on {exchange_id}: {str(e)}")
            return 0.0, df
    
    def is_future_contract(self, symbol: str) -> bool:
        """判断是否是期货合约
        
        Args:
            symbol: 交易对符号
            
        Returns:
            True如果是期货合约，否则False
        """
        # 检查常见的期货标识
        return (
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
    
    async def on_new_kline(self, exchange_id: str, symbol: str, 
                    kline_data: pd.DataFrame, is_new_candle: bool) -> Optional[Dict]:
        """处理新的K线数据
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            kline_data: K线数据DataFrame，包含多根K线
            is_new_candle: 是否是新的K线(True)或更新现有K线(False)
            
        Returns:
            如果检测到异常，返回异常信息字典；否则返回None
        """
        # 如果不是新K线，我们也检查，因为更新的数据也可能触发警报
        # 如果数据不足，直接返回
        if len(kline_data) < 2:
            return None
            
        # 检查是否在冷却期
        cooldown_key = f"{exchange_id}:{symbol}"
        if cooldown_key in self.alert_cooldowns:
            last_alert_time = self.alert_cooldowns[cooldown_key]
            if (datetime.now() - last_alert_time).total_seconds() < self.cooldown_seconds:
                return None
        
        try:
            # 确保数据按时间排序
            kline_data = kline_data.sort_values('timestamp')
            
            # 计算价格变化
            latest_price = kline_data['close'].iloc[-1]
            
            # 如果有足够的历史数据，使用lookback_periods周期前的价格
            if len(kline_data) >= self.lookback_periods:
                reference_price = kline_data['close'].iloc[-self.lookback_periods]
            else:
                # 否则使用最早的可用价格
                reference_price = kline_data['close'].iloc[0]
            
            price_change_percent = ((latest_price - reference_price) / reference_price) * 100
            
            # 计算成交量变化
            latest_volume = kline_data['volume'].iloc[-1]
            
            # 计算平均成交量(不包括最新的那根K线)
            if len(kline_data) > 1:
                avg_volume = kline_data['volume'].iloc[:-1].mean()
            else:
                avg_volume = latest_volume  # 没有历史数据时使用当前值
                
            volume_change_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0
            
            # 检查是否有异常
            is_abnormal_price = price_change_percent >= self.price_increase_threshold
            is_abnormal_volume = volume_change_ratio >= self.volume_spike_threshold
            
            # 同时满足价格和成交量异常条件
            if is_abnormal_price and is_abnormal_volume:
                # 检查是否是期货合约
                is_future = self.is_future_contract(symbol)
                
                # 创建异常信息
                anomaly = {
                    'exchange': exchange_id,
                    'symbol': symbol,
                    'timestamp': kline_data['timestamp'].iloc[-1],
                    'current_price': latest_price,
                    'reference_price': reference_price,
                    'price_change_percent': price_change_percent,
                    'current_volume': latest_volume,
                    'average_volume': avg_volume,
                    'volume_change_ratio': volume_change_ratio,
                    'detected_at': datetime.now(),
                    'is_future': is_future,  # 添加期货标识
                    'volume_ratio': volume_change_ratio  # 添加兼容性字段
                }
                
                # 异步计算30天价格分位数
                price_percentile, hist_data = await self.calculate_price_percentile(
                    exchange_id, symbol, latest_price
                )
                
                # 添加价格分位数信息
                anomaly['price_percentile'] = price_percentile
                
                # 如果有历史数据，添加一些额外的统计数据
                if not hist_data.empty:
                    anomaly['30d_high'] = hist_data['high'].max()
                    anomaly['30d_low'] = hist_data['low'].min()
                    anomaly['30d_avg'] = hist_data['close'].mean()
                
                # 保存到最近异常列表
                self.recent_anomalies[cooldown_key] = anomaly
                
                # 设置冷却时间
                self.alert_cooldowns[cooldown_key] = datetime.now()
                
                logger.info(f"Detected anomaly in {symbol} on {exchange_id}: "
                          f"Price +{price_change_percent:.2f}% (at {price_percentile:.2f}% percentile), "
                          f"Volume {volume_change_ratio:.2f}x")
                
                return anomaly
        
        except Exception as e:
            logger.error(f"Error analyzing kline data for {symbol} on {exchange_id}: {str(e)}")
        
        return None
    
    def get_recent_anomalies(self, max_age_minutes: int = 60) -> List[Dict]:
        """获取最近检测到的异常
        
        Args:
            max_age_minutes: 最大年龄(分钟)
            
        Returns:
            最近的异常列表
        """
        now = datetime.now()
        recent = []
        
        for key, anomaly in self.recent_anomalies.items():
            detected_at = anomaly['detected_at']
            age_minutes = (now - detected_at).total_seconds() / 60
            
            if age_minutes <= max_age_minutes:
                recent.append(anomaly)
        
        # 按检测时间排序
        recent.sort(key=lambda x: x['detected_at'], reverse=True)
        return recent
    
    def clear_old_anomalies(self, max_age_minutes: int = 60):
        """清理旧的异常记录
        
        Args:
            max_age_minutes: 最大年龄(分钟)
        """
        now = datetime.now()
        keys_to_remove = []
        
        for key, anomaly in self.recent_anomalies.items():
            detected_at = anomaly['detected_at']
            age_minutes = (now - detected_at).total_seconds() / 60
            
            if age_minutes > max_age_minutes:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.recent_anomalies[key]
            
        # 同时清理冷却期记录
        keys_to_remove = []
        for key, last_time in self.alert_cooldowns.items():
            if (now - last_time).total_seconds() > self.cooldown_seconds:
                keys_to_remove.append(key)
                
        for key in keys_to_remove:
            del self.alert_cooldowns[key] 