import os
import sys
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta

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
        
        logger.info(f"Initialized RealtimeMarketAnalyzer with: price_threshold={self.price_increase_threshold}%, "
                   f"volume_threshold={self.volume_spike_threshold}x, lookback={self.lookback_periods} periods")
    
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
                    'detected_at': datetime.now()
                }
                
                # 保存到最近异常列表
                self.recent_anomalies[cooldown_key] = anomaly
                
                # 设置冷却时间
                self.alert_cooldowns[cooldown_key] = datetime.now()
                
                logger.info(f"Detected anomaly in {symbol} on {exchange_id}: "
                          f"Price +{price_change_percent:.2f}%, Volume {volume_change_ratio:.2f}x")
                
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