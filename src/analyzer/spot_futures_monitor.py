import os
import sys
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class SpotFuturesMonitor:
    """监控交易所现货和期货交易对的价差"""
    
    def __init__(self, threshold: float = 0.1, basis_direction: str = 'both'):
        """初始化监控器
        
        Args:
            threshold: 价差阈值(百分比)，超过该值将触发警报
            basis_direction: 价差方向筛选，可选值:
                'both' - 同时监控升水和贴水 (默认)
                'premium' - 只监控升水 (期货价格 > 现货价格)
                'discount' - 只监控贴水 (期货价格 < 现货价格)
        """
        self.threshold = threshold
        
        # 验证并设置价差方向
        valid_directions = ['both', 'premium', 'discount']
        if basis_direction not in valid_directions:
            logger.warning(f"无效的basis_direction值: {basis_direction}，使用默认值'both'")
            self.basis_direction = 'both'
        else:
            self.basis_direction = basis_direction
            
        logger.info(f"初始化价差监控器：阈值={threshold}%，监控方向={self.basis_direction}")
        
    def _extract_base_symbol(self, symbol: str) -> str:
        """从交易对符号中提取基础符号
        
        Args:
            symbol: 交易对符号
            
        Returns:
            基础符号
        """
        # 处理常见的期货命名约定
        symbol = symbol.replace('PERP', '').replace('-SWAP', '')
        symbol = symbol.replace('_PERP', '').replace('-FUTURES', '')
        
        # 处理币对格式
        if '/' in symbol:
            parts = symbol.split('/')
            if len(parts) > 1:
                # 处理类似BTC/USDT的格式
                return parts[0].strip()
        
        # 处理其他格式
        for stablecoin in ['USDT', 'BUSD', 'USDC', 'USD']:
            if symbol.endswith(stablecoin):
                return symbol[:-len(stablecoin)].strip('_-:/')
        
        return symbol
        
    def _find_matching_pairs(self, spot_markets: Dict[str, pd.DataFrame], 
                            future_markets: Dict[str, pd.DataFrame]) -> List[Tuple[str, str]]:
        """查找匹配的现货和期货交易对
        
        Args:
            spot_markets: 现货市场数据
            future_markets: 期货市场数据
            
        Returns:
            匹配的(现货符号, 期货符号)列表
        """
        matched_pairs = []
        
        # 创建现货交易对映射以快速查询
        spot_symbols_map = {}
        for spot_symbol in spot_markets.keys():
            # 只处理USDT计价的现货交易对
            if 'USDT' in spot_symbol:
                base = self._extract_base_symbol(spot_symbol)
                if base:
                    spot_symbols_map[base] = spot_symbol
        
        # 从期货开始，查找对应的现货交易对
        for future_symbol in future_markets.keys():
            # 只处理USDT计价的期货交易对
            if 'USDT' in future_symbol:
                base = self._extract_base_symbol(future_symbol)
                if base in spot_symbols_map:
                    matched_pairs.append((spot_symbols_map[base], future_symbol))
                    logger.debug(f"匹配到交易对: 现货 {spot_symbols_map[base]} - 期货 {future_symbol}")
        
        return matched_pairs
    
    def calculate_price_difference(self, spot_df: pd.DataFrame, future_df: pd.DataFrame) -> Optional[float]:
        """计算现货和期货价格之间的百分比差异
        
        Args:
            spot_df: 现货OHLCV数据
            future_df: 期货OHLCV数据
            
        Returns:
            价格差异百分比，如果数据不足则返回None
        """
        if spot_df.empty or future_df.empty:
            return None
            
        # 获取最新收盘价
        spot_price = spot_df.iloc[-1]['close']
        future_price = future_df.iloc[-1]['close']
        
        if spot_price <= 0:
            return None
            
        # 计算未经调整的基差百分比 (future_price - spot_price) / spot_price * 100
        basis_percent = ((future_price - spot_price) / spot_price) * 100
        
        return basis_percent
    
    def detect_abnormal_basis(self, market_data: Dict[str, Dict[str, pd.DataFrame]]) -> List[Dict[str, Any]]:
        """检测现货和期货之间的异常价差
        
        Args:
            market_data: 交易所 -> 市场类型 -> 符号 -> DataFrame的市场数据嵌套字典
            
        Returns:
            包含异常价差信息的字典列表
        """
        abnormal_basis_alerts = []
        
        # 按交易所处理数据
        for exchange_id, market_types in market_data.items():
            # 检查交易所是否同时有现货和期货数据
            if 'spot' not in market_types or 'future' not in market_types:
                logger.debug(f"Exchange {exchange_id} does not have both spot and futures markets")
                continue
                
            spot_markets = market_types['spot']
            future_markets = market_types['future']
            
            # 找到匹配的现货和期货对
            matched_pairs = self._find_matching_pairs(spot_markets, future_markets)
            logger.info(f"Found {len(matched_pairs)} matched spot-futures pairs on {exchange_id}")
            
            # 分析每对匹配的现货和期货
            for spot_symbol, future_symbol in matched_pairs:
                if spot_symbol not in spot_markets or future_symbol not in future_markets:
                    continue
                    
                spot_df = spot_markets[spot_symbol]
                future_df = future_markets[future_symbol]
                
                # 计算价差
                price_diff = self.calculate_price_difference(spot_df, future_df)
                
                if price_diff is None:
                    continue
                
                # 确定基差方向
                is_premium = price_diff > 0  # 期货升水
                
                # 根据设置的方向筛选
                if self.basis_direction == 'premium' and not is_premium:
                    # 只监控升水，但这是贴水，跳过
                    continue
                elif self.basis_direction == 'discount' and is_premium:
                    # 只监控贴水，但这是升水，跳过
                    continue
                    
                # 检查价差是否超过阈值
                if abs(price_diff) >= self.threshold:
                    # 获取最新价格
                    spot_price = spot_df.iloc[-1]['close']
                    future_price = future_df.iloc[-1]['close']
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 根据基差方向设置备注
                    direction_text = "期货升水" if is_premium else "期货贴水"
                    
                    alert_info = {
                        'exchange': exchange_id,
                        'spot_symbol': spot_symbol,
                        'future_symbol': future_symbol,
                        'spot_price': spot_price,
                        'future_price': future_price,
                        'price_difference_percent': round(price_diff, 4),
                        'timestamp': timestamp,
                        'alert_type': 'spot_futures_basis',
                        'is_premium': is_premium,
                        'direction': 'premium' if is_premium else 'discount',
                        'notes': f"现货-期货价差 {price_diff:.4f}% ({direction_text}) 超过阈值 {self.threshold}%"
                    }
                    
                    abnormal_basis_alerts.append(alert_info)
                    logger.info(f"Abnormal spot-futures basis detected: {exchange_id} | {spot_symbol}/{future_symbol} | {price_diff:.4f}% ({direction_text})")
        
        return abnormal_basis_alerts 