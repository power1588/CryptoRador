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

class PerpExchangeMonitor:
    """监控不同交易所之间USDT永续合约的价格差异"""
    
    def __init__(self, 
                 exchanges: List[str] = ['binance', 'gate'], 
                 threshold: float = 0.2):
        """初始化监控器
        
        Args:
            exchanges: 要监控的交易所列表
            threshold: 价差阈值(百分比)，超过该值将触发警报
        """
        self.exchanges = exchanges
        self.threshold = threshold
        self.symbol_mapping = {}  # 用于存储不同交易所之间的交易对映射
        
        logger.info(f"初始化跨所永续合约价差监控器：交易所={exchanges}，阈值={threshold}%")
    
    def _normalize_symbol(self, exchange: str, symbol: str) -> str:
        """标准化交易对符号，以便在不同交易所之间匹配
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            
        Returns:
            标准化后的符号
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
    
    def _build_symbol_mapping(self, market_data: Dict[str, Dict[str, pd.DataFrame]]) -> None:
        """构建不同交易所之间的交易对映射
        
        Args:
            market_data: 市场数据字典
        """
        # 清空现有映射
        self.symbol_mapping = {}
        
        # 按交易所收集所有永续合约
        exchange_symbols = {}
        
        for exchange in self.exchanges:
            if exchange not in market_data:
                logger.warning(f"找不到交易所 {exchange} 的数据")
                continue
                
            if 'future' not in market_data[exchange]:
                logger.warning(f"交易所 {exchange} 没有期货数据")
                continue
                
            # 收集该交易所的所有永续合约及其标准化符号
            normalized_map = {}
            future_markets = market_data[exchange]['future']
            
            for symbol in future_markets.keys():
                # 只处理USDT计价的永续合约
                if 'USDT' in symbol:
                    normalized = self._normalize_symbol(exchange, symbol)
                    if normalized:
                        normalized_map[normalized] = symbol
                        
            exchange_symbols[exchange] = normalized_map
            logger.debug(f"交易所 {exchange} 有 {len(normalized_map)} 个USDT永续合约")
        
        # 找到在所有交易所中都存在的交易对
        if len(exchange_symbols) < 2:
            logger.warning("至少需要两个交易所的数据才能进行比较")
            return
            
        # 找到在所有监控的交易所中都存在的交易对
        common_bases = set()
        for exchange, symbols in exchange_symbols.items():
            if not common_bases:
                common_bases = set(symbols.keys())
            else:
                common_bases = common_bases.intersection(set(symbols.keys()))
        
        # 构建映射关系
        for base in common_bases:
            self.symbol_mapping[base] = {
                exchange: symbols[base] 
                for exchange, symbols in exchange_symbols.items() 
                if base in symbols
            }
        
        logger.info(f"找到 {len(self.symbol_mapping)} 个在所有交易所中都存在的USDT永续合约")
    
    def calculate_price_differences(self, 
                                    market_data: Dict[str, Dict[str, pd.DataFrame]]) -> List[Dict[str, Any]]:
        """计算不同交易所之间的价格差异
        
        Args:
            market_data: 交易所 -> 市场类型 -> 符号 -> DataFrame的市场数据嵌套字典
            
        Returns:
            包含价格差异信息的字典列表
        """
        # 首先建立交易对映射
        self._build_symbol_mapping(market_data)
        
        if not self.symbol_mapping:
            logger.warning("没有找到可比较的交易对")
            return []
            
        price_diff_alerts = []
        
        # 对每个基础交易对进行比较
        for base, exchange_symbols in self.symbol_mapping.items():
            # 确保至少有两个交易所的数据才能比较
            exchanges = list(exchange_symbols.keys())
            if len(exchanges) < 2:
                continue
                
            # 收集每个交易所的最新价格和交易量
            latest_prices = {}
            volumes_24h = {}
            
            for exchange in exchanges:
                symbol = exchange_symbols[exchange]
                
                # 检查数据是否存在
                if (exchange in market_data and 
                    'future' in market_data[exchange] and 
                    symbol in market_data[exchange]['future']):
                    
                    df = market_data[exchange]['future'][symbol]
                    if not df.empty:
                        latest_prices[exchange] = df.iloc[-1]['close']
                        
                        # 计算24小时交易量 (如果有足够数据)
                        if 'volume' in df.columns:
                            # 如果数据是按分钟存储，则需要60*24个数据点来计算24小时交易量
                            # 由于可能没有足够的数据点，我们使用可用的所有数据点来估算
                            available_volume = df['volume'].sum()
                            # 记录估算的24小时交易量
                            volumes_24h[exchange] = available_volume
            
            # 如果至少有两个交易所的价格可以比较
            if len(latest_prices) >= 2:
                # 对每对交易所进行价格差异计算
                for i, exchange1 in enumerate(latest_prices.keys()):
                    for exchange2 in list(latest_prices.keys())[i+1:]:
                        price1 = latest_prices[exchange1]
                        price2 = latest_prices[exchange2]
                        
                        if price1 <= 0 or price2 <= 0:
                            continue
                            
                        # 获取交易量
                        volume1 = volumes_24h.get(exchange1, 0)
                        volume2 = volumes_24h.get(exchange2, 0)
                        
                        # 交易量约束检查
                        # Binance上要求24小时交易量超过2000万
                        # Gate上要求24小时交易量超过100万
                        volume_threshold_passed = True
                        
                        if exchange1 == 'binance' and volume1 < 20_000_000:
                            volume_threshold_passed = False
                            logger.debug(f"{base} 在 Binance 的交易量不足: {volume1}, 需要: 20,000,000")
                        
                        if exchange2 == 'binance' and volume2 < 20_000_000:
                            volume_threshold_passed = False
                            logger.debug(f"{base} 在 Binance 的交易量不足: {volume2}, 需要: 20,000,000")
                            
                        if exchange1 == 'gate' and volume1 < 1_000_000:
                            volume_threshold_passed = False
                            logger.debug(f"{base} 在 Gate 的交易量不足: {volume1}, 需要: 1,000,000")
                            
                        if exchange2 == 'gate' and volume2 < 1_000_000:
                            volume_threshold_passed = False
                            logger.debug(f"{base} 在 Gate 的交易量不足: {volume2}, 需要: 1,000,000")
                        
                        if not volume_threshold_passed:
                            continue
                            
                        # 计算价格差异百分比 (price2 - price1) / price1 * 100
                        price_diff_percent = ((price2 - price1) / price1) * 100
                        
                        # 如果差异超过阈值，生成警报
                        if abs(price_diff_percent) >= self.threshold:
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 确定哪个交易所价格更高
                            higher_exchange = exchange2 if price2 > price1 else exchange1
                            lower_exchange = exchange1 if price2 > price1 else exchange2
                            higher_price = max(price1, price2)
                            lower_price = min(price1, price2)
                            
                            alert_info = {
                                'base_symbol': base,
                                'exchange1': exchange1,
                                'exchange2': exchange2,
                                'symbol1': exchange_symbols[exchange1],
                                'symbol2': exchange_symbols[exchange2],
                                'price1': price1,
                                'price2': price2,
                                'volume1': volume1,
                                'volume2': volume2,
                                'price_difference_percent': round(price_diff_percent, 4),
                                'higher_exchange': higher_exchange,
                                'lower_exchange': lower_exchange,
                                'higher_price': higher_price,
                                'lower_price': lower_price,
                                'timestamp': timestamp,
                                'alert_type': 'perp_exchange_difference',
                                'notes': f"{exchange1}与{exchange2}的{base}永续合约价差{abs(price_diff_percent):.4f}%，超过阈值{self.threshold}%"
                            }
                            
                            price_diff_alerts.append(alert_info)
                            logger.info(f"检测到跨所永续合约价差: {exchange1}/{exchange2} | {base} | {price_diff_percent:.4f}%")
        
        return price_diff_alerts 