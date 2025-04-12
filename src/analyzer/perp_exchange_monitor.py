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
    
    def calculate_price_differences(self, market_data: Dict[str, Dict[str, Dict[str, pd.DataFrame]]]) -> List[Dict]:
        """计算不同交易所之间的永续合约价差
        
        Args:
            market_data: 市场数据，格式为 {exchange_id: {market_type: {symbol: DataFrame}}}
            
        Returns:
            价差超过阈值的合约列表
        """
        alerts = []
        
        # 从配置文件获取交易量阈值
        volume_thresholds = settings.EXCHANGE_VOLUME_THRESHOLDS
        
        # 收集所有合约的成交量数据用于统计
        exchange_volumes = {exchange: [] for exchange in self.exchanges}
        
        # 遍历所有交易所
        for i, exchange1 in enumerate(self.exchanges):
            for exchange2 in self.exchanges[i+1:]:
                # 获取两个交易所的永续合约数据
                future1 = market_data.get(exchange1, {}).get('future', {})
                future2 = market_data.get(exchange2, {}).get('future', {})
                
                # 找到两个交易所共有的合约
                common_symbols = set(future1.keys()) & set(future2.keys())
                
                if not common_symbols:
                    logger.debug(f"交易所 {exchange1} 和 {exchange2} 之间没有共同的永续合约")
                    continue
                
                logger.debug(f"交易所 {exchange1} 和 {exchange2} 之间有 {len(common_symbols)} 个共同的永续合约")
                
                # 计算每个共同合约的价差
                for symbol in common_symbols:
                    try:
                        # 获取最新的价格数据
                        df1 = future1[symbol]
                        df2 = future2[symbol]
                        
                        if df1.empty or df2.empty:
                            logger.debug(f"合约 {symbol} 在 {exchange1} 或 {exchange2} 上没有数据")
                            continue
                            
                        # 获取最新价格和24小时交易量
                        price1 = df1['close'].iloc[-1]
                        price2 = df2['close'].iloc[-1]
                        
                        # 从ticker数据中获取24小时交易量
                        volume1 = df1['base_volume'].iloc[-1] if 'base_volume' in df1.columns else 0
                        volume2 = df2['base_volume'].iloc[-1] if 'base_volume' in df2.columns else 0
                        
                        # 收集成交量数据用于统计
                        if volume1 > 0:
                            exchange_volumes[exchange1].append(volume1)
                        if volume2 > 0:
                            exchange_volumes[exchange2].append(volume2)
                        
                        # 检查交易量是否满足阈值要求
                        if exchange1 in volume_thresholds and volume1 < volume_thresholds[exchange1]:
                            logger.debug(f"{symbol} 在 {exchange1} 的24小时交易量 {volume1:.2f} 低于阈值 {volume_thresholds[exchange1]:.2f}")
                            continue
                            
                        if exchange2 in volume_thresholds and volume2 < volume_thresholds[exchange2]:
                            logger.debug(f"{symbol} 在 {exchange2} 的24小时交易量 {volume2:.2f} 低于阈值 {volume_thresholds[exchange2]:.2f}")
                            continue
                        
                        # 计算价差百分比
                        price_diff = abs(price1 - price2) / min(price1, price2) * 100
                        
                        # 输出调试信息
                        logger.debug(
                            f"价差分析 - {symbol}:\n"
                            f"  {exchange1}: {price1:.8f} (24h成交量: {volume1:.2f})\n"
                            f"  {exchange2}: {price2:.8f} (24h成交量: {volume2:.2f})\n"
                            f"  价差: {price_diff:.4f}%\n"
                            f"  阈值: {self.threshold}%"
                        )
                        
                        # 如果价差超过阈值，添加到警报列表
                        if price_diff > self.threshold:
                            alert = {
                                'symbol': symbol,
                                'exchange1': exchange1,
                                'price1': price1,
                                'volume1': volume1,
                                'exchange2': exchange2,
                                'price2': price2,
                                'volume2': volume2,
                                'price_diff': price_diff,
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            alerts.append(alert)
                            logger.info(
                                f"发现价差机会 - {symbol}:\n"
                                f"  {exchange1}: {price1:.8f} (24h成交量: {volume1:.2f})\n"
                                f"  {exchange2}: {price2:.8f} (24h成交量: {volume2:.2f})\n"
                                f"  价差: {price_diff:.4f}%"
                            )
                            
                    except Exception as e:
                        logger.error(f"计算 {symbol} 价差时出错: {str(e)}")
        
        # 计算并输出每个交易所的成交量中位数
        for exchange, volumes in exchange_volumes.items():
            if volumes:
                median_volume = np.median(volumes)
                logger.info(f"交易所 {exchange} 的24小时成交量中位数: {median_volume:.2f} USDT")
                logger.info(f"交易所 {exchange} 共有 {len(volumes)} 个合约有成交量数据")
            else:
                logger.warning(f"交易所 {exchange} 没有有效的成交量数据")
        
        return alerts 