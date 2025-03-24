import os
import sys
import logging
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class MarketAnalyzer:
    """Analyzes market data to detect abnormal price and volume movements."""
    
    def __init__(self, price_threshold: float = None, volume_threshold: float = None):
        """Initialize the market analyzer.
        
        Args:
            price_threshold: Minimum price increase percentage to be considered abnormal
            volume_threshold: Minimum volume spike factor to be considered abnormal
        """
        self.price_threshold = price_threshold or settings.MIN_PRICE_INCREASE_PERCENT
        self.volume_threshold = volume_threshold or settings.VOLUME_SPIKE_THRESHOLD
        
    def calculate_price_change(self, df: pd.DataFrame) -> Optional[float]:
        """Calculate the percentage price change over the period.
        
        Args:
            df: DataFrame with OHLCV data
            
        Returns:
            Price change percentage or None if data is insufficient
        """
        if df.empty or len(df) < 2:
            return None
        
        first_price = df.iloc[0]['open']
        last_price = df.iloc[-1]['close']
        
        if first_price <= 0:
            return None
            
        return ((last_price - first_price) / first_price) * 100
    
    def calculate_volume_ratio(self, df: pd.DataFrame) -> Optional[float]:
        """Calculate the ratio of recent volume to historical average volume.
        
        Args:
            df: DataFrame with OHLCV data
            
        Returns:
            Volume ratio or None if data is insufficient
        """
        if df.empty or len(df) < 2:
            return None
            
        # Calculate the most recent volume
        recent_volume = df.iloc[-1]['volume']
        
        # Calculate the average of previous volumes (excluding most recent)
        if len(df) > 1:
            historical_volumes = df.iloc[:-1]['volume'].values
            avg_historical_volume = np.mean(historical_volumes)
            
            if avg_historical_volume > 0:
                return recent_volume / avg_historical_volume
        
        return None
    
    def is_future_contract(self, symbol: str) -> bool:
        """Determine if a symbol is a futures contract based on common naming patterns.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            True if likely a futures contract, False otherwise
        """
        # Common futures markers across different exchanges
        futures_markers = [
            'PERP', 
            ':USDT', 
            '/USDT-PERP', 
            '_PERP', 
            '-SWAP', 
            '_SWAP',
            '-FUTURES',
            '_usd_',
            '-usd-',
            '/USD:',
            '/USDT:'
        ]
        
        # Check if any marker is in the symbol
        return any(marker in symbol for marker in futures_markers)
    
    def is_stablecoin_pair(self, symbol: str) -> bool:
        """Check if the trading pair includes stablecoins on both sides.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            True if it's a stablecoin pair, False otherwise
        """
        stablecoins = ['USDT', 'USDC', 'DAI', 'BUSD', 'UST', 'TUSD', 'USDP', 'USDK', 'PAX']
        
        # Check for stablecoin pairs like USDT/USDC
        parts = symbol.replace('-', '/').split('/')
        if len(parts) >= 2:
            base = parts[0]
            quote = parts[1]
            if any(stable in base.upper() for stable in stablecoins) and any(stable in quote.upper() for stable in stablecoins):
                return True
        
        return False
    
    def detect_abnormal_movements(self, market_data: Dict[str, Dict[str, pd.DataFrame]]) -> List[Dict[str, Any]]:
        """Detect abnormal price and volume movements across all markets.
        
        Args:
            market_data: Dictionary of exchange -> symbol -> DataFrame with market data
            
        Returns:
            List of dictionaries containing information about abnormal movements
        """
        abnormal_movements = []
        
        for exchange_id, symbols_data in market_data.items():
            for symbol, df in symbols_data.items():
                # Skip stablecoin pairs as they typically don't have significant price movements
                if self.is_stablecoin_pair(symbol):
                    logger.debug(f"Skipping stablecoin pair: {symbol}")
                    continue
                    
                if df.empty or len(df) < settings.LOOKBACK_MINUTES:
                    logger.debug(f"Insufficient data for {symbol} on {exchange_id}")
                    continue
                
                price_change = self.calculate_price_change(df)
                volume_ratio = self.calculate_volume_ratio(df)
                
                if price_change is None or volume_ratio is None:
                    continue
                
                # Check if both price and volume thresholds are exceeded
                if price_change >= self.price_threshold and volume_ratio >= self.volume_threshold:
                    # Determine if it's a futures contract
                    is_future = self.is_future_contract(symbol)
                    
                    market_info = {
                        'exchange': exchange_id,
                        'symbol': symbol,
                        'price_change_percent': round(price_change, 2),
                        'volume_ratio': round(volume_ratio, 2),
                        'current_price': df.iloc[-1]['close'],
                        'timestamp': df.iloc[-1]['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                        'is_future': is_future,
                    }
                    
                    abnormal_movements.append(market_info)
                    logger.info(f"Abnormal movement detected: {market_info}")
        
        return abnormal_movements
