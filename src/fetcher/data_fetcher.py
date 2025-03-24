import os
import sys
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
import ccxt
import pandas as pd
from datetime import datetime, timedelta

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class MarketDataFetcher:
    """Fetches market data from crypto exchanges using CCXT."""
    
    def __init__(self):
        self.exchanges = {}
        self.initialize_exchanges()
        self._ensure_cache_dir()
        
    def _ensure_cache_dir(self):
        """Ensure the cache directory exists."""
        if not os.path.exists(settings.CACHE_DIR):
            os.makedirs(settings.CACHE_DIR)
    
    def initialize_exchanges(self):
        """Initialize exchange connections with CCXT."""
        for exchange_id in settings.EXCHANGES:
            try:
                # Get exchange class dynamically
                exchange_class = getattr(ccxt, exchange_id)
                
                # Configure with API keys if available and not in public-only mode
                config = {}
                if not settings.USE_PUBLIC_DATA_ONLY and exchange_id in settings.API_KEYS:
                    api_config = settings.API_KEYS[exchange_id]
                    # Only add credentials if they are non-empty
                    if api_config.get('api_key') and api_config.get('secret'):
                        config = api_config
                        logger.info(f"Using API credentials for {exchange_id}")
                    else:
                        logger.info(f"API credentials for {exchange_id} not provided, using public API")
                else:
                    logger.info(f"Using public API for {exchange_id} (public-only mode: {settings.USE_PUBLIC_DATA_ONLY})")
                
                # Add common configs
                config.update({
                    'enableRateLimit': True,
                    'timeout': 30000,
                })
                
                # Create exchange instance
                self.exchanges[exchange_id] = exchange_class(config)
                logger.info(f"Initialized exchange: {exchange_id}")
            except Exception as e:
                logger.error(f"Failed to initialize exchange {exchange_id}: {str(e)}")
    
    def get_all_markets(self, exchange_id: str, market_type: str = 'spot') -> List[Dict]:
        """Get all trading pairs from an exchange.
        
        Args:
            exchange_id: ID of the exchange
            market_type: 'spot' or 'future'
            
        Returns:
            List of market symbols
        """
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            logger.error(f"Exchange {exchange_id} not initialized")
            return []
        
        try:
            exchange.load_markets()
            
            # Filter markets by type
            markets = []
            for symbol, market in exchange.markets.items():
                # Handle different futures naming conventions across exchanges
                is_future = (
                    market.get('future', False) or 
                    market.get('swap', False) or
                    ('PERP' in symbol) or 
                    (':USDT' in symbol) or
                    ('_PERP' in symbol)
                )
                
                if market_type == 'spot' and not is_future:
                    markets.append(market)
                elif market_type == 'future' and is_future:
                    markets.append(market)
            
            logger.info(f"Found {len(markets)} {market_type} markets on {exchange_id}")
            return markets
        except Exception as e:
            logger.error(f"Error fetching markets from {exchange_id}: {str(e)}")
            return []
    
    def fetch_ohlcv(self, exchange_id: str, symbol: str, timeframe: str = '1m', 
                   limit: int = 5, since: Optional[int] = None) -> pd.DataFrame:
        """Fetch OHLCV (candlestick) data for a specific symbol.
        
        Args:
            exchange_id: ID of the exchange
            symbol: Trading pair symbol
            timeframe: Timeframe ('1m', '5m', etc.)
            limit: Number of candles to fetch
            since: Timestamp in milliseconds
            
        Returns:
            DataFrame with OHLCV data
        """
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            logger.error(f"Exchange {exchange_id} not initialized")
            return pd.DataFrame()
        
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol} on {exchange_id}: {str(e)}")
            return pd.DataFrame()
    
    def fetch_recent_data(self, lookback_minutes: int = 5) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Fetch recent market data across all configured exchanges and market types.
        
        Args:
            lookback_minutes: Minutes of historical data to fetch
            
        Returns:
            Dictionary of exchange -> symbol -> DataFrame
        """
        result = {}
        since = int((datetime.now() - timedelta(minutes=lookback_minutes)).timestamp() * 1000)
        
        for exchange_id in self.exchanges:
            result[exchange_id] = {}
            for market_type in settings.MARKET_TYPES:
                markets = self.get_all_markets(exchange_id, market_type)
                
                # For efficiency, limit to top N pairs by volume if there are many markets
                if len(markets) > 500:
                    logger.info(f"Too many markets ({len(markets)}) on {exchange_id}, sampling a subset for efficiency")
                    # We could implement a filtering mechanism here if needed
                    # For now, just take a sample to avoid rate limiting
                    markets = markets[:500]
                
                for market in markets:
                    symbol = market['symbol']
                    try:
                        df = self.fetch_ohlcv(
                            exchange_id=exchange_id,
                            symbol=symbol,
                            timeframe='1m',
                            limit=lookback_minutes,
                            since=since
                        )
                        
                        if not df.empty:
                            result[exchange_id][symbol] = df
                    except Exception as e:
                        logger.warning(f"Failed to fetch data for {symbol} on {exchange_id}: {str(e)}")
        
        return result
