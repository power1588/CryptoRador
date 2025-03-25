import os
import sys
from dotenv import load_dotenv

# 获取项目根目录路径
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载.env文件（优先从项目根目录加载）
env_path = os.path.join(ROOT_DIR, '.env')
load_dotenv(dotenv_path=env_path)

# Public/Private Mode
USE_PUBLIC_DATA_ONLY = os.getenv('USE_PUBLIC_DATA_ONLY', 'true').lower() == 'true'

# CCXT Configuration
EXCHANGES = os.getenv('EXCHANGES', 'binance,okx,bybit,gate').split(',')
API_KEYS = {
    'binance': {
        'api_key': os.getenv('BINANCE_API_KEY', ''),
        'secret': os.getenv('BINANCE_SECRET', ''),
    },
    'okx': {
        'api_key': os.getenv('OKX_API_KEY', ''),
        'secret': os.getenv('OKX_SECRET', ''),
        'password': os.getenv('OKX_PASSWORD', ''),
    },
    'bybit': {
        'api_key': os.getenv('BYBIT_API_KEY', ''),
        'secret': os.getenv('BYBIT_SECRET', ''),
    },
    'gate': {
        'api_key': os.getenv('GATE_API_KEY', ''),
        'secret': os.getenv('GATE_SECRET', ''),
    },
}

# Scanner Parameters
SCAN_INTERVAL_SECONDS = int(os.getenv('SCAN_INTERVAL_SECONDS', 30))
MIN_PRICE_INCREASE_PERCENT = float(os.getenv('MIN_PRICE_INCREASE_PERCENT', 2.0))
LOOKBACK_MINUTES = int(os.getenv('LOOKBACK_MINUTES', 5))
VOLUME_SPIKE_THRESHOLD = float(os.getenv('VOLUME_SPIKE_THRESHOLD', 5.0))
MARKET_TYPES = os.getenv('MARKET_TYPES', 'spot,future').split(',')

# 现货-期货价差监控参数
SPOT_FUTURES_DIFF_THRESHOLD = float(os.getenv('SPOT_FUTURES_DIFF_THRESHOLD', 0.1))
SPOT_FUTURES_BASIS_DIRECTION = os.getenv('SPOT_FUTURES_BASIS_DIRECTION', 'both')  # 'both', 'premium', 'discount'

# Async Scanner Parameters (Advanced)
MAX_CONCURRENT_REQUESTS = int(os.getenv('MAX_CONCURRENT_REQUESTS', 20))
REQUEST_TIMEOUT_SECONDS = int(os.getenv('REQUEST_TIMEOUT_SECONDS', 30))
RATE_LIMIT_FACTOR = float(os.getenv('RATE_LIMIT_FACTOR', 0.8))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
RETRY_DELAY_SECONDS = int(os.getenv('RETRY_DELAY_SECONDS', 2))

# Lark (Feishu) Configuration - 异常价格和成交量通知
LARK_WEBHOOK_URL = os.getenv('LARK_WEBHOOK_URL', '')
LARK_SECRET = os.getenv('LARK_SECRET', '')

# Lark (Feishu) Configuration - 专用于现货-期货价差通知
SPOT_FUTURES_LARK_WEBHOOK_URL = os.getenv('SPOT_FUTURES_LARK_WEBHOOK_URL', '')
SPOT_FUTURES_LARK_SECRET = os.getenv('SPOT_FUTURES_LARK_SECRET', '')

# Historical Data Cache
CACHE_DIR = os.getenv('CACHE_DIR', os.path.join(ROOT_DIR, '.cache'))
MAX_CACHE_AGE_HOURS = int(os.getenv('MAX_CACHE_AGE_HOURS', 24))

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', os.path.join(ROOT_DIR, 'crypto_radar.log'))
