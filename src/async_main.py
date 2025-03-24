import os
import sys
import signal
import logging
import asyncio
import time
import traceback
from typing import Dict, List, Any

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import settings
from src.fetcher.async_data_fetcher import AsyncMarketDataFetcher
from src.analyzer.market_analyzer import MarketAnalyzer
from src.notifier.lark_notifier import LarkNotifier

# 配置日志
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(settings.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class AsyncCryptoRador:
    """使用异步方式实现的加密货币市场扫描器"""
    
    def __init__(self):
        """初始化应用组件"""
        self.data_fetcher = AsyncMarketDataFetcher()
        self.market_analyzer = MarketAnalyzer()
        self.notifier = LarkNotifier()
        self.running = False
        self.max_concurrent_requests = 20  # 最大并发请求数
        self.consecutive_errors = 0  # 连续错误计数
        self.max_consecutive_errors = 3  # 允许的最大连续错误数
        
    async def setup(self):
        """设置和初始化组件"""
        logger.info("Initializing AsyncCryptoRador components...")
        await self.data_fetcher.initialize_exchanges(self.max_concurrent_requests)
        
    async def scan_markets(self):
        """执行单次市场扫描"""
        try:
            start_time = time.time()
            logger.info("Starting market scan...")
            
            # 异步获取市场数据
            market_data = await self.data_fetcher.fetch_recent_data(
                lookback_minutes=settings.LOOKBACK_MINUTES
            )
            
            fetch_time = time.time()
            logger.info(f"Data fetching completed in {fetch_time - start_time:.2f} seconds")
            
            # 检查是否获取到了数据
            total_symbols = sum(len(symbols) for symbols in market_data.values())
            if total_symbols == 0:
                logger.warning("No market data fetched, skipping analysis")
                self.consecutive_errors += 1
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.error(f"Reached {self.max_consecutive_errors} consecutive errors, consider checking API connectivity")
                return
            
            # 对获取的数据进行分析
            # 分析过程本身不需要异步，因为它是CPU密集型而非IO密集型操作
            abnormal_movements = self.market_analyzer.detect_abnormal_movements(market_data)
            
            analysis_time = time.time()
            logger.info(f"Data analysis completed in {analysis_time - fetch_time:.2f} seconds")
            
            # 发送通知(如果发现了异常行情)
            if abnormal_movements:
                logger.info(f"Found {len(abnormal_movements)} abnormal movements, sending notification")
                notification_success = self.notifier.send_notification(abnormal_movements)
                if notification_success:
                    logger.info("Notification sent successfully")
                else:
                    logger.error("Failed to send notification")
            else:
                logger.info("No abnormal movements detected")
                
            end_time = time.time()
            logger.info(f"Market scan completed in {end_time - start_time:.2f} seconds")
            
            # 重置连续错误计数
            self.consecutive_errors = 0
            
        except Exception as e:
            self.consecutive_errors += 1
            logger.error(f"Error during market scan: {str(e)}")
            logger.error(traceback.format_exc())
            
            if self.consecutive_errors >= self.max_consecutive_errors:
                logger.error(f"Reached {self.max_consecutive_errors} consecutive errors, consider restarting the application")
    
    async def run_scanner(self):
        """运行扫描器的主循环"""
        try:
            # 设置和初始化
            await self.setup()
            
            self.running = True
            logger.info(f"Starting AsyncCryptoRador with scan interval of {settings.SCAN_INTERVAL_SECONDS} seconds")
            
            # 立即执行第一次扫描
            await self.scan_markets()
            
            # 定期扫描循环
            while self.running:
                # 等待下一个扫描间隔
                await asyncio.sleep(settings.SCAN_INTERVAL_SECONDS)
                
                # 执行扫描
                if self.running:  # 再次检查，以防在sleep期间被停止
                    try:
                        await self.scan_markets()
                    except Exception as e:
                        logger.error(f"Unhandled exception in scan_markets: {str(e)}")
                        logger.error(traceback.format_exc())
                        # 继续循环，不要让单次扫描的失败导致整个程序崩溃
                    
        except Exception as e:
            logger.error(f"Error in scanner main loop: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            # 确保在退出时关闭所有连接
            await self.shutdown()
    
    async def shutdown(self):
        """关闭扫描器并清理资源"""
        if not self.running:
            return
            
        logger.info("Shutting down AsyncCryptoRador...")
        self.running = False
        
        try:
            # 关闭所有交易所连接
            await self.data_fetcher.close_all()
            logger.info("AsyncCryptoRador shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}")
    
    def handle_signal(self, sig, frame):
        """处理信号(如CTRL+C)"""
        logger.info(f"Received signal {sig}, initiating shutdown...")
        # 在主事件循环中安排关闭任务
        if self.running:
            asyncio.create_task(self.shutdown())

async def main_async():
    """异步主入口点"""
    app = AsyncCryptoRador()
    
    # 设置信号处理
    signal.signal(signal.SIGINT, app.handle_signal)
    signal.signal(signal.SIGTERM, app.handle_signal)
    
    try:
        await app.run_scanner()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
        await app.shutdown()
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(traceback.format_exc())
        await app.shutdown()

def main():
    """主入口点"""
    try:
        # 在Windows上需要使用不同的事件循环策略
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
        # 运行异步主函数
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Program interrupted")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main() 