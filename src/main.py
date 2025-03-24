import os
import signal
import logging
import sys
import time
from typing import Dict, List, Any

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import settings
from src.fetcher.data_fetcher import MarketDataFetcher
from src.analyzer.market_analyzer import MarketAnalyzer
from src.notifier.lark_notifier import LarkNotifier
from src.scheduler.task_scheduler import TaskScheduler

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(settings.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class CryptoRador:
    """Main application that orchestrates the crypto market scanning process."""
    
    def __init__(self):
        """Initialize the application components."""
        self.data_fetcher = MarketDataFetcher()
        self.market_analyzer = MarketAnalyzer()
        self.notifier = LarkNotifier()
        self.scheduler = TaskScheduler()
        self.running = False
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {sig}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def scan_markets(self):
        """Perform a single market scan."""
        try:
            logger.info("Starting market scan...")
            
            # Fetch market data
            market_data = self.data_fetcher.fetch_recent_data(
                lookback_minutes=settings.LOOKBACK_MINUTES
            )
            
            # Analyze for abnormal movements
            abnormal_movements = self.market_analyzer.detect_abnormal_movements(market_data)
            
            # Send notifications if any abnormal movements detected
            if abnormal_movements:
                logger.info(f"Found {len(abnormal_movements)} abnormal movements, sending notification")
                self.notifier.send_notification(abnormal_movements)
            else:
                logger.info("No abnormal movements detected")
                
            logger.info("Market scan completed")
        except Exception as e:
            logger.error(f"Error during market scan: {str(e)}")
    
    def start(self):
        """Start the market scanner."""
        if self.running:
            logger.warning("Scanner is already running")
            return
            
        self.running = True
        logger.info("Starting CryptoRador market scanner...")
        
        # Schedule the scanning task
        self.scheduler.add_job(self.scan_markets, "market_scanner")
        
        # Start the scheduler
        self.scheduler.start()
        
        # Run initial scan immediately
        self.scan_markets()
        
        logger.info(f"Scanner will run every {settings.SCAN_INTERVAL_SECONDS} seconds")
    
    def stop(self):
        """Stop the market scanner."""
        if not self.running:
            return
            
        logger.info("Stopping CryptoRador market scanner...")
        self.scheduler.stop()
        self.running = False
        logger.info("Scanner stopped")

def main():
    """Main entry point."""
    try:
        # Create and start the application
        app = CryptoRador()
        app.start()
        
        # Keep the main thread running
        while app.running:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
    
if __name__ == "__main__":
    main()
