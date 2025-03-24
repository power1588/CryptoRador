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
from src.fetcher.websocket_data_subscriber import WebSocketDataSubscriber
from src.analyzer.realtime_analyzer import RealtimeMarketAnalyzer
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

class EventDrivenCryptoRador:
    """事件驱动的加密货币市场监控系统，基于WebSocket实时数据"""
    
    def __init__(self):
        """初始化应用组件"""
        self.data_subscriber = WebSocketDataSubscriber()
        self.market_analyzer = RealtimeMarketAnalyzer()
        self.notifier = LarkNotifier()
        self.running = False
        self.anomaly_check_interval = 60  # 每60秒清理旧的异常记录
        
    async def on_kline_update(self, exchange_id: str, symbol: str, 
                       kline_data: Any, is_new_candle: bool):
        """处理K线更新事件
        
        Args:
            exchange_id: 交易所ID
            symbol: 交易对符号
            kline_data: K线数据
            is_new_candle: 是否是新的K线
        """
        # 分析K线数据
        anomaly = await self.market_analyzer.on_new_kline(
            exchange_id, symbol, kline_data, is_new_candle
        )
        
        # 如果检测到异常，发送通知
        if anomaly:
            # 创建包含单个异常的列表用于通知
            anomalies = [anomaly]
            notification_success = self.notifier.send_notification(anomalies)
            
            if notification_success:
                logger.info(f"Notification sent for {symbol} on {exchange_id}")
            else:
                logger.error(f"Failed to send notification for {symbol} on {exchange_id}")
    
    async def periodic_maintenance(self):
        """定期执行维护任务"""
        while self.running:
            try:
                # 清理旧的异常记录
                self.market_analyzer.clear_old_anomalies()
                
                # 可以添加其他周期性任务，如统计信息记录
                active_subscriptions = 0
                for exchange_id in self.data_subscriber.active_subscriptions:
                    active_subscriptions += len(self.data_subscriber.active_subscriptions[exchange_id])
                
                logger.info(f"Active subscriptions: {active_subscriptions}")
                logger.info(f"Recent anomalies: {len(self.market_analyzer.recent_anomalies)}")
                
            except Exception as e:
                logger.error(f"Error in periodic maintenance: {str(e)}")
            
            # 等待下一个间隔
            await asyncio.sleep(self.anomaly_check_interval)
    
    async def setup(self):
        """设置和初始化组件"""
        logger.info("Initializing EventDrivenCryptoRador components...")
        await self.data_subscriber.initialize_exchanges()
    
    async def start(self):
        """启动事件驱动系统"""
        try:
            # 初始化组件
            await self.setup()
            
            self.running = True
            logger.info("Starting EventDrivenCryptoRador")
            
            # 启动订阅器
            await self.data_subscriber.start()
            
            # 订阅所有配置的市场类型
            await self.data_subscriber.subscribe_all_markets(
                market_types=settings.MARKET_TYPES,
                callback=self.on_kline_update
            )
            
            # 启动定期维护任务
            maintenance_task = asyncio.create_task(self.periodic_maintenance())
            
            # 保持系统运行，直到被停止
            while self.running:
                await asyncio.sleep(1)
            
            # 取消维护任务
            maintenance_task.cancel()
            
        except Exception as e:
            logger.error(f"Error in event driven main: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            # 确保在退出时关闭所有连接
            await self.shutdown()
    
    async def shutdown(self):
        """停止系统并清理资源"""
        if not self.running:
            return
            
        logger.info("Shutting down EventDrivenCryptoRador...")
        self.running = False
        
        try:
            # 停止数据订阅器
            await self.data_subscriber.stop()
            logger.info("EventDrivenCryptoRador shutdown complete")
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
    app = EventDrivenCryptoRador()
    
    # 设置信号处理
    signal.signal(signal.SIGINT, app.handle_signal)
    signal.signal(signal.SIGTERM, app.handle_signal)
    
    try:
        await app.start()
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