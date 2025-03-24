import os
import sys
import asyncio
import logging
import time
import traceback
import aiohttp
import ccxt.pro as ccxtpro
import gc

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

async def test_ccxt_cleanup():
    """测试ccxt.pro交易所资源清理"""
    logger.info("创建交易所实例...")
    exchange = ccxtpro.binance({
        'enableRateLimit': True,
        'timeout': 30000
    })
    
    try:
        # 加载市场
        logger.info("加载市场数据...")
        await exchange.load_markets()
        logger.info("市场数据加载完成")
        
        # 获取一些数据
        logger.info("获取BTC/USDT最新ticker...")
        ticker = await exchange.fetch_ticker('BTC/USDT')
        logger.info(f"当前BTC价格: {ticker['last']} USDT")
        
        # 测试正确关闭
        logger.info("正确关闭交易所连接...")
        await exchange.close()
        logger.info("交易所连接已关闭")
        
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # 验证资源清理
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        logger.info(f"剩余任务数: {len(tasks)}")
        
        for task in tasks:
            if "ccxt" in str(task).lower():
                logger.warning(f"发现未关闭的ccxt任务: {task}")
        
        # 强制垃圾回收
        gc.collect()

async def test_aiohttp_cleanup():
    """测试aiohttp会话资源清理"""
    logger.info("创建aiohttp会话...")
    session = aiohttp.ClientSession()
    
    try:
        # 发送一个请求
        logger.info("发送HTTP请求...")
        async with session.get('https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT') as response:
            data = await response.json()
            logger.info(f"请求成功，BTC价格: {data['price']} USDT")
        
        # 正确关闭会话
        logger.info("正确关闭会话...")
        await session.close()
        logger.info("会话已关闭")
        
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # 验证资源清理
        for obj in gc.get_objects():
            if isinstance(obj, aiohttp.ClientSession) and not obj.closed:
                logger.warning(f"发现未关闭的ClientSession: {obj}")

async def main():
    """主测试函数"""
    logger.info("开始资源清理测试...")
    
    # 测试ccxt清理
    await test_ccxt_cleanup()
    
    # 测试aiohttp清理
    await test_aiohttp_cleanup()
    
    # 最终检查
    logger.info("执行最终资源检查...")
    
    # 检查剩余任务
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if tasks:
        logger.warning(f"发现{len(tasks)}个未完成的任务")
        for task in tasks:
            logger.warning(f"未完成任务: {task}")
            task.cancel()
        
        # 等待任务取消
        await asyncio.gather(*tasks, return_exceptions=True)
    else:
        logger.info("没有发现未完成的任务")
    
    # 检查aiohttp会话
    gc.collect()  # 强制垃圾回收
    sessions_found = False
    for obj in gc.get_objects():
        if isinstance(obj, aiohttp.ClientSession) and not obj.closed:
            sessions_found = True
            logger.warning(f"发现未关闭的ClientSession: {obj}")
            await obj.close()
    
    if not sessions_found:
        logger.info("没有发现未关闭的ClientSession")
    
    logger.info("资源清理测试完成")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("测试被用户中断")
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}")
        logger.error(traceback.format_exc()) 