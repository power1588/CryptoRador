import os
import sys
import json
import logging
import time
import hmac
import hashlib
import base64
import urllib.parse
from typing import Dict, List, Any, Optional
import requests
from datetime import datetime
import numpy as np
import pandas as pd

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class LarkNotifier:
    """Sends notifications to Lark (Feishu) group chat."""
    
    def __init__(self, webhook_url: Optional[str] = None, secret: Optional[str] = None):
        """Initialize the Lark notifier.
        
        Args:
            webhook_url: Lark webhook URL
            secret: Lark webhook secret for signature
        """
        self.webhook_url = webhook_url or settings.LARK_WEBHOOK_URL
        self.secret = secret or settings.LARK_SECRET
    
    def _generate_sign(self, timestamp: int) -> str:
        """Generate signature for Lark webhook.
        
        Args:
            timestamp: Current timestamp
            
        Returns:
            Base64 encoded signature
        """
        if not self.secret:
            return ""
        
        # 将timestamp转为字符串
        timestamp_str = str(timestamp)
        
        # 飞书官方示例代码（使用时间戳+换行符+密钥作为字符串）
        string_to_sign = '{}\n{}'.format(timestamp_str, self.secret)
        
        # 使用HMAC-SHA256计算签名
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"),  # 使用"时间戳\n密钥"作为密钥
            digestmod=hashlib.sha256
        ).digest()
        
        # Base64编码
        sign = base64.b64encode(hmac_code).decode('utf-8')
        
        logger.debug(f"Generated sign for timestamp {timestamp}: {sign}")
        return sign
    
    def _get_card_content(self, movement: dict) -> dict:
        """Generate card content for Lark notification.
        
        Args:
            movement: A movement dictionary
            
        Returns:
            Card content dictionary
        """
        # 检查是否为现货-期货价差报警
        if movement.get('alert_type') == 'spot_futures_basis':
            return self._get_spot_futures_card_content(movement)
            
        logger.debug(f"Formatting card message for one abnormal movement: {movement}")
        
        exchange = movement.get('exchange', 'Unknown')
        symbol = movement.get('symbol', 'Unknown')
        timestamp = movement.get('timestamp', '')
        current_price = movement.get('current_price', 0.0)
        price_change = movement.get('price_change_percent', 0.0)
        volume_ratio = movement.get('volume_ratio', 0.0)
        notes = movement.get('notes', '')
        
        # 价格分位数信息
        price_percentile = movement.get('price_percentile', None)
        price_30d_high = movement.get('30d_high', None)
        price_30d_low = movement.get('30d_low', None)
        price_30d_avg = movement.get('30d_avg', None)
        
        color = "red" if price_change > 0 else "green"
        title = f"{exchange} | {symbol} | 价格{'上涨' if price_change > 0 else '下跌'} {abs(price_change):.2f}%"
        
        elements = [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**交易所**: {exchange}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**交易对**: {symbol}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**时间**: {timestamp}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**当前价格**: {current_price}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**价格变动**: {price_change:.2f}%"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**成交量比**: {volume_ratio:.2f}"
                }
            }
        ]
        
        # 添加价格分位数信息（如果存在）
        if price_percentile is not None:
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**30天价格分位**: {price_percentile:.2f}%"
                }
            })
            
            # 添加一个可视化的分位数指示器
            percentile_bar = self._create_percentile_bar(price_percentile)
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"{percentile_bar}"
                }
            })
            
            # 添加30天高低价信息
            if price_30d_high is not None and price_30d_low is not None:
                elements.append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**30天价格区间**: {price_30d_low:.2f} - {price_30d_high:.2f}"
                    }
                })
        
        # 添加notes字段（如果存在）
        if notes:
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**备注**: {notes}"
                }
            })
            
        # 添加必需的关键词(使用斜体和小字体，不显眼但确保存在)
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"_crypto market alert_"
            }
        })
        
        card = {
            "elements": elements,
            "header": {
                "template": color,
                "title": {
                    "content": title,
                    "tag": "plain_text"
                }
            }
        }
        
        return {"msg_type": "interactive", "card": card}
    
    def _get_spot_futures_card_content(self, alert: dict) -> dict:
        """生成现货-期货价差报警的卡片内容
        
        Args:
            alert: 价差报警字典
            
        Returns:
            卡片内容字典
        """
        logger.debug(f"Formatting card message for spot-futures basis alert: {alert}")
        
        exchange = alert.get('exchange', 'Unknown')
        spot_symbol = alert.get('spot_symbol', 'Unknown')
        future_symbol = alert.get('future_symbol', 'Unknown')
        spot_price = alert.get('spot_price', 0.0)
        future_price = alert.get('future_price', 0.0)
        price_diff = alert.get('price_difference_percent', 0.0)
        timestamp = alert.get('timestamp', '')
        notes = alert.get('notes', '')
        
        # 基差超过0表示期货溢价，低于0表示期货贴水
        is_premium = price_diff > 0
        color = "orange"  # 使用橙色区分现货-期货价差报警
        title = f"{exchange} | 现货-期货异常基差 {abs(price_diff):.4f}%"
        
        elements = [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**交易所**: {exchange}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**现货交易对**: {spot_symbol}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**期货交易对**: {future_symbol}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**现货价格**: {spot_price}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**期货价格**: {future_price}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**基差**: {price_diff:.4f}% ({'期货溢价' if is_premium else '期货贴水'})"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**时间**: {timestamp}"
                }
            }
        ]
        
        # 添加notes字段（如果存在）
        if notes:
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**备注**: {notes}"
                }
            })
            
        # 添加必需的关键词(使用斜体和小字体，不显眼但确保存在)
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"_spot futures basis alert_"
            }
        })
        
        card = {
            "elements": elements,
            "header": {
                "template": color,
                "title": {
                    "content": title,
                    "tag": "plain_text"
                }
            }
        }
        
        return {"msg_type": "interactive", "card": card}
    
    def _create_percentile_bar(self, percentile: float) -> str:
        """创建一个可视化的百分位数条
        
        Args:
            percentile: 0-100之间的百分位数
            
        Returns:
            表示百分位的字符串
        """
        bar_length = 20  # 条的总长度
        position = int(round(percentile / 100 * bar_length))
        
        # 确保位置在有效范围内
        position = max(0, min(position, bar_length))
        
        # 使用不同的符号表示程度
        bar = "▁" * position + "△" + "▁" * (bar_length - position - 1)
        
        # 在条形图下添加刻度
        scale = "0%"+"─"*(int(bar_length/2)-2)+"50%"+"─"*(int(bar_length/2)-2)+"100%"
        
        return f"```\n{bar}\n{scale}\n```"
    
    def format_card_message(self, abnormal_movements: List[Dict[str, Any]]) -> Dict:
        """Format abnormal movements data as a Lark interactive card.
        
        Args:
            abnormal_movements: List of abnormal movements
            
        Returns:
            Formatted message dictionary
        """
        if not abnormal_movements:
            return None
            
        # 如果只有一个异常波动，直接使用单个卡片
        if len(abnormal_movements) == 1:
            return self._get_card_content(abnormal_movements[0])
            
        # 检查报警类型是否一致
        all_types = set(movement.get('alert_type', 'price_movement') for movement in abnormal_movements)
        
        # 如果全部是现货-期货价差报警，使用汇总卡片
        if len(all_types) == 1 and 'spot_futures_basis' in all_types:
            return self._format_spot_futures_summary_card(abnormal_movements)
            
        # 多个异常波动时，创建一个汇总卡片
        elements = []
        for i, movement in enumerate(abnormal_movements, 1):
            exchange = movement.get('exchange', 'Unknown')
            symbol = movement.get('symbol', 'Unknown')
            price_change = movement.get('price_change_percent', 0.0)
            volume_ratio = movement.get('volume_ratio', 0.0)
            current_price = movement.get('current_price', 0.0)
            
            # 价格分位数信息
            price_percentile = movement.get('price_percentile', None)
            price_30d_high = movement.get('30d_high', None)
            price_30d_low = movement.get('30d_low', None)
            
            # 基本信息
            content = f"**{i}. {exchange} | {symbol}**\n"
            content += f"价格: {current_price} (变动: {price_change:.2f}%)\n"
            content += f"成交量比: {volume_ratio:.2f}x\n"
            
            # 添加价格分位数信息（如果存在）
            if price_percentile is not None:
                content += f"30天价格分位: {price_percentile:.2f}%\n"
                
                # 如果有30天高低价信息
                if price_30d_high is not None and price_30d_low is not None:
                    content += f"30天区间: {price_30d_low:.2f} - {price_30d_high:.2f}"
            
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": content
                }
            })
            
            # 添加分隔线，除了最后一个
            if i < len(abnormal_movements):
                elements.append({
                    "tag": "hr"
                })
                
        # 添加必需的关键词(使用斜体和小字体，不显眼但确保存在)
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"_crypto market alert_"
            }
        })
                
        card = {
            "elements": elements,
            "header": {
                "template": "red",
                "title": {
                    "content": f"检测到 {len(abnormal_movements)} 个异常行情",
                    "tag": "plain_text"
                }
            }
        }
        
        return {"msg_type": "interactive", "card": card}
    
    def _format_spot_futures_summary_card(self, alerts: List[Dict[str, Any]]) -> Dict:
        """生成多个现货-期货价差报警的汇总卡片
        
        Args:
            alerts: 价差报警列表
            
        Returns:
            汇总卡片字典
        """
        elements = []
        
        # 添加标题
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**检测到 {len(alerts)} 个交易对出现现货-期货价差异常**"
            }
        })
        
        # 添加每个报警的简要信息
        for i, alert in enumerate(alerts, 1):
            exchange = alert.get('exchange', 'Unknown')
            spot_symbol = alert.get('spot_symbol', 'Unknown')
            future_symbol = alert.get('future_symbol', 'Unknown')
            price_diff = alert.get('price_difference_percent', 0.0)
            
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"{i}. **{exchange}** | 现货: {spot_symbol} | 期货: {future_symbol} | 基差: {price_diff:.4f}%"
                }
            })
        
        # 添加必需的关键词
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"_spot futures basis alerts_"
            }
        })
        
        card = {
            "elements": elements,
            "header": {
                "template": "orange",
                "title": {
                    "content": f"现货-期货基差异常报警汇总",
                    "tag": "plain_text"
                }
            }
        }
        
        return {"msg_type": "interactive", "card": card}
    
    def send_notification(self, abnormal_movements: List[Dict[str, Any]]) -> bool:
        """Send notification of abnormal movements to Lark.
        
        Args:
            abnormal_movements: List of detected abnormal market movements
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        if not abnormal_movements:
            logger.warning("No abnormal movements to send notification for")
            return False
            
        if not self.webhook_url:
            logger.error("Lark webhook URL is not configured")
            return False
            
        # 格式化消息
        message = self.format_card_message(abnormal_movements)
        if not message:
            logger.error("Failed to format card message")
            return False
            
        # 添加时间戳和签名
        timestamp = int(time.time())
        message["timestamp"] = timestamp
        message["sign"] = self._generate_sign(timestamp)
        
        try:
            # 发送请求
            response = requests.post(self.webhook_url, json=message)
            response_text = response.text
            
            if response.status_code == 200:
                response_json = response.json()
                if response_json.get('code') == 0:
                    logger.info(f"Successfully sent Lark notification for {len(abnormal_movements)} movements")
                    return True
                else:
                    logger.error(f"Failed to send Lark notification: {response_json}")
                    return False
            else:
                logger.error(f"Failed to send Lark notification, status code: {response.status_code}, response: {response_text}")
                return False
        except Exception as e:
            logger.exception(f"Error sending Lark notification: {str(e)}")
            return False

    def test_notification(self) -> bool:
        """Send a test notification to verify Lark webhook configuration works.
        
        Returns:
            True if notification was sent successfully, False otherwise
        """
        logger.info("Sending test notification to Lark")
        
        try:
            # Create a simple test movement
            test_movement = {
                'exchange': 'TEST',
                'symbol': 'BTC/USDT',
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'current_price': 50000.0,
                'reference_price': 48000.0,
                'price_change_percent': 4.17,
                'current_volume': 1000000.0,
                'average_volume': 500000.0,
                'volume_change_ratio': 2.0,
                'volume_ratio': 2.0,
                'is_future': False,
                'detected_at': datetime.now()
            }
            
            # 添加测试消息备注
            test_movement['notes'] = "测试消息 - 验证飞书通知功能是否正常"
            
            # Send test notification
            result = self.send_notification([test_movement])
            
            if result:
                logger.info("Test notification sent successfully!")
            else:
                logger.error("Failed to send test notification")
                
            return result
        except Exception as e:
            logger.error(f"Error sending test notification: {str(e)}")
            return False
