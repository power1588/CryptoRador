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
            
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            self.secret.encode("utf-8"), 
            string_to_sign.encode("utf-8"), 
            digestmod=hashlib.sha256
        ).digest()
        
        return base64.b64encode(hmac_code).decode('utf-8')
    
    def format_card_message(self, abnormal_movements: List[Dict[str, Any]]) -> Dict:
        """Format abnormal movements data as a Lark interactive card.
        
        Args:
            abnormal_movements: List of detected abnormal market movements
            
        Returns:
            Formatted Lark card message
        """
        timestamp = int(time.time())
        
        # Sort movements by price change (descending)
        sorted_movements = sorted(
            abnormal_movements, 
            key=lambda x: x['price_change_percent'], 
            reverse=True
        )
        
        # Create the card elements
        elements = []
        
        # Add header
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "**🚨 异常行情监测报告 🚨**"
            }
        })
        
        # Add timestamp
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**扫描时间**: {current_time}"
            }
        })
        
        # Add divider
        elements.append({"tag": "hr"})
        
        # Add each abnormal movement
        for idx, movement in enumerate(sorted_movements[:10]):  # Limit to top 10
            # 确保is_future字段存在，如果不存在则默认为False
            is_future = movement.get('is_future', False)
            market_type = "合约" if is_future else "现货"
            
            # 确保volume_ratio字段存在
            volume_ratio = movement.get('volume_ratio', movement.get('volume_change_ratio', 1.0))
            
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**{idx+1}. {movement['symbol']}** ({movement['exchange']} {market_type})\n"
                        f"📈 价格变动: **+{movement['price_change_percent']}%**\n"
                        f"📊 成交量倍数: **{volume_ratio}x**\n"
                        f"💰 当前价格: {movement['current_price']}\n"
                        f"⏰ 触发时间: {movement['timestamp']}"
                    )
                }
            })
            
            # Add divider between items
            if idx < len(sorted_movements[:10]) - 1:
                elements.append({"tag": "hr"})
        
        # Build the card message
        card = {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"发现 {len(abnormal_movements)} 个异常上涨交易对"
                },
                "template": "red" if len(abnormal_movements) > 5 else "orange"
            },
            "elements": elements
        }
        
        # Build the final message
        message = {
            "timestamp": timestamp,
            "sign": self._generate_sign(timestamp),
            "msg_type": "interactive",
            "card": card
        }
        
        return message
    
    def send_notification(self, abnormal_movements: List[Dict[str, Any]]) -> bool:
        """Send notifications about abnormal market movements to Lark.
        
        Args:
            abnormal_movements: List of detected abnormal market movements
            
        Returns:
            True if notification was sent successfully, False otherwise
        """
        if not abnormal_movements:
            logger.info("No abnormal movements to notify about")
            return True
            
        if not self.webhook_url:
            logger.error("Lark webhook URL not configured")
            return False
        
        try:
            # Format message
            message = self.format_card_message(abnormal_movements)
            
            # Send to Lark
            response = requests.post(
                url=self.webhook_url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(message)
            )
            
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("code") == 0:
                    logger.info(f"Successfully sent notification for {len(abnormal_movements)} abnormal movements")
                    return True
                else:
                    logger.error(f"Lark API error: {response_data}")
            else:
                logger.error(f"Failed to send notification: HTTP {response.status_code}")
            
            return False
        except Exception as e:
            logger.error(f"Exception while sending notification: {str(e)}")
            return False
