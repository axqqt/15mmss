import os
import aiohttp
import structlog
from datetime import datetime

logger = structlog.get_logger()

class DiscordNotifier:
    def __init__(self):
        self.webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
        if not self.webhook_url:
            raise ValueError("DISCORD_WEBHOOK_URL environment variable is not set")
        
    async def send_message(self, message):
        try:
            embed = {
                "title": "Market Structure Change Alert",
                "description": message,
                "color": 3447003,  # Blue color
                "timestamp": datetime.now().isoformat()
            }
            
            payload = {
                "embeds": [embed]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=payload) as response:
                    if response.status == 204:
                        logger.info("Discord notification sent successfully")
                    else:
                        logger.error(
                            "Failed to send Discord notification",
                            status=response.status
                        )
        except Exception as e:
            logger.error("Error sending Discord notification", error=str(e))