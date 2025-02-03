import os
import aiohttp
import structlog
from datetime import datetime
from typing import Dict, Optional, List, Union

logger = structlog.get_logger()


class DiscordNotifier:
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv('DISCORD_WEBHOOK_URL')
        if not self.webhook_url:
            raise ValueError(
                "DISCORD_WEBHOOK_URL environment variable is not set")

        # Add support for multiple webhooks and fallback mechanisms
        self.backup_webhooks = os.getenv(
            'DISCORD_BACKUP_WEBHOOKS', '').split(',')

    async def send_message(
        self,
        message: str,
        title: str = "Market Structure Change Alert",
        color: int = 3447003,
        additional_embeds: Optional[List[Dict]] = None
    ):
        """
        Send a message with enhanced flexibility and error handling

        Args:
            message (str): Main message content
            title (str, optional): Embed title
            color (int, optional): Embed color
            additional_embeds (List[Dict], optional): Additional embed objects
        """
        try:
            embed = {
                "title": title,
                "description": message,
                "color": color,
                "timestamp": datetime.now().isoformat()
            }

            payload = {
                "embeds": [embed] + (additional_embeds or [])
            }

            # Add rate limit handling and webhook fallback
            webhooks_to_try = [self.webhook_url] + self.backup_webhooks

            for webhook in webhooks_to_try:
                if not webhook:
                    continue

                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(webhook, json=payload) as response:
                            if response.status == 204:
                                logger.info(
                                    "Discord notification sent successfully")
                                return True
                            else:
                                logger.warning(
                                    "Failed to send Discord notification",
                                    status=response.status,
                                    webhook=webhook
                                )
                except Exception as e:
                    logger.error(
                        "Error sending Discord notification to webhook",
                        error=str(e),
                        webhook=webhook
                    )

            return False

        except Exception as e:
            logger.error(
                "Unexpected error in Discord notification", error=str(e))
            return False
