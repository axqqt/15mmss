import os
import aiohttp
import structlog
from datetime import datetime
import pytz
from typing import Dict, Optional, List, Union
import asyncio
from aiohttp import ClientTimeout

logger = structlog.get_logger()

class DiscordNotifier:
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv('DISCORD_WEBHOOK_URL')
        if not self.webhook_url:
            raise ValueError("DISCORD_WEBHOOK_URL environment variable is not set")

        # Support for multiple webhooks
        self.backup_webhooks = [
            url.strip() for url in os.getenv('DISCORD_BACKUP_WEBHOOKS', '').split(',')
            if url.strip()
        ]
        
        # Define New York timezone
        self.ny_tz = pytz.timezone('America/New_York')
        
        # Rate limiting parameters
        self.rate_limit_retries = 3
        self.rate_limit_delay = 2  # seconds
        self.timeout = ClientTimeout(total=10)  # 10 seconds timeout
        
        # Message formatting
        self.max_message_length = 2000
        self.max_embed_length = 1024

    def format_message(self, message: str) -> str:
        """Format message to comply with Discord's limits"""
        if len(message) > self.max_message_length:
            return message[:self.max_message_length - 3] + "..."
        return message

    def create_embed(
        self,
        message: str,
        title: str,
        color: int,
        timestamp: datetime
    ) -> Dict:
        """Create properly formatted embed with error checking"""
        description = self.format_message(message)
        
        embed = {
            "title": title[:256],  # Discord's title limit
            "description": description,
            "color": color,
            "timestamp": timestamp.isoformat()
        }

        # Add footer with timezone information
        embed["footer"] = {
            "text": f"Time: {timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        }

        return embed

    async def try_send_webhook(
        self,
        session: aiohttp.ClientSession,
        webhook_url: str,
        payload: Dict,
        retry: int = 0
    ) -> bool:
        """Try to send webhook with rate limit handling"""
        try:
            async with session.post(webhook_url, json=payload, timeout=self.timeout) as response:
                if response.status == 204:
                    logger.info("Discord notification sent successfully")
                    return True
                elif response.status == 429:  # Rate limited
                    if retry < self.rate_limit_retries:
                        retry_after = float(response.headers.get('Retry-After', self.rate_limit_delay))
                        logger.warning("Rate limited, waiting to retry", retry_after=retry_after)
                        await asyncio.sleep(retry_after)
                        return await self.try_send_webhook(session, webhook_url, payload, retry + 1)
                    else:
                        logger.error("Max retries reached for rate limit")
                        return False
                else:
                    logger.warning(
                        "Failed to send Discord notification",
                        status=response.status,
                        webhook=webhook_url
                    )
                    return False
        except asyncio.TimeoutError:
            logger.error("Timeout sending Discord notification", webhook=webhook_url)
            return False
        except Exception as e:
            logger.error(
                "Error sending Discord notification",
                error=str(e),
                webhook=webhook_url
            )
            return False

    async def send_message(
        self,
        message: str,
        title: str = "Market Structure Change Alert",
        color: int = 3447003,
        additional_embeds: Optional[List[Dict]] = None
    ) -> bool:
        """
        Send a message with enhanced error handling and formatting
        
        Args:
            message (str): Main message content
            title (str, optional): Embed title
            color (int, optional): Embed color (default: Discord blue)
            additional_embeds (List[Dict], optional): Additional embed objects
        
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        try:
            ny_time = datetime.now(self.ny_tz)
            
            # Create main embed
            embed = self.create_embed(
                message=message,
                title=f"15 Minute {title}",
                color=color,
                timestamp=ny_time
            )

            # Prepare payload
            payload = {
                "embeds": [embed] + (additional_embeds or [])
            }

            # Try primary and backup webhooks
            webhooks_to_try = [self.webhook_url] + self.backup_webhooks
            
            async with aiohttp.ClientSession() as session:
                for webhook in webhooks_to_try:
                    if not webhook:
                        continue
                        
                    success = await self.try_send_webhook(session, webhook, payload)
                    if success:
                        return True
                        
            logger.error("All webhooks failed")
            return False

        except Exception as e:
            logger.error("Unexpected error in Discord notification", error=str(e))
            return False