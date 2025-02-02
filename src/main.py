import asyncio
import yaml
from market_monitor import MarketStructureMonitor
import structlog
import os
from dotenv import load_dotenv

logger = structlog.get_logger()

def load_config():
    """Load configuration from yaml and environment"""
    # Load environment variables from .env file
    load_dotenv()
    
    # Verify required environment variables
    if not os.getenv('DISCORD_WEBHOOK_URL'):
        logger.error("DISCORD_WEBHOOK_URL environment variable is not set")
        logger.info("Please create a .env file with your Discord webhook URL")
        raise ValueError("Missing required environment variable: DISCORD_WEBHOOK_URL")
    
    # Load asset configuration
    try:
        with open('config/assets.yaml', 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error("assets.yaml not found")
        logger.info("Please create config/assets.yaml file")
        raise

async def main():
    try:
        # Load configuration
        config = load_config()
        
        # Create monitors for each asset
        monitors = []
        for category, assets in config['assets'].items():
            for asset in assets:
                monitor = MarketStructureMonitor(
                    symbol=asset,
                    category=category,
                    notification_config=config['notification']
                )
                monitors.append(monitor)
        
        # Run all monitors concurrently
        await asyncio.gather(*(monitor.run() for monitor in monitors))
        
    except Exception as e:
        logger.error(f"Failed to start service: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Starting Market Structure Monitor Service")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Service crashed: {str(e)}")
        raise