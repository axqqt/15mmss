import asyncio
import yaml
import os
import logging
from typing import Dict, Any, List
from market_monitor import MarketStructureMonitor
import structlog
from dotenv import load_dotenv
from notification import DiscordNotifier

logger = structlog.get_logger()

class ConfigLoader:
    @staticmethod
    def load_config(config_path: str = 'config/assets.yaml') -> Dict[str, Any]:
        """
        Robust configuration loading with multiple validation checks
        
        Args:
            config_path (str): Path to the configuration YAML file
        
        Returns:
            Dict[str, Any]: Loaded and validated configuration
        """
        # Load environment variables
        load_dotenv()

        # Validate environment variables
        required_env_vars = ['DISCORD_WEBHOOK_URL']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(
                "Missing required environment variables", 
                missing_vars=missing_vars
            )
            raise ValueError(f"Missing env vars: {', '.join(missing_vars)}")

        # Load configuration file
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            
            # Validate configuration structure
            ConfigLoader._validate_config(config)
            
            return config
        
        except FileNotFoundError:
            logger.error("Configuration file not found", path=config_path)
            raise
        except yaml.YAMLError as e:
            logger.error("YAML parsing error", error=str(e))
            raise
    
    @staticmethod
    def _validate_config(config: Dict[str, Any]):
        """Validate configuration schema and content"""
        required_keys = ['assets', 'notification']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing '{key}' key in configuration")
            
        if not isinstance(config['assets'], dict):
            raise ValueError("'assets' must be a dictionary")
            
        if not isinstance(config['notification'], dict):
            raise ValueError("'notification' must be a dictionary")
            
        # Validate each asset category and its contents
        for category, assets in config['assets'].items():
            if not isinstance(assets, list):
                raise ValueError(f"Assets in category '{category}' must be a list")
            if not assets:
                raise ValueError(f"Category '{category}' has no assets")

class MarketMonitorService:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.monitors: List[MarketStructureMonitor] = []
        self.notifier = DiscordNotifier()
        
    async def initialize_monitors(self):
        """Initialize all market structure monitors"""
        for category, assets in self.config['assets'].items():
            for asset in assets:
                monitor = MarketStructureMonitor(
                    symbol=asset,
                    category=category,
                    notification_config=self.config['notification']
                )
                self.monitors.append(monitor)
                logger.info(f"Initialized monitor", symbol=asset, category=category)
    
    async def run_monitor(self, monitor: MarketStructureMonitor):
        """Run a single monitor with error handling"""
        try:
            await monitor.run()
        except Exception as e:
            logger.error(
                "Monitor error",
                symbol=monitor.symbol,
                category=monitor.category,
                error=str(e)
            )
    
    async def start(self):
        """Start all monitors"""
        await self.initialize_monitors()
        
        # Create tasks for all monitors
        monitor_tasks = [
            self.run_monitor(monitor)
            for monitor in self.monitors
        ]
        
        # Run all monitors concurrently
        await asyncio.gather(*monitor_tasks, return_exceptions=True)

async def main():
    try:
        # Load configuration
        config = ConfigLoader.load_config()
        
        # Initialize and start service
        service = MarketMonitorService(config)
        await service.start()

    except Exception as e:
        logger.error("Failed to start service", error=str(e))
        raise

if __name__ == "__main__":
    try:
        logger.info("Starting Market Structure Monitor Service")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error("Service crashed", error=str(e))
        raise