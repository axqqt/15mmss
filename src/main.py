import asyncio
import yaml
import os
import logging
from typing import Dict, Any
from market_monitor import MarketStructureMonitor
import structlog
from dotenv import load_dotenv

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
        """
        Validate configuration schema and content
        
        Args:
            config (Dict[str, Any]): Configuration to validate
        
        Raises:
            ValueError: If configuration is invalid
        """
        if 'assets' not in config:
            raise ValueError("Missing 'assets' key in configuration")
        
        if 'notification' not in config:
            raise ValueError("Missing 'notification' key in configuration")
        
        # Additional validation can be added here

async def main():
    try:
        # Load configuration
        config = ConfigLoader.load_config()

        # Create monitors for each asset
        monitors = [
            MarketStructureMonitor(
                symbol=asset,
                category=category,
                notification_config=config['notification']
            )
            for category, assets in config['assets'].items()
            for asset in assets
        ]

        # Run all monitors concurrently with error handling
        await asyncio.gather(*(monitor.run() for monitor in monitors))

    except Exception as e:
        logger.error(f"Failed to start service", error=str(e))
        raise

if __name__ == "__main__":
    try:
        logger.info("Starting Market Structure Monitor Service")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Service crashed", error=str(e))
        raise