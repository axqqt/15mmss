import asyncio
import pandas as pd
import yfinance as yf
from datetime import datetime
import structlog
from notification import DiscordNotifier

logger = structlog.get_logger()

class MarketStructureMonitor:
    def __init__(self, symbol, category, notification_config):
        self.symbol = symbol
        self.category = category
        self.notification_config = notification_config
        self.previous_structure = None
        self.logger = logger.bind(symbol=symbol, category=category)
        self.notifier = DiscordNotifier()

    async def get_market_data(self):
        """Fetch market data using yfinance"""
        try:
            ticker = yf.Ticker(self.symbol)
            df = ticker.history(interval='15m', period='1d')
            return df
        except Exception as e:
            self.logger.error("Error fetching market data", error=str(e))
            return None

    def detect_swing_points(self, df, window=5):
        """Detect swing highs and lows"""
        if df is None or df.empty:
            return None
        
        df['Swing_High'] = df['High'].rolling(window=window, center=True).apply(
            lambda x: 1 if x.iloc[window//2] == max(x) else 0
        )
        df['Swing_Low'] = df['Low'].rolling(window=window, center=True).apply(
            lambda x: 1 if x.iloc[window//2] == min(x) else 0
        )
        return df

    def analyze_market_structure(self, df):
        """Analyze market structure based on swing points"""
        if df is None:
            return 'UNDEFINED'

        swing_highs = df[df['Swing_High'] == 1]['High'].tail(3)
        swing_lows = df[df['Swing_Low'] == 1]['Low'].tail(3)
        
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            last_two_highs = swing_highs.tail(2).values
            last_two_lows = swing_lows.tail(2).values
            
            if last_two_highs[1] > last_two_highs[0] and last_two_lows[1] > last_two_lows[0]:
                return 'UPTREND'
            elif last_two_highs[1] < last_two_highs[0] and last_two_lows[1] < last_two_lows[0]:
                return 'DOWNTREND'
            else:
                return 'CONSOLIDATION'
        
        return 'UNDEFINED'

    async def run(self):
        """Main monitoring loop"""
        self.logger.info("Starting monitoring")
        
        while True:
            try:
                df = await self.get_market_data()
                df = self.detect_swing_points(df)
                current_structure = self.analyze_market_structure(df)
                
                if self.previous_structure and current_structure != self.previous_structure:
                    message = (
                        f"Asset: {self.symbol} ({self.category})\n"
                        f"Previous Structure: {self.previous_structure}\n"
                        f"Current Structure: {current_structure}\n"
                        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    
                    self.logger.info(
                        "Market structure changed",
                        previous=self.previous_structure,
                        current=current_structure
                    )
                    
                    if self.notification_config['discord']['enabled']:
                        await self.notifier.send_message(message)
                
                self.previous_structure = current_structure
                
                # Wait for next check (15 minutes)
                await asyncio.sleep(900)
                
            except Exception as e:
                self.logger.error("Error in monitoring loop", error=str(e))
                await asyncio.sleep(60)