import asyncio
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import structlog
import pytz
from notification import DiscordNotifier

logger = structlog.get_logger()

class MarketStructureMonitor:
    def __init__(self, symbol, category, notification_config):
        self.symbol = symbol
        self.category = category
        self.notification_config = notification_config
        self.previous_structure = None
        self.ny_tz = pytz.timezone('America/New_York')
        self.notifier = DiscordNotifier()
        self.logger = logger.bind(symbol=symbol, category=category)
        
        self.trend_strength_threshold = 0.75
        self.volatility_factor = 1.5
        self.max_lookback = 20

    async def get_market_data(self):
        """Enhanced market data retrieval with improved context"""
        try:
            # Fix for gold symbol
            symbol = 'GC=F' if self.symbol == '$XAUUSD=X' else self.symbol
            
            ticker = yf.Ticker(symbol)
            df = ticker.history(interval='15m', period='5d')
            
            if df.empty:
                self.logger.error("No data received from Yahoo Finance")
                return None
                
            df['EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
            df['EMA_200'] = df['Close'].ewm(span=200, adjust=False).mean()
            df['ATR'] = self.calculate_atr(df)
            
            return df.tail(96 * 5)
        except Exception as e:
            self.logger.error("Market data retrieval error", error=str(e))
            return None

    def is_swing_high(self, df, current_idx, min_movement):
        """Fixed swing high validation"""
        window = 10
        
        # Convert index location to integer
        idx = df.index.get_loc(current_idx)
        if idx < window or idx >= len(df) - window:
            return 0
        
        # Use integer indexing for surrounding price comparison
        current_price = df['High'].iloc[idx]
        surrounding_prices = df['High'].iloc[idx-window:idx+window]
        
        is_local_max = current_price == surrounding_prices.max()
        movement_significant = (current_price - surrounding_prices.min()) > min_movement
        above_trend_line = current_price > df['EMA_50'].iloc[idx]
        
        return 1 if is_local_max and movement_significant and above_trend_line else 0

    def is_swing_low(self, df, current_idx, min_movement):
        """Fixed swing low validation"""
        window = 10
        
        # Convert index location to integer
        idx = df.index.get_loc(current_idx)
        if idx < window or idx >= len(df) - window:
            return 0
        
        # Use integer indexing for surrounding price comparison
        current_price = df['Low'].iloc[idx]
        surrounding_prices = df['Low'].iloc[idx-window:idx+window]
        
        is_local_min = current_price == surrounding_prices.min()
        movement_significant = (surrounding_prices.max() - current_price) > min_movement
        below_trend_line = current_price < df['EMA_50'].iloc[idx]
        
        return 1 if is_local_min and movement_significant and below_trend_line else 0

    def detect_advanced_swing_points(self, df):
        """Fixed swing point detection"""
        if df is None or len(df) < 20:
            return None

        try:
            volatility = df['ATR'].mean()
            min_movement = volatility * self.volatility_factor

            # Use apply with index values instead of direct comparison
            df['Swing_High'] = df.index.map(
                lambda idx: self.is_swing_high(df, idx, min_movement)
            )
            df['Swing_Low'] = df.index.map(
                lambda idx: self.is_swing_low(df, idx, min_movement)
            )
            
            return df
        except Exception as e:
            self.logger.error("Advanced swing detection error", error=str(e))
            return None

    # ... [rest of the methods remain the same] ...