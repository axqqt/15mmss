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
        
        # Enhanced tracking parameters
        self.trend_strength_threshold = 0.75  # More robust trend confirmation
        self.volatility_factor = 1.5  # Adaptive to market volatility
        self.max_lookback = 20  # Increased contextual analysis

    async def get_market_data(self):
        """Enhanced market data retrieval with improved context"""
        try:
            ticker = yf.Ticker(self.symbol)
            # Extended data for more comprehensive analysis
            df = ticker.history(interval='15m', period='5d')
            
            # Add technical indicators for trend confirmation
            df['EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
            df['EMA_200'] = df['Close'].ewm(span=200, adjust=False).mean()
            df['ATR'] = self.calculate_atr(df)
            
            return df.tail(96 * 5)  # Extended data for robust analysis
        except Exception as e:
            self.logger.error("Market data retrieval error", error=str(e))
            return None

    def calculate_atr(self, df, period=14):
        """Calculate Average True Range for volatility assessment"""
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift(1))
        low_close = np.abs(df['Low'] - df['Close'].shift(1))
        
        true_range = np.maximum(high_low, high_close, low_close)
        atr = true_range.rolling(window=period).mean()
        
        return atr

    def detect_advanced_swing_points(self, df):
        """
        Advanced swing point detection with multi-factor validation
        """
        if df is None or len(df) < 20:
            return None

        try:
            # Volatility-adjusted minimum movement
            volatility = df['ATR'].mean()
            min_movement = volatility * self.volatility_factor

            # Comprehensive swing point detection
            df['Swing_High'] = df.apply(
                lambda row: self.is_swing_high(df, row.name, min_movement), 
                axis=1
            )
            df['Swing_Low'] = df.apply(
                lambda row: self.is_swing_low(df, row.name, min_movement), 
                axis=1
            )
            
            return df
        except Exception as e:
            self.logger.error("Advanced swing detection error", error=str(e))
            return None

    def is_swing_high(self, df, index, min_movement):
        """Advanced swing high validation"""
        window = min(10, len(df) // 2)
        
        # Check multiple validation criteria
        if index < window or index >= len(df) - window:
            return 0
        
        current_price = df.loc[index, 'High']
        surrounding_prices = df.loc[index-window:index+window, 'High']
        
        is_local_max = current_price == surrounding_prices.max()
        movement_significant = (current_price - surrounding_prices.min()) > min_movement
        above_trend_line = current_price > df.loc[index, 'EMA_50']
        
        return 1 if is_local_max and movement_significant and above_trend_line else 0

    def is_swing_low(self, df, index, min_movement):
        """Advanced swing low validation"""
        window = min(10, len(df) // 2)
        
        # Check multiple validation criteria
        if index < window or index >= len(df) - window:
            return 0
        
        current_price = df.loc[index, 'Low']
        surrounding_prices = df.loc[index-window:index+window, 'Low']
        
        is_local_min = current_price == surrounding_prices.min()
        movement_significant = (surrounding_prices.max() - current_price) > min_movement
        below_trend_line = current_price < df.loc[index, 'EMA_50']
        
        return 1 if is_local_min and movement_significant and below_trend_line else 0

    def analyze_advanced_market_structure(self, df):
        """
        Enhanced market structure analysis with multi-factor trend confirmation
        """
        if df is None:
            return None

        try:
            # Robust trend identification
            swing_highs = df[df['Swing_High'] == 1]
            swing_lows = df[df['Swing_Low'] == 1]

            last_close = df['Close'].iloc[-1]
            last_ema_50 = df['EMA_50'].iloc[-1]
            last_ema_200 = df['EMA_200'].iloc[-1]

            # Advanced trend confirmation
            trend_strength = self.compute_trend_strength(df)

            if len(swing_highs) > 0 and len(swing_lows) > 0:
                # Complex trend validation
                uptrend_conditions = (
                    last_close > last_ema_50 and 
                    last_ema_50 > last_ema_200 and
                    trend_strength > self.trend_strength_threshold
                )

                downtrend_conditions = (
                    last_close < last_ema_50 and 
                    last_ema_50 < last_ema_200 and
                    trend_strength > self.trend_strength_threshold
                )

                if uptrend_conditions and self.previous_structure != 'UPTREND':
                    return 'UPTREND'
                elif downtrend_conditions and self.previous_structure != 'DOWNTREND':
                    return 'DOWNTREND'

            return None
        except Exception as e:
            self.logger.error("Advanced structure analysis error", error=str(e))
            return None

    def compute_trend_strength(self, df):
        """
        Compute trend strength using multiple indicators
        """
        price_momentum = np.mean(np.sign(df['Close'].diff()))
        ema_momentum = np.sign(df['EMA_50'].diff().iloc[-1])
        volatility_factor = 1 - (df['ATR'].iloc[-1] / df['Close'].iloc[-1])
        
        return abs(price_momentum * ema_momentum * volatility_factor)

    async def wait_until_next_day(self):
        """Wait until midnight NY time for daily reset"""
        while True:
            now = datetime.now(self.ny_tz)
            next_midnight = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            wait_seconds = (next_midnight - now).total_seconds()
            
            await asyncio.sleep(wait_seconds)
            # Reset tracking variables
            self.previous_structure = None
            self.logger.info("Daily reset completed", symbol=self.symbol)

    async def run(self):
        """Main monitoring loop with daily reset"""
        self.logger.info("Starting monitoring", symbol=self.symbol)
        
        # Start daily reset task concurrently
        reset_task = asyncio.create_task(self.wait_until_next_day())
        
        consecutive_errors = 0
        while True:
            try:
                df = await self.get_market_data()
                df = self.detect_advanced_swing_points(df)
                current_structure = self.analyze_advanced_market_structure(df)

                if (self.previous_structure and
                    current_structure is not None and
                        current_structure != self.previous_structure):

                    current_price = df['Close'].iloc[-1]
                    ny_time = datetime.now(self.ny_tz)
                    message = (
                        f"Market Structure Change Detected\n\n"
                        f"Asset: {self.symbol} ({self.category})\n"
                        f"Structure Change: {self.previous_structure} → {current_structure}\n"
                        f"Current Price: ${current_price:.2f}\n"
                        f"Time: {ny_time.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    )

                    self.logger.info(
                        "Market structure changed",
                        previous=self.previous_structure,
                        current=current_structure,
                        price=current_price
                    )

                    if self.notification_config['discord']['enabled']:
                        await self.notifier.send_message(message)

                if current_structure is not None:
                    self.previous_structure = current_structure

                consecutive_errors = 0
                await asyncio.sleep(900)  # 15 minutes

            except Exception as e:
                consecutive_errors += 1
                self.logger.error(
                    "Error in monitoring loop",
                    error=str(e),
                    consecutive_errors=consecutive_errors
                )

                # Exponential backoff for repeated errors
                wait_time = min(60 * (2 ** (consecutive_errors - 1)), 900)
                await asyncio.sleep(wait_time)