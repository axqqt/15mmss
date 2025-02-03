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
        self.last_swing_high = None
        self.last_swing_low = None
        self.logger = logger.bind(symbol=symbol, category=category)
        self.notifier = DiscordNotifier()

    async def get_market_data(self):
        """Fetch market data using yfinance"""
        try:
            ticker = yf.Ticker(self.symbol)
            # Fetch slightly more data for better context
            df = ticker.history(interval='15m', period='2d')
            # Only use the last day's worth of data for analysis
            return df.tail(96)  # 96 15-minute periods in 24 hours
        except Exception as e:
            self.logger.error("Error fetching market data", error=str(e))
            return None

    def detect_swing_points(self, df, window=5):
        """Detect swing highs and lows with improved validation"""
        if df is None or len(df) < window * 2:
            self.logger.warning("Insufficient data for swing detection")
            return None

        try:
            # Add validation for swing points
            # 0.2% minimum movement
            min_price_movement = df['High'].mean() * 0.002

            df['Swing_High'] = df['High'].rolling(window=window, center=True).apply(
                lambda x: 1 if (x.iloc[window//2] == max(x) and
                                max(x) - min(x) > min_price_movement) else 0
            )
            df['Swing_Low'] = df['Low'].rolling(window=window, center=True).apply(
                lambda x: 1 if (x.iloc[window//2] == min(x) and
                                max(x) - min(x) > min_price_movement) else 0
            )
            return df
        except Exception as e:
            self.logger.error("Error in swing point detection", error=str(e))
            return None

    def analyze_market_structure(self, df):
        """Analyze market structure with strict trend confirmation"""
        if df is None:
            return None

        try:
            # Find swing highs and lows
            swing_highs = df[df['Swing_High'] == 1]
            swing_lows = df[df['Swing_Low'] == 1]

            # Only consider confirmed swing points
            if len(swing_highs) > 0 and len(swing_lows) > 0:
                latest_high = swing_highs['High'].iloc[-1]
                latest_low = swing_lows['Low'].iloc[-1]

                # Get the last close price
                last_close = df['Close'].iloc[-1]

                # Check for uptrend: close above previous swing high
                if last_close > latest_high:
                    if self.previous_structure != 'UPTREND':
                        return 'UPTREND'

                # Check for downtrend: close below previous swing low
                elif last_close < latest_low:
                    if self.previous_structure != 'DOWNTREND':
                        return 'DOWNTREND'

            return None
        except Exception as e:
            self.logger.error(
                "Error in market structure analysis", error=str(e))
            return None

    async def run(self):
        """Main monitoring loop with enhanced messaging"""
        self.logger.info("Starting monitoring", symbol=self.symbol)
        consecutive_errors = 0

        while True:
            try:
                df = await self.get_market_data()
                df = self.detect_swing_points(df)
                current_structure = self.analyze_market_structure(df)

                if (self.previous_structure and
                    current_structure is not None and
                        current_structure != self.previous_structure):

                    current_price = df['Close'].iloc[-1]
                    message = (
                        f"Market Structure Change Detected\n\n"
                        f"Asset: {self.symbol} ({self.category})\n"
                        f"Structure Change: {self.previous_structure} → {current_structure}\n"
                        f"Current Price: ${current_price:.2f}\n"
                        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
