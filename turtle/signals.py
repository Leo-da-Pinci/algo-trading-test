"""
Signals Module
Implements Turtle Trading breakout detection:
- 20-day and 55-day highs/lows (entry signals)
- 10-day lows (exit signals)
- N calculation (20-day ATR for volatility)
"""

import pandas as pd
import numpy as np


class TurtleSignals:
    """Generate Turtle Trading signals from OHLCV data."""
    
    def __init__(self, data):
        """
        Initialize with OHLCV DataFrame.
        
        Args:
            data (pd.DataFrame): Must have columns [Open, High, Low, Close, Volume]
        """
        self.data = data.copy()
        self.signals = pd.DataFrame(index=data.index)
    
    def calculate_atr(self, period=20):
        """
        Calculate Average True Range (N for Turtles).
        
        Args:
            period (int): ATR lookback period (20 for Turtle)
        
        Returns:
            pd.Series: ATR values
        """
        high = self.data['High']
        low = self.data['Low']
        close = self.data['Close']
        
        # True Range components
        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        
        # Max of the three
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # Average True Range
        atr = tr.rolling(window=period).mean()
        
        return atr
    
    def calculate_breakouts(self, short_period=20, long_period=55):
        """
        Calculate 20-day and 55-day highs/lows.
        
        Args:
            short_period (int): Short-term lookback (20 for Turtle)
            long_period (int): Long-term lookback (55 for Turtle)
        
        Returns:
            pd.DataFrame: Columns with breakout levels
        """
        close = self.data['Close']
        high = self.data['High']
        low = self.data['Low']
        
        # 20-day signals
        self.signals['high_20'] = high.rolling(window=short_period).max()
        self.signals['low_20'] = low.rolling(window=short_period).min()
        
        # 55-day signals
        self.signals['high_55'] = high.rolling(window=long_period).max()
        self.signals['low_55'] = low.rolling(window=long_period).min()
        
        # 10-day low (exit signal)
        self.signals['low_10'] = low.rolling(window=10).min()
        
        # Current close for comparison
        self.signals['close'] = close
        
        return self.signals
    
    def identify_long_entries(self):
        """
        Identify long entry signals.
        Entry: Close breaks above 55-day high (or 20-day as confirmation).
        
        Returns:
            pd.Series: Boolean, True on breakout days
        """
        self.signals['entry_55'] = self.signals['close'] > self.signals['high_55'].shift(1)
        self.signals['entry_20'] = self.signals['close'] > self.signals['high_20'].shift(1)
        
        return self.signals
    
    def identify_exits(self):
        """
        Identify exit signals.
        Exit: Close falls below 10-day low.
        
        Returns:
            pd.Series: Boolean, True on exit days
        """
        self.signals['exit_10'] = self.signals['close'] < self.signals['low_10'].shift(1)
        
        return self.signals
    
    def identify_pyramid_levels(self, entry_price, n, position_unit=1):
        """
        Calculate pyramid entry levels.
        Pyramid: Add 1 unit every 1N move above entry.
        
        Args:
            entry_price (float): Initial entry price
            n (float): Average True Range (volatility)
            position_unit (int): Current position size
        
        Returns:
            list: Pyramid entry prices [entry, entry+N, entry+2N, ...]
        """
        pyramid_levels = [entry_price]
        for i in range(1, 4):  # Add up to 3 more units (4 total)
            pyramid_levels.append(entry_price + (i * n))
        
        return pyramid_levels
    
    def generate_full_signals(self, short_period=20, long_period=55):
        """
        Generate complete signal set.
        
        Returns:
            pd.DataFrame: All signals and levels
        """
        # Breakout levels
        self.calculate_breakouts(short_period, long_period)
        
        # ATR (N)
        self.signals['N'] = self.calculate_atr(period=20)
        
        # Entry signals
        self.identify_long_entries()
        
        # Exit signals
        self.identify_exits()
        
        # Stop loss (2N below entry)
        self.signals['stop_loss_level'] = self.signals['close'] - (2 * self.signals['N'])
        
        return self.signals
    
    def get_signal_summary(self, lookback=1):
        """
        Get most recent signal summary.
        
        Args:
            lookback (int): How many bars back (1 = today)
        
        Returns:
            dict: Current signal state
        """
        if len(self.signals) < lookback:
            return None
        
        idx = -lookback
        row = self.signals.iloc[idx]
        
        return {
            'date': self.signals.index[idx],
            'close': row['close'],
            'high_20': row['high_20'],
            'high_55': row['high_55'],
            'low_10': row['low_10'],
            'N': row['N'],
            'entry_55': row.get('entry_55', False),
            'entry_20': row.get('entry_20', False),
            'exit_10': row.get('exit_10', False),
            'stop_loss': row.get('stop_loss_level', np.nan),
        }


if __name__ == '__main__':
    # Example usage
    from data_fetcher import DataFetcher
    
    print("=" * 80)
    print("🦞 Testing Turtle Signals on Crude Oil")
    print("=" * 80)
    
    # Fetch data
    fetcher = DataFetcher()
    data = fetcher.fetch_commodity('CL=F', years=5)  # 5 years for quick test
    
    if data is not None:
        # Generate signals
        signals = TurtleSignals(data)
        signals.generate_full_signals()
        
        # Show recent signals
        print("\nMost recent signals (last 10 days):")
        print(signals.signals[['close', 'high_55', 'high_20', 'low_10', 'N', 'entry_55', 'exit_10']].tail(10))
        
        # Summary of last day
        print("\nLatest signal summary:")
        summary = signals.get_signal_summary(lookback=1)
        if summary:
            for key, value in summary.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.2f}")
                else:
                    print(f"  {key}: {value}")
    
    print("=" * 80)
