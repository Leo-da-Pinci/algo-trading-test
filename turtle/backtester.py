"""
Backtester Module
Simulates Turtle Trading strategy on historical data.
Tracks entries, exits, pyramids, P&L, and performance metrics.
"""

import pandas as pd
import numpy as np
from datetime import datetime


class Position:
    """Represents a single open position."""
    
    def __init__(self, commodity, entry_date, entry_price, units, n, stop_price):
        self.commodity = commodity
        self.entry_date = entry_date
        self.entry_price = entry_price
        self.units = units
        self.n = n
        self.stop_price = stop_price
        self.exit_date = None
        self.exit_price = None
        self.exit_type = None  # 'stop', 'exit_signal', 'manual'
        self.pyramid_count = 1
    
    def update_stop(self, new_stop):
        """Update stop loss (trailing stop)."""
        self.stop_price = max(self.stop_price, new_stop)
    
    def close(self, exit_date, exit_price, exit_type='stop'):
        """Close the position."""
        self.exit_date = exit_date
        self.exit_price = exit_price
        self.exit_type = exit_type
    
    def pnl(self):
        """Calculate P&L if closed."""
        if self.exit_price is None:
            return None
        return (self.exit_price - self.entry_price) * self.units
    
    def pnl_pct(self):
        """Calculate P&L %."""
        if self.exit_price is None:
            return None
        return ((self.exit_price - self.entry_price) / self.entry_price) * 100


class TurtleBacktester:
    """Backtest Turtle Trading strategy on historical data."""
    
    def __init__(self, signals_dict, account_size=1_000_000, risk_percent=2.0):
        """
        Initialize backtester.
        
        Args:
            signals_dict (dict): {commodity: TurtleSignals object}
            account_size (float): Starting capital
            risk_percent (float): Risk per trade (%)
        """
        self.signals_dict = signals_dict
        self.account_size = account_size
        self.risk_percent = risk_percent
        self.risk_per_trade = account_size * (risk_percent / 100.0)
        
        self.positions = []  # Closed positions
        self.open_positions = {}  # {commodity: [Position]}
        self.trades = []
    
    def get_contract_size(self, commodity):
        """Return contract multiplier for commodity."""
        contract_sizes = {
            'Crude Oil': 1000,
            'RBOB Gasoline': 42000,
            'Gold': 100,
            'Silver': 5000,
            'Copper': 25000,
            'Corn': 5000,
            'Soybeans': 5000,
            'Natural Gas': 10000,
        }
        return contract_sizes.get(commodity, 1)
    
    def backtest(self):
        """Run the backtest."""
        # Get list of dates (use first commodity's index)
        first_commodity = list(self.signals_dict.keys())[0]
        dates = self.signals_dict[first_commodity].signals.index
        
        print(f"Backtesting from {dates[0].date()} to {dates[-1].date()}...")
        
        for date in dates:
            self._process_day(date)
        
        # Close any remaining open positions at last price
        for commodity in list(self.open_positions.keys()):
            for position in self.open_positions[commodity]:
                if position.exit_date is None:
                    last_close = self.signals_dict[commodity].signals.loc[dates[-1], 'close']
                    position.close(dates[-1], last_close, exit_type='eod')
                    self.positions.append(position)
        
        self._compile_results()
        return self.trades
    
    def _process_day(self, date):
        """Process one day of trading."""
        for commodity, signals in self.signals_dict.items():
            if date not in signals.signals.index:
                continue
            
            signal_row = signals.signals.loc[date]
            close = signal_row['close']
            high = self.signals_dict[commodity].data.loc[date, 'High']
            low = self.signals_dict[commodity].data.loc[date, 'Low']
            n = signal_row.get('N', np.nan)
            
            # Check stop losses first
            self._check_stops(commodity, date, low)
            
            # Check for entry signals
            if signal_row.get('entry_55', False) and n > 0:
                self._enter_position(commodity, date, close, n)
    
    def _enter_position(self, commodity, date, entry_price, n):
        """Enter a new position."""
        contract_size = self.get_contract_size(commodity)
        units = self.risk_per_trade / n
        contracts = units / contract_size
        
        stop_price = entry_price - (2 * n)
        
        position = Position(
            commodity=commodity,
            entry_date=date,
            entry_price=entry_price,
            units=contracts,
            n=n,
            stop_price=stop_price
        )
        
        if commodity not in self.open_positions:
            self.open_positions[commodity] = []
        
        self.open_positions[commodity].append(position)
    
    def _check_stops(self, commodity, date, low):
        """Check if any stops are hit."""
        if commodity not in self.open_positions:
            return
        
        remaining = []
        for position in self.open_positions[commodity]:
            if low <= position.stop_price:
                # Stop hit
                position.close(date, position.stop_price, exit_type='stop')
                self.positions.append(position)
            else:
                remaining.append(position)
        
        self.open_positions[commodity] = remaining
    
    def _compile_results(self):
        """Compile trade results."""
        for position in self.positions:
            if position.exit_price is not None:
                self.trades.append({
                    'commodity': position.commodity,
                    'entry_date': position.entry_date,
                    'entry_price': position.entry_price,
                    'exit_date': position.exit_date,
                    'exit_price': position.exit_price,
                    'units': position.units,
                    'pnl': position.pnl(),
                    'pnl_pct': position.pnl_pct(),
                    'exit_type': position.exit_type,
                    'days_held': (position.exit_date - position.entry_date).days,
                })
    
    def get_summary(self):
        """Get backtest summary statistics."""
        if not self.trades:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'gross_pnl': 0,
                'avg_pnl_per_trade': 0,
                'profit_factor': 0,
                'max_drawdown': 0,
            }
        
        trades_df = pd.DataFrame(self.trades)
        
        winning = trades_df[trades_df['pnl'] > 0]
        losing = trades_df[trades_df['pnl'] < 0]
        
        gross_wins = winning['pnl'].sum()
        gross_losses = abs(losing['pnl'].sum())
        
        return {
            'total_trades': len(trades_df),
            'winning_trades': len(winning),
            'losing_trades': len(losing),
            'win_rate': (len(winning) / len(trades_df) * 100) if len(trades_df) > 0 else 0,
            'gross_pnl': trades_df['pnl'].sum(),
            'avg_pnl_per_trade': trades_df['pnl'].mean(),
            'profit_factor': gross_wins / gross_losses if gross_losses > 0 else 0,
            'avg_winner': winning['pnl'].mean() if len(winning) > 0 else 0,
            'avg_loser': losing['pnl'].mean() if len(losing) > 0 else 0,
        }
    
    def print_summary(self):
        """Print summary to console."""
        summary = self.get_summary()
        
        print("\n" + "=" * 80)
        print("🦞 BACKTEST SUMMARY")
        print("=" * 80)
        print(f"Total Trades:       {summary['total_trades']}")
        print(f"Winning Trades:     {summary['winning_trades']}")
        print(f"Losing Trades:      {summary['losing_trades']}")
        print(f"Win Rate:           {summary['win_rate']:.1f}%")
        print(f"Gross P&L:          ${summary['gross_pnl']:,.0f}")
        print(f"Avg P&L per Trade:  ${summary['avg_pnl_per_trade']:,.0f}")
        print(f"Profit Factor:      {summary['profit_factor']:.2f}")
        print(f"Avg Winner:         ${summary['avg_winner']:,.0f}")
        print(f"Avg Loser:          ${summary['avg_loser']:,.0f}")
        print("=" * 80)
        
        # By commodity
        print("\nBy Commodity:")
        trades_df = pd.DataFrame(self.trades)
        for commodity in trades_df['commodity'].unique():
            comm_trades = trades_df[trades_df['commodity'] == commodity]
            print(f"  {commodity}: {len(comm_trades)} trades, ${comm_trades['pnl'].sum():,.0f} P&L")
        
        print("\n" + "=" * 80)


if __name__ == '__main__':
    # Example usage
    from data_fetcher import DataFetcher
    from signals import TurtleSignals
    
    print("=" * 80)
    print("🦞 Running Backtest on 5 Years of Data")
    print("=" * 80)
    
    # Fetch data
    fetcher = DataFetcher()
    raw_data = fetcher.fetch_all_commodities(years=5, exclude=['NG=F'])
    
    # Generate signals
    signals_dict = {}
    for commodity, data in raw_data.items():
        sig = TurtleSignals(data)
        sig.generate_full_signals()
        signals_dict[commodity] = sig
    
    # Run backtest
    bt = TurtleBacktester(signals_dict, account_size=1_000_000, risk_percent=2.0)
    bt.backtest()
    bt.print_summary()
