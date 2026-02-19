"""
Backtester Module
Runs the Turtle Trading system on historical data.
Simulates trades, tracks P&L, and calculates performance metrics.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from data_fetcher import DataFetcher
from signals import TurtleSignals
from position_sizing import PositionSizer


class TurtleBacktester:
    """Backtest the Turtle Trading system on commodity futures."""
    
    def __init__(self, account_size=1000000, risk_percent=2.0, commission_per_trade=0):
        """
        Initialize backtester.
        
        Args:
            account_size (float): Starting capital
            risk_percent (float): Risk per trade (default 2%)
            commission_per_trade (float): Commission per trade (optional)
        """
        self.initial_account = account_size
        self.current_account = account_size
        self.risk_percent = risk_percent
        self.commission = commission_per_trade
        
        self.sizer = PositionSizer(account_size, risk_percent)
        self.trades = []
        self.positions = {}
        self.equity_curve = [account_size]
        self.dates = []
    
    def backtest_single_commodity(self, ticker, data, short_period=20, long_period=55):
        """
        Backtest a single commodity.
        
        Args:
            ticker (str): Commodity ticker
            data (pd.DataFrame): OHLCV data
            short_period (int): Short breakout period (20 for Turtle)
            long_period (int): Long breakout period (55 for Turtle)
        
        Returns:
            dict: Trade statistics for this commodity
        """
        signals = TurtleSignals(data)
        signals.generate_full_signals(short_period, long_period)
        
        trades = []
        in_position = False
        entry_bar_idx = None
        entry_price = None
        entry_contracts = 0
        
        for i in range(long_period, len(data)):
            bar_date = data.index[i]
            close = data['Close'].iloc[i]
            low = data['Low'].iloc[i]
            
            signal_row = signals.signals.iloc[i]
            
            # Exit logic: Close below 10-day low
            if in_position and not pd.isna(signal_row['low_10']):
                if close < signal_row['low_10']:
                    exit_price = close
                    pnl = (exit_price - entry_price) * self.sizer.contract_specs[ticker]['multiplier'] * entry_contracts
                    
                    trades.append({
                        'ticker': ticker,
                        'entry_date': data.index[entry_bar_idx],
                        'entry_price': entry_price,
                        'exit_date': bar_date,
                        'exit_price': exit_price,
                        'contracts': entry_contracts,
                        'pnl': pnl,
                        'pnl_percent': ((exit_price - entry_price) / entry_price) * 100 if entry_price != 0 else 0,
                        'bars_held': i - entry_bar_idx,
                    })
                    
                    in_position = False
                    entry_price = None
                    entry_bar_idx = None
                    entry_contracts = 0
            
            # Entry logic: Close above 55-day high
            if not in_position and signal_row['entry_55']:
                entry_price = close
                entry_bar_idx = i
                n = signal_row['N']
                
                # Calculate position size
                if pd.notna(n) and n > 0:
                    sizing = self.sizer.calculate_units(entry_price, n, ticker)
                    if sizing:
                        in_position = True
                        entry_contracts = sizing['contracts']
        
        return {
            'ticker': ticker,
            'trades': trades,
            'total_trades': len(trades),
            'winning_trades': len([t for t in trades if t['pnl'] > 0]),
            'losing_trades': len([t for t in trades if t['pnl'] < 0]),
            'gross_pnl': sum(t['pnl'] for t in trades),
            'avg_pnl_per_trade': np.mean([t['pnl'] for t in trades]) if trades else 0,
        }
    
    def backtest_portfolio(self, tickers, years=10, exclude_tickers=None):
        """
        Backtest multiple commodities as a portfolio.
        
        Args:
            tickers (list): List of commodity tickers
            years (int): Years of historical data
            exclude_tickers (list): Tickers to skip
        
        Returns:
            dict: Portfolio performance metrics
        """
        exclude_tickers = exclude_tickers or []
        fetcher = DataFetcher()
        
        all_results = {}
        all_trades = []
        
        print("=" * 80)
        print("🦞 TURTLE TRADING BACKTEST")
        print("=" * 80)
        print(f"Account Size: ${self.initial_account:,.0f}")
        print(f"Risk per Trade: {self.risk_percent}%")
        print(f"Historical Period: {years} years")
        print()
        
        for ticker in tickers:
            if ticker in exclude_tickers:
                continue
            
            print(f"Backtesting {ticker}...", end=" ", flush=True)
            
            # Fetch data
            data = fetcher.fetch_commodity(ticker, years)
            if data is None:
                print("❌ Failed")
                continue
            
            # Run backtest
            result = self.backtest_single_commodity(ticker, data)
            all_results[ticker] = result
            all_trades.extend(result['trades'])
            
            print(f"✓ ({result['total_trades']} trades)")
        
        print()
        print("=" * 80)
        print("PORTFOLIO SUMMARY")
        print("=" * 80)
        
        # Portfolio stats
        total_trades = len(all_trades)
        total_pnl = sum(t['pnl'] for t in all_trades)
        winning_trades = len([t for t in all_trades if t['pnl'] > 0])
        losing_trades = len([t for t in all_trades if t['pnl'] < 0])
        
        print(f"Total Trades: {total_trades}")
        print(f"Winning Trades: {winning_trades}")
        print(f"Losing Trades: {losing_trades}")
        print(f"Win Rate: {(winning_trades/total_trades*100):.1f}%" if total_trades > 0 else "N/A")
        print(f"Gross P&L: ${total_pnl:,.2f}")
        print(f"Avg P&L per Trade: ${np.mean([t['pnl'] for t in all_trades]) if all_trades else 0:,.2f}")
        print()
        
        # By commodity
        print("By Commodity:")
        for ticker, result in sorted(all_results.items()):
            print(f"  {ticker}: {result['total_trades']} trades, ${result['gross_pnl']:,.2f} P&L")
        
        print("=" * 80)
        
        return {
            'by_commodity': all_results,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'gross_pnl': total_pnl,
            'all_trades': all_trades,
        }


if __name__ == '__main__':
    # Run backtest
    backtester = TurtleBacktester(account_size=1000000, risk_percent=2.0)
    
    tickers = ['CL=F', 'GC=F', 'ZS=F', 'ZC=F', 'SI=F', 'HG=F']
    
    results = backtester.backtest_portfolio(tickers, years=10, exclude_tickers=['NG=F'])
