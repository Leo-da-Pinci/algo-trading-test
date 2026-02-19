"""
Position Sizing Module
Implements Turtle Trading position sizing based on N (ATR) and account risk.
Core principle: Same dollar risk per trade regardless of volatility.
"""

import pandas as pd
import numpy as np


class PositionSizer:
    """Calculate position sizes using Turtle Trading methodology."""
    
    def __init__(self, account_size, risk_percent=2.0):
        """
        Initialize position sizer.
        
        Args:
            account_size (float): Total account capital
            risk_percent (float): Risk per trade as % of account (default 2%)
        """
        self.account_size = account_size
        self.risk_percent = risk_percent
        self.risk_per_trade = account_size * (risk_percent / 100.0)
    
    def calculate_position_size(self, entry_price, n, contract_size=1):
        """
        Calculate position size in units.
        
        Formula: Units = Risk Amount / N
        Then multiply by contract multiplier.
        
        Args:
            entry_price (float): Entry price
            n (float): Average True Range (volatility)
            contract_size (int): Multiplier (1000 for CL, 42000 for RB, etc.)
        
        Returns:
            dict: {
                'units': position size in contracts,
                'dollar_value': notional exposure,
                'risk_dollars': dollar risk on this position,
                'stop_price': 2N below entry
            }
        """
        if n <= 0:
            return None
        
        # Calculate position in units of N
        units = self.risk_per_trade / n
        
        # Scale by contract multiplier
        position_contracts = units / contract_size if contract_size > 0 else 0
        
        # Notional value
        notional = entry_price * contract_size * position_contracts
        
        # Stop loss (2N below entry)
        stop_price = entry_price - (2 * n)
        stop_value = abs(stop_price - entry_price) * contract_size * position_contracts
        
        return {
            'units': units,
            'contracts': position_contracts,
            'notional_value': notional,
            'risk_dollars': self.risk_per_trade,
            'stop_price': stop_price,
            'stop_value': stop_value,
        }
    
    def pyramid_position(self, entry_price, n, num_pyramids=4):
        """
        Calculate pyramid entry levels.
        Pyramid: Add 1 unit every 1N move above entry.
        
        Args:
            entry_price (float): Initial entry price
            n (float): Average True Range
            num_pyramids (int): Total units to add (default 4 = 1 initial + 3 pyramids)
        
        Returns:
            list: Pyramid levels with details
        """
        pyramids = []
        
        for i in range(num_pyramids):
            level_price = entry_price + (i * n)
            pyramids.append({
                'level': i + 1,
                'price': level_price,
                'units_added': 1,
                'cumulative_units': i + 1,
            })
        
        return pyramids
    
    def calculate_position_by_commodity(self, signals_dict, contract_sizes):
        """
        Calculate positions for multiple commodities.
        
        Args:
            signals_dict (dict): {commodity: TurtleSignals object}
            contract_sizes (dict): {commodity: contract_multiplier}
        
        Returns:
            pd.DataFrame: Summary of all positions
        """
        positions = []
        
        for commodity, signals in signals_dict.items():
            summary = signals.get_signal_summary(lookback=1)
            
            if summary and not np.isnan(summary['N']):
                size = self.calculate_position_size(
                    entry_price=summary['close'],
                    n=summary['N'],
                    contract_size=contract_sizes.get(commodity, 1)
                )
                
                if size:
                    positions.append({
                        'commodity': commodity,
                        'close': summary['close'],
                        'N': summary['N'],
                        'contracts': size['contracts'],
                        'notional_value': size['notional_value'],
                        'risk_dollars': size['risk_dollars'],
                        'stop_price': size['stop_price'],
                        'entry_55': summary['entry_55'],
                        'entry_20': summary['entry_20'],
                    })
        
        return pd.DataFrame(positions)
    
    def portfolio_risk(self, positions_df):
        """
        Calculate total portfolio risk.
        
        Args:
            positions_df (pd.DataFrame): Output from calculate_position_by_commodity
        
        Returns:
            dict: Portfolio-level risk metrics
        """
        total_notional = positions_df['notional_value'].sum()
        total_risk = positions_df['risk_dollars'].sum()
        
        return {
            'num_positions': len(positions_df),
            'total_notional': total_notional,
            'total_risk_dollars': total_risk,
            'pct_of_account_at_risk': (total_risk / self.account_size) * 100,
            'leverage': total_notional / self.account_size if self.account_size > 0 else 0,
        }


if __name__ == '__main__':
    # Example usage
    print("=" * 80)
    print("🦞 Testing Position Sizing")
    print("=" * 80)
    
    # Initialize with $1M account, 2% risk
    sizer = PositionSizer(account_size=1_000_000, risk_percent=2.0)
    
    print(f"\nAccount Size: ${sizer.account_size:,.0f}")
    print(f"Risk per Trade: {sizer.risk_percent}% = ${sizer.risk_per_trade:,.0f}")
    
    # Example: CL at $63.79 with N=2.37
    print("\n" + "-" * 80)
    print("EXAMPLE 1: Crude Oil (CL)")
    print("-" * 80)
    
    cl_position = sizer.calculate_position_size(
        entry_price=63.79,
        n=2.37,
        contract_size=1000  # CL is 1000 barrels per contract
    )
    
    if cl_position:
        print(f"Entry Price: ${cl_position['notional_value'] / (cl_position['contracts'] * 1000):.2f}")
        print(f"N (Volatility): $2.37")
        print(f"Position Size: {cl_position['contracts']:.2f} contracts")
        print(f"Notional Exposure: ${cl_position['notional_value']:,.0f}")
        print(f"Risk Amount: ${cl_position['risk_dollars']:,.0f}")
        print(f"Stop Loss Price: ${cl_position['stop_price']:.2f}")
        print(f"Risk if Stopped: ${cl_position['stop_value']:,.0f}")
    
    # Example: ZS at $11.55 with N=0.1857
    print("\n" + "-" * 80)
    print("EXAMPLE 2: Soybeans (ZS)")
    print("-" * 80)
    
    zs_position = sizer.calculate_position_size(
        entry_price=11.55,
        n=0.1857,
        contract_size=5000  # ZS is 5000 bushels per contract
    )
    
    if zs_position:
        print(f"Entry Price: ${zs_position['notional_value'] / (zs_position['contracts'] * 5000):.2f}/bu")
        print(f"N (Volatility): ${zs_position['stop_price']:.4f}/bu")
        print(f"Position Size: {zs_position['contracts']:.2f} contracts")
        print(f"Notional Exposure: ${zs_position['notional_value']:,.0f}")
        print(f"Risk Amount: ${zs_position['risk_dollars']:,.0f}")
        print(f"Stop Loss Price: ${zs_position['stop_price']:.2f}/bu")
        print(f"Risk if Stopped: ${zs_position['stop_value']:,.0f}")
    
    # Pyramid example
    print("\n" + "-" * 80)
    print("PYRAMID EXAMPLE: CL at $63.79")
    print("-" * 80)
    
    pyramids = sizer.pyramid_position(entry_price=63.79, n=2.37, num_pyramids=4)
    print("Add units at these levels:")
    for p in pyramids:
        print(f"  Level {p['level']}: ${p['price']:.2f} - Add {p['units_added']} unit (Total: {p['cumulative_units']})")
    
    print("\n" + "=" * 80)
