"""
Position Sizing Module
Implements Turtle Trading position sizing using N (ATR-20).
- Calculates units based on 2% account risk
- Determines stop losses (2N below entry)
- Calculates pyramid levels (add 1N apart)
"""

import pandas as pd
import numpy as np


class PositionSizer:
    """Calculate position sizes using Turtle Trading rules."""
    
    def __init__(self, account_size, risk_percent=2.0, contract_specs=None):
        """
        Initialize position sizer.
        
        Args:
            account_size (float): Total account capital
            risk_percent (float): Risk per trade as % of account (default 2%)
            contract_specs (dict): {ticker: {'unit_multiplier': X}} 
                e.g., {'CL=F': {'unit_multiplier': 1000}} for 1000 barrels/contract
        """
        self.account_size = account_size
        self.risk_percent = risk_percent
        self.risk_per_trade = (account_size * risk_percent) / 100.0
        
        # Standard commodity contract specs
        self.contract_specs = contract_specs or {
            'CL=F': {'multiplier': 1000, 'name': 'Crude Oil (barrels)'},
            'RB=F': {'multiplier': 42000, 'name': 'RBOB Gasoline (gallons)'},
            'GC=F': {'multiplier': 100, 'name': 'Gold (troy oz)'},
            'SI=F': {'multiplier': 5000, 'name': 'Silver (troy oz)'},
            'HG=F': {'multiplier': 25000, 'name': 'Copper (lbs)'},
            'ZC=F': {'multiplier': 5000, 'name': 'Corn (bushels)'},
            'ZS=F': {'multiplier': 5000, 'name': 'Soybeans (bushels)'},
            'NG=F': {'multiplier': 10000, 'name': 'Natural Gas (MMBtu)'},
        }
    
    def calculate_units(self, entry_price, n, ticker=None):
        """
        Calculate position size in contracts/units.
        
        Turtle rule: Contracts = Account_Risk / (N × multiplier)
        where Account_Risk = 2% of account
        
        Args:
            entry_price (float): Entry price
            n (float): Average True Range (volatility per unit)
            ticker (str): Optional ticker for contract specs
        
        Returns:
            dict: {
                'contracts': number of contracts to trade,
                'risk_dollars': $ at risk,
                'notional_value': total $ controlled,
                'multiplier': items per contract
            }
        """
        if n <= 0 or entry_price <= 0:
            return None
        
        # Get multiplier for contract
        multiplier = 1
        if ticker and ticker in self.contract_specs:
            multiplier = self.contract_specs[ticker]['multiplier']
        
        # Turtle rule: Contracts = Risk / (N × multiplier)
        # N is per-unit change, multiplier converts to per-contract
        n_per_contract = n * multiplier
        raw_contracts = self.risk_per_trade / n_per_contract
        
        # Round to whole contracts
        contracts = round(raw_contracts)
        
        # Notional value = contracts × entry_price × multiplier
        notional_value = contracts * entry_price * multiplier
        
        return {
            'contracts': contracts,
            'risk_dollars': self.risk_per_trade,
            'n_per_unit': n,
            'n_per_contract': n_per_contract,
            'raw_contracts': raw_contracts,
            'notional_value': notional_value,
            'multiplier': multiplier,
        }
    
    def calculate_stop_loss(self, entry_price, n, direction='long'):
        """
        Calculate stop loss price.
        
        Turtle rule: Stop = Entry ± 2N
        - Long: Stop = Entry - 2N
        - Short: Stop = Entry + 2N
        
        Args:
            entry_price (float): Entry price
            n (float): Average True Range
            direction (str): 'long' or 'short'
        
        Returns:
            float: Stop loss price
        """
        if direction == 'long':
            return entry_price - (2 * n)
        elif direction == 'short':
            return entry_price + (2 * n)
        else:
            raise ValueError("Direction must be 'long' or 'short'")
    
    def calculate_pyramid_levels(self, entry_price, n, max_pyramids=4, direction='long'):
        """
        Calculate pyramid entry levels.
        
        Turtle rule: Add 1 unit every 1N move in profit direction
        
        Args:
            entry_price (float): Initial entry price
            n (float): Average True Range (spacing between pyramids)
            max_pyramids (int): Max number of positions (default 4)
            direction (str): 'long' or 'short'
        
        Returns:
            list: Pyramid prices [entry, entry±N, entry±2N, ...]
        """
        pyramids = [entry_price]
        
        for i in range(1, max_pyramids):
            if direction == 'long':
                level = entry_price + (i * n)
            else:  # short
                level = entry_price - (i * n)
            
            pyramids.append(level)
        
        return pyramids
    
    def calculate_risk_reward(self, entry_price, stop_loss, target_price, units, ticker=None):
        """
        Calculate risk/reward metrics for a trade.
        
        Args:
            entry_price (float): Entry price
            stop_loss (float): Stop loss price
            target_price (float): Profit target price
            units (int): Number of contracts
            ticker (str): Optional, for contract specs
        
        Returns:
            dict: Risk/reward analysis
        """
        multiplier = 1
        if ticker and ticker in self.contract_specs:
            multiplier = self.contract_specs[ticker]['multiplier']
        
        risk_per_unit = abs(entry_price - stop_loss)
        reward_per_unit = abs(target_price - entry_price)
        
        risk_dollars = risk_per_unit * units * multiplier
        reward_dollars = reward_per_unit * units * multiplier
        
        ratio = reward_dollars / risk_dollars if risk_dollars > 0 else 0
        
        return {
            'risk_per_unit': risk_per_unit,
            'reward_per_unit': reward_per_unit,
            'risk_dollars': risk_dollars,
            'reward_dollars': reward_dollars,
            'risk_reward_ratio': ratio,
            'units': units,
            'multiplier': multiplier,
        }
    
    def portfolio_summary(self, positions):
        """
        Calculate portfolio-level risk metrics.
        
        Args:
            positions (list): List of dicts with 'units', 'risk_dollars', 'ticker'
        
        Returns:
            dict: Total risk, diversification, etc.
        """
        total_risk = sum(p.get('risk_dollars', 0) for p in positions)
        total_notional = sum(p.get('notional_value', 0) for p in positions)
        
        return {
            'total_risk_dollars': total_risk,
            'total_notional_value': total_notional,
            'num_positions': len(positions),
            'risk_as_percent_of_account': (total_risk / self.account_size) * 100,
        }


if __name__ == '__main__':
    print("=" * 80)
    print("🦞 Testing Position Sizing")
    print("=" * 80)
    
    # Example: $1M account
    sizer = PositionSizer(account_size=1000000, risk_percent=2.0)
    
    # Scenario: Crude Oil entry at $63.79 with N=$2.37
    entry = 63.79
    n = 2.37
    ticker = 'CL=F'
    
    units_calc = sizer.calculate_units(entry, n, ticker)
    print(f"\nCrude Oil Entry Scenario:")
    print(f"  Entry Price: ${entry}")
    print(f"  N (ATR-20): ${n}")
    print(f"  Account Risk (2%): ${sizer.risk_per_trade:,.2f}")
    print(f"  N per Contract (N × multiplier): ${units_calc['n_per_contract']:,.2f}")
    print(f"  Contracts: {units_calc['contracts']}")
    print(f"  Notional Value: ${units_calc['notional_value']:,.2f}")
    
    stop = sizer.calculate_stop_loss(entry, n, direction='long')
    print(f"\nStop Loss:")
    print(f"  Stop Price: ${stop:.2f}")
    print(f"  Risk per Contract: ${(entry - stop):.2f} × {units_calc['multiplier']} = ${(entry - stop) * units_calc['multiplier']:,.2f}")
    
    pyramids = sizer.calculate_pyramid_levels(entry, n, max_pyramids=4)
    print(f"\nPyramid Levels:")
    for i, level in enumerate(pyramids):
        print(f"  Level {i+1}: ${level:.2f}")
    
    # Risk/reward example (5% above entry as target)
    target = entry * 1.05
    rr = sizer.calculate_risk_reward(entry, stop, target, units_calc['contracts'], ticker)
    print(f"\nRisk/Reward (5% target):")
    print(f"  Risk: ${rr['risk_dollars']:,.2f}")
    print(f"  Reward: ${rr['reward_dollars']:,.2f}")
    print(f"  Ratio: 1:{rr['risk_reward_ratio']:.2f}")
    
    print("=" * 80)
