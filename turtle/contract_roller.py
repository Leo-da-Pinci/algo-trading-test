"""
Contract Roller Module
Handles contract expirations and rolls positions to next month automatically.
Allows Turtle positions to survive across contract cycles.
"""

import pandas as pd
from datetime import datetime, timedelta


class ContractRoller:
    """Manage contract rollovers and expirations."""
    
    def __init__(self):
        """Initialize contract roller with standard rollover schedules."""
        # Days before expiration to initiate roll
        self.roll_days_before = 14
        
        # Contract expiration patterns (approximate, varies by exchange)
        self.expiration_months = {
            'CL': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],  # Every month
            'RB': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],  # Every month
            'GC': [2, 4, 6, 8, 10, 12],  # Feb, Apr, Jun, Aug, Oct, Dec
            'SI': [1, 3, 5, 7, 9, 12],   # Odd months + Dec
            'HG': [1, 3, 5, 7, 9, 12],   # Odd months + Dec
            'ZC': [3, 5, 7, 9, 12],      # Mar, May, Jul, Sep, Dec
            'ZS': [1, 3, 5, 7, 8, 9, 11],  # Jan, Mar, May, Jul, Aug, Sep, Nov
            'NG': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],  # Every month
        }
    
    def get_contract_expiration(self, symbol, year, month):
        """
        Get estimated expiration date for a contract.
        
        Args:
            symbol (str): Contract symbol (e.g., 'CL')
            year (int): Year
            month (int): Month (1-12)
        
        Returns:
            datetime: Estimated last trading day
        """
        # Last 3 business days of the month, roughly
        # This is approximate - real dates vary by contract
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        # Go back 3 days (rough estimate for last trading day)
        expiration = next_month - timedelta(days=3)
        
        return expiration
    
    def should_roll(self, current_date, symbol, contract_month):
        """
        Check if it's time to roll the contract.
        
        Args:
            current_date (datetime): Current date
            symbol (str): Contract symbol
            contract_month (int): Current contract month (1-12)
        
        Returns:
            dict: {
                'should_roll': bool,
                'days_to_expiration': int,
                'next_contract_month': int,
            }
        """
        expiration = self.get_contract_expiration(
            symbol,
            current_date.year,
            contract_month
        )
        
        days_to_expiration = (expiration - current_date).days
        
        should_roll = days_to_expiration <= self.roll_days_before
        
        # Determine next contract month
        expiration_months = self.expiration_months.get(symbol, [1, 3, 5, 7, 9, 12])
        next_month = None
        
        for m in expiration_months:
            if m > contract_month:
                next_month = m
                break
        
        if next_month is None:
            # Wrap to next year
            next_month = expiration_months[0]
        
        return {
            'should_roll': should_roll,
            'days_to_expiration': days_to_expiration,
            'next_contract_month': next_month,
            'expiration_date': expiration,
        }
    
    def execute_roll(self, position, new_contract_month, new_entry_price):
        """
        Execute a contract roll.
        
        Args:
            position (dict): Current position details
            new_contract_month (int): Next contract month
            new_entry_price (float): Entry price for next contract
        
        Returns:
            dict: Rolled position
        """
        rolled = {
            'original_contract_month': position.get('contract_month'),
            'new_contract_month': new_contract_month,
            'units': position.get('units'),
            'entry_price': new_entry_price,
            'original_entry_price': position.get('entry_price'),
            'roll_date': datetime.now(),
            'n': position.get('n'),
        }
        
        return rolled


class PyramidManager:
    """Manage pyramiding (adding to winning positions)."""
    
    def __init__(self):
        self.pyramid_levels = {}  # {position_id: [levels]}
    
    def create_pyramid_levels(self, position_id, entry_price, n, max_pyramids=4):
        """
        Create pyramid levels for a position.
        
        Args:
            position_id (str): Unique position identifier
            entry_price (float): Initial entry price
            n (float): Average True Range
            max_pyramids (int): Total units including initial (default 4)
        
        Returns:
            list: Pyramid level details
        """
        levels = []
        
        for i in range(max_pyramids):
            price = entry_price + (i * n)
            levels.append({
                'level': i + 1,
                'price': price,
                'units_to_add': 1 if i > 0 else 0,  # Don't count initial entry
                'triggered': i == 0,  # Initial entry is triggered
            })
        
        self.pyramid_levels[position_id] = levels
        return levels
    
    def check_pyramid_trigger(self, position_id, current_price):
        """
        Check if any pyramid level should be triggered.
        
        Args:
            position_id (str): Position identifier
            current_price (float): Current market price
        
        Returns:
            dict: Triggered level details, or None
        """
        if position_id not in self.pyramid_levels:
            return None
        
        levels = self.pyramid_levels[position_id]
        
        for level in levels:
            if not level['triggered'] and current_price >= level['price']:
                level['triggered'] = True
                return {
                    'level': level['level'],
                    'price': level['price'],
                    'units_to_add': level['units_to_add'],
                }
        
        return None
    
    def get_pyramid_status(self, position_id):
        """Get current pyramid status for a position."""
        if position_id not in self.pyramid_levels:
            return None
        
        levels = self.pyramid_levels[position_id]
        triggered = sum(1 for l in levels if l['triggered'])
        total = len(levels)
        
        return {
            'triggered_levels': triggered,
            'total_levels': total,
            'cumulative_units': triggered,
            'levels': levels,
        }


class PositionTracker:
    """Track positions across contract rolls and pyramids."""
    
    def __init__(self):
        self.positions = {}  # {position_id: position_data}
        self.position_counter = 0
    
    def open_position(self, commodity, entry_date, entry_price, units, n):
        """
        Open a new position.
        
        Returns:
            str: Position ID for tracking
        """
        pos_id = f"{commodity}_{self.position_counter}"
        self.position_counter += 1
        
        self.positions[pos_id] = {
            'commodity': commodity,
            'entry_date': entry_date,
            'entry_price': entry_price,
            'current_price': entry_price,
            'units': units,
            'n': n,
            'total_units': units,  # Track cumulative pyramids
            'status': 'open',
            'pyramids_added': 0,
        }
        
        return pos_id
    
    def add_pyramid(self, pos_id, pyramid_price, units):
        """Add pyramid to existing position."""
        if pos_id not in self.positions:
            return None
        
        pos = self.positions[pos_id]
        pos['total_units'] += units
        pos['pyramids_added'] += 1
        pos['last_pyramid_date'] = datetime.now()
        
        return pos
    
    def update_price(self, pos_id, current_price):
        """Update position price."""
        if pos_id in self.positions:
            self.positions[pos_id]['current_price'] = current_price
    
    def close_position(self, pos_id, close_price, close_date):
        """Close a position."""
        if pos_id not in self.positions:
            return None
        
        pos = self.positions[pos_id]
        pos['status'] = 'closed'
        pos['close_price'] = close_price
        pos['close_date'] = close_date
        pos['pnl'] = (close_price - pos['entry_price']) * pos['total_units']
        pos['pnl_pct'] = ((close_price - pos['entry_price']) / pos['entry_price']) * 100
        
        return pos
    
    def get_position(self, pos_id):
        """Get position details."""
        return self.positions.get(pos_id)
    
    def get_open_positions(self):
        """Get all open positions."""
        return {k: v for k, v in self.positions.items() if v['status'] == 'open'}
    
    def get_position_summary(self, pos_id):
        """Get summary for a position."""
        if pos_id not in self.positions:
            return None
        
        pos = self.positions[pos_id]
        unrealized_pnl = (pos['current_price'] - pos['entry_price']) * pos['total_units']
        
        return {
            'position_id': pos_id,
            'commodity': pos['commodity'],
            'status': pos['status'],
            'entry_price': pos['entry_price'],
            'current_price': pos['current_price'],
            'total_units': pos['total_units'],
            'pyramids_added': pos['pyramids_added'],
            'unrealized_pnl': unrealized_pnl,
            'unrealized_pnl_pct': (unrealized_pnl / (pos['entry_price'] * pos['total_units'])) * 100 if pos['entry_price'] > 0 else 0,
        }


if __name__ == '__main__':
    print("=" * 80)
    print("🦞 Testing Contract Roller, Pyramiding, and Position Tracker")
    print("=" * 80)
    
    # Test ContractRoller
    print("\n" + "-" * 80)
    print("CONTRACT ROLLER")
    print("-" * 80)
    
    roller = ContractRoller()
    today = datetime(2026, 2, 19)
    
    result = roller.should_roll(today, 'CL', 2)  # Feb CL contract
    print(f"CL Feb contract on {today.date()}:")
    print(f"  Should roll? {result['should_roll']}")
    print(f"  Days to expiration: {result['days_to_expiration']}")
    print(f"  Next contract month: {result['next_contract_month']}")
    
    # Test PyramidManager
    print("\n" + "-" * 80)
    print("PYRAMID MANAGER")
    print("-" * 80)
    
    pm = PyramidManager()
    pm.create_pyramid_levels('CL_001', entry_price=63.79, n=2.37, max_pyramids=4)
    print("Pyramid levels created for CL_001:")
    
    # Check pyramid triggers
    prices = [63.79, 66.00, 68.00, 69.50, 71.00]
    for price in prices:
        trigger = pm.check_pyramid_trigger('CL_001', price)
        if trigger:
            print(f"  Price ${price:.2f}: Pyramid Level {trigger['level']} TRIGGERED (+{trigger['units_to_add']} units)")
    
    status = pm.get_pyramid_status('CL_001')
    print(f"Final status: {status['triggered_levels']}/{status['total_levels']} levels triggered")
    
    # Test PositionTracker
    print("\n" + "-" * 80)
    print("POSITION TRACKER")
    print("-" * 80)
    
    pt = PositionTracker()
    pos_id = pt.open_position('Crude Oil', datetime(2026, 2, 5), 63.79, 1, 2.37)
    print(f"Position opened: {pos_id}")
    
    pt.add_pyramid(pos_id, 66.16, 1)
    pt.add_pyramid(pos_id, 68.53, 1)
    pt.update_price(pos_id, 68.00)
    
    summary = pt.get_position_summary(pos_id)
    print(f"Position summary:")
    for key, value in summary.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    print("\n" + "=" * 80)
