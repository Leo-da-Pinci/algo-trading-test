"""
Data Fetcher Module
Pulls historical OHLCV data for commodities from yfinance.
Handles 10+ years of futures data with contract rollover considerations.
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class DataFetcher:
    """Fetch and clean commodity futures data."""
    
    def __init__(self):
        """Initialize with standard commodity tickers."""
        self.commodities = {
            'Crude Oil': 'CL=F',
            'RBOB Gasoline': 'RB=F',
            'Gold': 'GC=F',
            'Silver': 'SI=F',
            'Copper': 'HG=F',
            'Corn': 'ZC=F',
            'Soybeans': 'ZS=F',
            'Natural Gas': 'NG=F',
        }
    
    def fetch_commodity(self, ticker, years=10):
        """
        Fetch historical data for a commodity.
        
        Args:
            ticker (str): Yahoo Finance ticker (e.g., 'CL=F')
            years (int): Years of historical data to fetch
        
        Returns:
            pd.DataFrame: OHLCV data with columns [Open, High, Low, Close, Volume]
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * years)
        
        print(f"Fetching {ticker} from {start_date.date()} to {end_date.date()}...", end=" ", flush=True)
        
        try:
            data = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                progress=False,
                timeout=30
            )
            
            if data is None or len(data) == 0:
                print("❌ No data returned")
                return None
            
            # Clean column names (yfinance may return multi-index for multiple tickers)
            if isinstance(data.columns, pd.MultiIndex):
                data = data.iloc[:, :5]  # Get first 5 columns (OHLCV)
                data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            else:
                data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            
            # Remove NaN rows
            data = data.dropna()
            
            # Ensure date index
            data.index.name = 'Date'
            
            print(f"✓ ({len(data)} trading days)")
            
            return data
        
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def fetch_all_commodities(self, years=10, exclude=None):
        """
        Fetch data for all commodities.
        
        Args:
            years (int): Years of data
            exclude (list): Tickers to skip (e.g., ['NG=F'])
        
        Returns:
            dict: {commodity_name: DataFrame}
        """
        exclude = exclude or []
        data_dict = {}
        
        for name, ticker in self.commodities.items():
            if ticker in exclude:
                continue
            
            data = self.fetch_commodity(ticker, years)
            if data is not None:
                data_dict[name] = data
        
        return data_dict
    
    def export_csv(self, data_dict, output_dir='data'):
        """
        Export fetched data to CSV files.
        
        Args:
            data_dict (dict): {name: DataFrame}
            output_dir (str): Directory to save CSV files
        """
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        for name, data in data_dict.items():
            filename = f"{output_dir}/{name.replace(' ', '_').lower()}.csv"
            data.to_csv(filename)
            print(f"Saved: {filename}")


if __name__ == '__main__':
    # Example usage
    fetcher = DataFetcher()
    
    # Fetch key commodities (10 years, exclude NG for testing)
    print("=" * 80)
    print("🦞 Fetching 10 years of commodity data...")
    print("=" * 80)
    
    data = fetcher.fetch_all_commodities(years=10, exclude=['NG=F'])
    
    print()
    print("=" * 80)
    print("Summary:")
    for name, df in data.items():
        print(f"{name}: {len(df)} trading days | {df.index[0].date()} to {df.index[-1].date()}")
    print("=" * 80)
