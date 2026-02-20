"""
Interactive Brokers Connector
Real-time data and order execution via IB API.
No yfinance rate limits. Direct access to your brokerage.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData
import threading
import time
import queue


class IBDataFetcher(EWrapper, EClient):
    """Connect to Interactive Brokers and fetch commodity data."""
    
    def __init__(self):
        EClient.__init__(self, self)
        self.data_queue = queue.Queue()
        self.contract_details = {}
        self.bar_data = {}
        self.next_order_id = None
    
    def error(self, reqId, errorCode, errorString):
        """Handle IB errors."""
        if errorCode != 2104:  # Ignore market data subscription status messages
            print(f"IB Error {errorCode}: {errorString}")
    
    def connectAndRun(self, host='127.0.0.1', port=7497, clientId=1):
        """
        Connect to IB TWS/Gateway.
        
        Args:
            host: Local IP (7497 for paper, 7496 for live)
            port: Port number
            clientId: Client ID (must be unique per connection)
        """
        self.connect(host, port, clientId)
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        time.sleep(1)  # Give connection time to establish
    
    def nextValidId(self, orderId):
        """Callback for next valid order ID."""
        self.next_order_id = orderId
    
    def contractDetails(self, reqId, contractDetails):
        """Callback for contract details."""
        self.contract_details[reqId] = contractDetails
    
    def historicalData(self, reqId, bar):
        """Callback for historical bar data."""
        if reqId not in self.bar_data:
            self.bar_data[reqId] = []
        
        self.bar_data[reqId].append({
            'date': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
        })
    
    def historicalDataEnd(self, reqId, start, end):
        """Called when historical data fetch is complete."""
        self.data_queue.put(('data_ready', reqId))
    
    def create_commodity_contract(self, symbol, currency='USD', exchange='NYMEX'):
        """
        Create an IB contract object for a commodity future.
        
        Args:
            symbol (str): Commodity symbol (CL, GC, ZS, etc.)
            currency (str): Currency
            exchange (str): Exchange (NYMEX, COMEX, CBOT, etc.)
        
        Returns:
            Contract: IB Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "FUT"
        contract.currency = currency
        contract.exchange = exchange
        contract.lastTradeDateOrContractMonth = ""  # Empty = front month
        return contract
    
    def get_historical_data(self, symbol, duration='252 D', bar_size='1 day', 
                           exchange='NYMEX', req_id=1):
        """
        Fetch historical data for a commodity.
        
        Args:
            symbol (str): Commodity symbol (CL, GC, etc.)
            duration (str): How far back (252 D = 1 year, 10 Y = 10 years)
            bar_size (str): Bar size (1 day, 1 hour, etc.)
            exchange (str): Exchange name
            req_id (int): Request ID
        
        Returns:
            pd.DataFrame: OHLCV data
        """
        contract = self.create_commodity_contract(symbol, exchange=exchange)
        
        self.reqHistoricalData(
            req_id,
            contract,
            datetime.now().strftime('%Y%m%d %H:%M:%S'),
            duration,
            bar_size,
            "TRADES",
            1,
            1,
            False,
            []
        )
        
        # Wait for data
        timeout = time.time() + 30
        while time.time() < timeout:
            try:
                event, returned_req_id = self.data_queue.get(timeout=1)
                if event == 'data_ready' and returned_req_id == req_id:
                    break
            except queue.Empty:
                pass
        
        # Convert to DataFrame
        if req_id in self.bar_data:
            df = pd.DataFrame(self.bar_data[req_id])
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
            df.set_index('date', inplace=True)
            return df
        
        return None


class IBOrderExecutor:
    """Execute orders through Interactive Brokers."""
    
    def __init__(self, ib_client):
        self.ib = ib_client
        self.order_status = {}
    
    def place_market_order(self, symbol, quantity, action='BUY', exchange='NYMEX'):
        """
        Place a market order for a commodity future.
        
        Args:
            symbol (str): Commodity symbol (CL, GC, etc.)
            quantity (int): Number of contracts
            action (str): BUY or SELL
            exchange (str): Exchange
        
        Returns:
            int: Order ID
        """
        if self.ib.next_order_id is None:
            print("Error: Order ID not available")
            return None
        
        order_id = self.ib.next_order_id
        self.ib.next_order_id += 1
        
        contract = self.ib.create_commodity_contract(symbol, exchange=exchange)
        
        order = {
            'action': action,
            'totalQuantity': quantity,
            'orderType': 'MKT',
        }
        
        # Convert dict to IB Order object (simplified)
        from ibapi.order import Order
        ib_order = Order()
        ib_order.action = action
        ib_order.totalQuantity = quantity
        ib_order.orderType = 'MKT'
        
        self.ib.placeOrder(order_id, contract, ib_order)
        
        return order_id
    
    def place_stop_order(self, symbol, quantity, stop_price, exchange='NYMEX'):
        """
        Place a stop-loss order.
        
        Args:
            symbol (str): Commodity symbol
            quantity (int): Number of contracts
            stop_price (float): Stop price
            exchange (str): Exchange
        
        Returns:
            int: Order ID
        """
        if self.ib.next_order_id is None:
            print("Error: Order ID not available")
            return None
        
        order_id = self.ib.next_order_id
        self.ib.next_order_id += 1
        
        contract = self.ib.create_commodity_contract(symbol, exchange=exchange)
        
        from ibapi.order import Order
        order = Order()
        order.action = 'SELL'
        order.totalQuantity = quantity
        order.orderType = 'STP'
        order.auxPrice = stop_price
        
        self.ib.placeOrder(order_id, contract, order)
        
        return order_id


class IBLiveDataStream:
    """Stream live market data from IB."""
    
    def __init__(self, ib_client):
        self.ib = ib_client
        self.subscriptions = {}
    
    def subscribe_to_market_data(self, symbol, req_id=100):
        """
        Subscribe to live market data for a commodity.
        
        Args:
            symbol (str): Commodity symbol
            req_id (int): Request ID for this subscription
        """
        contract = self.ib.create_commodity_contract(symbol)
        self.ib.reqMktData(req_id, contract, "", False, False, [])
        self.subscriptions[symbol] = req_id
        print(f"Subscribed to live data for {symbol}")
    
    def unsubscribe(self, symbol):
        """Unsubscribe from market data."""
        if symbol in self.subscriptions:
            req_id = self.subscriptions[symbol]
            self.ib.cancelMktData(req_id)
            del self.subscriptions[symbol]


if __name__ == '__main__':
    print("=" * 80)
    print("🦞 Interactive Brokers Data Connector")
    print("=" * 80)
    print("\nExample usage:")
    print("  fetcher = IBDataFetcher()")
    print("  fetcher.connectAndRun(host='127.0.0.1', port=7497)")
    print("  data = fetcher.get_historical_data('CL', duration='252 D')")
    print("  print(data)")
    print("\nNote: Requires IB TWS/Gateway running on localhost:7497 (paper) or 7496 (live)")
    print("=" * 80)
