# Imports for the Program
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.utils import iswrapper

import time
import threading
import datetime
import sys

class TestTradingData(EWrapper, EClient):
    ''' Serves as the Client and the Wrapper '''

    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.orderId = None

        # Connect to TWS API
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = threading.Thread(target=self.run)
        thread.start()

    @iswrapper
    def nextValidId(self, orderId):
        ''' Provides the next order ID '''
        self.orderId = orderId
        # print('NextValidId: '.format(orderId))
        print('The order id is: {}'.format(orderId))

    @iswrapper
    def tickByTickMidPoint(self, reqId, tick_timestamp, midpoint):
        ''' Callback to reqTickByTickData '''
        print('\nThe Midpoint Tick from tickByTickMidPoint Callback: {}'.format(midpoint))
        print('The timestamp of the realtime tick from tickByTickMidPoint: {}'.format(tick_timestamp))

    @iswrapper
    def tickPrice(self, reqId, field, price, attribs):
        ''' Callback to reqMktData '''
        print('\nFrom tickPrice callback - field is: {}'.format(field))
        if field == 1:
            self.bid_price = price
            print('Bid Price is: {}'.format(price))
        elif field == 2:
            self.ask_price = price
            print('Ask Price is: {}'.format(price))
        elif field == 4:
            self.last_price = price # Last price at which the contract traded
            print('Last Price is: {}'.format(price))
        elif field == 6:
            self.high_price = price
            print ('High Price is: {}'.format(price))
        elif field == 7:
            self.low_price = price
            print ('Low Price is: {}'.format(price))
        elif field == 9:
            self.close_price = price # The last available closing price for the previous day
            print('Close Price is: {}'.format(price))
        elif (field == 66 or field == 67 or field == 68 or field == 72 or field == 73 or field == 75): # Codes used for delayed data
            self.delayed_price = price 
            print('Delayed  Price is: {}'.format(price))       
    
    @iswrapper
    def tickSize(self, reqId, field, size):
        ''' Callback to reqMktData '''
        print('\nFrom tickSize callback - Requested field is: {} and Size is: {}'.format(field, size))

    @iswrapper
    def realtimeBar(self, reqId, bar_timestamp, open, high, low, close, volume, WAP, count):
        ''' Callback to reqMktData '''
        print('\nOpen: {}, High: {}, Low: {}, Close: {} at Bar Timestamp: {}'.format(open, high, low, close, bar_timestamp))

    @iswrapper
    def historicalData(self, reqId, bar):
        ''' Callback to reqHistoricalData '''
        print('\nOpen: {}, High: {}, Low: {}, Close: {}'.format(bar.open, bar.high, bar.low, bar.close))

    @iswrapper    
    def error(self, reqId, code, msg):
        print('Error Code: {}'.format(code))
        print('Error Message: {}'.format(msg))

def main():
    # Create the client and Connect to TWS API
    client = TestTradingData('127.0.0.1', 7497, 7)
    time.sleep(3) #Sleep interval to allow time for connection to server
    client.orderId = None

    # Define a Contract
    # contract = Contract()
    # contract.symbol = 'AAPL'
    # contract.secType = 'STK'
    # contract.exchange = 'SMART'
    # contract.currency = 'USD'
    contract = Contract()
    contract.symbol = 'INFY'
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'INR' 
    contract.primaryExchange = "NSE"

    # Define 10 Ticks containing midpoint data
    client.reqTickByTickData(1, contract, 'MidPoint', 10, True)

    # Request market data
    client.reqMarketDataType(4) # Switch to live (1) frozen (2) delayed (3) delayed frozen (4).
    client.reqMktData(2, contract, '', False, False, [])

    # Request Real Time Bars of 5 seconds
    client.reqRealTimeBars(3, contract, 5, 'MIDPOINT', False, [])

    # Request Historical Bars
    now = datetime.datetime.now().strftime("%Y%m%d, %H:%M:%S")
    client.reqHistoricalData(4, contract, now, '2 D', '5 mins', 'BID', False, 1, False, [])

    # Disconnect from TWS
    time.sleep(10)
    client.disconnect()
    
if __name__ == "__main__":
    main()