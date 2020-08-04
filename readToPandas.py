# This programs reads OHLCV data to pandas DataFrame and writes to csv file

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

import pandas as pd

class TestHistoricalData(EWrapper, EClient):
    ''' Serves as the Client and the Wrapper '''

    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.candle_data = [] #Initialize variable to store candle stick data

        # Connect to TWS API
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = threading.Thread(target=self.run)
        thread.start()

    @iswrapper
    def historicalData(self, reqId, bar):
        ''' Callback to reqHistoricalData '''
        #self.candle_data.clear()
        self.candle_data.append([bar.date, bar.open, bar.high, bar.low, bar.close]) # only the ohlcv data is of interest to us.

    @iswrapper    
    def error(self, reqId, code, msg):
        print('Error Code: {}'.format(code))
        print('Error Message: {}'.format(msg))

def main():
    # Create the client and Connect to TWS API
    client = TestHistoricalData('127.0.0.1', 7497, 7)
    time.sleep(3) #Sleep interval to allow time for connection to server

    # Define a Contract
    contract = Contract()
    contract.symbol = 'INFY'
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'INR' 
    contract.primaryExchange = "NSE"

    # Request Historical Bars
    now = datetime.datetime.now().strftime("%Y%m%d, %H:%M:%S")
    client.reqHistoricalData(4, contract, now, '2 D', '15 mins', 'BID', False, 1, False, [])
    time.sleep(5) # Allow enough time for data to be returned.
    df = pd.DataFrame(client.candle_data, columns=['DateTime', 'Open', 'High', 'Low', 'Close'])
    df['DateTime'] = pd.to_datetime(df['DateTime']) # Convert to pandas DateTime format
    print(df.tail())
    print(df.columns)
    df.to_csv('infy.csv')

    # Disconnect from TWS
    time.sleep(5)
    client.disconnect()

if __name__ == "__main__":
    main()