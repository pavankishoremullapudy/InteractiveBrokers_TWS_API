''' Read historical data, store OHLCV in Pandas Dataframe and write to CSV '''
# Imports used in the program
from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper

from datetime import datetime
from threading import Thread
import time
import pandas as pd

class ReadTicker(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    def __init__(self, addr, port, client_id):
        EClient.__init__(self, self)

        # Initialize properties
        self.symbols = {'ACC':'44652144','INFY':'44652017', 'WIPRO':'44652030'} # IB symbol could be different from NSE symbol, so use conId
        self.candle_dict = {}

        # Connect to TWS
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()

    @iswrapper
    def historicalData(self, req_id, bar):
        ''' Callback to reqHistoricalData '''

        # Add the stock prices to the dictionary
        self.candle_dict['Date'].append(bar.date)
        self.candle_dict['Open'].append(bar.open)
        self.candle_dict['High'].append(bar.high)
        self.candle_dict['Low'].append(bar.low)
        self.candle_dict['Close'].append(bar.close)
        self.candle_dict['Volume'].append(bar.volume) # whatToShow in reqHistoricalData should be TRADES

    def error(self, req_id, code, msg):
        print('Error Code: {} & Error Message: {}'.format(code, msg))

def main():

    # Create the client and connect to TWS
    client = ReadTicker('127.0.0.1', 7497, 7)
    time.sleep(3) #Sleep interval to allow time for connection to server

    # Get expiration dates for contracts
    for symbol in client.symbols:

        # Define the contract
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'NSE'
        contract.conId = client.symbols[symbol]
        print('Contract ID for symbol {} is: {}'.format(symbol, client.symbols[symbol])) #take conId from IB website or contractDetails callback
        contract.currency = "INR"

        # Initialize the candle data dictionary
        for v in ['Date','Open', 'High', 'Low', 'Close', 'Volume']:
            client.candle_dict[v] = []

        # Request OHLCV data
        now = datetime.now().strftime("%Y%m%d %H:%M:%S")
        client.reqHistoricalData(4, contract, now, '2 D', '5 mins', 'TRADES', False, 1, False, [])
        time.sleep(3) # Allow enough time for data to be returned.
        df = pd.DataFrame(client.candle_dict, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['Date'] = pd.to_datetime(df['Date']) # Convert to pandas DateTime format
        # print(df.tail())
        # print(df.columns)                
        df.to_csv(symbol + '.csv', encoding='utf-8', index=False)
        client.candle_dict.clear()

    # Disconnect from TWS
    client.disconnect()

if __name__ == '__main__':
    main()