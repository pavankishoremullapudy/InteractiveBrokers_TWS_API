''' Reads continuous futures contracts '''

from datetime import datetime, timedelta
from threading import Thread
import time
import pandas as pd
import sys
import collections

from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper

import pdb

class WriteFuturesToCSV(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    def __init__(self, addr, port, client_id):
        EClient.__init__(self, self)

        # Initialize properties
        self.orderId = None
        self.accountsList = ""        
        self.local_symbol = None
        self.multiplier = None
        self.symbols = {'BANKNIFTY':'NSE','NIFTY50':'NSE'}
        # self.symbols = {'TCS':'NSE','SBIN':'NSE'}
        self.reached_historicalDataEnd_callback = False # when historicalDataEnd callback is complete, this is set to True
        # This flag is Used to set the wait time . Easier alternative to Event loop of threading
        self.reached_contractDetailsEnd_callback = False
        
        self.date_dq = collections.deque() # Store Contract ID in deque
        self.open_dq = collections.deque() # Store Contract ID in deque
        self.high_dq = collections.deque() # Store Contract ID in deque
        self.low_dq = collections.deque() # Store Contract ID in deque
        self.close_dq = collections.deque() # Store Contract ID in deque
        self.volume_dq = collections.deque() # Store Contract ID in deque

        # Connect to TWS
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        
    @iswrapper
    def nextValidId(self, orderId):
        ''' Provides the next order ID '''
        self.orderId = orderId

    @iswrapper
    def managedAccounts(self, accountsList):
        ''' Provides the Account Details. Needed to test if we are connected to TWS API '''
        self.accountsList = accountsList

    @iswrapper
    def contractDetails(self, req_id, details):
        ''' Called in response to reqContractDetails '''
        # Obtain data for the contract
        self.local_symbol = details.contract.localSymbol
        self.multiplier = details.contract.multiplier
        self.conId = details.contract.conId
        print ('From contractDetails Callback for local symbol {} , Multiplier is : {} Contract ID is : {}'.format(self.local_symbol, self.multiplier, self.conId))
        
    @iswrapper    
    def contractDetailsEnd(self, req_id: int):
        '''After all contracts matching the request were returned, this method will mark the end of their reception.'''
        # print("ContractDetailsEnd. ReqId:", req_id)
        self.reached_contractDetailsEnd_callback = True
            
    @iswrapper
    def historicalData(self, req_id, bar):
        ''' Called in response to reqHistoricalData '''
        # Add the futures prices to the deque        
        self.date_dq.append(bar.date)
        self.open_dq.append(bar.open)
        self.high_dq.append(bar.high)
        self.low_dq.append(bar.low)
        self.close_dq.append(bar.close)
        self.volume_dq.append(bar.volume)
        
    @iswrapper
    def historicalDataEnd(self, req_id: int, start: str, end: str):
        '''Marks the ending of the historical bars reception.'''
        # print('HistoricalDataEnd. ReqId: {} from {} to {}'.format( req_id, start, end))
        self.reached_historicalDataEnd_callback = True
        
    def error(self, req_id, code, msg):
        print('Error {}: {}'.format(code, msg))

def main():
    # Create the client and connect to TWS
    client = WriteFuturesToCSV('127.0.0.1', 7497, 0)
    while client.orderId is None:
        time.sleep(0.1)
        
    print ('\n   Good to Go. We are Connected to TWS')
    print ('   The Order Id is: {}'.format(client.orderId))
    print ('   The Account Details are: {}'.format(client.accountsList))

    # Get expiration dates for contracts
    for count, symbol in enumerate(client.symbols):
        # Define contract of interest
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CONTFUT"
        contract.exchange = client.symbols[symbol]
        contract.primaryExchange = client.symbols[symbol]
        contract.currency = "INR"
        contract.includeExpired = True
        
        client.reqContractDetails(count, contract)
        while (client.reached_contractDetailsEnd_callback is False):
            time.sleep(0.1)
        if client.reached_contractDetailsEnd_callback is True:
            client.reached_contractDetailsEnd_callback = False

        # Request historical data for each contract
        if client.local_symbol:
            print ('Local Symbol : {}'.format(client.local_symbol))
            print ('Symbol : {}'.format(symbol))
            
            # Initialize the deque
            client.date_dq.clear()
            client.open_dq.clear()
            client.high_dq.clear()
            client.low_dq.clear()
            client.close_dq.clear()
            client.volume_dq.clear()

            # Set additional contract data
            contract.localSymbol = client.local_symbol
            contract.multiplier = client.multiplier

            # Request historical data
            query_time = (datetime.today().date() - timedelta(days=0)).strftime("%Y%m%d %H:%M:%S")
            client.reqHistoricalData(count, contract, query_time,
                '2 Y', '1 day', 'TRADES', 1, 1, False, [])            
            
            while (client.reached_historicalDataEnd_callback is False):
                time.sleep(0.1)
            if client.reached_historicalDataEnd_callback is True:
                client.reached_historicalDataEnd_callback = False
            
            #  Create List
            date_list = list(client.date_dq)
            open_list = list(client.open_dq)
            high_list = list(client.high_dq)
            low_list = list(client.low_dq)
            close_list = list(client.close_dq)
            volume_list = list(client.volume_dq)
            combined_list = [date_list, open_list, high_list, low_list, close_list, volume_list]
            
            # Write data to a CSV file
            df = pd.DataFrame(combined_list).transpose()
            df.columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            df['Date'] = pd.to_datetime(df['Date']) # Convert to pandas DateTime format
            df = df[df.Volume != 0] # Drop all entries for which volume traded = 0
            # print(df.tail())
            # print(df.columns)
            df.to_csv(client.local_symbol + '.csv', encoding='utf-8', index=False)
            
            combined_list = [] # Empty the List
        else:
            print('Could not access contract data')
            sys.exit()

    # Disconnect from TWS. But first cancel open Subscriptions
    client.cancelHistoricalData(count) # provide ID of original request
    client.disconnect()

if __name__ == '__main__':
    # pdb.set_trace()
    main()