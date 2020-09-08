# This program uses the IB scanner and tries to pick all NSE stocks that are optionable

# Imports for the Program
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper

from ibapi.scanner import ScannerSubscription
from ibapi.tag_value import TagValue

import time
import threading
import datetime
import sys

import pandas as pd

class StockScanner(EWrapper, EClient):
    ''' Serves as the Client and the Wrapper '''

    def __init__(self, addr, port, client_id):
        EClient. __init__(self, self)

        # Connect to TWS API
        self.connect(addr, port, client_id)
        self.count = 0

        # Launch the client thread
        thread = threading.Thread(target=self.run)
        thread.start()
    
    @iswrapper
    def scannerData(self, reqId, rank, details, distance, benchmark, projection, legsStr):

        # Print the symbols in the returned results
        print('{}: {} : {}'.format(rank, details.contract.symbol, details.contract.secType))
        self.count += 1
        
    @iswrapper
    def scannerDataEnd(self, reqId):
    
        # Print the number of results
        print('Number of results: {}'.format(self.count))
    
    def error(self, reqId, code, msg):
        print('Error {}: {}'.format(code, msg))

def main():

    # Create the client and connect to TWS
    client = StockScanner('127.0.0.1', 7497, 7)
    time.sleep(3)
    
    '''
    Note 1: Retreiving directly the stocks and using filter "hasOptionsIs" dont seem to work.
    Note 2: A beter approach is to retreive the Single Stock Futures.

    # Create the object ScannerSubscription
    scanSub = ScannerSubscription() # Defines a market scanner request
    scanSub.instrument = 'STOCK.HK'
    scanSub.locationCode = 'STK.HK.NSE'
    scanSub.scanCode = 'ALL_SYMBOLS_ASC'
    
    # Set additional filter criteria
    tagvalues = []
    tagvalues.append(TagValue('avgOptVolumeAbove', '0')) # This dont seem to give desired result !!!
    tagvalues.append(TagValue('hasOptionsIs', 'Yes')) # This dont seem to give desired result !!!
    print(tagvalues)

    # Request the scanner subscription
    client.reqScannerSubscription(7, scanSub, [], tagvalues)
    '''

    # Create the object ScannerSubscription
    scanSub = ScannerSubscription() # Defines a market scanner request
    scanSub.instrument = 'SSF.HK' # SSF = Single Stock Futures
    scanSub.locationCode = 'SSF.HK.NSE'
    scanSub.scanCode = 'ALL_SYMBOLS_ASC'
    scanSub.abovePrice = '500'

    # Request the scanner subscription
    client.reqScannerSubscription(7, scanSub, [], [])
    # Disconnect from TWS
    time.sleep(5)
    client.disconnect()

if __name__ == '__main__':
    main()