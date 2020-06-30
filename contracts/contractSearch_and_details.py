# Imports for the Program
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.utils import iswrapper

import time
import threading

class TestContract(EWrapper, EClient):
    ''' Serves as the Client and the Wrapper '''

    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        # Connect to TWS API
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = threading.Thread(target=self.run)
        thread.start()
        
    @iswrapper
    def symbolSamples(self, reqId, contractDescriptions):
        # Print the symbols in the returned results
        print ('...')
        print('Number of Contract descriptions in List: {}'.format(len(contractDescriptions)))
        for contractDescription in contractDescriptions:
            print('Symbol: {}'.format(contractDescription.contract.symbol))
            print('Contract ID: {}'.format(contractDescription.contract.conId))
            print('Contract Security Type: {}'.format(contractDescription.contract.secType))
            print('Contract Primary Exchange: {}'.format(contractDescription.contract.primaryExchange))
            print('Contract Currency: {}'.format(contractDescription.contract.currency))

        # Select the first symbol
        self.symbol = contractDescriptions[0].contract.symbol

    @iswrapper
    def contractDetails(self, reqId, details):
        print('\nDetails of first symbol follows:')
        print('Market Name: {}'.format(details.marketName))
        print('Long Name: {}'.format(details.longName))
        print('Industry Classification: {}'.format(details.industry))
        print('Industry Category: {}'.format(details.category))
        print('Subcategory: {}'.format(details.subcategory))
        print('Contract ID: {}'.format(details.contract.conId))
        print('Time Zone for the Product: {}'.format(details.timeZoneId))
        print('Trading Hours of the Product: {}'.format(details.tradingHours))
        print('\nFull Contract Details, Unformatted:', reqId, " ", details)
        
    @iswrapper
    def contractDetailsEnd(self, reqId):
        print ('...The End of Contract Details...for reqId:{} '.format(reqId))

    def error(self, reqId, code, msg):
        print('Error Code: {}'.format(code))
        print('Error Message: {}'.format(msg))

def main():
    # Create the client and connect to TWS API
    client = TestContract('127.0.0.1', 7497, 700)
    time.sleep(3)

    # Request descriptions of contract starting with "goog"
    client.reqMatchingSymbols(0, 'goog')
    time.sleep(3)

    # Request details for the Stock
    contract = Contract ()
    contract.symbol = client.symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"

    client.reqContractDetails(1, contract)
    
    time.sleep(3)
    client.disconnect()
   
if __name__ == "__main__":
    main()