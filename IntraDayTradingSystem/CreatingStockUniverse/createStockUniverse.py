# -*- coding: utf-8 -*-
"""
Created on Tue Sep  1 14:10:27 2020

@author: pavan

This program reads the file titled "scraped_data.csv".
Calls the reqContractDetails to get the conId of each IB Symbol scrapped from web
Writes a CSV file with IB Symbol and conID
"""

# Imports for the Program
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.utils import iswrapper

import time
import threading
import pandas as pd
import collections

class CreateStockUniverse(EWrapper, EClient):
    ''' Class that Serves as the Client and the Wrapper '''

    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        
        # Initialization
        self.orderId = None
        self.accountsList = ""

        df_scraped_data = pd.read_csv('scraped_data.csv') # Read csv into pandas DataFrame
        self.df_scraped_data = df_scraped_data
        self.conId_dq = collections.deque() # Store Contract ID in deque

        # Connect to TWS API
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = threading.Thread(target=self.run)
        thread.start()

    @iswrapper
    def nextValidId(self, orderId):
        ''' Provides the next order ID '''
        self.orderId = orderId
        # print('\nThe order id is: {}'.format(orderId))

    @iswrapper
    def managedAccounts(self, accountsList):
        ''' Provides the Account Details. Needed to test if we are connected to TWS API '''
        self.accountsList = accountsList
        print ('Type is: {}'.format(type(self.accountsList)))
        # print('\nThe Account Details are: {}'.format(accountsList))

    @iswrapper
    def contractDetails(self, reqId, details):
        ''' Callback from '''
        print('Details from contractDetails Callback follows:')
        print('Market Name: {}'.format(details.marketName)) # symbol=TATACONSU, marketname=TATACONSUM
        print('Long Name: {}'.format(details.longName))
        print('Contract ID: {}'.format(details.contract.conId))
        # print('Time Zone for the Product: {}'.format(details.timeZoneId))
        # print(details)
        self.conId_dq.append(details.contract.conId)
        
    @iswrapper
    def contractDetailsEnd(self, reqId):
        print ('...The End of Contract Details...for reqId:{} \n'.format(reqId))
        
    @iswrapper    
    def error(self, reqId, code, msg):
        print('\nError Code: {}'.format(code))
        print('Error Message: {}'.format(msg))
        
def main():
    
    count = 1
    while count < 10:
        # Create the client and Connect to TWS API
        client = CreateStockUniverse('127.0.0.1', 7497, 7)
        time.sleep(count)

        # Request from TWS, the next valid ID for the order
        client.reqIds(1) # In the callback, if we receive an Order Id, then we are connected.
        time.sleep(count)
        if client.orderId is None:
            print ('\n   Connection to TWS Not established. No of attempts = {}'.format(count))
            count += 1
        else:
            print ('\n   Good to Go. We are Connected to TWS')
            print ('   The Order Id is: {}'.format(client.orderId))
            print ('   The Account Details are: {}'.format(client.accountsList))
            break
    
    for ib_symbol in client.df_scraped_data['IB Symbol']:
        print('IB Symbol from Scraped Data CSV file: {}'.format(ib_symbol))
        
        contract = Contract ()
        contract.symbol = ib_symbol
        contract.secType = "STK"
        contract.primaryExchange = "NSE"
        contract.currency = "INR"
    
        client.reqContractDetails(1, contract)
        time.sleep(3) #Sleep interval to allow time for connection to server
        
    conId_list = list(client.conId_dq)
    df_conId = pd.DataFrame([conId_list]).transpose()
    df_conId.columns = ['Contract ID']
    df_final = client.df_scraped_data.join(df_conId)
    print ('Length of df_scraped_data: {}'.format(len(client.df_scraped_data)))
    print ('Length of df_conId: {}'.format(len(df_conId)))
    print('Length of df_final: {}'.format(len(df_final)))
    df_final.to_csv('stock_universe_web_scraping'+ '.csv', encoding='utf-8', index=False)
    
    # Disconnect from TWS
    time.sleep(3)
    client.disconnect()
   
if __name__ == "__main__":
    main()