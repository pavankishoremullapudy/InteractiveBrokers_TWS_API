# Program for handling messages from reqAccountSummary callback's in TWS API

# Imports used in the program
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper
from ibapi.client import Contract
from ibapi.order import Order
from ibapi.execution import ExecutionFilter

from threading import Thread, Event

import queue
import collections
import pandas as pd
import sys
import time
import datetime

class TestReq(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    def __init__(self, addr, port, client_id):
        EClient.__init__(self, self)

        # Initialize properties
        self.orderId = None
        self.accountsList = None
        self.max_wait_time = 5
        
        self.accountSummary_queue = queue.Queue() # initialize empty queue for accountSummary callbacks

        self.nextValidId_available = Event() # Initialize an Event object
        self.accountSummaryEnd_available = Event() # Initialize an Event object

        # Connect to TWS
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        
    @iswrapper
    def nextValidId(self, orderId):
        ''' Provides the next order ID '''
        self.orderId = orderId
        self.nextValidId_available.set() #internal flag is set to True
        print('Order Id from nextValidId callback is: {}'.format(orderId))

    @iswrapper
    def managedAccounts(self, accountsList):
        ''' Provides the Account Details. Can use to test if we are connected to TWS API '''
        self.accountsList = accountsList

    @iswrapper
    def accountSummary(self, reqId, account, tag, value, currency):
        ''' Callback to reqAccountSummary. Read Information about the Account '''
        if hasattr(self, "accountSummary_queue"):
            self.accountSummary_queue.put(
                {
                    "reqId": reqId,
                    "account": account,
                    "tag": tag,
                    "value": value,
                    "currency": currency,
                }
            )
                    
    @iswrapper
    def accountSummaryEnd(self, reqId: int):
        print("In Callback accountSummaryEnd. ReqId:", reqId)
        self.accountSummaryEnd_available.set() #internal flag is set to True

    @iswrapper    
    def error(self, req_id, code, msg):
        ''' Called if an error occurs '''
        print('Error Request Id: {}, Error Code: {}, Error Message: {}'.format(req_id, code, msg))

# Create the client and connect to TWS
client = TestReq('127.0.0.1', 7497, 0)
client.nextValidId_available.wait() # block thread until internal flag is set to True

client.reqAccountSummary( 9001, "All", "TotalCashValue, BuyingPower, AvailableFunds")

try:
    # Get data from queue (if it shows up) or eventually timeout
    acct_summary = client.accountSummary_queue.get(timeout=client.max_wait_time)
    # print ('printing Q contents {}'.format(acct_summary))
    print('Account {}: {} '.format(acct_summary['tag'], acct_summary['value']))
except queue.Empty:
    print("accountSummary_queue was empty until timeout period")
    acct_summary = None
else: # if try condition is successful with no execption, this 'else' follows
    # https://stackoverflow.com/questions/6517953/clear-all-items-from-the-queue
    while not client.accountSummary_queue.empty(): # If the queue is not empty
        try:
            acct_summary = client.accountSummary_queue.get(False)
            print('Account {}: {} '.format(acct_summary['tag'], acct_summary['value']))
        except queue.Empty:
            continue
        client.accountSummary_queue.task_done() # task_done() is used to inform task completion

del client.accountSummary_queue #Delete the queue

time.sleep(3)
client.disconnect()