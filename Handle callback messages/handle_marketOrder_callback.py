# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 22:32:41 2020

@author: pavan
"""
# Imports used in the program

import pandas as pd
import collections
import datetime
import time
import sys
import queue
import pdb

from threading import Thread, Event
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.client import Contract
from ibapi.order import Order
from ibapi.utils import iswrapper

class TestApp(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    def __init__(self, addr, port, client_id):
        EClient.__init__(self, self)
              
        # Initialize properties
        self.orderId = None

        self.orderStatus_queue = queue.Queue() # initialize empty queue for holding callbacks

        self.nextValidId_available = Event() # Initialize an Event object
        self.execDetailsEnd_available = Event() # Initialize an Event object
        self.execDetails_available = Event() # instead of of execDetailsEnd_available for market orders

        self.order_executed = False # taken from from execDetails callback
        self.order_submitted = False # taken from from orderStatus callback

        # Connect to TWS
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()

    @iswrapper
    def nextValidId(self, orderId):
        ''' Provides the next order ID '''
        # Callback Trigerred after 1) successfully connecting to the TWS 2) after invoking the IBApi.EClient.reqIds method.
        self.orderId = orderId
        self.nextValidId_available.set() #internal flag is set to True
        print('Order Id from nextValidId callback is: {}'.format(orderId))      

    # Active orders will be delivered via 2 callback's - The openOrder callback and The orderStatus callback. 
    @iswrapper
    def openOrder(self, orderId, contract, order, state):
        ''' Callback for the submitted order '''
        pass
                
    @iswrapper
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        ''' Callback for the submitted order. '''

        if hasattr(self, "orderStatus_queue"):
            self.orderStatus_queue.put(
                {
                    "status": status,
                    "filled": filled,
                    "remaining": remaining,
                    "avgFillPrice": avgFillPrice,
                    "orderId": orderId,
                    "permId": permId,
                }
            )
    # No need to call reqExecutions to get notified of future executions. 
    # They come automatically, in the same way as openOrder and orderStatus notifications.
    @iswrapper
    def execDetails(self, reqId, contract, execution):
        print("\nIn callback ExecDetails. ReqId: {}, Symbol: {}, SecType: {}, Currency: {}, Execution: {}".format(reqId, contract.localSymbol, contract.secType, contract.currency, execution))
        self.order_executed = True
        self.execDetails_available.set() #internal flag is set to True        
    
    @iswrapper
    def execDetailsEnd(self, reqId):
        # Triggered Only if you request all executions via reqExecutions
        # Triggered after all execDetails are received.
        print("In callback execDetailsEnd. ReqId: {}".format(reqId))
        self.execDetailsEnd_available.set() #internal flag is set to True

    @iswrapper    
    def error(self, req_id, code, msg):
        ''' Called if an error occurs '''
        print('Error Request Id: {}, Error Code: {}, Error Message: {}'.format(req_id, code, msg))

print ("Main Program Starts Here. Going to Connect to TWS")
# Create the client and connect to TWS
client = TestApp('127.0.0.1', 7497, 4)
client.nextValidId_available.wait() # block thread until internal flag is set to True

# Define the futures Contract prior to placing order
contract = Contract()
contract.symbol = 'TCS'
contract.secType = 'STK'
contract.exchange = 'NSE'
contract.currency = 'INR'
contract.primaryExchange = "NSE"

# Define the Order
order = Order()
order.action = 'BUY'
order.totalQuantity = 4
order.orderType = 'MKT'

# Request from TWS, the next valid ID for the order. 
client.nextValidId_available.clear() # internal flag is set to False
client.reqIds(-1) # The parameter is always ignored.
client.nextValidId_available.wait() # block thread until internal flag is set to True

# Place the order
if client.orderId:
    # for Market Orders use execDetails callback instead of orderStatus
    # execDetails callback event is trigerred ONLY if the order placed is Executed
    
    client.execDetails_available.clear() # internal flag is set to False
    client.placeOrder(client.orderId, contract, order)
    try:
        client.execDetails_available.wait(timeout=5)
    except:
        print ('    *** An Exception Occured in client.execDetails_available.wait(timeout=5) ***    ')
    else: # if try condition is successful with no execption, this 'else' follows
        if client.order_executed == True:
            print ("    *** \nOrder is Successfully Executed ***\n")
        else:
            print ('    *** \nOrder NOT Executed ***\n')
        # Handle the q from orderStatus callback
        # https://stackoverflow.com/questions/6517953/clear-all-items-from-the-queue
        while not client.orderStatus_queue.empty(): # If the queue is not empty
            try:
                order_status = client.orderStatus_queue.get(False)
                print('Status of Order: {}, Filled Positions: {}, Remaining Positions: {}, OrderId: {}, permId: {}'.format(order_status['status'], order_status['filled'], order_status['remaining'], order_status['orderId'], order_status['permId']))
                if client.orderId == order_status['orderId']: # for the latest orderId
                    if (order_status['status'] == 'Submitted') or (order_status['status'] == 'Filled'): # if for even 1 message, the status is filled or submitted
                        client.order_submitted = True                                     
            except queue.Empty:
                continue
            client.orderStatus_queue.task_done() # task_done() is used to inform task completion
        del client.orderStatus_queue # Delete the queue
else:
    print ('Order ID not received. Terminating Application')
    client.disconnect()
    sys.exit()

print('Feedback from orderStatus callback - Is order submitted?: {}'.format(client.order_submitted))
print('Feedback form execDetails callback - Is order executed? : {}'.format(client.order_executed))
print ("Going to Disconnect from TWS")
time.sleep(1)
client.disconnect()