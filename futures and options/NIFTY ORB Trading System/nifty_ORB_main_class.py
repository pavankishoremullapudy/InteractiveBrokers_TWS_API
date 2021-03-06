# -*- coding: utf-8 -*-
"""
Created on Sat Sep 12 17:38:25 2020

@author: pavan

This program defines the Main Class and its callback mthods for Nifty_ORB_Algo.py program

"""
# Imports used in the program

import pandas as pd
import collections
import queue
import datetime
import sys, os

from threading import Thread, Event

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper
from ibapi.order import Order

# import pdb

class NiftyORB(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    def __init__(self, addr, port, client_id):
        EClient.__init__(self, self)

        # Initialize properties
        self.orderId = None
        self.accountsList = None       
        self.local_symbol = None
        self.multiplier = None
        self.daily_data_requested = None # use this flag for naming of the CSV file
        self.time_difference = None
        self.position_dict = {} # for storing the open positions from the position callback
        self.openOrder_dict = {} # for storing the open Orders from the openOrder callback
        self.orders_list = [] # store the Order IDs of parent and stopLoss
        
        self.date_dq = collections.deque() # to Store date in deque
        self.open_dq = collections.deque() # to Store open in deque
        self.high_dq = collections.deque() # to Store high in deque
        self.low_dq = collections.deque() # to Store low in deque
        self.close_dq = collections.deque() # to Store close in deque
        self.volume_dq = collections.deque() # to Store volume in deque
        
        self.orderStatus_queue = queue.Queue() # initialize empty queue for holding callback orderStatus

        self.nextValidId_available = Event() # Initialize an Event object
        self.execDetails_available = Event() # use after call to placeOrder. The outlier
        self.contractDetailsEnd_available = Event() # Initialize an Event object
        self.historicalDataEnd_available = Event() # Initialize an Event object
        self.currentTime_available = Event() # Initialize an Event object
        self.accountSummaryEnd_available = Event() # Initialize an Event object
        self.execDetailsEnd_available = Event() # Initialize an Event object
        self.positionEnd_available = Event() # Initialize an Event object
        self.completedOrdersEnd_available = Event() # Initialize an Event object
        self.openOrderEnd_available = Event() # Initialize an Event object

        self.order_executed = False # use to check execDetails callback
        self.order_submitted = False # use to cjeck orderStatus callback
        
        # self.symbols = {'BANKNIFTY':'NSE','NIFTY50':'NSE'}

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
        print('\nAccount: {} & {}: {}'.format(account, tag, value))
        # print('The currency on which the Value is expressed: {}'.format(currency))
        
    @iswrapper
    def accountSummaryEnd(self, reqId: int):
        # print("In Callback accountSummaryEnd. ReqId:", reqId)
        self.accountSummaryEnd_available.set() #internal flag is set to True

    @iswrapper
    def contractDetails(self, req_id, details):
        ''' Called in response to reqContractDetails '''
        # Obtain data for the contract
        self.local_symbol = details.contract.localSymbol
        self.multiplier = details.contract.multiplier
        self.conId = details.contract.conId
        # print ('From contractDetails Callback for local symbol {} , Multiplier is : {} Contract ID is : {}'.format(self.local_symbol, self.multiplier, self.conId))
        
    @iswrapper    
    def contractDetailsEnd(self, req_id: int):
        '''After all contracts matching the request were returned, this method will mark the end of their reception.'''
        # print("ContractDetailsEnd. ReqId:", req_id)
        self.contractDetailsEnd_available.set() #internal flag is set to True
            
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
        # print('Date: {}, Open: {}, High: {}, Low: {}, Close: {}'.format(bar.date, bar.open, bar.high, bar.low, bar.close))
                
    @iswrapper
    def historicalDataEnd(self, req_id: int, start: str, end: str):
        '''Marks the ending of the historical bars reception.'''
        print('HistoricalDataEnd. ReqId: {} from {} to {}'.format( req_id, start, end))      
        # Create List
        combined_list = [list(self.date_dq), list(self.open_dq), list(self.high_dq), list(self.low_dq), list(self.close_dq), list(self.volume_dq)]
        
        # Write data to a CSV file
        df = pd.DataFrame(combined_list).transpose()
        df.columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df['Date'] = pd.to_datetime(df['Date']) # Convert to pandas DateTime format
        df = df[df.Volume != 0] # Drop all entries for which volume traded = 0

        if self.daily_data_requested is True:
            file_name = self.local_symbol + '_Daily' + '.csv'
        else:
            file_name = self.local_symbol + '_Minute' + '.csv'

        df.to_csv(file_name, encoding='utf-8', index=False)
       
        # Clear the deque and list
        for v in [self.date_dq, self.open_dq, self.high_dq, self.low_dq, self.close_dq, self.volume_dq]:
            v.clear()
        combined_list = []
        
        self.historicalDataEnd_available.set() #internal flag is set to True
    
    @iswrapper
    def currentTime(self, curr_time):
        '''Callback to get current time from IB servers'''
        # t1 = datetime.datetime.fromtimestamp(curr_time) # IB Time
        # print('Current time from IB server: {}'.format(t1))
        # # t2 = datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")
        # t2 = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S') # Current time from system
        # print ('Current time from OS: {}'.format(t2))
        # if t2 > t1: # If the current time from system is ahead of IB time
        #     self.time_difference = (t2-t1).seconds
        # else:
        #     self.time_difference = 0
        self.currentTime_available.set() #internal flag is set to True

    # Active orders will be delivered via 2 callback's - The openOrder callback and The orderStatus callback. 
    @iswrapper
    def openOrder(self, orderId, contract, order, state):
        ''' Callback for the submitted order '''
        # print('\nOrder Status: {}'.format(state.status))
        # print ('Order Id: {}'.format(orderId))
        # print('Current initial margin: {}'.format(state.initMarginBefore))
        # print('Current maintanence margin: {}'.format(state.maintMarginBefore))
        # print('Comission Charged: {}'.format(state.commission))
        # print('Completed Time: {}'.format(state.completedTime))
        # print('Warning Text: {}'.format(state.warningText))
        self.openOrder_dict.update({contract.localSymbol:orderId})
                
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
            
    # When all orders have been sent to the client application you will receive a IBApi.EWrapper.openOrderEnd event:
    @iswrapper
    def openOrderEnd(self):
        print("In Callback OpenOrderEnd")
        self.openOrderEnd_available.set() #internal flag is set to True
    
    # No need to call reqExecutions to get notified of future executions. 
    # They come automatically, in the same way as openOrder and orderStatus notifications.
    @iswrapper
    def execDetails(self, reqId, contract, execution):
        print("\nIn callback ExecDetails. ReqId: {}, Symbol: {}, SecType: {}, Currency: {}, Execution: {}".format(reqId, contract.localSymbol, contract.secType, contract.currency, execution))
        self.order_executed = True
        self.execDetails_available.set() #internal flag is set to True
    
    @iswrapper
    def execDetailsEnd(self, reqId):
        print("In callback execDetailsEnd. ReqId: {}".format(reqId))
        self.execDetailsEnd_available.set() #internal flag is set to True

    @iswrapper
    def position(self, account, contract, pos, avgCost):
        ''' Provides the portfolios Open Positions'''
        # if contract.symbol == "NIFTY50":
        print('\nNo. of Positions held in {} : {} '.format(contract.localSymbol, pos))
        print('The average purchase/selling price: {}'.format(avgCost))
        self.position_dict.update({contract.localSymbol:pos})

    @iswrapper
    def positionEnd(self):
        '''Callback positionEnd is trigerred after completing callback position'''
        print("positionEnd")
        self.positionEnd_available.set() #internal flag is set to True
    
    # List of all completed Orders since Midnight
    @iswrapper
    def completedOrder(self, contract, order, state):
        ''' Callback to reqCompletedOrders '''
        # print ("In callback completedOrder.")
        print('For Symbol: {}, the Order Status is: {}, Completed Time is: {}'.format(contract.localSymbol, state.status, state.completedTime ))
    
    @iswrapper
    def completedOrdersEnd(self):
        ''' Notifies the end of the completed orders' reception '''
        print ("completedOrdersEnd")
        self.completedOrdersEnd_available.set() #internal flag is set to True

    @iswrapper    
    def error(self, req_id, code, msg):
        ''' Called if an error occurs '''
        if code == 2104:
            print('Connections Info - Market data farm connection is OK:hfarm')
        elif code == 2106:
            print('Connections Info - HMDS data farm connection is OK:hkhmds')
        elif code == 2158:
            print('Connections Info - Sec-def data farm connection is OK:secdefhk')
        elif code == 2137:
            print('Request Id: {}, Code: {}, Message: {}'.format(req_id, code, msg))
        elif code == 206:
            print('Order Canceled')
        elif code == 399: # When order is placed after Exchange Timings.
            print('Warning Request Id: {}, Warning Code: {}, Warning Message: {}'.format(req_id, code, msg))
        elif code == 201: # Order Rejected
            print('Error Request Id: {}, Error Code: {}, Error Message: {}'.format(req_id, code, msg))
            self.disconnect()
            os._exit(9999999) # This will exit the entire process without any cleanup
            # https://stackoverflow.com/questions/905189/why-does-sys-exit-not-exit-when-called-inside-a-thread-in-python
        elif code == 202: # Order Cancelled
            print('Message Request Id: {}, Code: {}, Message: {}'.format(req_id, code, msg))
        else:
            print('Error Request Id: {}, Error Code: {}, Error Message: {}'.format(req_id, code, msg))

    @staticmethod
    def OrderWithStop(parentOrderId:int, action:str, quantity:int, stopLossPrice:float):
        print('parentOrderId: {}'.format(parentOrderId))
        #This will be our main or "parent" order
        parent = Order()
        parent.orderId = parentOrderId
        parent.action = action
        parent.orderType = "MKT"
        parent.totalQuantity = quantity
        #The parent and children orders will need this attribute set to False to prevent accidental executions.
        #The LAST CHILD will have it set to True, 
        parent.transmit = False

        stopLoss = Order()
        stopLoss.orderId = parent.orderId + 1
        stopLoss.action = "SELL" if action == "BUY" else "BUY"
        stopLoss.orderType = "STP"
        #Stop trigger price
        stopLoss.auxPrice = stopLossPrice
        stopLoss.totalQuantity = quantity
        stopLoss.parentId = parentOrderId
        #In this case, the low side order will be the last child being sent. Therefore, it needs to set this attribute to True 
        #to activate all its predecessors
        stopLoss.transmit = True

        ordersList = [parent, stopLoss]
        return ordersList