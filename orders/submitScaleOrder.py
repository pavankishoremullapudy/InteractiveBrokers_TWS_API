# -*- coding: utf-8 -*-
"""
Created on Mon Feb  8 22:21:43 2021

@author: pavan

This program shows how to handle Two Profit Takers and 1 Adjustable Stop packed as 1 Order

For a Stock, I want to place a Limit Order with 2 Profit Takers and 1 Stop Loss. If Profit Taker 1
is hit, then the stop loss should decrease by the commensurate amount (so after Profit Taker 1 is hit,
the stop loss quantity is now updated to be equal to Profit Taker 2 quantity). How to do this by
packing this entire set of operation's into 1 order?
"""

# Imports for the Program
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.utils import iswrapper

import time
import threading
import sys

class TestScaleOrder(EWrapper, EClient):
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
        print('\nThe order id is: {}'.format(orderId))

    @iswrapper
    def openOrder(self, orderId, contract, order, state):
        ''' Callback for the submitted order '''
        print('Order Status from openOrder callback: {}'.format(state.status))
        
    @iswrapper
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        ''' Check the status of the submitted order '''
        print('Status of Order: {}'.format(status))
        print('No. of Filled Positions: {}'.format(filled))
        print('No. of remaining positions: {}'.format(remaining))
        print('Why Held: {}'.format(whyHeld)) # value 'locate'used when trying to locate shares for short sell
        print('Average Fill Price: {}'.format(avgFillPrice))
        print('permId: {}'.format(permId))

    @iswrapper    
    def error(self, reqId, code, msg):
        print('Error Code: {}'.format(code))
        print('Error Message: {}'.format(msg))
    
    @staticmethod
    def ScaleBracketOrder(parentOrderId:int, action:str, quantity:float, limitPrice:float, takeProfitLimitPrice1:float, takeProfitLimitPrice2:float, stopLossPrice:float, triggerPrice:float, adjustedStopPrice:float):
        print('Bracket Order parentOrderId: {}'.format(parentOrderId))
        #This will be our main or "parent" order
        parent = Order()
        parent.orderId = parentOrderId
        parent.action = action
        parent.orderType = "LMT"
        parent.totalQuantity = quantity
        parent.lmtPrice = limitPrice
        #The parent and children orders will need this attribute set to False to prevent accidental executions.
        #The LAST CHILD will have it set to True, 
        parent.transmit = False

        takeProfit = Order()
        takeProfit.orderId = parent.orderId + 1
        takeProfit.action = "SELL" if action == "BUY" else "BUY"
        takeProfit.orderType = "LMT"
        takeProfit.totalQuantity = quantity
        takeProfit.lmtPrice = takeProfitLimitPrice1
        takeProfit.scaleInitLevelSize = 40 # Quantity for profit target 1
        takeProfit.scaleSubsLevelSize = 60 # Quantity for profit target 2
        takeProfit.scalePriceIncrement = (takeProfitLimitPrice2 - takeProfitLimitPrice1)
        takeProfit.parentId = parentOrderId
        takeProfit.transmit = False

        stopLoss = Order()
        stopLoss.orderId = parent.orderId + 2
        stopLoss.action = "SELL" if action == "BUY" else "BUY"
        stopLoss.orderType = "STP"
        #Stop trigger price
        stopLoss.auxPrice = stopLossPrice
        stopLoss.totalQuantity = quantity
        stopLoss.triggerPrice = triggerPrice
        stopLoss.adjustedStopPrice = adjustedStopPrice
        stopLoss.parentId = parentOrderId
        #In this case, the low side order will be the last child being sent. Therefore, it needs to set this attribute to True 
        #to activate all its predecessors
        stopLoss.transmit = True

        ScaleBracketOrder = [parent, takeProfit, stopLoss]
        return ScaleBracketOrder

def main():
    # Create the client and Connect to TWS API
    client = TestScaleOrder('127.0.0.1', 7497, 7)
    time.sleep(3)
    client.orderId = None

    # Define a Contract
    contract = Contract()
    contract.symbol = 'SBIN'
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'INR'
    contract.primaryExchange = "NSE"

    # Request from TWS, the next valid ID for the order
    client.reqIds(1)
    time.sleep(3)

    quantity = 100
    limitPrice = 100
    takeProfitLimitPrice1 = 105
    takeProfitLimitPrice2 = 110
    stopLossPrice = 95
    triggerPrice = 105 # Enter the first takeprofit Price here
    adjustedStopPrice = 96  # as soon as triggerprice is hit, this will be the new stoploss Price. 
                            # Can be retained as the original stopLossPrice

    # Place a Scale Bracket Order
    if client.orderId:
        bracket = TestScaleOrder.ScaleBracketOrder(client.orderId, "BUY", quantity, limitPrice, takeProfitLimitPrice1, takeProfitLimitPrice2, stopLossPrice, triggerPrice, adjustedStopPrice)
        for o in bracket:
            client.placeOrder(o.orderId, contract, o)
        time.sleep(3)
    else:
        print ('Order ID not received. Terminating Application')
        sys.exit()
    
    # Obtain information about open positions
    client.reqPositions()
    time.sleep(3)

    # Disconnect from TWS
    client.disconnect()
    
if __name__ == "__main__":
    main()