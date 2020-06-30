# Imports for the Program
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.utils import iswrapper

import time
import threading
import sys

class TestBracketOrder(EWrapper, EClient):
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
        print('Order Status: {}'.format(state.status))
        print('The accounts current initail margin: {}'.format(state.initMarginBefore))
        print('The accounts current maintanence margin: {}'.format(state.maintMarginBefore))
        print('Comission Charged: {}'.format(state.commission))
        print('Completed Time: {}'.format(state.completedTime))
        print('Warning Text: {}'.format(state.warningText))
        
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
    def position(self, account, contract, pos, avgCost):
        ''' Provides the portfolios Open Positions'''
        print('No. of Positions held in {} : {} '.format(contract.symbol, pos))
        print('The average cost of the position: {}'.format(avgCost))

    @iswrapper
    def accountSummary(self, reqId, account, tag, value, currency):
        '''Read Information about the Account'''
        print('Account {} : {} = {}'.format(account, tag, value))
        print('The currency on which the Value is expressed: {}'.format(currency))

    @iswrapper    
    def error(self, reqId, code, msg):
        print('Error Code: {}'.format(code))
        print('Error Message: {}'.format(msg))

    @iswrapper
    def execDetails(self, req_id, contract, execution):
        print('Order Executed: ', req_id, contract.symbol, contract.secType, contract.currency, execution.execId, execution.orderId, execution.shares, execution.lastLiquidity)
    
    @staticmethod
    def BracketOrder(parentOrderId:int, action:str, quantity:float, limitPrice:float, takeProfitLimitPrice:float, stopLossPrice:float):
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
        takeProfit.lmtPrice = takeProfitLimitPrice
        takeProfit.parentId = parentOrderId
        takeProfit.transmit = False

        stopLoss = Order()
        stopLoss.orderId = parent.orderId + 2
        stopLoss.action = "SELL" if action == "BUY" else "BUY"
        stopLoss.orderType = "STP"
        #Stop trigger price
        stopLoss.auxPrice = stopLossPrice
        stopLoss.totalQuantity = quantity
        stopLoss.parentId = parentOrderId
        #In this case, the low side order will be the last child being sent. Therefore, it needs to set this attribute to True 
        #to activate all its predecessors
        stopLoss.transmit = True

        bracketOrder = [parent, takeProfit, stopLoss]
        return bracketOrder

def main():
    # Create the client and Connect to TWS API
    client = TestBracketOrder('127.0.0.1', 7497, 7)
    time.sleep(3)
    client.orderId = None

    # Define a Contract
    contract = Contract()
    contract.symbol = 'INFY'
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'INR'
    contract.primaryExchange = "NSE"

    # Request from TWS, the next valid ID for the order
    client.reqIds(1)
    time.sleep(3)

    # Place a Bracket Order
    if client.orderId:
        bracket = TestBracketOrder.BracketOrder(client.orderId, "BUY", 100, 30, 40, 20)
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