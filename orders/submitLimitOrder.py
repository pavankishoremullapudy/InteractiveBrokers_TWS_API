# Imports for the Program
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.utils import iswrapper

import time
import threading
import sys

class TestLimitOrder(EWrapper, EClient):
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

def main():
    # Create the client and Connect to TWS API
    client = TestLimitOrder('127.0.0.1', 7497, 7)
    time.sleep(3)
    client.orderId = None

    # Define a Contract
    contract = Contract()
    contract.symbol = 'SBIN'
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'INR'
    contract.primaryExchange = "NSE"
    
    # Define the Limit Order for BUY/SELL
    order = Order()
    order.action = 'BUY'
    order.totalQuantity = 42
    order.orderType = 'LMT'
    order.lmtPrice = '10.2' # Place the limit order far off from the actaul market price. For Test
    #order.transmit = False # when set to false, the order wont actually be executed when the program is run
    
    # Request from TWS, the next valid ID for the order
    client.reqIds(1)
    time.sleep(3)

    # Place the order
    if client.orderId:
        client.placeOrder(client.orderId, contract, order)
        time.sleep(3)
    else:
        print ('Order ID not received. Terminating Application')
        sys.exit()

    # Obtain information about open positions
    client.reqPositions()
    time.sleep(3)

    # Obtain information about Account
    client.reqAccountSummary(7, 'All', 'AccountType')
    time.sleep(3)

    # Cancel Order
    print ('Cancelling the Order')
    client.cancelOrder(client.orderId) # Cancel the above order which has been placed

    # Disconnect from TWS
    time.sleep(3)
    client.disconnect()
    
if __name__ == "__main__":
    main()