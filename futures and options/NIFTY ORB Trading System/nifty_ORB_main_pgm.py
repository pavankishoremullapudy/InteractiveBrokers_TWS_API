# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 11:53:26 2020

@author: pavan

Rules for this Strategy
-----------------------
Open Range Breakout(ORB): The first X minute Candlestick after the trading for the day has begun,
defines the "Open Range". (X can be 5 mins, 10 mins, 30 mins etc.,). Let's consider a 5 minute 
candlestick for this example. 
   If any subsequent candlestick closes above the High of the OR, then take a Long Position. (Buy Breakout).
   If however any subsequent candlestick closes below the Low of the OR, then take a Short Position.(Sell Breakout)
Note: To escape false breakout and minor data volatilities, multiply the High / Low of the first candle by 0.1 percentage (0.001)
Add/Subtract this to get the open range high/low.

LONG POSITION:
-------------
Entry Condition:
	Note the Low and High of the first 5 minute candle. This is the Open Range(OR). If any of the
    subsequent 5 minute candlestick Close above the High of the "Open Range", it is considered as a Breakout.

Fixed Stop:
    This is a "False Stop". It is placed at 1 ATR below the Open Range Low.
    1) To handle an emergency situations of sudden high volatility.
    2) Many times the 5 minute candlestick will go below the Open Range Low but actually Close above the Open Range Low.
       This is a false signal which needs to be weeded out. So cannot use Open Range Low as Stop Price. 
    
Exit Condition:
    If the 5 min Candlestick Close below the Open Range Low or if time is more than 15:15 hrs

SHORT POSITION:
--------------
The Inverse of the Long Position is used for a Short Position.
"""
# Imports used In the program
from nifty_ORB_main_class import NiftyORB
from tech_indicators import ATR, SuperTrend

from ibapi.client import Contract
from ibapi.order import Order
from ibapi.execution import ExecutionFilter

import pandas as pd
import sys
import time
import datetime
import queue
import math

import pdb

# Functions used In the program

# https://stackoverflow.com/questions/28425705/python-rounding-a-floating-point-number-to-nearest-0-05/28425782
def round_nearest(x, a): 
    return round(round(x / a) * a, -int(math.floor(math.log10(a))))

def get_contract_details(client):
    ''' Define the contract of intrest'''
    # pdb.set_trace()
    contract = Contract()
    contract.symbol = 'NIFTY50'
    contract.secType = "CONTFUT"
    contract.exchange = 'NSE'
    contract.primaryExchange = 'NSE'
    contract.currency = "INR"
    contract.includeExpired = True
    
    # Get complete details about the contract from IB's database
    client.contractDetailsEnd_available.clear() # internal flag is set to False
    client.reqContractDetails(1, contract)
    client.contractDetailsEnd_available.wait() # block thread until internal flag is set to True
    
    if client.local_symbol:
        print ('Local Symbol : {}'.format(client.local_symbol))
        # Set additional contract data
        contract.localSymbol = client.local_symbol
        contract.multiplier = client.multiplier
    else:
        print('Could not access contract data From reqContractDetails')
        exit_program(client)
    
    return contract

def get_daily_candle_data(client, contract):
    ''' Get End of Day Data till Yesterday '''
    client.historicalDataEnd_available.clear() #internal flag is set to False
    client.daily_data_requested = True # for name of csv file
    # query_time = (datetime.datetime.today().date() - timedelta(days=0)).strftime("%Y%m%d %H:%M:%S")
    query_time = datetime.datetime.now().strftime("%Y%m%d %H:%M:%S") 
    client.reqHistoricalData(2, contract, query_time, '2 Y', '1 day', 'TRADES', 1, 1, False, [])
    client.historicalDataEnd_available.wait() # block thread until internal flag is set to True
    # client.cancelHistoricalData(2) # cancel the subscription
    client.daily_data_requested = False
    
def get_minute_candle_data(client, contract):
    ''' Get 5 minute till Now '''
    client.historicalDataEnd_available.clear() #internal flag is set to False
    client.daily_data_requested = False # for name of csv file as _Minute
    query_time = datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")
    client.reqHistoricalData(3, contract, query_time, '45 D', '5 mins', 'TRADES', False, 1, False, [])
    client.historicalDataEnd_available.wait() # block thread until internal flag is set to True
    print ('Finished get_minute_candle_data at time: {}'.format(datetime.datetime.now()))
    # client.cancelHistoricalData(3) # cancel the subscription

def calc_yest_data(client):
    ''' Calculate yesterdays ATR and get yesterdays close'''
    df_daily = pd.read_csv(client.local_symbol + '_Daily' + '.csv')
    df_daily = ATR(df_daily, 14, ohlc=['Open', 'High', 'Low', 'Close'])
    yest_ATR = df_daily['ATR_14'].iloc[-2]
    yest_close = df_daily['Close'].iloc[-2]
    print ('Yesterdays ATR: {}, Yesterdays Close: {}'.format(yest_ATR, yest_close))
    return yest_ATR, yest_close

def calc_todays_data(client, df_minute, yest_close):
    '''calculate todays True Range from minute data '''
    query_date = datetime.datetime.now().strftime("%Y-%m-%d")
    df_temp = df_minute[(df_minute['Date'] >= query_date)] # use for reguar trading hours

    if df_temp.empty:
        print ("DataFrame is EMPTY. Today's Data Not Available.")
        exit_program(client)
    # ..........
    # df_temp = df_minute[(df_minute['Date'] > '2020-12-16')] # for late night testing uncomment this 
    # ..........
    todays_high = df_temp['High'].max()
    todays_low = df_temp['Low'].min()
    todays_TR = max([(todays_high - todays_low), abs(todays_high - yest_close), abs(todays_low - yest_close)])
    todays_OR_low = (df_temp['Low'].iloc[0])
    todays_OR_high = df_temp['High'].iloc[0]    
    todays_OR_low = todays_OR_low * 0.999 # Subtract 0.1 percent from low
    todays_OR_high = todays_OR_high * 1.001 # Add a 0.1 percent to high
    print('Todays TR: {}, Todays OR Low: {}, Todays OR High: {}'.format(todays_TR, todays_OR_low, todays_OR_high))
    return todays_TR, todays_OR_low, todays_OR_high

def place_order_with_stop(client, action, quantity, stopLoss):
    ''' Place the Order with IB '''
    # Define the futures Contract prior to placing order
    contract = Contract()
    contract.localSymbol = client.local_symbol
    contract.secType = 'FUT'
    contract.exchange = 'NSE'
    contract.currency = 'INR'
    contract.primaryExchange = "NSE"

    # Request from TWS, the next valid ID for the order. 
    client.nextValidId_available.clear() # internal flag is set to False
    client.reqIds(-1) # The parameter is always ignored.
    client.nextValidId_available.wait() # block thread until internal flag is set to True

    # Place the order
    if client.orderId:
    # for Market Orders use execDetails callback instead of orderStatus
    # execDetails callback event is trigerred ONLY if the order placed is Executed    
        client.execDetails_available.clear() # internal flag is set to False
        if hasattr(client, "orderStatus_queue"): # Clear the existing messages in the Q , if any
            with client.orderStatus_queue.mutex:
                client.orderStatus_queue.queue.clear()

        bracket = NiftyORB.OrderWithStop(client.orderId, action, quantity, stopLoss)
        for o in bracket:
            client.orders_list.append(o.orderId)
            client.placeOrder(o.orderId, contract, o)
               
        try:
            client.execDetails_available.wait(timeout=5)
        except:
            print ('    *** An Exception Occured in client.execDetails_available.wait(timeout=5) in place_order_with_stop ***    ')
        else: # if try condition is successful with no execption, this 'else' follows
            if client.order_executed == True:
                print ("    *** \nOrder is Successfully Executed ***\n")
            else:
                print ('    *** \nOrder NOT Executed ***\n')
            
            # Handle the q from orderStatus callback
            # https://stackoverflow.com/questions/6517953/clear-all-items-from-the-queue
            if hasattr(client, "orderStatus_queue"):
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
                # del client.orderStatus_queue # Delete the queue
    else:
        print ('Order ID not received. Terminating Application')
        client.disconnect()
        sys.exit()

    if (client.order_executed == True) or (client.order_submitted == True):
        order_placed = True # set flag to True
    else:
        order_placed = False

    client.order_submitted = False # reset the flag to False for future use
    client.order_executed = False # reset the flag to False for future use
    return order_placed

def request_positions(client):
    ''' All positions sent initially, and then only updates as positions change '''
    client.positionEnd_available.clear() # internal flag is set to False
    client.reqPositions()
    client.positionEnd_available.wait() # block thread until internal flag is set to True
    client.cancelPositions()

def close_open_position(client, action:str, quantity:int):
    quantity = int(quantity)
    print ('Entered Close Position')
    # Define the futures Contract prior to placing order
    contract = Contract()
    contract.localSymbol = client.local_symbol
    contract.secType = 'FUT'
    contract.exchange = 'NSE'
    contract.currency = 'INR'
    contract.primaryExchange = "NSE"

    # Define the Limit Order for BUY/SELL
    order = Order()
    order.action = action
    order.totalQuantity = quantity
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
        if hasattr(client, "orderStatus_queue"): # Clear the existing messages in the Q , if any
            with client.orderStatus_queue.mutex:
                client.orderStatus_queue.queue.clear()
        # Place the order
        client.placeOrder(client.orderId, contract, order)
        try:
            client.execDetails_available.wait(timeout=5)
        except:
            print ('    *** An Exception Occured in client.execDetails_available.wait(timeout=5) in close_open_position ***    ')
        else: # if try condition is successful with no execption, this 'else' follows
            if client.order_executed == True:
                print ("    *** \nOrder is Successfully Executed ***\n")
            else:
                print ('    *** \nOrder NOT Executed ***\n')
            # Handle the q from orderStatus callback
            if hasattr(client, "orderStatus_queue"):
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

    if (client.order_executed == True) or (client.order_submitted == True):
        position_closed = True # set flag to True
    else:
        position_closed = False

    client.order_submitted = False # reset the flag to False for future use
    client.order_executed = False # reset the flag to False for future use
    return position_closed

def close_open_orders(client):
    client.openOrder_dict.clear() # Empty the open orders dict
    client.openOrderEnd_available.clear()
    client.openOrder_dict.clear() # Removes all Items from dictionary before making a request for openOrders
    client.reqAllOpenOrders()
    client.openOrderEnd_available.wait(timeout=5)
    # Cancels an active order placed by from the same API client ID. The last one is orderId of SL
    # If net connection problem, using orders_list could be problematic. Pick orderId from openOrder callback
    # client.cancelOrder(client.orders_list[-1])
    if client.local_symbol in client.openOrder_dict:
        print('Open Order ID to be cancelled: {}'.format(client.openOrder_dict[client.local_symbol]))
        client.cancelOrder(client.openOrder_dict[client.local_symbol])
    # client.reqGlobalCancel() #  will cancel all open orders, regardless of how they were originally placed

def exit_program(client):
    print ("Going to disconnect from TWS and then exit the program")
    client.disconnect()# disconnect from TWS and then exit the program
    sys.exit()

def main():
    # Variables used in pgm
    # market_open_time = datetime.time(9,15)
    market_close_time = datetime.time(15,30)
    my_latest_entry_time = datetime.time(15,00) # Dont take any new positions after this time
    my_close_time = datetime.time(15,15) # Close all Open Positions and Open Orders at this time

    # Connect to TWS only after 9:22 AM IST
    # pgm_start_time = int(9) * 60 + int(22) # as int(hr) +60 * int(min):
    # print ("Program Start is: {}".format(pgm_start_time))
    # present_time = (datetime.datetime.now().hour * 60 + datetime.datetime.now().minute)
    # print ('Present Time is: {} which in Date format is: {}'.format(present_time, datetime.datetime.now()))

    # while present_time <= pgm_start_time:
    #     time.sleep(10)
    #     present_time = (datetime.datetime.now().hour * 60 + datetime.datetime.now().minute)
    
    # Connect to TWS only after 9:22 AM IST
    pgm_start_time = datetime.time(9,22) # Start the program Only after 9:22
    print ("Program Start Time is: {}. Is the TWS / IB Gateway program Running?".format(pgm_start_time))
    while datetime.datetime.now().time() <= pgm_start_time:
        time.sleep(10)

    # Create the client and connect to TWS
    client = NiftyORB('127.0.0.1', 7497, 0)
    client.nextValidId_available.wait() # block thread until internal flag is set to True
    
    print ('\n   Good to Go Baba. We are Connected to TWS')
    print ('   The Order Id is: {}'.format(client.orderId))
    print ('   The Account Details are: {}\n'.format(client.accountsList))
    
    # Obtain information about Account
    client.accountSummaryEnd_available.clear() # internal flag is set to False
    client.reqAccountSummary(9001, 'All', 'AvailableFunds')
    client.accountSummaryEnd_available.wait() # block thread until flag is set to True
    client.cancelAccountSummary(9001) # Cancel if subscription is no longer needed

    contract = get_contract_details(client) # get contract details
    get_daily_candle_data(client, contract) # get the daily candkle data
    yest_ATR, yest_close = calc_yest_data(client) # calc yest ATR and get yest close
 
    interval_minutes = 5
    order_placed = False

    # while order_placed is False:
    while ((datetime.datetime.now().time() <= market_close_time)):
        if ((datetime.datetime.now().minute % interval_minutes == 0) and (datetime.datetime.now().second == 0)):
            print('\nStarting loop_start_time')
            # print ("datetime minute: {}, datetime second: {}".format(datetime.datetime.now().minute, datetime.datetime.now().second))
            loop_start_time = time.time() # use this for calculation of Sleep Time

            client.currentTime_available.clear() #internal flag is set to False
            client.reqCurrentTime() # Request current time from IB Server
            client.currentTime_available.wait(timeout=3) # block thread until internal flag is set to True

            # Not Required becos time diff is barely 1 or 2 seconds.
            # if client.time_difference > 0: # if OS time is ahead of IB time
            #     print ('Sleep for: {} seconds'.format(client.time_difference))
            #     time.sleep(client.time_difference)
            # else: # if IB time is a second or two little ahead, no props
            #     pass            

            # Get minute Data
            get_minute_candle_data(client, contract)
            df_minute = pd.read_csv(client.local_symbol + '_Minute' + '.csv')
            df_minute['Date'] = pd.to_datetime(df_minute['Date']) # Convert to pandas DateTime format

            # Find the time difference of minutes alone between the current time and the last
            # First 5 minute Candlestick starts at 9:15. Second 5 minute Candlestick starts at 9:25 and last candle starts at 15:25 pm
            # This is to Double check the timestamp of last datapoint from Nifty OHLCV csv file to ensure it is latest
            if (datetime.datetime.now().minute - df_minute['Date'].iloc[-1].minute) == 0: # then we have picked up the new candlestick that just started
                print('We have picked up the latest datapoint also')
                df_minute = df_minute[:-1] # Drop the last 1 row

            # Get today's OR Low, OR High and TR
            todays_TR, todays_OR_low, todays_OR_high = calc_todays_data(client, df_minute, yest_close)

            if order_placed is False:
                if datetime.datetime.now().time() >= my_latest_entry_time:
                    print('Come out of the while Loop. Dont Open any positions after 3:00 PM')
                    break # come out of the while loop
                else: # Enter position only before 3:00 pm afternoon IST
                    '''
                    # If more than 75% ATR is consumed, Dont Enter trade in this instrument for today
                    if todays_TR > 0.75*(yest_ATR):
                        print('Todays TR is: {} and Yesterdays ATR is: {}'.format(todays_TR, yest_ATR))
                        print ('More than 75 % of Daily ATR Consumed. Time to Exit the Program')
                        exit_program(client)
                    '''    
                    quantity = 75 # Minimum for Nifty

                    # For a Long position
                    if df_minute['Close'].iloc[-1] > todays_OR_high:
                        action = 'BUY'
                        stopLoss = todays_OR_low - yest_ATR
                        stopLoss = round_nearest(stopLoss, .05)
                        order_placed = place_order_with_stop(client, action, quantity, stopLoss)

                    # For a Short position
                    if df_minute['Close'].iloc[-1] < todays_OR_low:
                        action = 'SELL'
                        stopLoss = todays_OR_high + yest_ATR
                        print('todays_OR_high is: {}'.format(todays_OR_high))
                        print('yest_ATR is: {}'.format(yest_ATR))
                        stopLoss = round_nearest(stopLoss, .05)
                        order_placed = place_order_with_stop(client, action, quantity, stopLoss)

            if order_placed is True:
                ''' First close the Open Positions, then close the Open Orders '''
                print ('Order placed is True - In the Logic to Handle the Exit')
                request_positions(client)
                if client.local_symbol in client.position_dict:
                    print ('# of open positions in local symbol {} is: {}'.format(client.local_symbol, client.position_dict[client.local_symbol]))
                    if client.position_dict[client.local_symbol] > 0: # Long has been initially taken, after close above OR High
                        if ((df_minute['Close'].iloc[-1] < todays_OR_low) or (datetime.datetime.now().time() >= my_close_time)):
                            print ('Long has been initially taken, & Last Candle below OR Low. Or Time > 3:15. Now place a short to Exit position')
                            position_closed = close_open_position(client, 'SELL', client.position_dict[client.local_symbol])
                            if position_closed == True:
                                order_placed = None
                            else:
                                print('The position_closed is: {}'.format(position_closed))
                    elif client.position_dict[client.local_symbol] < 0: # Short has been initially taken, after close below OR Low
                        if ((df_minute['Close'].iloc[-1] > todays_OR_high) or (datetime.datetime.now().time() >= my_close_time)):
                            print ('Short has been initially taken & last candle above OR High. Or time > 3:15. Now place a Long to Exit position')
                            position_closed = close_open_position(client, 'BUY', abs(client.position_dict[client.local_symbol]))
                            if position_closed == True:
                                order_placed = None
                            else:
                                print('The position_closed is: {}'.format(position_closed))                            
                    elif client.position_dict[client.local_symbol] == 0:
                        print ('No of Open Positions is Zero. All Positions Squared Off. Do Nothing')
                        order_placed = None
                else:
                    print ('Local Symbol not in Position Dict !!!')

                client.position_dict.clear() # Removes all Items from dictionary

            if order_placed is None: # Have already closed the open positions. Now close open orders
                close_open_orders(client) # call the function to close all the open orders
                break # come out of the while loop

            sleep_time = (interval_minutes*60) - (time.time() - loop_start_time) - 60 # incase the first run is at 09:15:28
            if sleep_time > 0:
                pass
            else:
                sleep_time = 1
            print('.....Grrr..... Going to Sleep Mode for: {} seconds starting: {}'.format(sleep_time, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            time.sleep(sleep_time)

    # If time is > than 3:15 , then close all open positions, if any, and exit.

    # Request All Completed Orders today before disconnecting
    client.completedOrdersEnd_available.clear() # internal flag is set to False
    client.reqCompletedOrders(True) # apiOnly Orders
    client.completedOrdersEnd_available.wait(timeout=5) # block thread until internal flag is set to True or 10 seconds

    client.disconnect()

if __name__ == '__main__':
    main()