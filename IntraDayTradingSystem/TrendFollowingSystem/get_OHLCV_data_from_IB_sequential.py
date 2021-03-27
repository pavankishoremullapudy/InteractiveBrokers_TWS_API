''' Read historical data, store OHLCV in Pandas Dataframe and write to CSV '''
# Imports used in the program
from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper

import datetime
from threading import Thread, Event

import pandas as pd
import collections
import queue
import datetime, time
import sys, os, glob, shutil
import pandas as pd

class ReadOHLCV(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    def __init__(self, addr, port, client_id):
        EClient.__init__(self, self)

        # Initialize properties
        # self.symbols = {'ACC':'44652144','INFY':'44652017', 'BAJFINANC':'247227232'} # IB symbol could be different from NSE symbol, so use conId
        self.candle_dict = {}
        self.accountsList = None
        self.orderId = None

        self.historicalDataEnd_available = Event() # Initialize an Event object
        self.nextValidId_available = Event() # Initialize an Event object

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
    def historicalData(self, req_id, bar):
        ''' Callback to reqHistoricalData '''

        # Add the stock prices to the dictionary
        self.candle_dict['Date'].append(bar.date)
        self.candle_dict['Open'].append(bar.open)
        self.candle_dict['High'].append(bar.high)
        self.candle_dict['Low'].append(bar.low)
        self.candle_dict['Close'].append(bar.close)
        self.candle_dict['Volume'].append(bar.volume) # whatToShow in reqHistoricalData should be TRADES

    @iswrapper
    def historicalDataEnd(self, req_id: int, start: str, end: str):
        '''Marks the ending of the historical bars reception.'''
        self.historicalDataEnd_available.set() #internal flag is set to True

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
        elif code == 502: # Couldn't connect to TWS . Not logged in or Configuratio incorrect
            print('Error Request Id: {}, Error Code: {}, Error Message: {}'.format(req_id, code, msg))
            self.disconnect()
            os._exit(9999999) # This will exit the entire process without any cleanup
            # https://stackoverflow.com/questions/905189/why-does-sys-exit-not-exit-when-called-inside-a-thread-in-python
        else:
            print('Error Request Id: {}, Error Code: {}, Error Message: {}'.format(req_id, code, msg))

# Functions used in the program
def prepare_directory():
    '''Prepare the directory to store CSV data'''
    full_path = os.path.realpath(__file__)
    current_directory = os.path.dirname(full_path)
    src_directory = current_directory + "\\temp_data\\"
    dest_directory = current_directory + "\\RTD_from_IB\\"
    src_search_criteria = src_directory + "*.csv"
    dest_search_criteria = dest_directory + "*.csv"
    src_files_with_csv_ext = glob.glob(src_search_criteria)
    dest_files_with_csv_ext = glob.glob(dest_search_criteria)

    # If directory does Not exist, create it.
    # If it already exists, identify all the csv file in the directory and delete them.
    if not os.path.exists(src_directory):
        os.makedirs(src_directory)
    else:
        for ind_file in src_files_with_csv_ext:
            os.remove(ind_file)
    #
    if not os.path.exists(dest_directory):
        os.makedirs(dest_directory)
    else:
        for ind_file in dest_files_with_csv_ext:
            os.remove(ind_file)

def get_shortlisted_symbols():
    '''Get all the shortlisted stocks into a DataFrame and write to a list '''
    full_path = os.path.realpath(__file__)
    current_directory = os.path.dirname(full_path)

    input_file = current_directory + "\\N100L15_Symbols.csv"
    df_symbols = pd.read_csv(input_file)
    df_symbols = df_symbols.dropna()
    df_symbols = df_symbols[['IB Symbol', 'Contract ID']]
    df_symbols['IB Symbol'] = df_symbols['IB Symbol'].astype(str)
    df_symbols['Contract ID'] = df_symbols['Contract ID'].astype(int)
    df_symbols = df_symbols.sort_values('IB Symbol')
    df_symbols = df_symbols.reset_index(drop=True) # This resets the index to the default integer index
    # convert the two columns in pandas DF to dictionary
    symbols_dict = dict(zip(df_symbols['IB Symbol'], df_symbols['Contract ID'])) # Convert the two columns to a Dictionary
    print ("# of shortlisted symbols is: ", len(df_symbols))
    print ('Dictionary of Symbols and Contract ID: {} '.format(symbols_dict))
    return symbols_dict

def main():

    market_close_time = datetime.time(15,30)
    interval_minutes = 5

    pgm_start_time = datetime.time(9,21) # Start the program Only after 9:21
    print ("Program Start Time is: {}. Is the TWS / IB Gateway program Running?".format(pgm_start_time))
    while datetime.datetime.now().time() <= pgm_start_time:
        time.sleep(10)

    prepare_directory() # Call function to prepare directory
    symbols_dict = get_shortlisted_symbols() # call function to get list of all symbols

    # Create the client and connect to TWS
    client = ReadOHLCV('127.0.0.1', 7497, 7)
    client.nextValidId_available.wait() # block thread until internal flag is set to True

    print ('\n   Good to Go Baba. We are Connected to TWS for Retreiving OHLCV Data')
    print ('   The Order Id is: {}'.format(client.orderId))
    print ('   The Account Details are: {}\n'.format(client.accountsList))

    # Write the OHLCV data for symbols to csv file
    # write_to_csv(client,symbols_dict)
    while ((datetime.datetime.now().time() <= market_close_time)):
        if ((datetime.datetime.now().minute % interval_minutes == 0) and (datetime.datetime.now().second == 0)):
            print('\nStarting loop_start_time')
            print ("datetime minute: {}, datetime second: {}".format(datetime.datetime.now().minute, datetime.datetime.now().second))
            loop_start_time = time.time() # use this for calculation of Sleep Time

            for symbol in symbols_dict:

                # Define the contract
                contract = Contract()
                contract.symbol = symbol
                contract.secType = 'STK'
                contract.exchange = 'NSE'
                contract.conId = symbols_dict[symbol]
                print('Contract ID for symbol {} is: {}'.format(symbol, symbols_dict[symbol])) # take conId from IB website or contractDetails callback
                contract.currency = "INR"

                # Initialize the candle data dictionary
                for v in ['Date','Open', 'High', 'Low', 'Close', 'Volume']:
                    client.candle_dict[v] = []

                # Request OHLCV data
                now = datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")
                client.historicalDataEnd_available.clear() #internal flag is set to False
                client.reqHistoricalData(4, contract, now, '5 D', '5 mins', 'TRADES', False, 1, False, [])
                client.historicalDataEnd_available.wait() # block thread until internal flag is set to True

                time.sleep(3) # Allow enough time for data to be returned.
                df = pd.DataFrame(client.candle_dict, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['Date'] = pd.to_datetime(df['Date']) # Convert to pandas DateTime format
                df = df[df.Volume != 0] # Drop all entries for which volume traded = 0
                # print(df.tail(5))
                # print(df.columns)

                # write to csv
                full_path = os.path.realpath(__file__)
                current_directory = os.path.dirname(full_path)
                src_directory = current_directory + "\\temp_data\\"
                dest_directory = current_directory + "\\RTD_from_IB\\"
                # write to a temporary directory
                file_name = symbol + '_Minute' + '.csv'
                src_file = os.path.join(src_directory, file_name)
                df.to_csv(src_file, encoding='utf-8', index=False)

                # The copy is necessiated so that any other program that is reading the csv files
                # from the destination directory (RTD from Google) continually, will have modified date
                # after the entire writing is done (and NOT while the writing is being done).
                dest_file = os.path.join(dest_directory, file_name)
                shutil.copyfile(src_file, dest_file)

                client.candle_dict.clear()

            sleep_time = (interval_minutes*60) - (time.time() - loop_start_time) - 60 # incase the first run is at 09:15:28
            if sleep_time > 0:
                pass
            else:
                sleep_time = 1
            print('.....Grrr..... Going to Sleep Mode for: {} seconds starting: {}'.format(sleep_time, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            time.sleep(sleep_time)
    
    # # If time is > than 3:30 , then exit.
    if (datetime.datetime.now().time() >= market_close_time):
        client.cancelHistoricalData(4) # cancel the subscription
        print('Going to Disconnect from TWS')
        time.sleep(3)
        client.disconnect()

if __name__ == '__main__':
    main()