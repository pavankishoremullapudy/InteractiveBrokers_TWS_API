# Request Scan Parameters

# Imports for the Program
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper

import threading
import time

class StockScanner(EWrapper, EClient):
    ''' Serves as the Client and the Wrapper '''

    def __init__(self, addr, port, client_id):
        EClient. __init__(self, self)

        # Connect to TWS API
        self.connect(addr, port, client_id)
        self.count = 0

        # Launch the client thread
        thread = threading.Thread(target=self.run)
        thread.start()
    
    def error(self, reqId, code, msg):
        print('Error {}: {}'.format(code, msg))

    @iswrapper
    def scannerParameters(self, xml: str):
        ''' Callback for reqScannerParameters '''
        super().scannerParameters(xml)
        #open('log/scanner.xml', 'w').write(xml)
        open('scanner.xml', 'w').write(xml)
        print("ScannerParameters received.")

def main():

    # Create the client and connect to TWS
    client = StockScanner('127.0.0.1', 7497, 7)
    time.sleep(3)

    # Request the scanner parameters
    client.reqScannerParameters()
    time.sleep(3)
    
    # Disconnect from TWS
    time.sleep(5)       
    client.disconnect()

if __name__ == '__main__':
    main()