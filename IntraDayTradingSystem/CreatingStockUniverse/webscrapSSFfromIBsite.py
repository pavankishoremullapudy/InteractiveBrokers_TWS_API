# -*- coding: utf-8 -*-
"""
Created on Fri Aug 28 21:02:19 2020

@author: pavan

1) Go to the SSF list on IB Webpage for NSE stocks
2) Retreive them using Webscraping
3) Remove NIFTY, BANKNIFTY & currency like USDINR.
4) This gives us only the Stocks which are optionable
"""
# Imports used in the program
import pandas as pd
from bs4 import BeautifulSoup
import requests
import pdb

# Declarartions
requests_headers = {'User-Agent': 'Mozilla/5.0 6(Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36'}
url1 = "https://www.interactivebrokers.com/en/index.php?f=2222&exch=nse&showcategories=SSF" # first page has 100 ssf
url2 = "https://www.interactivebrokers.com/en/index.php?f=2222&exch=nse&showcategories=SSF&p=&cc=&limit=100&page=2" # presently less than 100

def get_table_data(url, data):
    """
    The function to retreive data and header using requests module from url1 and url2
        If data=True, retreive only Data from the table in the url
        If data=False, retreive the header from the table in the url

    Returns df
    """
    response = requests.get(url, headers=requests_headers)
    print('\nThe URL: {}'.format(response.url))
    print('The Status Code from Requests: {}'.format(response.status_code)) # print status code
    # soup = BeautifulSoup(response.text, 'lxml') # Parse the HTML as a string
    soup = BeautifulSoup(response.content, 'lxml') # Parse the HTML as a string
    table = soup.find_all('table')[2] # Grab the third table on the webpage
    df = pd.DataFrame(columns=range(0,4), index = [0])
    
    for row_marker, row in enumerate(table.find_all('tr')):

        if data==True:
            columns = row.find_all(['td']) # Capture only the Table Data Cells.
        else:
            columns = row.find_all(['th']) # Capture only the Table Header Cells.
            
        try:
            df.loc[row_marker] = [column.get_text() for column in columns]
        except ValueError:
            # It's a safe way to handle when [column.get_text() for column in columns] is empty list.
            continue
    return df

def main():
    """
    The main function to handle DataFrame and write header and data from page 1 & page 2 to csv
    
    Returns: None

    """
    df_table_header = get_table_data(url1, data=False)
    if df_table_header.iloc[0,0] == 'IB Symbol': # confirm 'IB Symbol' content of first row , then change next column
        df_table_header.iloc[0,1] = 'Product Description' # Original description is too long. Cut it short
    else:
        print ("WARNING !!!.Some Change in the Header Data of IB webitse. ***CHECK ALL DATA***")
    
    df_table_data_P1 = get_table_data(url1, data=True)
    df_table_data_P2 = get_table_data(url2, data=True)
    
    df_final = pd.concat([df_table_header, df_table_data_P1, df_table_data_P2])
    
    df_final = df_final.rename(columns=df_final.iloc[0]) # Rename first row to column header
    df_final = df_final.dropna() # Drop rows where atleast one element is missing
    df_final = df_final[df_final.Symbol != 'Symbol'] # if there is row with text 'symbol', exclude that
    df_final = df_final[df_final.Symbol != 'NIFTY'] # If there is row with 'NIFTY50', exclude that
    df_final = df_final[df_final.Symbol != 'BANKNIFTY'] # If there is row with 'BANKNIFTY', exclude that
    df_final = df_final[df_final.Symbol != 'USDINR'] # If there is row with 'USDINR', exclude that
    df_final = df_final[df_final.Symbol != 'EURINR'] # If there is row with 'EURINR', exclude that
    df_final = df_final[df_final.Symbol != 'GBPINR'] # If there is row with 'GBPINR', exclude that
    df_final = df_final[df_final.Symbol != 'JPYINR'] # If there is row with 'JPYINR', exclude that
    df_final = df_final.reset_index(drop=True)
    df_final.to_csv('scraped_data.csv', index=False) # dont write row names
 
if __name__ == "__main__":
    main()