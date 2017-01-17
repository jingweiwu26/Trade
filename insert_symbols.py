# -*- coding: utf-8 -*-
"""
Created on Sun Jan 15 10:26:36 2017

@author: jwu
"""

# insert_symbols.py

from __future__ import print_function

import datetime
from math import ceil

import bs4
import MySQLdb as mdb
import requests

def obtain_parse_wikisnp500():
    
# Stores the current time, for the created_at record
# now=datetime.datetime.utcnow()
    
# Use requests and bs4 to download the list of sp500
# companies and obtain the symbol table 

# return a list of tuples
    
    response=requests.get("http://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup=bs4.BeautifulSoup(response.text)
    
    #This selects the first table, using CSS selector syntax
    # and then ignores the header row([1:])
    symbolslist=soup.select('table')[0].select('tr')[1:]
    symbols=[]
    for i, symbol in enumerate(symbolslist):
        tds=symbol.select('td')
        symbols.append(
        (tds[0].select('a')[0].text, #Ticker
        'stock',
        tds[1].select('a')[0].text, #name
        tds[3].text, #sector
        'USD', datetime.datetime.utcnow(), datetime.datetime.utcnow()))
    
    return symbols

"""
print("&&&&&&&&&&&&&&&&&&&&&&&")
print(tds)
print("&&&&&&&&&&&&&&&&&&&&&&&")
print(tds[0])
print("&&&&&&&&&&&&&&&&&&&&&&&")
print(tds[0].select('a'))
print("&&&&&&&&&&&&&&&&&&&&&&&")
print(tds[0].select('a')[0].text)
"""

def insert_snp500_symbols(symbols):

#insert the SP500 symbols into the MYSQL database
# Connect to mySQL instance
    
    db_host='localhost'
    db_user='sec_user'
    db_pass="password"
    db_name='securities_master'
    
    con=mdb.connect(host=db_host,user=db_user,passwd=db_pass,db=db_name)
    
    #Create the insert strings
    column_str='ticker,instrument,name,sector,currency,created_date,last_updated_date'
    insert_str=("%s, " *7)[:-2]
    final_str="INSERT INTO symbol (%s) VALUES (%s)" %  (column_str,insert_str)
    
    
    with con:
        cur=con.cursor()
        cur.executemany(final_str,symbols)

if __name__=="__main__":
    symbols=obtain_parse_wikisnp500()
    insert_snp500_symbols(symbols)
    print ("%s symbols were successfully added." % len(symbols) )