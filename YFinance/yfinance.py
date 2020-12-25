import os
import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
dotenv_local_path = '/Users/amit/Coding/Projects/TeslaStock/.env'
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)

# Create connection for TSLA
tsla = yf.Ticker("TSLA")


# Use environment variable to access start & end dates
#hist = tsla.history(start="2020-12-01", end="2020-12-23")
END_DATE = os.environ.get('END_DATE')
END_DATE = datetime( int(END_DATE[0:4]), int(END_DATE[5:7]), int(END_DATE[8:10]) )
increment = timedelta(days=1)
END_DATE = END_DATE + increment
hist = tsla.history(start=os.environ.get('START_DATE'), end=END_DATE)


# Wrangle yfinance data
df_tsla = hist
df_tsla = df_tsla.reset_index()
df_tsla.columns = df_tsla.columns.str.replace(' ', '_').str.lower()
df_tsla['change'] = df_tsla['close']-df_tsla['close'].shift(1)
df_tsla['percent_change'] = (df_tsla['close']/df_tsla['close'].shift(1))-1
df_tsla['market_cap'] = df_tsla['close']*df_tsla['volume']/1000000
df_tsla[['open','high','low','close','change']] = df_tsla[['open','high','low','close','change']].round(2)
df_tsla[['percent_change']] = df_tsla[['percent_change']].round(4)
df_tsla[['market_cap']] = df_tsla[['market_cap']].round(1)
df_tsla.to_csv('/Users/amit/Coding/Projects/TeslaStock/YFinance/yfinance.csv', index=False)
