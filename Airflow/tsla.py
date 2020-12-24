import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import papermill as pm
import os
import numpy as np
import pandas as pd
import yfinance as yf
from newsapi import NewsApiClient
import json
import math
from datetime import datetime, timedelta, date
import csv
import base64
import requests
import html
import nltk
from nltk import regexp_tokenize
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from dotenv import load_dotenv
dotenv_local_path = './.env'""
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)

from dotenv import load_dotenv
dotenv_local_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path=dotenv_local_path, verbose=True) 


# ----------------------------------------------------------------------------------------------------
# Setup DAG

default_args = {
	'owner':'Amit',
	'start_date':datetime.now(),
	'retries':0
}

dag = DAG(
	'tsla',
	default_args = default_args,
	description = 'tsla',
	max_active_runs = 1
)

# ----------------------------------------------------------------------------------------------------
# Obtain TSLA stock data from yFinance API

def etl_yfinance():
	"""
	Gets stock data from yfinance API.
	Performs ETL.
	"""
	tsla = yf.Ticker("TSLA")
	df_tsla = df_tsla.reset_index()
	df_tsla.columns = df_tsla.columns.str.replace(' ', '_').str.lower()
	df_tsla['change'] = df_tsla['close']-df_tsla['close'].shift(1)
	df_tsla['percent_change'] = (df_tsla['close']/df_tsla['close'].shift(1))-1
	df_tsla['market_cap'] = df_tsla['close']*df_tsla['volume']/1000000
	df_tsla[['open','high','low','close','change']] = df_tsla[['open','high','low','close','change']].round(2)
	df_tsla[['percent_change']] = df_tsla[['percent_change']].round(4)
	df_tsla[['market_cap']] = df_tsla[['market_cap']].round(1)
	

t1 = PythonOperator(
	task_id = 'etl_yfinance_data',
	python_callable = etl_yfinance,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Obtain headlines from NewsAPI

def get_headlines():
	"""
	Gets headlines mentioning Tesla from bloomberg.com
	"""
	newsapi = NewsApiClient(api_key=os.environ.get("NEWS_API_KEY"))

	# To get around 100 result limit, we will make a request for each day
	file_path_headlines = "./headlines.csv"

	# Delete csv to overwrite
	if os.path.exists(file_path_headlines):
	    os.remove(file_path_headlines)

	# Create new CSV with headers
	with open(file_path_headlines, 'w', newline='') as f:
	    w = csv.writer(f)
	    w.writerow(['date','source','title'])

	# Loop for article headlines
	start_date = '2020-12-01'
	end_date = '2020-12-23'

	start_date2 = datetime( int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10]) )
	end_date2 = datetime( int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10]) )
	increment = timedelta(days=1)

	i = start_date2
	while i <= end_date2:
	    
	    news = newsapi.get_everything(
	    q = 'Tesla',
	    #sources = 'Bloomberg, Reuters',
	    domains = 'bloomberg.com',
	    from_param = i,
	    to = i,
	    language = 'en',
	    sort_by = 'publishedAt'
	    )
	    
	    with open(file_path_headlines, 'a') as g:
	        for x in news['articles']:
	            g.write(f"{i.strftime('%Y-%m-%d')}, {x['source']['id']}, {x['title']}\n")
	            
	    i += increment


t2 = PythonOperator(
	task_id = 'get_newsapi_headlines',
	python_callable = get_headlines,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Obtain total headlines count

def get_article_count():
	"""
	Gets count of all articles mentioning Tesla. 
	The source is not limited to all articles written in English.
	There is no limitation on source (can be publishers besides Bloomberg).
	"""

	file_path_news = "./news_count.csv"

	# Delete csv to overwrite
	if os.path.exists(file_path_news):
	    os.remove(file_path_news)

	# Create new CSV with headers
	with open(file_path_news, 'w', newline='') as f:
	    w = csv.writer(f)
	    w.writerow(['date','number_articles'])

	# Loop for article headlines
	start_date = '2020-12-01'
	end_date = '2020-12-23'

	start2 = datetime( int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10]) )
	end2 = datetime( int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10]) )
	increment = timedelta(days=1)

	i = start2
	while i <= end2:
	    
	    news = newsapi.get_everything(
	    q = 'Tesla',
	    domains = 'bloomberg.com',
	    from_param = i,
	    to = i,
	    language = 'en',
	    sort_by = 'publishedAt'
	    )
	    
	    with open(file_path_news, 'a') as g:
	        g.write(f"{i.strftime('%Y-%m-%d')}, {news['totalResults']}\n")
	            
	    i += increment


t3 = PythonOperator(
	task_id = 'get_article_count',
	python_callable = get_article_count,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# 




















