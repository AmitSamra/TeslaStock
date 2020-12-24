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
# Obtain total number of tweets mentioning Tesla


def get_tweet_count():
	"""
	Gets count of tweets mentioning Tesla
	"""

	# Authentication

	# Load Keys
	client_key = os.environ.get('CLIENT_KEY')
	client_secret = os.environ.get('CLIENT_SECRET')

	# Encode keys using base64
	key_secret = '{}:{}'.format(client_key, client_secret).encode('ascii')
	b64_encoded_key = base64.b64encode(key_secret)
	b64_encoded_key = b64_encoded_key.decode('ascii')

	# POST request to auth endpoint to obtain Bearer Token
	base_url = 'https://api.twitter.com/'
	auth_url = '{}oauth2/token'.format(base_url)

	auth_headers = {
	    'Authorization': 'Basic {}'.format(b64_encoded_key),
	    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
	}

	auth_data = {
	    'grant_type': 'client_credentials'
	}

	auth_resp = requests.post(auth_url, headers=auth_headers, data=auth_data)
	access_token = auth_resp.json()['access_token']


	# Premium search for tweet count
	search_headers = {
	    'Authorization': 'Bearer {}'.format(access_token)    
	}

	search_params = {
	    'query': 'Tesla',
	    'fromDate': '202012010000',
	    'toDate': '202012230000',
	    'bucket': 'day'
	}

	search_url = '{}1.1/tweets/search/fullarchive/full/counts.json'.format(base_url)
	search_resp = requests.get(search_url, headers=search_headers, params=search_params)
	tweet_data = search_resp.json()


	# Save results in JSON file
	with open('./tsla_tweet_count.json', 'w') as f:
	    json.dump(tweet_data, f, indent=4, sort_keys=True)


	# Save only releveant portion of request
	relevant_data = []
	with open('./tsla_tweet_count.json', 'r') as f:
	    data = json.load(f)

	for i in data['results']:
	    relevant_data.append({'tweet_date':i['timePeriod'][0:8], 'tweet_count':i['count']})

	# Convert tweet_date to datetime object
	for i in relevant_data:
	    i['tweet_date'] = datetime.strptime(i['tweet_date'], '%Y%m%d')

	# Reformat date
	for i in relevant_data:
	    i['tweet_date']=datetime.strftime(i['tweet_date'],'%Y-%m-%d')

	relevant_data
	    
	# Save relevant data in CSV
	with open('./tslta_tweet_count.csv', 'w', newline='') as f:
	    w = csv.writer(f)
	    w.writerow(['tweet_date', 'tweet_count'])
	    
	    count = 0
	    for i in relevant_data:
	        if count == 0:
	            pass
	            #header = i.keys()
	            #w.writerow(header)
	            #count += 1
	        w.writerow(i.values())


t4 = PythonOperator(
	task_id = 'get_tweet_count',
	python_callable = get_tweet_count,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Perform NLP on headlines

def nlp():
	"""
	Perofrms NLP on headlines obtained from NewsAPI.
	Calculates a sentiment/polarity score for each headline.
	Outputs a CSV with a sentiment/polarity score for each day.
	"""

	# Load headlines2 CSV
	df = pd.read_csv('./headlines2.csv')

	# Perform NLP
	sid = SentimentIntensityAnalyzer()

	for index, row in df.iterrows():
	    title = row['title']
	    token = regexp_tokenize(title, pattern=r"\s|[\.,;']", gaps=True)
	    
	    stop_words = stopwords.words('english')
	    headline_filtered = []
	    for i in token:
	        if i not in stop_words:
	            headline_filtered.append(i) 
	    
	    headline_pos = []
	    headline_neu = []
	    headline_neg = []
	    
	    for word in headline_filtered:
	        if (sid.polarity_scores(word)['compound']) > 0:
	            headline_pos.append(word)
	        elif (sid.polarity_scores(word)['compound']) < 0:
	            headline_neg.append(word)
	        else:
	            headline_neu.append(word)

	    score = round((1*len(headline_pos) - 1*len(headline_neg) + 0*len(headline_neu))/len(headline_filtered),2)
	    df.at[index, 'sentiment_score'] = score


	# Create a copy of df_headlines to perform grouping operations
	df2 = df.copy()
	df2.drop(['source', 'title'], axis=1, inplace=True)

	# Group headlines by date and calculate average score
	df2_group = df2.groupby('date', as_index=False).mean()
	df2_group = df2_group.rename(columns = {'sentiment_score': 'daily_sentiment_score'})

	# Save df2_group to csv
	df2_group.to_csv('sentiment_score.csv', index=False)


t5 = PythonOperator(
	task_id = 'nlp',
	python_callable = nlp,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Perform NLP on headlines




















