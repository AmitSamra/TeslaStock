import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
import numpy as np
import pandas as pd
import json
from datetime import datetime, timedelta, date
import csv
import requests

from dotenv import load_dotenv
dotenv_local_path = './.env'
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

yfinance_path = '/Users/amit/Coding/Projects/TeslaStock/YFinance/yfinance.py'
	
yfinance = BashOperator(
	task_id = 'yfinance_csv',
	bash_command = 'python {}'.format(yfinance_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Obtain headlines from NewsAPI

newsapi_path = '/Users/amit/Coding/Projects/TeslaStock/News/headlines.py'

headlines = BashOperator(
	task_id = 'headlines_csv',
	bash_command = 'python {}'.format(newsapi_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Obtain total article count

newsapi_path = '/Users/amit/Coding/Projects/TeslaStock/News/news_count.py'

article_count = BashOperator(
	task_id = 'article_count_csv',
	bash_command = 'python {}'.format(newsapi_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Obtain total number of tweets mentioning Tesla

tweet_path = '/Users/amit/Coding/Projects/TeslaStock/Twitter/twitter.py'

article_count = BashOperator(
	task_id = 'tweet_count_csv',
	bash_command = 'python {}'.format(twitter_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Perform NLP on headlines

tweet_path = '/Users/amit/Coding/Projects/TeslaStock/Twitter/twitter.py'

article_count = BashOperator(
	task_id = 'tweet_count_csv',
	bash_command = 'python {}'.format(twitter_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Combine stock price, article count & sentiment score into one df






















