import os
import numpy as np
import pandas as pd
import yfinance as yf
from newsapi import NewsApiClient
import json
import math
from datetime import timedelta, datetime
from dotenv import load_dotenv
dotenv_local_path = '/Users/amit/Coding/Projects/TeslaStock/.env'
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)


# Connect to NewsAPI
newsapi = NewsApiClient(api_key=os.environ.get("NEWS_API_KEY"))

# Set output path
file_path_article_count = '/Users/amit/Coding/Projects/TeslaStock/News/article_count.csv'

# Delete csv to overwrite
if os.path.exists(file_path_article_count):
    os.remove(file_path_article_count)

# Create new CSV with headers

with open(file_path_article_count, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['date','number_articles'])

# Loop for article headlines
start_date = os.environ.get('START_DATE')
end_date = os.environ.get('END_DATE')

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
    
    with open(file_path_article_count, 'a') as g:
        g.write(f"{i.strftime('%Y-%m-%d')}, {news['totalResults']}\n")
            
    i += increment
