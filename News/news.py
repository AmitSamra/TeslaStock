import os
import numpy as np
import pandas as pd
import yfinance as yf
from newsapi import NewsApiClient
import json
import math
from datetime import timedelta, datetime
from dotenv import load_dotenv
dotenv_local_path = '../.env'
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)


# Connect to NewsAPI
newsapi = NewsApiClient(api_key=os.environ.get("NEWS_API_KEY"))


# See list of sources
#sources = newsapi.get_sources()
#file_path = './sources.json'
#with open(file_path, 'w') as f:
#    json.dump(sources, f, indent=4, sort_keys=True)


# Get total results needed for pagination
news = newsapi.get_everything(
q='Tesla',
sources='bloomberg',
from_param='2020-11-03',
to='2020-12-03',
language='en',
sort_by='publishedAt',
page_size=100
)
total_results=news["totalResults"]
max_page = math.ceil(total_results/100)


"""
NewAPI does not allow more than 100 results per request so we cannot use the code below.
Instead, we must create a request for each day, assuming there are less than 100 articles per day.

# Get all articles and store in JSON file
year = datetime.now().year
month = datetime.now().month
day = datetime.now().day

start_date = '2020-11-03'
end_date = '2020-12-03'

page_count = 1

while page_count <= 1:
    news = newsapi.get_everything(
    q='Tesla',
    sources='bloomberg',
    from_param=start_date,
    to=end_date,
    language='en',
    sort_by='publishedAt',
    page_size=100,
    page=page_count
    )
    
    file_path = f"./articles_{year}-{month}-{day}_{page_count}.json"
    with open(file_path, 'w') as f:
        json.dump(news, f, indent=4, sort_keys=True)
    
    page_count += 1
"""

# Loop over dates and store result in CSV
start_date = '2020-11-03'
end_date = '2020-12-03'

start2 = date( int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10]) )
end2 = date( int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10]) )
increment = timedelta(days=1)

i = start2

while i <= end2:
    
    news = newsapi.get_everything(
    q = 'Tesla',
    sources = 'bloomberg, reuters',
    from_param = start2,
    to = end2,
    language = 'en',
    sort_by = 'publishedAt',
    page_size = 100,
    page = page_count
    )
    
    i += increment

