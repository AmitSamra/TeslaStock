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


# See list of sources
#sources = newsapi.get_sources()
#file_path = './sources.json'
#with open(file_path, 'w') as f:
#    json.dump(sources, f, indent=4, sort_keys=True)

"""
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

# To get around 100 result limit, we will make a request for each day

file_path_headlines = '/Users/amit/Coding/Projects/TeslaStock/News/headlines.csv'

# Delete csv to overwrite
if os.path.exists(file_path_headlines):
    os.remove(file_path_headlines)

# Create new CSV with headers

with open(file_path_headlines, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['date','source','title'])

# Loop for article headlines
start_date = os.environ.get('START_DATE')
end_date = os.environ.get('END_DATE')

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
    
    #with open(f"./headlines_{i.strftime('%Y-%m-%d')}", 'w') as f:
    #    json.dump(news, f, indent=4, sort_keys=True)
    
    with open(file_path_headlines, 'a') as g:
        for x in news['articles']:
            g.write(f"{i.strftime('%Y-%m-%d')}, {x['source']['id']}, {x['title']}\n")
            
    i += increment


# Concatenate title columns

file_path_headlines2 = '/Users/amit/Coding/Projects/TeslaStock/News/headlines2.csv'

with open(file_path_headlines, 'r') as f, open(file_path_headlines2, 'w') as g:
    r = csv.reader(f)
    next(r) # Skip header row
    w = csv.writer(g)
    w.writerow(['date','source','title']) # Write header in g
    
    for row in r:
        date, source, *content = [x.strip() for x in row] # Remove white spaces in title cells
        w.writerow([date, source, ' '.join(content)]) # Merge cells 

