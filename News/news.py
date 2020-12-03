import os
import numpy as np
import pandas as pd
import yfinance as yf
from newsapi import NewsApiClient
import json
from dotenv import load_dotenv
dotenv_local_path = '../.env'
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)


# Connect to NewsAPI
newsapi = NewsApiClient(api_key=os.environ.get("NEWS_API_KEY"))

# See list of sources
sources = newsapi.get_sources()
file_path = './sources.json'
with open(file_path, 'w') as f:
    json.dump(sources, f, indent=4, sort_keys=True)