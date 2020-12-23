import os
import numpy as np
import pandas as pd
import json
from datetime import datetime, timedelta, date
import csv
import nltk
from dotenv import load_dotenv
dotenv_local_path = './.env'""
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)


# Import headlines2.csv
df = pd.read_csv('./headlines2.csv')
title1 = df.iloc[0]['title']


# Tokenize title
token = nltk.word_tokenize(title1)


# Remove stopwords from title
stop_words = stopwords.words('english')

headline_filtered = []
for i in tokens:
    if i not in stop_words:
        headline_filtered.append(i)


