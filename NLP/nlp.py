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


# Analyze sentiment 
sid = SentimentIntensityAnalyzer()

for index, row in df.iterrows():
    title = row['title']
    token = regexp_tokenize(title, pattern=r"\s|[\.,;']", gaps=True)
    
    stop_words = stopwords.words('english')
    headline_filtered = []
    for i in tokens:
        if i not in stop_words:
            headline_filtered.append(i) 
    
    headline_pos = []
    headline_neu = []
    headline_neg = []
    
    for word in headline_filtered:
        if (sid.polarity_scores(word)['compound']) >= 0.5:
            headline_pos.append(word)
        elif (sid.polarity_scores(word)['compound']) <= -0.5:
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


# Save df_group to csv
df2_group.to_csv('sentiment_score.csv', index=False)

