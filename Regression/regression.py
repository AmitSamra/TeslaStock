import os
import numpy as np
import pandas as pd


# Merge yfinance data with 
df_tsla = pd.read_csv('/Users/amit/Coding/Projects/TeslaStock/YFinance/yfinance.csv', parse_dates=['date'])
df_tweet_count = pd.read_csv('/Users/amit/Coding/Projects/TeslaStock/Twitter/tweet_count.csv', parse_dates=['tweet_date'])
df_tweet_count = df_tweet_count.rename(columns = {'tweet_date': 'date'})
df_merged = pd.merge(df_tsla, df_tweet_count, on='date', how='outer')

# Add daily article count
df_article_count = pd.read_csv('/Users/amit/Coding/Projects/TeslaStock/News/article_count.csv', parse_dates=['date'])
df_merged = pd.merge(df_merged, df_article_count, on='date', how='outer')

# Add daily sentiment score
df2_group = pd.read_csv('/Users/amit/Coding/Projects/TeslaStock/NLP/sentiment_score.csv', parse_dates=['date'])
df_merged = pd.merge(df_merged, df2_group, on='date', how='outer')

# Remove unnecessary columns and add day of week column
df_merged = df_merged.drop(['high', 'low', 'volume', 'dividends', 'stock_splits'], axis=1)
df_merged = df_merged.sort_values('date')
df_merged['day_week'] = df_merged['date'].dt.day_name()


# Fill dates for the weekends
all_dates = pd.date_range(start='2020-12-01', end='2020-12-23', freq='D')
all_dates = pd.DataFrame({'date':all_dates})
df_merged = pd.merge(df_merged, all_dates, on='date', how='outer')
df_merged = df_merged.sort_values('date')
df_merged['day_week'] = df_merged['date'].dt.day_name()

# Fill in na values
df_merged['daily_sentiment_score'] = df_merged['daily_sentiment_score'].fillna(0)


